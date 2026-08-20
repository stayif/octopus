package settlement

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/billing"
	"github.com/bestruirui/octopus/internal/db"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm"
)

type GateReason string

const (
	GateAmount GateReason = "AMOUNT"
	GateCount  GateReason = "COUNT"
	GateAge    GateReason = "AGE"
)

type Limits struct {
	MaxPendingAmountMicrounits int64
	MaxPendingCount            int64
	MaxPendingAge              time.Duration
}

func (limits Limits) Validate() error {
	if limits.MaxPendingAmountMicrounits <= 0 {
		return fmt.Errorf("pending charge amount limit must be positive")
	}
	if limits.MaxPendingCount <= 0 {
		return fmt.Errorf("pending charge count limit must be positive")
	}
	if limits.MaxPendingAge <= 0 {
		return fmt.Errorf("pending charge age limit must be positive")
	}
	return nil
}

type GateError struct {
	Reason GateReason
}

func (err *GateError) Error() string {
	return fmt.Sprintf("pending charge %s limit reached", strings.ToLower(string(err.Reason)))
}

type Service struct {
	client       billing.Client
	limits       Limits
	pollInterval time.Duration
	wake         chan struct{}
	stop         chan struct{}
	done         chan struct{}
	startOnce    sync.Once
	closeOnce    sync.Once
	settleMu     sync.Mutex
}

var (
	defaultServiceMu sync.RWMutex
	defaultService   *Service
)

func NewService(client billing.Client, limits Limits, pollInterval time.Duration) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("billing client is required")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("settlement poll interval must be positive")
	}
	return &Service{
		client:       client,
		limits:       limits,
		pollInterval: pollInterval,
		wake:         make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}, nil
}

func SetDefaultService(service *Service) {
	defaultServiceMu.Lock()
	defer defaultServiceMu.Unlock()
	defaultService = service
}

func DefaultService() *Service {
	defaultServiceMu.RLock()
	defer defaultServiceMu.RUnlock()
	return defaultService
}

func (service *Service) CheckProviderGate(ctx context.Context, accountID string, now time.Time) error {
	if service == nil || strings.TrimSpace(accountID) == "" {
		return fmt.Errorf("settlement gate binding is invalid")
	}
	var aggregate struct {
		Count  int64
		Amount int64
	}
	if err := db.GetDB().WithContext(ctx).Model(&dbmodel.HoneyChargeOutbox{}).
		Select("COUNT(*) AS count, COALESCE(SUM(charge_microunits), 0) AS amount").
		Where("account_id = ? AND status = ?", accountID, dbmodel.HoneyChargePending).
		Scan(&aggregate).Error; err != nil {
		return fmt.Errorf("read pending charge aggregate: %w", err)
	}
	if aggregate.Amount >= service.limits.MaxPendingAmountMicrounits {
		return &GateError{Reason: GateAmount}
	}
	if aggregate.Count >= service.limits.MaxPendingCount {
		return &GateError{Reason: GateCount}
	}
	if aggregate.Count == 0 {
		return nil
	}
	var oldest dbmodel.HoneyChargeOutbox
	if err := db.GetDB().WithContext(ctx).
		Where("account_id = ? AND status = ?", accountID, dbmodel.HoneyChargePending).
		Order("created_at ASC, id ASC").First(&oldest).Error; err != nil {
		return fmt.Errorf("read oldest pending charge: %w", err)
	}
	if !oldest.CreatedAt.IsZero() && now.UTC().Sub(oldest.CreatedAt.UTC()) >= service.limits.MaxPendingAge {
		return &GateError{Reason: GateAge}
	}
	return nil
}

func (service *Service) Start() {
	if service == nil {
		return
	}
	service.startOnce.Do(func() {
		go service.run()
	})
}

func (service *Service) Wake() {
	if service == nil {
		return
	}
	select {
	case service.wake <- struct{}{}:
	default:
	}
}

func (service *Service) Close() error {
	if service == nil {
		return nil
	}
	service.closeOnce.Do(func() {
		close(service.stop)
	})
	select {
	case <-service.done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("settlement worker did not stop")
	}
	return nil
}

func (service *Service) run() {
	defer close(service.done)
	ticker := time.NewTicker(service.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-service.stop:
			return
		case <-service.wake:
		case <-ticker.C:
		}
		for {
			processed, _ := service.RunOnce(context.Background())
			if !processed {
				break
			}
		}
	}
}

func (service *Service) RunOnce(ctx context.Context) (bool, error) {
	if service == nil {
		return false, fmt.Errorf("settlement service is unavailable")
	}
	service.settleMu.Lock()
	defer service.settleMu.Unlock()

	now := time.Now().UTC()
	var pending dbmodel.HoneyChargeOutbox
	err := db.GetDB().WithContext(ctx).
		Where("status = ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", dbmodel.HoneyChargePending, now).
		Order("created_at ASC, id ASC").First(&pending).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load pending charge: %w", err)
	}

	request, err := chargeRequest(pending)
	if err == nil {
		_, err = service.client.Charge(ctx, request)
	}
	if err != nil {
		service.recordFailure(ctx, pending, now, err)
		return true, err
	}

	err = db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		settledAt := now
		outboxUpdate := tx.Model(&dbmodel.HoneyChargeOutbox{}).
			Where("id = ? AND generation_id = ? AND billing_event_id = ? AND account_id = ? AND role_id = ? AND status = ?",
				pending.ID, pending.GenerationID, pending.BillingEventID, pending.AccountID, pending.RoleID, dbmodel.HoneyChargePending).
			Updates(map[string]any{
				"status":          dbmodel.HoneyChargeSettled,
				"attempt_count":   pending.AttemptCount + 1,
				"last_attempt_at": now,
				"next_attempt_at": nil,
				"last_error_code": "",
				"settled_at":      settledAt,
				"updated_at":      now,
			})
		if outboxUpdate.Error != nil {
			return outboxUpdate.Error
		}
		if outboxUpdate.RowsAffected != 1 {
			return fmt.Errorf("pending charge settlement transition was not applied")
		}
		attemptUpdate := tx.Model(&dbmodel.HoneyGenerationAttempt{}).
			Where("generation_id = ? AND billing_event_id = ? AND account_id = ? AND role_id = ? AND status = ?",
				pending.GenerationID, pending.BillingEventID, pending.AccountID, pending.RoleID, dbmodel.HoneyGenerationPendingCharge).
			Updates(map[string]any{
				"status":     dbmodel.HoneyGenerationCompleted,
				"updated_at": now,
			})
		if attemptUpdate.Error != nil {
			return attemptUpdate.Error
		}
		if attemptUpdate.RowsAffected != 1 {
			return fmt.Errorf("Honey generation settlement transition was not applied")
		}
		return nil
	})
	if err != nil {
		return true, fmt.Errorf("commit pending charge settlement: %w", err)
	}
	return true, nil
}

func chargeRequest(pending dbmodel.HoneyChargeOutbox) (billing.ChargeRequest, error) {
	if pending.GenerationID == "" || pending.BillingEventID == "" || pending.AccountID == "" ||
		pending.RoleID == "" || pending.APIKeyID <= 0 || pending.ReceiptID == "" ||
		pending.ExternalModel == "" || pending.PricingVersion == "" || pending.ProviderRef == "" ||
		pending.ChargeMicrounits < 0 {
		return billing.ChargeRequest{}, fmt.Errorf("pending charge identity is invalid")
	}
	kind := billing.ChargeKind(pending.ChargeKind)
	if kind != billing.ChargeKindChatTokens && kind != billing.ChargeKindImageGeneration {
		return billing.ChargeRequest{}, fmt.Errorf("pending charge kind is invalid")
	}
	return billing.ChargeRequest{
		AccountID:      pending.AccountID,
		APIKeyID:       pending.APIKeyID,
		BillingEventID: pending.BillingEventID,
		ReceiptID:      pending.ReceiptID,
		ExternalModel:  pending.ExternalModel,
		PricingVersion: pending.PricingVersion,
		ChargeKind:     kind,
		Usage: billing.Usage{
			InputTokens:      pending.InputTokens,
			OutputTokens:     pending.OutputTokens,
			CacheReadTokens:  pending.CacheReadTokens,
			CacheWriteTokens: pending.CacheWriteTokens,
		},
		GenerationCount:  pending.GenerationCount,
		ChargeMicrounits: pending.ChargeMicrounits,
		ProviderRef:      pending.ProviderRef,
	}, nil
}

func (service *Service) recordFailure(ctx context.Context, pending dbmodel.HoneyChargeOutbox, now time.Time, failure error) {
	attempt := pending.AttemptCount + 1
	backoff := time.Second << min(attempt-1, 8)
	next := now.Add(backoff)
	code := "ERROR"
	if status, ok := billing.StatusCode(failure); ok {
		code = fmt.Sprintf("HTTP_%d", status)
	} else if errors.Is(failure, context.DeadlineExceeded) {
		code = "TIMEOUT"
	} else if errors.Is(failure, context.Canceled) {
		code = "CANCELED"
	}
	if len(code) > 64 {
		code = code[:64]
	}
	_ = db.GetDB().WithContext(context.WithoutCancel(ctx)).Model(&dbmodel.HoneyChargeOutbox{}).
		Where("id = ? AND status = ?", pending.ID, dbmodel.HoneyChargePending).
		Updates(map[string]any{
			"attempt_count":   attempt,
			"last_attempt_at": now,
			"next_attempt_at": next,
			"last_error_code": code,
			"updated_at":      now,
		}).Error
}

func HTTPStatusForGate(err error) (int, GateReason, bool) {
	var gate *GateError
	if !errors.As(err, &gate) {
		return 0, "", false
	}
	return http.StatusTooManyRequests, gate.Reason, true
}
