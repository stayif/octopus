package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/llm"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	honeyAttemptModeHeader   = "X-Honey-Attempt-Mode"
	honeyBillingIDHeader     = "X-Honey-Billing-Event-ID"
	honeyGenerationIDHeader  = "X-Honey-Generation-ID"
	honeyRequestDigestHeader = "X-Honey-Request-Digest"
	honeyGenerationState     = "X-Honey-Generation-State"
	honeyGenerationReplayed  = "X-Honey-Generation-Replayed"

	honeyAttemptModeStart       = "START"
	honeyAttemptModeResolveOnly = "RESOLVE_ONLY"
	honeyGenerationResultLimit  = 16 << 20
)

var (
	honeyGenerationIdentifier = regexp.MustCompile(`^main:[0-9a-f]{64}$`)
	honeyBillingIdentifier    = regexp.MustCompile(`^chat:[0-9a-f]{64}$`)
	honeySHA256Identifier     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

var errHoneyGenerationHandled = errors.New("Honey generation request handled")

type honeyGenerationExecution struct {
	attempt dbmodel.HoneyGenerationAttempt
}

type honeyGenerationBinding struct {
	generationID       string
	billingEventID     string
	accountID          string
	roleID             string
	apiKeyID           int
	routeFormat        string
	requestModel       string
	requestDigest      string
	providerBodyDigest string
}

func prepareHoneyGeneration(
	c *gin.Context,
	inboundType llm.APIFormat,
	internalRequest *llm.Request,
) (*honeyGenerationExecution, error) {
	mode := strings.TrimSpace(c.GetHeader(honeyAttemptModeHeader))
	generationID := strings.TrimSpace(c.GetHeader(honeyGenerationIDHeader))
	billingEventID := strings.TrimSpace(c.GetHeader(honeyBillingIDHeader))
	requestDigest := strings.TrimSpace(c.GetHeader(honeyRequestDigestHeader))
	if mode == "" && generationID == "" && requestDigest == "" {
		return nil, nil
	}
	if (mode != honeyAttemptModeStart && mode != honeyAttemptModeResolveOnly) ||
		!honeyGenerationIdentifier.MatchString(generationID) ||
		!honeyBillingIdentifier.MatchString(billingEventID) ||
		!honeySHA256Identifier.MatchString(requestDigest) {
		resp.Error(c, http.StatusBadRequest, "Honey generation identity is invalid")
		return nil, errHoneyGenerationHandled
	}
	if !c.GetBool("billing_enabled") {
		resp.Error(c, http.StatusUnauthorized, "Honey generation requires a billing-bound API key")
		return nil, errHoneyGenerationHandled
	}
	accountID := strings.TrimSpace(c.GetString("billing_account_id"))
	roleID := strings.TrimSpace(c.GetString("billing_role_id"))
	apiKeyID := c.GetInt("api_key_id")
	if accountID == "" || roleID == "" || apiKeyID <= 0 || internalRequest == nil || internalRequest.RawRequest == nil {
		resp.Error(c, http.StatusUnauthorized, "Honey generation binding is invalid")
		return nil, errHoneyGenerationHandled
	}
	bodyDigest := sha256.Sum256(internalRequest.RawRequest.Body)
	binding := honeyGenerationBinding{
		generationID:       generationID,
		billingEventID:     billingEventID,
		accountID:          accountID,
		roleID:             roleID,
		apiKeyID:           apiKeyID,
		routeFormat:        string(inboundType),
		requestModel:       internalRequest.Model,
		requestDigest:      requestDigest,
		providerBodyDigest: hex.EncodeToString(bodyDigest[:]),
	}

	ctx := context.WithoutCancel(c.Request.Context())
	if mode == honeyAttemptModeStart {
		candidate := dbmodel.HoneyGenerationAttempt{
			GenerationID:       binding.generationID,
			BillingEventID:     binding.billingEventID,
			AccountID:          binding.accountID,
			RoleID:             binding.roleID,
			APIKeyID:           binding.apiKeyID,
			RouteFormat:        binding.routeFormat,
			RequestModel:       binding.requestModel,
			RequestDigest:      binding.requestDigest,
			ProviderBodyDigest: binding.providerBodyDigest,
			Status:             dbmodel.HoneyGenerationAccepted,
		}
		if err := db.GetDB().WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&candidate).Error; err != nil {
			resp.Error(c, http.StatusServiceUnavailable, "Honey generation state is unavailable")
			return nil, errHoneyGenerationHandled
		}
	}

	attempt, err := loadHoneyGeneration(ctx, binding.generationID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_, billingErr := loadHoneyGenerationByBilling(ctx, binding.billingEventID)
		if billingErr == nil {
			writeHoneyGenerationConflict(c)
			return nil, errHoneyGenerationHandled
		}
		if !errors.Is(billingErr, gorm.ErrRecordNotFound) {
			resp.Error(c, http.StatusServiceUnavailable, "Honey generation state is unavailable")
			return nil, errHoneyGenerationHandled
		}
		if mode == honeyAttemptModeResolveOnly {
			writeHoneyGenerationState(c, dbmodel.HoneyGenerationAmbiguous)
			return nil, errHoneyGenerationHandled
		}
		resp.Error(c, http.StatusServiceUnavailable, "Honey generation state is unavailable")
		return nil, errHoneyGenerationHandled
	}
	if err != nil {
		resp.Error(c, http.StatusServiceUnavailable, "Honey generation state is unavailable")
		return nil, errHoneyGenerationHandled
	}
	if !attemptMatchesHoneyBinding(attempt, binding) {
		writeHoneyGenerationConflict(c)
		return nil, errHoneyGenerationHandled
	}

	switch attempt.Status {
	case dbmodel.HoneyGenerationAccepted:
		if mode == honeyAttemptModeResolveOnly {
			writeHoneyGenerationState(c, dbmodel.HoneyGenerationAccepted)
			return nil, errHoneyGenerationHandled
		}
		return &honeyGenerationExecution{attempt: attempt}, nil
	case dbmodel.HoneyGenerationRunning, dbmodel.HoneyGenerationAmbiguous:
		writeHoneyGenerationState(c, attempt.Status)
		return nil, errHoneyGenerationHandled
	case dbmodel.HoneyGenerationCompleted:
		writeHoneyGenerationResult(c, attempt, true)
		return nil, errHoneyGenerationHandled
	default:
		writeHoneyGenerationState(c, dbmodel.HoneyGenerationAmbiguous)
		return nil, errHoneyGenerationHandled
	}
}

func loadHoneyGeneration(ctx context.Context, generationID string) (dbmodel.HoneyGenerationAttempt, error) {
	var attempt dbmodel.HoneyGenerationAttempt
	err := db.GetDB().WithContext(ctx).Where("generation_id = ?", generationID).First(&attempt).Error
	return attempt, err
}

func loadHoneyGenerationByBilling(ctx context.Context, billingEventID string) (dbmodel.HoneyGenerationAttempt, error) {
	var attempt dbmodel.HoneyGenerationAttempt
	err := db.GetDB().WithContext(ctx).Where("billing_event_id = ?", billingEventID).First(&attempt).Error
	return attempt, err
}

func attemptMatchesHoneyBinding(attempt dbmodel.HoneyGenerationAttempt, binding honeyGenerationBinding) bool {
	return attempt.GenerationID == binding.generationID &&
		attempt.BillingEventID == binding.billingEventID &&
		attempt.AccountID == binding.accountID &&
		attempt.RoleID == binding.roleID &&
		attempt.APIKeyID == binding.apiKeyID &&
		attempt.RouteFormat == binding.routeFormat &&
		attempt.RequestModel == binding.requestModel &&
		attempt.RequestDigest == binding.requestDigest &&
		attempt.ProviderBodyDigest == binding.providerBodyDigest
}

func (execution *honeyGenerationExecution) claim(c *gin.Context) (bool, error) {
	if execution == nil {
		return false, fmt.Errorf("missing Honey generation execution")
	}
	ctx := context.WithoutCancel(c.Request.Context())
	now := time.Now().UTC()
	result := db.GetDB().WithContext(ctx).Model(&dbmodel.HoneyGenerationAttempt{}).
		Where("generation_id = ? AND status = ?", execution.attempt.GenerationID, dbmodel.HoneyGenerationAccepted).
		Updates(map[string]any{
			"status":     dbmodel.HoneyGenerationRunning,
			"started_at": now,
			"updated_at": now,
		})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected == 1 {
		execution.attempt.Status = dbmodel.HoneyGenerationRunning
		execution.attempt.StartedAt = &now
		return true, nil
	}
	attempt, err := loadHoneyGeneration(ctx, execution.attempt.GenerationID)
	if err != nil {
		return false, err
	}
	switch attempt.Status {
	case dbmodel.HoneyGenerationCompleted:
		writeHoneyGenerationResult(c, attempt, true)
	case dbmodel.HoneyGenerationAccepted, dbmodel.HoneyGenerationRunning, dbmodel.HoneyGenerationAmbiguous:
		writeHoneyGenerationState(c, attempt.Status)
	default:
		writeHoneyGenerationState(c, dbmodel.HoneyGenerationAmbiguous)
	}
	return false, nil
}

func (execution *honeyGenerationExecution) saveReceipt(c *gin.Context, receiptID string) error {
	if execution == nil || receiptID == "" {
		return fmt.Errorf("invalid Honey billing receipt")
	}
	ctx := context.WithoutCancel(c.Request.Context())
	result := db.GetDB().WithContext(ctx).Model(&dbmodel.HoneyGenerationAttempt{}).
		Where("generation_id = ? AND status = ? AND (receipt_id = '' OR receipt_id = ?)",
			execution.attempt.GenerationID, dbmodel.HoneyGenerationAccepted, receiptID).
		Update("receipt_id", receiptID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		attempt, err := loadHoneyGeneration(ctx, execution.attempt.GenerationID)
		if err != nil {
			return err
		}
		if attempt.ReceiptID != receiptID {
			return fmt.Errorf("Honey billing receipt conflicts with durable attempt")
		}
	}
	execution.attempt.ReceiptID = receiptID
	return nil
}

func (execution *honeyGenerationExecution) complete(
	c *gin.Context,
	status int,
	contentType string,
	body []byte,
) (dbmodel.HoneyGenerationAttempt, error) {
	if execution == nil || len(body) > honeyGenerationResultLimit {
		return dbmodel.HoneyGenerationAttempt{}, fmt.Errorf("Honey generation result is not persistable")
	}
	if status < 100 || status > 599 {
		status = http.StatusOK
	}
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/json"
	}
	ctx := context.WithoutCancel(c.Request.Context())
	now := time.Now().UTC()
	result := db.GetDB().WithContext(ctx).Model(&dbmodel.HoneyGenerationAttempt{}).
		Where("generation_id = ? AND status = ?", execution.attempt.GenerationID, dbmodel.HoneyGenerationRunning).
		Updates(map[string]any{
			"status":                dbmodel.HoneyGenerationCompleted,
			"response_status":       status,
			"response_content_type": contentType,
			"response_body":         append([]byte(nil), body...),
			"completed_at":          now,
			"updated_at":            now,
		})
	if result.Error != nil {
		return dbmodel.HoneyGenerationAttempt{}, result.Error
	}
	if result.RowsAffected != 1 {
		return dbmodel.HoneyGenerationAttempt{}, fmt.Errorf("Honey generation completion transition was not applied")
	}
	attempt, err := loadHoneyGeneration(ctx, execution.attempt.GenerationID)
	if err != nil {
		return dbmodel.HoneyGenerationAttempt{}, err
	}
	return attempt, nil
}

func (execution *honeyGenerationExecution) markAmbiguous(c *gin.Context) error {
	if execution == nil {
		return nil
	}
	ctx := context.WithoutCancel(c.Request.Context())
	now := time.Now().UTC()
	return db.GetDB().WithContext(ctx).Model(&dbmodel.HoneyGenerationAttempt{}).
		Where("generation_id = ? AND status = ?", execution.attempt.GenerationID, dbmodel.HoneyGenerationRunning).
		Updates(map[string]any{
			"status":     dbmodel.HoneyGenerationAmbiguous,
			"updated_at": now,
		}).Error
}

func writeHoneyGenerationConflict(c *gin.Context) {
	c.Header(honeyGenerationState, string(dbmodel.HoneyGenerationAmbiguous))
	resp.Error(c, http.StatusConflict, "Honey generation identity conflict")
}

func writeHoneyGenerationState(c *gin.Context, state dbmodel.HoneyGenerationStatus) {
	c.Header(honeyGenerationState, string(state))
	if state == dbmodel.HoneyGenerationRunning || state == dbmodel.HoneyGenerationAccepted {
		c.Header("Retry-After", "1")
	}
	resp.Error(c, http.StatusConflict, "Honey generation result is not replayable")
}

func writeHoneyGenerationResult(c *gin.Context, attempt dbmodel.HoneyGenerationAttempt, replayed bool) {
	contentType := strings.TrimSpace(attempt.ResponseContentType)
	if contentType == "" {
		contentType = "application/json"
	}
	c.Header(honeyGenerationState, string(dbmodel.HoneyGenerationCompleted))
	if replayed {
		c.Header(honeyGenerationReplayed, "true")
	} else {
		c.Header(honeyGenerationReplayed, "false")
	}
	if strings.HasPrefix(strings.ToLower(contentType), "text/event-stream") {
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("X-Accel-Buffering", "no")
	}
	status := attempt.ResponseStatus
	if status < 100 || status > 599 {
		status = http.StatusOK
	}
	c.Data(status, contentType, attempt.ResponseBody)
}

func resetHoneyGenerationResponseHeaders(c *gin.Context) {
	for _, header := range []string{"Content-Type", "Content-Length", "Cache-Control", "Connection", "X-Accel-Buffering"} {
		c.Writer.Header().Del(header)
	}
}

type honeyGenerationCaptureWriter struct {
	gin.ResponseWriter
	status   int
	size     int
	body     bytes.Buffer
	overflow bool
}

func newHoneyGenerationCaptureWriter(writer gin.ResponseWriter) *honeyGenerationCaptureWriter {
	return &honeyGenerationCaptureWriter{
		ResponseWriter: writer,
		status:         http.StatusOK,
		size:           -1,
	}
}

func (writer *honeyGenerationCaptureWriter) WriteHeader(status int) {
	if status > 0 && !writer.Written() {
		writer.status = status
	}
}

func (writer *honeyGenerationCaptureWriter) WriteHeaderNow() {
	if !writer.Written() {
		writer.size = 0
	}
}

func (writer *honeyGenerationCaptureWriter) Write(data []byte) (int, error) {
	writer.WriteHeaderNow()
	writer.size += len(data)
	if writer.body.Len()+len(data) > honeyGenerationResultLimit {
		writer.overflow = true
		return len(data), nil
	}
	_, _ = writer.body.Write(data)
	return len(data), nil
}

func (writer *honeyGenerationCaptureWriter) WriteString(data string) (int, error) {
	return writer.Write([]byte(data))
}

func (writer *honeyGenerationCaptureWriter) Flush() {
	writer.WriteHeaderNow()
}

func (writer *honeyGenerationCaptureWriter) Status() int {
	return writer.status
}

func (writer *honeyGenerationCaptureWriter) Size() int {
	return writer.size
}

func (writer *honeyGenerationCaptureWriter) Written() bool {
	return writer.size >= 0
}

func (writer *honeyGenerationCaptureWriter) Body() []byte {
	return append([]byte(nil), writer.body.Bytes()...)
}

var _ gin.ResponseWriter = (*honeyGenerationCaptureWriter)(nil)
