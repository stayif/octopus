package op

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

var (
	ErrImageRelayLogNotFound    = errors.New("image relay log not found")
	ErrImageRelayLogAuditFailed = errors.New("image relay log audit failed")
)

type ImageRelayLogAuditResult struct {
	Pass             bool   `json:"pass"`
	RelayLogID       int64  `json:"relay_log_id"`
	RequestID        string `json:"request_id"`
	ReceiptID        string `json:"receipt_id,omitempty"`
	RequestModelName string `json:"request_model_name"`
	RouteFormat      string `json:"route_format"`
	ProfileID        string `json:"profile_id,omitempty"`
	ProfileVersion   string `json:"profile_version,omitempty"`
	GenerationCount  int    `json:"generation_count"`
	DurationMillis   int    `json:"duration_ms"`
	ResultCode       int    `json:"result_code"`
}

func isStructuredImageRelayLog(relayLog model.RelayLog) bool {
	return model.IsImageRoute(relayLog.RouteType, relayLog.RouteFormat)
}

func metadataOnlyImageRelayLog(relayLog model.RelayLog) model.RelayLog {
	return model.RelayLog{
		ID:               relayLog.ID,
		Time:             relayLog.Time,
		RequestModelName: relayLog.RequestModelName,
		UseTime:          relayLog.UseTime,
		TotalAttempts:    relayLog.TotalAttempts,
		RequestID:        relayLog.RequestID,
		ReceiptID:        relayLog.ReceiptID,
		RouteType:        relayLog.RouteType,
		RouteFormat:      relayLog.RouteFormat,
		ProfileID:        relayLog.ProfileID,
		ProfileVersion:   relayLog.ProfileVersion,
		GenerationCount:  relayLog.GenerationCount,
		ResultCode:       relayLog.ResultCode,
	}
}

func AuditImageRelayLog(ctx context.Context, requestID, receiptID string) (ImageRelayLogAuditResult, error) {
	requestID = strings.TrimSpace(requestID)
	receiptID = strings.TrimSpace(receiptID)
	if (requestID == "") == (receiptID == "") {
		return ImageRelayLogAuditResult{}, fmt.Errorf("provide exactly one request_id or receipt_id")
	}
	identifier := requestID
	if identifier == "" {
		identifier = receiptID
	}
	if !validAuditIdentifier(identifier) {
		return ImageRelayLogAuditResult{}, fmt.Errorf("audit identifier is invalid")
	}

	logsByID := make(map[int64]model.RelayLog, 2)
	relayLogCacheLock.Lock()
	for _, relayLog := range relayLogCache {
		if (requestID != "" && relayLog.RequestID == requestID) || (receiptID != "" && relayLog.ReceiptID == receiptID) {
			logsByID[relayLog.ID] = relayLog
		}
	}
	relayLogCacheLock.Unlock()

	query := db.GetDB().WithContext(ctx)
	if requestID != "" {
		query = query.Where("request_id = ?", requestID)
	} else {
		query = query.Where("receipt_id = ?", receiptID)
	}
	var persisted []model.RelayLog
	if err := query.Limit(2).Find(&persisted).Error; err != nil {
		return ImageRelayLogAuditResult{}, fmt.Errorf("query image relay log: %w", err)
	}
	for _, relayLog := range persisted {
		logsByID[relayLog.ID] = relayLog
	}
	if len(logsByID) == 0 {
		return ImageRelayLogAuditResult{}, ErrImageRelayLogNotFound
	}
	if len(logsByID) != 1 {
		return ImageRelayLogAuditResult{}, fmt.Errorf("%w: audit identifier matched multiple relay logs", ErrImageRelayLogAuditFailed)
	}

	var relayLog model.RelayLog
	for _, candidate := range logsByID {
		relayLog = candidate
	}
	if err := ValidateImageRelayLogAudit(relayLog); err != nil {
		return ImageRelayLogAuditResult{}, fmt.Errorf("%w: %v", ErrImageRelayLogAuditFailed, err)
	}

	return ImageRelayLogAuditResult{
		Pass:             true,
		RelayLogID:       relayLog.ID,
		RequestID:        relayLog.RequestID,
		ReceiptID:        relayLog.ReceiptID,
		RequestModelName: relayLog.RequestModelName,
		RouteFormat:      relayLog.RouteFormat,
		ProfileID:        relayLog.ProfileID,
		ProfileVersion:   relayLog.ProfileVersion,
		GenerationCount:  relayLog.GenerationCount,
		DurationMillis:   relayLog.UseTime,
		ResultCode:       relayLog.ResultCode,
	}, nil
}

// ValidateImageRelayLogAudit verifies the stored row by shape, not by searching for known canaries.
// Passing means forbidden content has no populated relay_logs field in which it could be recovered.
func ValidateImageRelayLogAudit(relayLog model.RelayLog) error {
	if !model.IsImageRoute(relayLog.RouteType, relayLog.RouteFormat) {
		return fmt.Errorf("relay log is not a structured image route")
	}
	if relayLog.RequestModelName == "" || !validPublicMetadata(relayLog.RequestModelName) {
		return fmt.Errorf("public product model is invalid")
	}
	if relayLog.RequestID == "" || !validAuditIdentifier(relayLog.RequestID) {
		return fmt.Errorf("request ID is invalid")
	}
	if relayLog.ReceiptID != "" && !validAuditIdentifier(relayLog.ReceiptID) {
		return fmt.Errorf("receipt ID is invalid")
	}
	if relayLog.ProfileID != "" && !validPublicMetadata(relayLog.ProfileID) {
		return fmt.Errorf("profile ID is invalid")
	}
	if relayLog.ProfileVersion != "" && !validPublicMetadata(relayLog.ProfileVersion) {
		return fmt.Errorf("profile version is invalid")
	}
	if relayLog.GenerationCount < 1 {
		return fmt.Errorf("generation count is invalid")
	}
	if relayLog.UseTime < 0 || relayLog.TotalAttempts < 0 || relayLog.ResultCode < 100 || relayLog.ResultCode > 599 {
		return fmt.Errorf("result metadata is invalid")
	}

	if relayLog.RequestContent != "" || relayLog.ResponseContent != "" || relayLog.Error != "" || len(relayLog.Attempts) != 0 {
		return fmt.Errorf("content-bearing relay log fields are populated")
	}
	if relayLog.RequestAPIKeyName != "" || relayLog.ChannelId != 0 || relayLog.ChannelName != "" || relayLog.ActualModelName != "" {
		return fmt.Errorf("provider-private relay log fields are populated")
	}
	if relayLog.InputTokens != 0 || relayLog.OutputTokens != 0 || relayLog.Ftut != 0 || relayLog.Cost != 0 {
		return fmt.Errorf("non-allowlisted image metrics are populated")
	}
	return nil
}

func validAuditIdentifier(value string) bool {
	return validBoundedIdentifier(value, 128, false)
}

func validPublicMetadata(value string) bool {
	if strings.Contains(value, "://") {
		return false
	}
	return validBoundedIdentifier(value, 128, true)
}

func validBoundedIdentifier(value string, maxLen int, allowSlash bool) bool {
	if value == "" || len(value) > maxLen {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') {
			continue
		}
		switch char {
		case '-', '_', '.', ':':
			continue
		case '/':
			if allowSlash {
				continue
			}
		}
		return false
	}
	return true
}
