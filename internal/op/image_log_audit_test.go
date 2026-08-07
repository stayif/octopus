package op

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/looplj/axonhub/llm"
)

func TestValidateImageRelayLogAuditUsesStrictMetadataAllowlist(t *testing.T) {
	safe := model.RelayLog{
		ID:               125,
		Time:             1_700_000_000,
		RequestModelName: "honey-image-v1",
		UseTime:          250,
		TotalAttempts:    1,
		RequestID:        "oct-1250001",
		ReceiptID:        "receipt-1250001",
		RouteType:        string(llm.RequestTypeImage),
		RouteFormat:      string(llm.APIFormatOpenAIImageGeneration),
		ProfileID:        "honey-default",
		ProfileVersion:   "v1",
		GenerationCount:  2,
		ResultCode:       200,
	}
	if err := ValidateImageRelayLogAudit(safe); err != nil {
		t.Fatalf("safe metadata-only log rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*model.RelayLog)
	}{
		{name: "request content", mutate: func(log *model.RelayLog) { log.RequestContent = "synthetic-prompt-canary" }},
		{name: "response content", mutate: func(log *model.RelayLog) { log.ResponseContent = "https://assets.invalid/private" }},
		{name: "provider error", mutate: func(log *model.RelayLog) { log.Error = "provider-payload-canary" }},
		{name: "attempt payload", mutate: func(log *model.RelayLog) { log.Attempts = []model.ChannelAttempt{{Msg: "provider-payload-canary"}} }},
		{name: "provider channel", mutate: func(log *model.RelayLog) { log.ChannelName = "private-provider" }},
		{name: "provider model", mutate: func(log *model.RelayLog) { log.ActualModelName = "private-provider-model" }},
		{name: "api key label", mutate: func(log *model.RelayLog) { log.RequestAPIKeyName = "private-key-label" }},
		{name: "unsafe public model", mutate: func(log *model.RelayLog) { log.RequestModelName = "https://assets.invalid/private" }},
		{name: "non-image route", mutate: func(log *model.RelayLog) { log.RouteType = string(llm.RequestTypeChat) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := safe
			tt.mutate(&candidate)
			if err := ValidateImageRelayLogAudit(candidate); err == nil {
				t.Fatalf("unsafe image relay log passed audit: %+v", candidate)
			}
		})
	}
}

func TestMetadataOnlyImageRelayLogDropsContentAtPersistenceBoundary(t *testing.T) {
	unsafe := model.RelayLog{
		ID:                125,
		Time:              1_700_000_000,
		RequestModelName:  "honey-image-v1",
		RequestAPIKeyName: "private-key-label",
		ChannelId:         9,
		ChannelName:       "private-provider",
		ActualModelName:   "private-provider-model",
		InputTokens:       10,
		OutputTokens:      20,
		Cost:              1,
		RequestContent:    "synthetic-prompt-canary",
		ResponseContent:   "https://assets.invalid/private",
		Error:             "provider-payload-canary",
		Attempts:          []model.ChannelAttempt{{Msg: "provider-payload-canary"}},
		TotalAttempts:     1,
		RequestID:         "oct-1250003",
		RouteType:         string(llm.RequestTypeImage),
		RouteFormat:       string(llm.APIFormatOpenAIImageGeneration),
		GenerationCount:   1,
		UseTime:           250,
		ResultCode:        200,
	}

	sanitized := metadataOnlyImageRelayLog(unsafe)
	if err := ValidateImageRelayLogAudit(sanitized); err != nil {
		t.Fatalf("persistence sanitizer did not produce an auditable row: %v", err)
	}
}

func TestAuditImageRelayLogFindsPersistedRowByRequestOrReceiptID(t *testing.T) {
	if err := db.InitDB("sqlite", filepath.Join(t.TempDir(), "octopus.db"), false); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	relayLogCacheLock.Lock()
	relayLogCache = relayLogCache[:0]
	relayLogCacheLock.Unlock()

	relayLog := model.RelayLog{
		ID:               125,
		Time:             1_700_000_000,
		RequestModelName: "honey-image-v1",
		UseTime:          250,
		TotalAttempts:    1,
		RequestID:        "oct-1250002",
		ReceiptID:        "receipt-1250002",
		RouteType:        string(llm.RequestTypeImage),
		RouteFormat:      string(llm.APIFormatOpenAIImageGeneration),
		GenerationCount:  1,
		ResultCode:       200,
	}
	if err := db.GetDB().Create(&relayLog).Error; err != nil {
		t.Fatalf("create relay log: %v", err)
	}

	for _, lookup := range []struct {
		requestID string
		receiptID string
	}{
		{requestID: relayLog.RequestID},
		{receiptID: relayLog.ReceiptID},
	} {
		result, err := AuditImageRelayLog(context.Background(), lookup.requestID, lookup.receiptID)
		if err != nil {
			t.Fatalf("AuditImageRelayLog: %v", err)
		}
		if !result.Pass || result.RelayLogID != relayLog.ID || result.RequestID != relayLog.RequestID {
			t.Fatalf("unexpected audit result: %+v", result)
		}
	}
}
