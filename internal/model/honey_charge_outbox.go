package model

import "time"

type HoneyChargeOutboxStatus string

const (
	HoneyChargePending HoneyChargeOutboxStatus = "PENDING"
	HoneyChargeSettled HoneyChargeOutboxStatus = "SETTLED"
)

// HoneyChargeOutbox stores only stable billing facts. Provider response bodies
// remain on HoneyGenerationAttempt and private chat state remains in Runtime.
type HoneyChargeOutbox struct {
	ID               uint64                  `json:"id" gorm:"primaryKey"`
	GenerationID     string                  `json:"generation_id" gorm:"size:128;not null;uniqueIndex"`
	BillingEventID   string                  `json:"billing_event_id" gorm:"size:128;not null;uniqueIndex"`
	AccountID        string                  `json:"account_id" gorm:"size:128;not null;index:idx_honey_charge_account_pending,priority:1"`
	RoleID           string                  `json:"role_id" gorm:"size:128;not null"`
	APIKeyID         int                     `json:"api_key_id" gorm:"not null"`
	ReceiptID        string                  `json:"receipt_id" gorm:"size:128;not null"`
	ExternalModel    string                  `json:"external_model" gorm:"size:255;not null"`
	PricingVersion   string                  `json:"pricing_version" gorm:"size:128;not null"`
	ChargeKind       string                  `json:"charge_kind" gorm:"size:32;not null"`
	InputTokens      int64                   `json:"input_tokens" gorm:"not null"`
	OutputTokens     int64                   `json:"output_tokens" gorm:"not null"`
	CacheReadTokens  int64                   `json:"cache_read_tokens" gorm:"not null"`
	CacheWriteTokens int64                   `json:"cache_write_tokens" gorm:"not null"`
	GenerationCount  int64                   `json:"generation_count" gorm:"not null"`
	ChargeMicrounits int64                   `json:"charge_microunits" gorm:"not null"`
	ProviderRef      string                  `json:"provider_ref" gorm:"size:128;not null"`
	Status           HoneyChargeOutboxStatus `json:"status" gorm:"size:16;not null;index;index:idx_honey_charge_account_pending,priority:2"`
	AttemptCount     int                     `json:"attempt_count" gorm:"not null"`
	LastAttemptAt    *time.Time              `json:"last_attempt_at,omitempty"`
	NextAttemptAt    *time.Time              `json:"next_attempt_at,omitempty" gorm:"index"`
	LastErrorCode    string                  `json:"last_error_code,omitempty" gorm:"size:64"`
	SettledAt        *time.Time              `json:"settled_at,omitempty"`
	CreatedAt        time.Time               `json:"created_at" gorm:"index"`
	UpdatedAt        time.Time               `json:"updated_at"`
}
