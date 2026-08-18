package model

import "time"

type HoneyGenerationStatus string

const (
	HoneyGenerationAccepted  HoneyGenerationStatus = "ACCEPTED"
	HoneyGenerationRunning   HoneyGenerationStatus = "RUNNING"
	HoneyGenerationCompleted HoneyGenerationStatus = "COMPLETED"
	HoneyGenerationAmbiguous HoneyGenerationStatus = "AMBIGUOUS"
)

// HoneyGenerationAttempt is the durable execution record for one Honey main-chat
// generation. It intentionally stores identity, execution state, billing metadata,
// and the necessary provider result only; Honey Runtime remains the authority for
// user and assistant chat history.
type HoneyGenerationAttempt struct {
	ID                  uint64                `json:"id" gorm:"primaryKey"`
	GenerationID        string                `json:"generation_id" gorm:"size:128;not null;uniqueIndex"`
	BillingEventID      string                `json:"billing_event_id" gorm:"size:128;not null;uniqueIndex"`
	AccountID           string                `json:"account_id" gorm:"size:128;not null;index"`
	RoleID              string                `json:"role_id" gorm:"size:128;not null"`
	APIKeyID            int                   `json:"api_key_id" gorm:"not null"`
	RouteFormat         string                `json:"route_format" gorm:"size:64;not null"`
	RequestModel        string                `json:"request_model" gorm:"size:255;not null"`
	RequestDigest       string                `json:"request_digest" gorm:"size:64;not null"`
	ProviderBodyDigest  string                `json:"provider_body_digest" gorm:"size:64;not null"`
	Status              HoneyGenerationStatus `json:"status" gorm:"size:16;not null;index"`
	ReceiptID           string                `json:"receipt_id,omitempty" gorm:"size:128"`
	ResponseStatus      int                   `json:"response_status,omitempty"`
	ResponseContentType string                `json:"response_content_type,omitempty" gorm:"size:255"`
	ResponseBody        []byte                `json:"-"`
	StartedAt           *time.Time            `json:"started_at,omitempty"`
	CompletedAt         *time.Time            `json:"completed_at,omitempty"`
	CreatedAt           time.Time             `json:"created_at"`
	UpdatedAt           time.Time             `json:"updated_at"`
}
