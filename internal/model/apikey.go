package model

type APIKey struct {
	ID              int     `json:"id" gorm:"primaryKey"`
	Name            string  `json:"name" gorm:"not null"`
	APIKey          string  `json:"api_key" gorm:"not null"`
	Enabled         bool    `json:"enabled" gorm:"default:true"`
	ExpireAt        int64   `json:"expire_at,omitempty"`
	MaxCost         float64 `json:"max_cost,omitempty"`
	SupportedModels string  `json:"supported_models,omitempty"`
	OwnerAccountID  string  `json:"owner_account_id,omitempty" gorm:"index"`
	OwnerRoleID     string  `json:"owner_role_id,omitempty" gorm:"index"`
	BillingEnabled  bool    `json:"billing_enabled,omitempty" gorm:"default:false"`
}
