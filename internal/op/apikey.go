package op

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/cache"
)

var ownerIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

var apiKeyCache = cache.New[int, model.APIKey](16)
var apiKeyIDMap = cache.New[string, int](16)
var apiKeyBindingLock sync.Mutex

func APIKeyCreate(key *model.APIKey, ctx context.Context) error {
	apiKeyBindingLock.Lock()
	defer apiKeyBindingLock.Unlock()
	if key.BillingEnabled {
		key.SupportedModels = strings.TrimSpace(key.SupportedModels)
		key.MaxCost = 0
	}
	if err := validateAPIKeyBinding(*key, 0); err != nil {
		return err
	}
	if err := db.GetDB().WithContext(ctx).Create(key).Error; err != nil {
		return fmt.Errorf("failed to create API key: %w", err)
	}
	apiKeyCache.Set(key.ID, *key)
	apiKeyIDMap.Set(key.APIKey, key.ID)
	return nil
}

func APIKeyUpdate(key *model.APIKey, ctx context.Context) error {
	apiKeyBindingLock.Lock()
	defer apiKeyBindingLock.Unlock()
	existing, ok := apiKeyCache.Get(key.ID)
	if !ok {
		return fmt.Errorf("API key not found")
	}
	key.OwnerAccountID = existing.OwnerAccountID
	key.OwnerRoleID = existing.OwnerRoleID
	key.BillingEnabled = existing.BillingEnabled
	if existing.BillingEnabled {
		key.SupportedModels = existing.SupportedModels
		key.MaxCost = 0
	}
	if err := validateAPIKeyBinding(*key, key.ID); err != nil {
		return err
	}
	if err := db.GetDB().WithContext(ctx).Omit("api_key").Save(key).Error; err != nil {
		return fmt.Errorf("failed to update API key: %w", err)
	}
	key.APIKey = existing.APIKey
	apiKeyCache.Set(key.ID, *key)
	return nil
}

func APIKeyList(ctx context.Context) ([]model.APIKey, error) {
	keys := make([]model.APIKey, 0, apiKeyCache.Len())
	for _, apiKey := range apiKeyCache.GetAll() {
		keys = append(keys, apiKey)
	}
	return keys, nil
}

func APIKeyGet(id int, ctx context.Context) (model.APIKey, error) {
	apiKey, ok := apiKeyCache.Get(id)
	if !ok {
		return model.APIKey{}, fmt.Errorf("API key not found")
	}
	return apiKey, nil
}

func APIKeyGetByAPIKey(apiKey string, ctx context.Context) (model.APIKey, error) {
	id, ok := apiKeyIDMap.Get(apiKey)
	if !ok {
		return model.APIKey{}, fmt.Errorf("API key not found")
	}
	return APIKeyGet(id, ctx)
}

func APIKeyDelete(id int, ctx context.Context) error {
	apiKeyBindingLock.Lock()
	defer apiKeyBindingLock.Unlock()
	k, ok := apiKeyCache.Get(id)
	if !ok {
		return fmt.Errorf("API key not found")
	}
	if err := StatsAPIKeyDel(id); err != nil {
		return fmt.Errorf("failed to delete stats API key: %v", err)
	}
	result := db.GetDB().WithContext(ctx).Delete(&k)
	if result.RowsAffected == 0 {
		return fmt.Errorf("API key not found")
	}
	if result.Error != nil {
		return fmt.Errorf("failed to delete API key: %w", result.Error)
	}
	apiKeyCache.Del(k.ID)
	apiKeyIDMap.Del(k.APIKey)
	return nil
}

func APIKeyGetByOwner(accountID string, ctx context.Context) (model.APIKey, error) {
	for _, apiKey := range apiKeyCache.GetAll() {
		if apiKey.BillingEnabled && apiKey.OwnerAccountID == accountID {
			return apiKey, nil
		}
	}
	return model.APIKey{}, fmt.Errorf("API key owner not found")
}

func validateAPIKeyBinding(key model.APIKey, currentID int) error {
	if !key.BillingEnabled {
		return nil
	}
	if !ownerIdentifier.MatchString(key.OwnerAccountID) || !ownerIdentifier.MatchString(key.OwnerRoleID) {
		return fmt.Errorf("billing owner identity is invalid")
	}
	models := strings.Split(key.SupportedModels, ",")
	if len(models) != 1 || strings.TrimSpace(models[0]) == "" {
		return fmt.Errorf("billing API key must bind exactly one public model")
	}
	key.SupportedModels = strings.TrimSpace(models[0])
	if key.MaxCost != 0 {
		return fmt.Errorf("billing API key cannot use legacy max cost")
	}
	for _, existing := range apiKeyCache.GetAll() {
		if existing.ID == currentID || !existing.BillingEnabled {
			continue
		}
		if existing.OwnerAccountID == key.OwnerAccountID || existing.OwnerRoleID == key.OwnerRoleID {
			return fmt.Errorf("billing owner already has an API key")
		}
	}
	return nil
}

func apiKeyRefreshCache(ctx context.Context) error {
	apiKeys := []model.APIKey{}
	if err := db.GetDB().WithContext(ctx).Find(&apiKeys).Error; err != nil {
		return err
	}
	for _, apiKey := range apiKeys {
		apiKeyCache.Set(apiKey.ID, apiKey)
		apiKeyIDMap.Set(apiKey.APIKey, apiKey.ID)
	}
	return nil
}
