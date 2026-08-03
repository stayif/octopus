package op

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/cache"
)

var llmModelCache = cache.New[string, model.LLMInfo](16)
var pricingVersion = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func userBillingPriceConfigured(price model.UserBillingPrice) bool {
	return price.PricingVersion != "" ||
		price.InputMicrounitsPerMillion != 0 ||
		price.OutputMicrounitsPerMillion != 0 ||
		price.CacheReadMicrounitsPerMillion != 0 ||
		price.CacheWriteMicrounitsPerMillion != 0
}

func validateUserBillingPrice(price model.UserBillingPrice) error {
	if !userBillingPriceConfigured(price) {
		return nil
	}
	if !pricingVersion.MatchString(price.PricingVersion) {
		return fmt.Errorf("user billing pricing version is invalid")
	}
	rates := []int64{
		price.InputMicrounitsPerMillion,
		price.OutputMicrounitsPerMillion,
		price.CacheReadMicrounitsPerMillion,
		price.CacheWriteMicrounitsPerMillion,
	}
	nonzero := false
	for _, rate := range rates {
		if rate < 0 {
			return fmt.Errorf("user billing price cannot be negative")
		}
		nonzero = nonzero || rate > 0
	}
	if !nonzero {
		return fmt.Errorf("user billing price must contain a nonzero rate")
	}
	return nil
}

func LLMList(ctx context.Context) ([]model.LLMInfo, error) {
	models := make([]model.LLMInfo, 0, llmModelCache.Len())
	for _, info := range llmModelCache.GetAll() {
		models = append(models, info)
	}
	return models, nil
}

func LLMUpdate(model model.LLMInfo, ctx context.Context) error {
	existing, ok := llmModelCache.Get(model.Name)
	if !ok {
		return fmt.Errorf("model not found")
	}
	// Older Octopus admin clients know only the Provider-cost fields. Preserve
	// the independent user price when such a client updates the model.
	if !userBillingPriceConfigured(model.UserBillingPrice) {
		model.UserBillingPrice = existing.UserBillingPrice
	}
	if err := validateUserBillingPrice(model.UserBillingPrice); err != nil {
		return err
	}
	if err := db.GetDB().WithContext(ctx).Save(model).Error; err != nil {
		return err
	}
	llmModelCache.Set(model.Name, model)
	return nil
}

func LLMDelete(modelName string, ctx context.Context) error {
	_, ok := llmModelCache.Get(modelName)
	if !ok {
		return fmt.Errorf("model not found")
	}
	if err := db.GetDB().WithContext(ctx).Delete(&model.LLMInfo{Name: modelName}).Error; err != nil {
		return err
	}
	llmModelCache.Del(modelName)
	return nil
}
func LLMBatchDelete(modelNames []string, ctx context.Context) error {
	if len(modelNames) == 0 {
		return nil
	}
	if err := db.GetDB().WithContext(ctx).Where("name IN ?", modelNames).Delete(&model.LLMInfo{}).Error; err != nil {
		return err
	}
	llmModelCache.Del(modelNames...)
	return nil
}
func LLMCreate(model model.LLMInfo, ctx context.Context) error {
	model.Name = strings.ToLower(model.Name)
	if err := validateUserBillingPrice(model.UserBillingPrice); err != nil {
		return err
	}
	_, ok := llmModelCache.Get(model.Name)
	if ok {
		return fmt.Errorf("model already exists")
	}
	if err := db.GetDB().WithContext(ctx).Create(&model).Error; err != nil {
		return err
	}
	llmModelCache.Set(model.Name, model)
	return nil
}
func LLMBatchCreate(llmInfos []model.LLMInfo, ctx context.Context) error {
	if len(llmInfos) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(llmInfos))
	newLLMInfos := make([]model.LLMInfo, 0, len(llmInfos))
	for _, llmInfo := range llmInfos {
		llmInfo.Name = strings.ToLower(llmInfo.Name)
		if err := validateUserBillingPrice(llmInfo.UserBillingPrice); err != nil {
			return err
		}
		if _, ok := seen[llmInfo.Name]; ok {
			continue
		}
		if _, ok := llmModelCache.Get(llmInfo.Name); ok {
			continue
		}
		seen[llmInfo.Name] = struct{}{}
		newLLMInfos = append(newLLMInfos, llmInfo)
	}
	if len(newLLMInfos) == 0 {
		return nil
	}
	if err := db.GetDB().WithContext(ctx).Create(&newLLMInfos).Error; err != nil {
		return err
	}
	for _, llmInfo := range newLLMInfos {
		llmModelCache.Set(llmInfo.Name, llmInfo)
	}
	return nil
}
func LLMGet(name string) (model.LLMPrice, error) {
	info, ok := llmModelCache.Get(name)
	if !ok {
		return model.LLMPrice{}, fmt.Errorf("model not found")
	}
	return info.LLMPrice, nil
}

func LLMInfoGet(name string) (model.LLMInfo, error) {
	info, ok := llmModelCache.Get(strings.ToLower(name))
	if !ok {
		return model.LLMInfo{}, fmt.Errorf("model not found")
	}
	return info, nil
}

func llmRefreshCache(ctx context.Context) error {
	models := []model.LLMInfo{}
	if err := db.GetDB().WithContext(ctx).Find(&models).Error; err != nil {
		return err
	}
	for _, model := range models {
		llmModelCache.Set(model.Name, model)
	}
	return nil
}
