package helper

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func TestLLMPriceDeletePreservesUserBillingOnlyModel(t *testing.T) {
	if err := db.InitDB("sqlite", filepath.Join(t.TempDir(), "octopus.db"), false); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := op.LLMCreate(model.LLMInfo{
		Name: "honey-image-v1",
		UserBillingPrice: model.UserBillingPrice{
			PricingVersion:               "xud128-cny-v1",
			GenerationMicrounitsPerImage: 200_000,
		},
	}, ctx); err != nil {
		t.Fatalf("LLMCreate user-priced model: %v", err)
	}
	if err := op.LLMCreate(model.LLMInfo{Name: "provider-removed-model"}, ctx); err != nil {
		t.Fatalf("LLMCreate empty model: %v", err)
	}

	if err := LLMPriceDeleteFromDBWithNoPrice(
		[]string{"honey-image-v1", "provider-removed-model"}, ctx,
	); err != nil {
		t.Fatalf("LLMPriceDeleteFromDBWithNoPrice: %v", err)
	}

	if _, err := op.LLMInfoGet("honey-image-v1"); err != nil {
		t.Fatalf("user-priced model was deleted: %v", err)
	}
	if _, err := op.LLMInfoGet("provider-removed-model"); err == nil {
		t.Fatal("model without provider or user price was retained")
	}
}
