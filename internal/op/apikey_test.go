package op

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestBillingAPIKeyHasOneImmutableOwnerEvenUnderConcurrentCreation(t *testing.T) {
	apiKeyCache.Clear()
	apiKeyIDMap.Clear()
	if err := db.InitDB("sqlite", filepath.Join(t.TempDir(), "octopus.db"), false); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() {
		apiKeyCache.Clear()
		apiKeyIDMap.Clear()
		_ = db.Close()
	})

	const attempts = 8
	start := make(chan struct{})
	var successes atomic.Int32
	var group sync.WaitGroup
	for i := 0; i < attempts; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			key := model.APIKey{
				Name:            fmt.Sprintf("Honey concurrent %d", index),
				APIKey:          fmt.Sprintf("sk-octopus-concurrent-%d", index),
				Enabled:         true,
				SupportedModels: "honey-chat",
				OwnerAccountID:  "account-a",
				OwnerRoleID:     "role-a",
				BillingEnabled:  true,
			}
			if APIKeyCreate(&key, context.Background()) == nil {
				successes.Add(1)
			}
		}(i)
	}
	close(start)
	group.Wait()

	if successes.Load() != 1 {
		t.Fatalf("successful concurrent bindings=%d want=1", successes.Load())
	}
	key, err := APIKeyGetByOwner("account-a", context.Background())
	if err != nil {
		t.Fatalf("APIKeyGetByOwner: %v", err)
	}
	originalID := key.ID
	key.OwnerAccountID = "account-b"
	key.OwnerRoleID = "role-b"
	key.SupportedModels = "another-model"
	if err := APIKeyUpdate(&key, context.Background()); err != nil {
		t.Fatalf("APIKeyUpdate: %v", err)
	}
	updated, err := APIKeyGet(originalID, context.Background())
	if err != nil {
		t.Fatalf("APIKeyGet: %v", err)
	}
	if updated.OwnerAccountID != "account-a" || updated.OwnerRoleID != "role-a" || updated.SupportedModels != "honey-chat" {
		t.Fatalf("billing owner/model changed: %+v", updated)
	}

	legacy := model.APIKey{
		Name:    "legacy",
		APIKey:  "sk-octopus-legacy",
		Enabled: true,
	}
	if err := APIKeyCreate(&legacy, context.Background()); err != nil {
		t.Fatalf("legacy API key must remain compatible: %v", err)
	}
}

func TestLLMUpdatePreservesIndependentUserBillingPrice(t *testing.T) {
	llmModelCache.Clear()
	if err := db.InitDB("sqlite", filepath.Join(t.TempDir(), "octopus.db"), false); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() {
		llmModelCache.Clear()
		_ = db.Close()
	})

	info := model.LLMInfo{
		Name: "honey-chat",
		LLMPrice: model.LLMPrice{
			Input: 1,
		},
		UserBillingPrice: model.UserBillingPrice{
			PricingVersion:             "xud112-v1",
			InputMicrounitsPerMillion:  1_000_000,
			OutputMicrounitsPerMillion: 2_000_000,
		},
	}
	if err := LLMCreate(info, context.Background()); err != nil {
		t.Fatalf("LLMCreate: %v", err)
	}
	if err := LLMUpdate(model.LLMInfo{
		Name:     "honey-chat",
		LLMPrice: model.LLMPrice{Input: 3},
	}, context.Background()); err != nil {
		t.Fatalf("LLMUpdate: %v", err)
	}
	updated, err := LLMInfoGet("honey-chat")
	if err != nil {
		t.Fatalf("LLMInfoGet: %v", err)
	}
	if updated.Input != 3 || updated.PricingVersion != "xud112-v1" || updated.OutputMicrounitsPerMillion != 2_000_000 {
		t.Fatalf("independent prices were not preserved: %+v", updated)
	}

	invalid := info
	invalid.Name = "invalid-price"
	invalid.PricingVersion = ""
	if err := LLMCreate(invalid, context.Background()); err == nil {
		t.Fatal("configured user billing price without a version must fail")
	}
}
