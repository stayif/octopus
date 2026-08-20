package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bestruirui/octopus/internal/conf"
)

func TestBillingServiceTokenFileIsSingleSecretAndModeProtected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "billing-service-token")
	if err := os.WriteFile(path, []byte("xud162-billing-service-token-at-least-32-bytes\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	token, err := billingServiceToken(conf.Billing{ServiceTokenFile: path})
	if err != nil {
		t.Fatalf("billingServiceToken: %v", err)
	}
	if token != "xud162-billing-service-token-at-least-32-bytes" {
		t.Fatal("billing service token file was not normalized")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod token: %v", err)
	}
	if _, err := billingServiceToken(conf.Billing{ServiceTokenFile: path}); err == nil {
		t.Fatal("group-readable billing service token file must fail closed")
	}
	if _, err := billingServiceToken(conf.Billing{
		ServiceToken:     "direct-token",
		ServiceTokenFile: path,
	}); err == nil {
		t.Fatal("ambiguous direct and file billing credentials must fail closed")
	}
}
