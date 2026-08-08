package billing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCalculateGenerationChargeUsesOnlyHoneyFixedPrice(t *testing.T) {
	price := Price{
		Model:                        "honey-image-v1",
		Version:                      "image-v1",
		GenerationMicrounitsPerImage: 250,
	}

	charge, err := CalculateGenerationCharge(price, 1)
	if err != nil {
		t.Fatalf("CalculateGenerationCharge: %v", err)
	}
	if charge != 250 {
		t.Fatalf("charge=%d want=250", charge)
	}
	if _, err := CalculateCharge(price, Usage{InputTokens: 1}); err == nil {
		t.Fatal("image price must not masquerade as token billing")
	}
	if _, err := CalculateGenerationCharge(price, 2); err == nil {
		t.Fatal("first release must reject generation count other than one")
	}
}

func TestHTTPClientPreservesAdmissionRejectionStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "insufficient balance", http.StatusPaymentRequired)
	}))
	defer server.Close()
	client, err := NewHTTPClient(server.URL, "xud128-service-token-that-is-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = client.Admit(context.Background(), AdmissionRequest{
		AccountID:      "account-a",
		APIKeyID:       101,
		BillingEventID: "chat-turn-zero-balance",
	})
	if err == nil {
		t.Fatal("zero-balance admission must fail")
	}
	status, ok := StatusCode(err)
	if !ok || status != http.StatusPaymentRequired {
		t.Fatalf("status=(%d,%t) err=%v", status, ok, err)
	}
	var statusError *HTTPStatusError
	if !errors.As(err, &statusError) {
		t.Fatalf("error does not preserve HTTP status: %v", err)
	}
}

func TestHTTPClientAdmitsAndReplaysOnePostchargeReceipt(t *testing.T) {
	var admissions int
	var charges int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer xud128-service-token-that-is-at-least-32-bytes" {
			t.Fatal("missing service authorization")
		}
		switch r.URL.Path {
		case "/internal/v1/billing/admissions":
			admissions++
			var request AdmissionRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode admission: %v", err)
			}
			if request.AccountID != "account-a" || request.APIKeyID != 101 || request.BillingEventID != "chat-turn-1" {
				t.Fatalf("admission=%+v", request)
			}
			json.NewEncoder(w).Encode(Admission{ReceiptID: "receipt-stable", Status: "ALLOWED"})
		case "/internal/v1/billing/charges":
			charges++
			var request ChargeRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode charge: %v", err)
			}
			if request.BillingEventID != "chat-turn-1" || request.ReceiptID != "receipt-stable" || request.ChargeKind != ChargeKindChatTokens {
				t.Fatalf("charge=%+v", request)
			}
			json.NewEncoder(w).Encode(Charge{
				ReceiptID:              request.ReceiptID,
				ChargeMicrounits:       request.ChargeMicrounits,
				BalanceAfterMicrounits: 400,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, "xud128-service-token-that-is-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	admission, err := client.Admit(context.Background(), AdmissionRequest{
		AccountID:      "account-a",
		APIKeyID:       101,
		BillingEventID: "chat-turn-1",
	})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	request := ChargeRequest{
		AccountID:        "account-a",
		APIKeyID:         101,
		BillingEventID:   "chat-turn-1",
		ReceiptID:        admission.ReceiptID,
		ExternalModel:    "honey-chat",
		PricingVersion:   "chat-v1",
		ChargeKind:       ChargeKindChatTokens,
		Usage:            Usage{InputTokens: 100, OutputTokens: 50},
		ChargeMicrounits: 600,
		ProviderRef:      "channel-1",
	}
	first, err := client.Charge(context.Background(), request)
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	replay, err := client.Charge(context.Background(), request)
	if err != nil {
		t.Fatalf("replay Charge: %v", err)
	}
	if first != replay || first.ReceiptID != admission.ReceiptID {
		t.Fatalf("first=%+v replay=%+v admission=%+v", first, replay, admission)
	}
	if admissions != 1 || charges != 2 {
		t.Fatalf("admissions=%d charges=%d", admissions, charges)
	}
}
