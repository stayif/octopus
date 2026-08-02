package billing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCalculateChargeUsesExternalModelPriceAndTokenBreakdown(t *testing.T) {
	price := Price{
		Model:                          "honey-chat",
		Version:                        "test-v1",
		InputMicrounitsPerMillion:      1_000_000,
		OutputMicrounitsPerMillion:     2_000_000,
		CacheReadMicrounitsPerMillion:  500_000,
		CacheWriteMicrounitsPerMillion: 1_500_000,
	}
	usage := Usage{
		InputTokens:      100,
		OutputTokens:     50,
		CacheReadTokens:  20,
		CacheWriteTokens: 10,
	}

	charge, err := CalculateCharge(price, usage)
	if err != nil {
		t.Fatalf("CalculateCharge: %v", err)
	}
	// Non-cached input: 70, output: 100, cache read: 10, cache write: 15.
	if charge != 195 {
		t.Fatalf("charge=%d want=195", charge)
	}

	// Provider identity is deliberately absent from the calculation input. The same
	// public model price therefore cannot vary when Octopus selects another channel.
	secondCharge, err := CalculateCharge(price, usage)
	if err != nil || secondCharge != charge {
		t.Fatalf("provider-independent charge=%d err=%v", secondCharge, err)
	}
}

func TestCalculateChargeRejectsUnverifiableUsage(t *testing.T) {
	price := Price{
		Model:                      "honey-chat",
		Version:                    "test-v1",
		InputMicrounitsPerMillion:  1,
		OutputMicrounitsPerMillion: 1,
	}

	if _, err := CalculateCharge(price, Usage{}); err == nil {
		t.Fatal("zero usage must fail closed")
	}
	if _, err := CalculateCharge(price, Usage{
		InputTokens:     10,
		CacheReadTokens: 11,
	}); err == nil {
		t.Fatal("cache tokens greater than input must fail closed")
	}
}

func TestMaximumChargeRequiresOutputBoundAndCoversRequestBytes(t *testing.T) {
	price := Price{
		Model:                          "honey-chat",
		Version:                        "test-v1",
		InputMicrounitsPerMillion:      1_000_000,
		OutputMicrounitsPerMillion:     2_000_000,
		CacheReadMicrounitsPerMillion:  3_000_000,
		CacheWriteMicrounitsPerMillion: 4_000_000,
	}
	maximum, err := MaximumCharge(price, 100, 50)
	if err != nil {
		t.Fatalf("MaximumCharge: %v", err)
	}
	// Every request byte is a conservative upper token bound at the highest input
	// class rate, plus the explicit output-token ceiling.
	if maximum != 500 {
		t.Fatalf("maximum=%d want=500", maximum)
	}
	if _, err := MaximumCharge(price, 100, 0); err == nil {
		t.Fatal("missing output bound must fail closed")
	}
}

func TestHTTPClientReservesSettlesAndCancelsWithServiceCredential(t *testing.T) {
	serviceToken := "xud112-service-token-that-is-at-least-32-bytes"
	var reservationID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+serviceToken {
			t.Fatalf("Authorization=%q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/internal/v1/billing/reservations":
			var request ReserveRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode reserve: %v", err)
			}
			if request.AccountID != "account-a" || request.APIKeyID != 101 || request.MaxChargeMicrounits != 800 {
				t.Fatalf("reserve=%+v", request)
			}
			reservationID = "res-test"
			json.NewEncoder(w).Encode(Reservation{
				ReservationID:      reservationID,
				ReservedMicrounits: 800,
				Status:             "RESERVED",
			})
		case "/internal/v1/billing/settlements":
			var request SettlementRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode settlement: %v", err)
			}
			if request.ReservationID != reservationID || request.ExternalModel != "honey-chat" || request.ChargeMicrounits != 600 {
				t.Fatalf("settlement=%+v", request)
			}
			json.NewEncoder(w).Encode(Settlement{
				ReceiptID:              request.ReceiptID,
				ChargeMicrounits:       request.ChargeMicrounits,
				BalanceAfterMicrounits: 400,
			})
		case "/internal/v1/billing/cancellations":
			json.NewEncoder(w).Encode(Reservation{
				ReservationID:      reservationID,
				ReservedMicrounits: 800,
				Status:             "CANCELED",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, serviceToken, server.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	ctx := context.Background()
	reservation, err := client.Reserve(ctx, ReserveRequest{
		AccountID:           "account-a",
		APIKeyID:            101,
		RequestID:           "request-1",
		MaxChargeMicrounits: 800,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	settlement, err := client.Settle(ctx, SettlementRequest{
		ReservationID:    reservation.ReservationID,
		ReceiptID:        "receipt-1",
		ExternalModel:    "honey-chat",
		PricingVersion:   "test-v1",
		Usage:            Usage{InputTokens: 100, OutputTokens: 50, CacheReadTokens: 20},
		ChargeMicrounits: 600,
		ProviderRef:      "provider-a",
	})
	if err != nil || settlement.BalanceAfterMicrounits != 400 {
		t.Fatalf("Settle=%+v err=%v", settlement, err)
	}
	if _, err := client.Cancel(ctx, reservation.ReservationID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}
