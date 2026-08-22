package relay

import (
	"context"
	"testing"

	"github.com/bestruirui/octopus/internal/billing"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/looplj/axonhub/llm"
)

type postchargeClient struct {
	admissions []billing.AdmissionRequest
	charges    []billing.ChargeRequest
}

func (client *postchargeClient) Admit(_ context.Context, request billing.AdmissionRequest) (billing.Admission, error) {
	client.admissions = append(client.admissions, request)
	return billing.Admission{ReceiptID: "receipt-image-job-1", Status: "ALLOWED"}, nil
}

func (client *postchargeClient) Charge(_ context.Context, request billing.ChargeRequest) (billing.Charge, error) {
	client.charges = append(client.charges, request)
	return billing.Charge{ReceiptID: request.ReceiptID, ChargeMicrounits: request.ChargeMicrounits}, nil
}

func TestImageFailoverChargesOnceOnlyAfterFinalSuccess(t *testing.T) {
	client := &postchargeClient{}
	run := &relayRun{
		metrics: &RelayMetrics{
			APIKeyID:        101,
			RequestModel:    "honey-image-v1",
			RouteType:       llm.RequestTypeImage,
			RouteFormat:     llm.APIFormatOpenAIImageGeneration,
			GenerationCount: 1,
			BillingUsage: billing.Usage{
				InputTokens:  95,
				OutputTokens: 4_354,
			},
		},
		billing: &billingState{
			client:         client,
			price:          billing.Price{Model: "honey-image-v1", Version: "image-v1", GenerationMicrounitsPerImage: 250},
			billingEventID: "image-job-1",
			admission:      billing.Admission{ReceiptID: "receipt-image-job-1", Status: "ALLOWED"},
			accountID:      "account-a",
		},
	}
	attempts := []dbmodel.ChannelAttempt{
		{ChannelID: 1, Status: dbmodel.AttemptFailed},
		{ChannelID: 2, Status: dbmodel.AttemptSuccess},
	}

	if err := run.finalizeBilling(context.Background(), attempts, false); err != nil {
		t.Fatalf("failed run finalization: %v", err)
	}
	if len(client.charges) != 0 {
		t.Fatalf("failed run charges=%d want=0", len(client.charges))
	}
	if err := run.finalizeBilling(context.Background(), attempts, true); err != nil {
		t.Fatalf("successful run finalization: %v", err)
	}
	if len(client.charges) != 1 {
		t.Fatalf("successful failover charges=%d want=1", len(client.charges))
	}
	charge := client.charges[0]
	if charge.ChargeKind != billing.ChargeKindImageGeneration || charge.GenerationCount != 1 || charge.ChargeMicrounits != 250 || charge.ProviderRef != "channel-2" {
		t.Fatalf("charge=%+v", charge)
	}
	if charge.Usage != (billing.Usage{}) {
		t.Fatalf("image charge included provider token usage: %+v", charge.Usage)
	}
}

func TestImageRoutesRequireBillingReceiptBeforeProvider(t *testing.T) {
	tests := []struct {
		name        string
		routeType   llm.RequestType
		routeFormat llm.APIFormat
		generation  bool
		want        bool
	}{
		{
			name:        "image route",
			routeType:   llm.RequestTypeImage,
			routeFormat: llm.APIFormatOpenAIImageGeneration,
			want:        true,
		},
		{
			name:        "durable generation",
			routeType:   llm.RequestTypeChat,
			routeFormat: llm.APIFormatOpenAIChatCompletion,
			generation:  true,
			want:        true,
		},
		{
			name:        "ordinary chat keeps legacy fail-open boundary",
			routeType:   llm.RequestTypeChat,
			routeFormat: llm.APIFormatOpenAIChatCompletion,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &relayRun{
				metrics: &RelayMetrics{
					RouteType:   tt.routeType,
					RouteFormat: tt.routeFormat,
				},
			}
			if tt.generation {
				run.generation = &honeyGenerationExecution{}
			}
			if got := run.requiresBillingReceipt(); got != tt.want {
				t.Fatalf("requiresBillingReceipt() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestProviderCannotOverrideRelayReceiptHeaders(t *testing.T) {
	for _, key := range []string{
		"X-Octopus-Request-ID",
		"x-octopus-request-id",
		"X-Octopus-Receipt-ID",
		"x-octopus-receipt-id",
		"X-OCTOPUS-RECEIPT-ID",
	} {
		if !isRelayOwnedResponseHeader(key) {
			t.Fatalf("%q was not classified as a relay-owned response header", key)
		}
	}
	if isRelayOwnedResponseHeader("X-Provider-Request-ID") {
		t.Fatal("unrelated Provider header was classified as relay-owned")
	}
}
