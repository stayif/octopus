package relay

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/looplj/axonhub/llm"
)

func TestImageRelayMetricsAreMetadataOnlyForAllResponseShapes(t *testing.T) {
	generationCount := int64(2)
	request := &llm.Request{
		Model:       "honey-image-v1",
		RequestType: llm.RequestTypeImage,
		APIFormat:   llm.APIFormatOpenAIImageGeneration,
		Image: &llm.ImageRequest{
			Prompt: "synthetic-prompt-canary",
			N:      &generationCount,
		},
	}

	tests := []struct {
		name     string
		response []byte
		canary   string
	}{
		{
			name:     "json provider payload",
			response: []byte(`{"provider":{"private":"json-provider-canary"},"data":[{"url":"https://assets.invalid/private.png?signature=json-signed-canary"}]}`),
			canary:   "json-provider-canary",
		},
		{
			name:     "url response",
			response: []byte("https://assets.invalid/private.png?signature=url-signed-canary"),
			canary:   "url-signed-canary",
		},
		{
			name:     "base64 response",
			response: []byte(`{"b64_json":"YmFzZTY0LWltYWdlLWNhbmFyeQ=="}`),
			canary:   "YmFzZTY0LWltYWdlLWNhbmFyeQ==",
		},
		{
			name:     "binary response",
			response: append([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, []byte("binary-image-canary")...),
			canary:   "binary-image-canary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := newRelayMetrics(7, request, request.APIFormat, time.Unix(1_700_000_000, 0))
			metrics.RequestID = "oct-1250001"
			metrics.ReceiptID = "receipt-1250001"
			metrics.ProfileID = "honey-default"
			metrics.ProfileVersion = "v1"
			metrics.ResultCode = 200

			if metrics.InternalRequest != nil {
				t.Fatal("image metrics must not retain the parsed request")
			}
			metrics.captureResponse(tt.response)
			if len(metrics.InternalResponse) != 0 {
				t.Fatal("image metrics must not retain the provider response")
			}

			attempts := []model.ChannelAttempt{{
				ChannelID:   9,
				ChannelName: "private-provider",
				ModelName:   "private-provider-model",
				Status:      model.AttemptFailed,
				Msg:         "provider-error-canary " + tt.canary,
			}}
			relayLog := metrics.buildRelayLog(
				errors.New("provider-error-canary "+tt.canary),
				250*time.Millisecond,
				attempts,
				9,
				"private-provider",
			)

			encoded, err := json.Marshal(relayLog)
			if err != nil {
				t.Fatalf("marshal relay log: %v", err)
			}
			serialized := string(encoded)
			for _, forbidden := range []string{
				"synthetic-prompt-canary",
				tt.canary,
				"provider-error-canary",
				"private-provider",
				"private-provider-model",
			} {
				if strings.Contains(serialized, forbidden) {
					t.Fatalf("image relay log leaked %q: %s", forbidden, serialized)
				}
			}

			if relayLog.RequestModelName != "honey-image-v1" || relayLog.GenerationCount != 2 {
				t.Fatalf("safe image metadata missing: %+v", relayLog)
			}
			if relayLog.RequestID != "oct-1250001" || relayLog.ReceiptID != "receipt-1250001" {
				t.Fatalf("audit identifiers missing: %+v", relayLog)
			}
			if relayLog.RouteType != string(llm.RequestTypeImage) || relayLog.RouteFormat != string(llm.APIFormatOpenAIImageGeneration) {
				t.Fatalf("structured route metadata missing: %+v", relayLog)
			}
			if relayLog.RequestContent != "" || relayLog.ResponseContent != "" || relayLog.Error != "" || len(relayLog.Attempts) != 0 {
				t.Fatalf("image content fields must be empty: %+v", relayLog)
			}
			if relayLog.TotalAttempts != 1 || relayLog.UseTime != 250 || relayLog.ResultCode != 200 {
				t.Fatalf("safe result metadata missing: %+v", relayLog)
			}
		})
	}
}

func TestImageRouteDetectionUsesStructuredRouteInsteadOfContentKeywords(t *testing.T) {
	imageRequest := &llm.Request{
		Model:       "honey-image-v1",
		RequestType: llm.RequestTypeImage,
		APIFormat:   llm.APIFormatOpenAIImageGeneration,
		Image:       &llm.ImageRequest{Prompt: "plain synthetic canary"},
	}
	imageMetrics := newRelayMetrics(1, imageRequest, imageRequest.APIFormat, time.Now())
	if !imageMetrics.isImageRoute() {
		t.Fatal("structured image request was not classified as image")
	}
	for _, routeFormat := range []llm.APIFormat{
		llm.APIFormatOpenAIImageGeneration,
		llm.APIFormatOpenAIImageEdit,
		llm.APIFormatOpenAIImageVariation,
	} {
		metrics := newRelayMetrics(1, &llm.Request{
			Model:       "honey-image-v1",
			RequestType: llm.RequestTypeChat,
		}, routeFormat, time.Now())
		if !metrics.isImageRoute() || metrics.InternalRequest != nil {
			t.Fatalf("structured route format %q was not classified as image", routeFormat)
		}
	}

	chatRequest := &llm.Request{
		Model:       "honey-chat",
		RequestType: llm.RequestTypeChat,
		APIFormat:   llm.APIFormatOpenAIChatCompletion,
		ExtraBody:   json.RawMessage(`{"note":"image signed_url base64 binary prompt"}`),
	}
	chatMetrics := newRelayMetrics(1, chatRequest, chatRequest.APIFormat, time.Now())
	if chatMetrics.isImageRoute() {
		t.Fatal("non-image request was classified from content keywords")
	}
	chatMetrics.captureResponse([]byte(`{"text":"non-image-response-canary"}`))
	relayLog := chatMetrics.buildRelayLog(nil, time.Millisecond, nil, 3, "chat-channel")
	if !strings.Contains(relayLog.RequestContent, "signed_url") {
		t.Fatalf("non-image request logging regressed: %s", relayLog.RequestContent)
	}
	if !strings.Contains(relayLog.ResponseContent, "non-image-response-canary") {
		t.Fatalf("non-image response logging regressed: %s", relayLog.ResponseContent)
	}
}
