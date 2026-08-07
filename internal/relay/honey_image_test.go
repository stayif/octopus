package relay

import (
	"encoding/json"
	"testing"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

func TestHoneyImageRequestPolicyDefaultsToVerifiedBoundary(t *testing.T) {
	request := honeyImageRequest(`{"model":"honey-image-v1","prompt":"test prompt"}`)

	if err := applyHoneyImageRequestPolicy(llm.APIFormatOpenAIImageGeneration, request); err != nil {
		t.Fatalf("applyHoneyImageRequestPolicy() error = %v", err)
	}
	if request.Image.N == nil || *request.Image.N != 1 {
		t.Fatalf("n = %v, want 1", request.Image.N)
	}
	if request.Image.ResponseFormat != "url" {
		t.Fatalf("response_format = %q, want url", request.Image.ResponseFormat)
	}
	if request.Image.Size != "" || request.Image.Quality != "" {
		t.Fatalf("size/quality = %q/%q, want omitted", request.Image.Size, request.Image.Quality)
	}
}

func TestHoneyImageRequestPolicyAcceptsExplicitVerifiedBoundary(t *testing.T) {
	request := honeyImageRequest(`{"model":"honey-image-v1","prompt":"test prompt","n":1,"response_format":"url"}`)
	request.Image.N = pointerToInt64(1)
	request.Image.ResponseFormat = "url"

	if err := applyHoneyImageRequestPolicy(llm.APIFormatOpenAIImageGeneration, request); err != nil {
		t.Fatalf("applyHoneyImageRequestPolicy() error = %v", err)
	}
}

func TestHoneyImageRequestPolicyRejectsUnverifiedOptions(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		edit func(*llm.ImageRequest)
	}{
		{name: "size", raw: `{"model":"honey-image-v1","prompt":"test","size":"1024x1024"}`, edit: func(image *llm.ImageRequest) { image.Size = "1024x1024" }},
		{name: "quality", raw: `{"model":"honey-image-v1","prompt":"test","quality":"medium"}`, edit: func(image *llm.ImageRequest) { image.Quality = "medium" }},
		{name: "multiple images", raw: `{"model":"honey-image-v1","prompt":"test","n":2}`, edit: func(image *llm.ImageRequest) { image.N = pointerToInt64(2) }},
		{name: "base64", raw: `{"model":"honey-image-v1","prompt":"test","response_format":"b64_json"}`, edit: func(image *llm.ImageRequest) { image.ResponseFormat = "b64_json" }},
		{name: "unknown", raw: `{"model":"honey-image-v1","prompt":"test","mystery":true}`, edit: func(*llm.ImageRequest) {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := honeyImageRequest(tt.raw)
			tt.edit(request.Image)
			if err := applyHoneyImageRequestPolicy(llm.APIFormatOpenAIImageGeneration, request); err == nil {
				t.Fatal("applyHoneyImageRequestPolicy() error = nil, want rejection")
			}
		})
	}
}

func TestHoneyImageRequestPolicyDoesNotAffectOtherModels(t *testing.T) {
	request := &llm.Request{Model: "another-image-model", Image: &llm.ImageRequest{Size: "custom"}}
	if err := applyHoneyImageRequestPolicy(llm.APIFormatOpenAIImageGeneration, request); err != nil {
		t.Fatalf("applyHoneyImageRequestPolicy() error = %v", err)
	}
}

func TestRestoreHoneyImageResponseFormat(t *testing.T) {
	outbound := &httpclient.Request{
		APIFormat: llm.APIFormatOpenAIImageGeneration.String(),
		Body:      []byte(`{"model":"gpt-image-2","prompt":"test","size":"1024x1024","quality":"medium"}`),
	}
	if err := restoreHoneyImageResponseFormat(outbound, honeyImageModel); err != nil {
		t.Fatalf("restoreHoneyImageResponseFormat() error = %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(outbound.Body, &body); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if body["response_format"] != "url" || body["n"] != float64(1) {
		t.Fatalf("verified fields = %v/%v, want url/1", body["response_format"], body["n"])
	}
	if _, ok := body["size"]; ok {
		t.Fatal("size was forwarded")
	}
	if _, ok := body["quality"]; ok {
		t.Fatal("quality was forwarded")
	}
}

func TestValidateHoneyImageResponseFailsClosed(t *testing.T) {
	valid := []byte(`{"created":1,"data":[{"url":"https://example.com/image.png"}]}`)
	if err := validateHoneyImageResponse(honeyImageModel, valid); err != nil {
		t.Fatalf("validateHoneyImageResponse(valid) error = %v", err)
	}

	invalid := [][]byte{
		[]byte(`{"data":[]}`),
		[]byte(`{"data":[{"url":"https://example.com/a.png"},{"url":"https://example.com/b.png"}]}`),
		[]byte(`{"data":[{"b64_json":"abc"}]}`),
		[]byte(`{"data":[{"url":"http://example.com/image.png"}]}`),
		[]byte(`not-json`),
	}
	for _, body := range invalid {
		if err := validateHoneyImageResponse(honeyImageModel, body); err == nil {
			t.Fatalf("validateHoneyImageResponse(%q) error = nil, want rejection", body)
		}
	}
}

func honeyImageRequest(raw string) *llm.Request {
	return &llm.Request{
		Model:      honeyImageModel,
		Image:      &llm.ImageRequest{Prompt: "test prompt"},
		RawRequest: &httpclient.Request{Body: []byte(raw)},
	}
}
