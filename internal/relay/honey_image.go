package relay

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer"
)

const honeyImageModel = "honey-image-v1"

var honeyImageRequestFields = map[string]struct{}{
	"model":           {},
	"n":               {},
	"prompt":          {},
	"response_format": {},
}

// applyHoneyImageRequestPolicy exposes only the provider behavior that XUD-126
// verified on both real OpenAI-compatible endpoints. Size and quality are
// deliberately absent: both providers accepted 1024x1024/medium but returned a
// 1254x1254 image, so advertising either option would be a silent downgrade.
func applyHoneyImageRequestPolicy(inboundType llm.APIFormat, request *llm.Request) error {
	if inboundType != llm.APIFormatOpenAIImageGeneration || request == nil || request.Model != honeyImageModel {
		return nil
	}
	if request.Image == nil || request.RawRequest == nil || len(request.RawRequest.Body) == 0 {
		return invalidHoneyImageRequest()
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(request.RawRequest.Body, &fields); err != nil {
		return invalidHoneyImageRequest()
	}
	for field := range fields {
		if _, ok := honeyImageRequestFields[field]; !ok {
			return invalidHoneyImageRequest()
		}
	}
	if request.Image.Prompt == "" {
		return invalidHoneyImageRequest()
	}

	if request.Image.N == nil {
		request.Image.N = pointerToInt64(1)
	} else if *request.Image.N != 1 {
		return invalidHoneyImageRequest()
	}
	if request.Image.ResponseFormat == "" {
		request.Image.ResponseFormat = "url"
	} else if request.Image.ResponseFormat != "url" {
		return invalidHoneyImageRequest()
	}
	return nil
}

func invalidHoneyImageRequest() error {
	return fmt.Errorf("%w: honey-image-v1 supports only n=1 and response_format=url; size, quality, and other image options are unavailable", transformer.ErrInvalidRequest)
}

func pointerToInt64(value int64) *int64 {
	return &value
}

// restoreHoneyImageResponseFormat compensates for AxonHub intentionally
// omitting response_format for GPT Image models. The two configured compatible
// providers were both verified with response_format=url.
func restoreHoneyImageResponseFormat(outbound *httpclient.Request, requestModel string) error {
	if outbound == nil || requestModel != honeyImageModel || outbound.APIFormat != llm.APIFormatOpenAIImageGeneration.String() {
		return nil
	}
	var body map[string]any
	if err := json.Unmarshal(outbound.Body, &body); err != nil {
		return fmt.Errorf("restore honey image response format: %w", err)
	}
	body["response_format"] = "url"
	body["n"] = 1
	delete(body, "size")
	delete(body, "quality")
	modified, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("restore honey image response format: %w", err)
	}
	outbound.Body = modified
	return nil
}

// validateHoneyImageResponse runs after provider transformation but before any
// provider response is written to the client. A mismatch is therefore a closed
// 502 and cannot silently become base64 or multiple images.
func validateHoneyImageResponse(requestModel string, body []byte) error {
	if requestModel != honeyImageModel {
		return nil
	}
	var response struct {
		Data []struct {
			B64JSON string `json:"b64_json"`
			URL     string `json:"url"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil || len(response.Data) != 1 {
		return fmt.Errorf("invalid honey image provider response")
	}
	item := response.Data[0]
	parsed, err := url.Parse(item.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || item.B64JSON != "" {
		return fmt.Errorf("invalid honey image provider response")
	}
	return nil
}
