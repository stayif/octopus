package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer"
	"github.com/looplj/axonhub/llm/transformer/openai"
)

const (
	luchikeyImageJobTransportMetadata = "octopus.luchikey_image_job_transport"
	luchikeyImageJobCreatePath        = "/api/relay/image-jobs/generations"
	luchikeyImageJobPollPath          = "/api/relay/image-jobs/"
	luchikeyImageJobRecoveryPath      = "/api/relay/image-jobs"
	luchikeyImageJobRatio             = "1:1"
	luchikeyImageJobQuality           = "standard"
	luchikeyImageJobCount             = 1
	luchikeyImageJobPollInterval      = 3 * time.Second
	luchikeyImageJobMaxWait           = 5 * time.Minute
)

// luchikeyImageJobsOutbound is a deliberately narrow adapter for Luchikey's
// documented asynchronous image Job API. Its ratio/quality/count fields are
// provider-private transport requirements, not honey-image-v1 capabilities.
type luchikeyImageJobsOutbound struct {
	transformer.Outbound
	baseURL      string
	apiKey       string
	clientJobID  string
	pollInterval time.Duration
	maxWait      time.Duration
}

func newLuchikeyImageJobsOutbound(baseURL, apiKey, clientJobID string) (transformer.Outbound, error) {
	normalizedBaseURL, err := normalizeLuchikeyImageJobBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, fmt.Errorf("luchikey image jobs API key is required")
	}
	if !validLuchikeyClientJobID(clientJobID) {
		return nil, fmt.Errorf("luchikey image jobs client job ID is invalid")
	}
	delegate, err := openai.NewOutboundTransformer(normalizedBaseURL, apiKey)
	if err != nil {
		return nil, fmt.Errorf("create OpenAI image response transformer: %w", err)
	}
	return &luchikeyImageJobsOutbound{
		Outbound:     delegate,
		baseURL:      normalizedBaseURL,
		apiKey:       apiKey,
		clientJobID:  clientJobID,
		pollInterval: luchikeyImageJobPollInterval,
		maxWait:      luchikeyImageJobMaxWait,
	}, nil
}

func (o *luchikeyImageJobsOutbound) APIFormat() llm.APIFormat {
	return llm.APIFormatOpenAIImageGeneration
}

func (o *luchikeyImageJobsOutbound) TransformRequest(_ context.Context, request *llm.Request) (*httpclient.Request, error) {
	if request == nil || request.RequestType != llm.RequestTypeImage || request.APIFormat != llm.APIFormatOpenAIImageGeneration || request.Image == nil {
		return nil, fmt.Errorf("%w: luchikey image jobs supports image generation only", transformer.ErrInvalidRequest)
	}
	image := request.Image
	if request.Model == "" || image.Prompt == "" || image.N == nil || *image.N != 1 || image.ResponseFormat != "url" {
		return nil, fmt.Errorf("%w: luchikey image jobs requires model, prompt, n=1, and response_format=url", transformer.ErrInvalidRequest)
	}
	if image.Size != "" || image.Quality != "" || len(image.Images) != 0 || len(image.Mask) != 0 || image.User != "" ||
		image.Background != "" || image.OutputFormat != "" || image.OutputCompression != nil || image.InputFidelity != "" ||
		image.Moderation != "" || image.PartialImages != nil || image.Style != "" {
		return nil, fmt.Errorf("%w: unsupported public option for luchikey image jobs", transformer.ErrInvalidRequest)
	}

	body, err := json.Marshal(luchikeyImageJobCreateRequest{
		Prompt:      image.Prompt,
		Model:       request.Model,
		Ratio:       luchikeyImageJobRatio,
		Quality:     luchikeyImageJobQuality,
		Count:       luchikeyImageJobCount,
		ClientJobID: o.clientJobID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal luchikey image job request: %w", err)
	}

	return &httpclient.Request{
		Method:      http.MethodPost,
		URL:         o.baseURL + luchikeyImageJobCreatePath,
		Headers:     http.Header{"Content-Type": []string{"application/json"}, "Accept": []string{"application/json"}},
		Body:        body,
		Auth:        &httpclient.AuthConfig{Type: httpclient.AuthTypeBearer, APIKey: o.apiKey},
		RequestType: llm.RequestTypeImage.String(),
		APIFormat:   llm.APIFormatOpenAIImageGeneration.String(),
		Metadata:    map[string]string{luchikeyImageJobTransportMetadata: "true"},
		TransformerMetadata: map[string]any{
			"model": request.Model,
		},
		SkipInboundQueryMerge: true,
	}, nil
}

func (o *luchikeyImageJobsOutbound) CustomizeExecutor(executor pipeline.Executor) pipeline.Executor {
	return &luchikeyImageJobsExecutor{
		Executor:     executor,
		baseURL:      o.baseURL,
		clientJobID:  o.clientJobID,
		pollInterval: o.pollInterval,
		maxWait:      o.maxWait,
	}
}

type luchikeyImageJobsExecutor struct {
	pipeline.Executor
	baseURL      string
	clientJobID  string
	pollInterval time.Duration
	maxWait      time.Duration
}

func (e *luchikeyImageJobsExecutor) Do(ctx context.Context, request *httpclient.Request) (*httpclient.Response, error) {
	if request == nil || e.Executor == nil {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, http.MethodPost, e.baseURL+luchikeyImageJobCreatePath)
	}
	jobCtx, cancel := context.WithTimeout(ctx, e.maxWait)
	defer cancel()

	createResponse, err := e.Executor.Do(jobCtx, request)
	var job luchikeyImageJobData
	if err == nil {
		job, err = parseLuchikeyImageJobResponse(createResponse)
		if err != nil {
			return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, request.Method, request.URL)
		}
	} else {
		// The provider documents client_job_id recovery for ambiguous create
		// failures. Query once and continue only an already-created Job; never
		// repeat the POST, even when the create response was a timeout or 5xx.
		recovered, found := e.recoverCreatedJob(jobCtx, request, err)
		if !found {
			return nil, sanitizeLuchikeyImageJobExecutorError(jobCtx, err, request.Method, request.URL)
		}
		job = recovered
	}
	if job.ClientJobID != "" && job.ClientJobID != e.clientJobID {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, request.Method, request.URL)
	}
	jobID, err := job.providerJobID()
	if err != nil {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, request.Method, request.URL)
	}
	if job.Status == luchikeyImageJobStatusSucceeded {
		return e.openAIImageResponse(request, job)
	}
	if job.Status == luchikeyImageJobStatusFailed {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, request.Method, request.URL)
	}
	if !job.pending() || jobID == "" {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, request.Method, request.URL)
	}

	pollURL := e.baseURL + luchikeyImageJobPollPath + url.PathEscape(jobID)
	for {
		if err := waitForLuchikeyImageJobPoll(jobCtx, e.pollInterval); err != nil {
			return nil, sanitizeLuchikeyImageJobExecutorError(jobCtx, err, http.MethodGet, pollURL)
		}
		pollRequest := &httpclient.Request{
			Method:                http.MethodGet,
			URL:                   pollURL,
			Headers:               request.Headers.Clone(),
			RequestType:           llm.RequestTypeImage.String(),
			APIFormat:             llm.APIFormatOpenAIImageGeneration.String(),
			TransformerMetadata:   request.TransformerMetadata,
			SkipInboundQueryMerge: true,
		}
		pollResponse, pollErr := e.Executor.Do(jobCtx, pollRequest)
		if pollErr != nil {
			if retryLuchikeyImageJobPoll(jobCtx, pollErr) {
				continue
			}
			return nil, sanitizeLuchikeyImageJobExecutorError(jobCtx, pollErr, pollRequest.Method, pollRequest.URL)
		}
		job, err = parseLuchikeyImageJobResponse(pollResponse)
		if err != nil || !job.matchesProviderJobID(jobID) {
			return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, pollRequest.Method, pollRequest.URL)
		}
		switch job.Status {
		case luchikeyImageJobStatusQueued, luchikeyImageJobStatusRunning:
			continue
		case luchikeyImageJobStatusSucceeded:
			return e.openAIImageResponse(request, job)
		case luchikeyImageJobStatusFailed:
			return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, pollRequest.Method, pollRequest.URL)
		default:
			return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, pollRequest.Method, pollRequest.URL)
		}
	}
}

func (e *luchikeyImageJobsExecutor) recoverCreatedJob(ctx context.Context, createRequest *httpclient.Request, createErr error) (luchikeyImageJobData, bool) {
	if ctx.Err() != nil || createRequest == nil || !ambiguousLuchikeyImageJobCreateError(createErr) {
		return luchikeyImageJobData{}, false
	}
	recoveryURL := e.baseURL + luchikeyImageJobRecoveryPath + "?" + url.Values{"client_job_id": []string{e.clientJobID}}.Encode()
	recoveryRequest := &httpclient.Request{
		Method:                http.MethodGet,
		URL:                   recoveryURL,
		Headers:               createRequest.Headers.Clone(),
		RequestType:           llm.RequestTypeImage.String(),
		APIFormat:             llm.APIFormatOpenAIImageGeneration.String(),
		TransformerMetadata:   createRequest.TransformerMetadata,
		SkipInboundQueryMerge: true,
	}
	recoveryResponse, err := e.Executor.Do(ctx, recoveryRequest)
	if err != nil {
		return luchikeyImageJobData{}, false
	}
	job, err := parseLuchikeyImageJobRecoveryResponse(recoveryResponse, e.clientJobID)
	return job, err == nil
}

func (e *luchikeyImageJobsExecutor) DoStream(context.Context, *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	return nil, fmt.Errorf("luchikey image jobs does not support streaming")
}

func (e *luchikeyImageJobsExecutor) openAIImageResponse(request *httpclient.Request, job luchikeyImageJobData) (*httpclient.Response, error) {
	imageURL, err := job.singleHTTPSImageURL(e.baseURL)
	if err != nil {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, http.MethodGet, e.baseURL+luchikeyImageJobPollPath)
	}
	body, err := json.Marshal(struct {
		Created int64 `json:"created"`
		Data    []struct {
			URL string `json:"url"`
		} `json:"data"`
	}{
		Created: time.Now().Unix(),
		Data: []struct {
			URL string `json:"url"`
		}{{URL: imageURL}},
	})
	if err != nil {
		return nil, safeLuchikeyImageJobHTTPError(http.StatusBadGateway, http.MethodGet, e.baseURL+luchikeyImageJobPollPath)
	}
	return &httpclient.Response{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       body,
		Request:    request,
	}, nil
}

type luchikeyImageJobCreateRequest struct {
	Prompt      string `json:"prompt"`
	Model       string `json:"model"`
	Ratio       string `json:"ratio"`
	Quality     string `json:"quality"`
	Count       int    `json:"count"`
	ClientJobID string `json:"client_job_id"`
}

type luchikeyImageJobEnvelope struct {
	OK   bool                 `json:"ok"`
	Data luchikeyImageJobData `json:"data"`
}

type luchikeyImageJobRecoveryEnvelope struct {
	OK   bool                         `json:"ok"`
	Data luchikeyImageJobRecoveryData `json:"data"`
}

type luchikeyImageJobRecoveryData struct {
	Jobs  []luchikeyImageJobData `json:"jobs"`
	Data  []luchikeyImageJobData `json:"data"`
	Items []luchikeyImageJobData `json:"items"`
}

type luchikeyImageJobData struct {
	ID          string                  `json:"id"`
	JobID       string                  `json:"job_id"`
	ClientJobID string                  `json:"client_job_id"`
	Status      string                  `json:"status"`
	Result      *luchikeyImageJobResult `json:"result"`
}

type luchikeyImageJobResult struct {
	Data   []luchikeyImageJobImage `json:"data"`
	Images []json.RawMessage       `json:"images"`
}

type luchikeyImageJobImage struct {
	URL     string `json:"url"`
	B64JSON string `json:"b64_json"`
}

const (
	luchikeyImageJobStatusQueued    = "queued"
	luchikeyImageJobStatusRunning   = "running"
	luchikeyImageJobStatusSucceeded = "succeeded"
	luchikeyImageJobStatusFailed    = "failed"
)

func parseLuchikeyImageJobResponse(response *httpclient.Response) (luchikeyImageJobData, error) {
	if response == nil || len(response.Body) == 0 {
		return luchikeyImageJobData{}, fmt.Errorf("invalid luchikey image job response")
	}
	var envelope luchikeyImageJobEnvelope
	if err := json.Unmarshal(response.Body, &envelope); err != nil || !envelope.OK {
		return luchikeyImageJobData{}, fmt.Errorf("invalid luchikey image job response")
	}
	return envelope.Data, nil
}

func parseLuchikeyImageJobRecoveryResponse(response *httpclient.Response, clientJobID string) (luchikeyImageJobData, error) {
	if response == nil || len(response.Body) == 0 {
		return luchikeyImageJobData{}, fmt.Errorf("invalid luchikey image job recovery response")
	}
	var envelope luchikeyImageJobRecoveryEnvelope
	if err := json.Unmarshal(response.Body, &envelope); err != nil || !envelope.OK {
		return luchikeyImageJobData{}, fmt.Errorf("invalid luchikey image job recovery response")
	}
	jobs := envelope.Data.Jobs
	if jobs == nil {
		jobs = envelope.Data.Data
	}
	if jobs == nil {
		jobs = envelope.Data.Items
	}
	if len(jobs) != 1 || jobs[0].ClientJobID != clientJobID {
		return luchikeyImageJobData{}, fmt.Errorf("luchikey image job recovery did not return exactly one matching job")
	}
	return jobs[0], nil
}

func (job luchikeyImageJobData) providerJobID() (string, error) {
	if job.ID != "" && job.JobID != "" && job.ID != job.JobID {
		return "", fmt.Errorf("conflicting luchikey image job IDs")
	}
	if job.ID != "" {
		return job.ID, nil
	}
	return job.JobID, nil
}

func (job luchikeyImageJobData) matchesProviderJobID(expected string) bool {
	actual, err := job.providerJobID()
	return err == nil && (actual == "" || actual == expected)
}

func (job luchikeyImageJobData) pending() bool {
	return job.Status == luchikeyImageJobStatusQueued || job.Status == luchikeyImageJobStatusRunning
}

func (job luchikeyImageJobData) singleHTTPSImageURL(baseURL string) (string, error) {
	if job.Result == nil {
		return "", fmt.Errorf("missing luchikey image job result")
	}
	candidates := append([]luchikeyImageJobImage(nil), job.Result.Data...)
	for _, rawImage := range job.Result.Images {
		var imageURL string
		if err := json.Unmarshal(rawImage, &imageURL); err == nil {
			candidates = append(candidates, luchikeyImageJobImage{URL: imageURL})
			continue
		}
		var image luchikeyImageJobImage
		if err := json.Unmarshal(rawImage, &image); err != nil {
			return "", fmt.Errorf("invalid luchikey image job result")
		}
		candidates = append(candidates, image)
	}
	if len(candidates) != 1 || candidates[0].URL == "" || candidates[0].B64JSON != "" {
		return "", fmt.Errorf("luchikey image job did not return exactly one URL")
	}
	parsedURL, err := url.Parse(candidates[0].URL)
	if err != nil {
		return "", fmt.Errorf("invalid luchikey image job URL")
	}
	if !parsedURL.IsAbs() {
		parsedBaseURL, parseErr := url.Parse(baseURL + "/")
		if parseErr != nil {
			return "", fmt.Errorf("invalid luchikey image job base URL")
		}
		parsedURL = parsedBaseURL.ResolveReference(parsedURL)
	}
	if parsedURL.Scheme != "https" || parsedURL.Host == "" {
		return "", fmt.Errorf("luchikey image job URL is not HTTPS")
	}
	return parsedURL.String(), nil
}

func enforceLuchikeyImageJobPrivateTransport(request *httpclient.Request) error {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return fmt.Errorf("invalid luchikey image job transport")
	}
	allowed := map[string]struct{}{
		"prompt": {}, "model": {}, "ratio": {}, "quality": {}, "count": {}, "client_job_id": {},
	}
	for field := range body {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("invalid luchikey image job transport")
		}
	}
	var transport luchikeyImageJobCreateRequest
	if err := json.Unmarshal(request.Body, &transport); err != nil || transport.Prompt == "" || transport.Model == "" ||
		transport.Ratio != luchikeyImageJobRatio || transport.Quality != luchikeyImageJobQuality ||
		transport.Count != luchikeyImageJobCount || !validLuchikeyClientJobID(transport.ClientJobID) {
		return fmt.Errorf("invalid luchikey image job transport")
	}
	return nil
}

func normalizeLuchikeyImageJobBaseURL(baseURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("luchikey image jobs requires an HTTPS origin base URL")
	}
	parsed.Path = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func validLuchikeyClientJobID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		switch char {
		case '_', '.', ':', '-':
			continue
		default:
			return false
		}
	}
	return true
}

func waitForLuchikeyImageJobPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryLuchikeyImageJobPoll(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var httpErr *httpclient.Error
	if !errors.As(err, &httpErr) {
		return true
	}
	return httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= http.StatusInternalServerError
}

func ambiguousLuchikeyImageJobCreateError(err error) bool {
	var httpErr *httpclient.Error
	if !errors.As(err, &httpErr) {
		return true
	}
	return httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= http.StatusInternalServerError
}

func sanitizeLuchikeyImageJobExecutorError(ctx context.Context, err error, method, requestURL string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return safeLuchikeyImageJobHTTPError(http.StatusGatewayTimeout, method, requestURL)
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	var httpErr *httpclient.Error
	if errors.As(err, &httpErr) {
		statusCode := httpErr.StatusCode
		if statusCode < 400 || statusCode > 599 {
			statusCode = http.StatusBadGateway
		}
		return safeLuchikeyImageJobHTTPError(statusCode, method, requestURL)
	}
	return safeLuchikeyImageJobHTTPError(http.StatusBadGateway, method, requestURL)
}

func safeLuchikeyImageJobHTTPError(statusCode int, method, requestURL string) *httpclient.Error {
	return &httpclient.Error{
		Method:     method,
		URL:        requestURL,
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Body:       []byte(`{"error":{"message":"image provider job failed","type":"api_error"}}`),
	}
}
