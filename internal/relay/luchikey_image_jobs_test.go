package relay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

func TestLuchikeyImageJobsCreatesOncePollsSameJobAndNormalizesSuccess(t *testing.T) {
	const (
		baseURL     = "https://image.luchikey.test"
		clientJobID = "oct-126-success"
		jobID       = "relay_success"
		resultPath  = "/api/relay/image-jobs/relay_success/files/output-1.png?token=signed-canary"
	)
	recorder := &recordingImageJobExecutor{do: func(call int, request *httpclient.Request) (*httpclient.Response, error) {
		switch call {
		case 0:
			return imageJobResponse(http.StatusAccepted, `{"ok":true,"data":{"id":"relay_success","job_id":"relay_success","client_job_id":"oct-126-success","status":"queued"}}`), nil
		case 1:
			return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"id":"relay_success","status":"running"}}`), nil
		case 2:
			return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"job_id":"relay_success","status":"succeeded","requested_size":"1024x1024","actual_size":"1254x1254","result":{"data":[{"url":"/api/relay/image-jobs/relay_success/files/output-1.png?token=signed-canary"}]}}}`), nil
		default:
			t.Fatalf("unexpected provider request %d: %s %s", call, request.Method, request.URL)
			return nil, errors.New("unexpected provider request")
		}
	}}

	outbound, createRequest := testLuchikeyImageJobsOutbound(t, baseURL, clientJobID)
	response, err := outbound.CustomizeExecutor(recorder).Do(t.Context(), createRequest)
	if err != nil {
		t.Fatalf("async image job execution failed: %v", err)
	}
	if len(recorder.calls) != 3 {
		t.Fatalf("provider calls = %d, want one create and two polls", len(recorder.calls))
	}
	postCount := 0
	for index, call := range recorder.calls {
		switch call.Method {
		case http.MethodPost:
			postCount++
			if index != 0 || call.URL != baseURL+luchikeyImageJobCreatePath {
				t.Fatalf("create request = %s %s", call.Method, call.URL)
			}
			assertLuchikeyPrivateCreateBody(t, call.Body, clientJobID)
		case http.MethodGet:
			if call.URL != baseURL+luchikeyImageJobPollPath+jobID {
				t.Fatalf("poll URL = %s, want same job %s", call.URL, jobID)
			}
		default:
			t.Fatalf("unexpected method %s", call.Method)
		}
	}
	if postCount != 1 {
		t.Fatalf("create requests = %d, want exactly one", postCount)
	}

	llmResponse, err := outbound.TransformResponse(t.Context(), response)
	if err != nil {
		t.Fatalf("normalize OpenAI image response: %v", err)
	}
	wantURL := baseURL + resultPath
	if llmResponse.Image == nil || len(llmResponse.Image.Data) != 1 || llmResponse.Image.Data[0].URL != wantURL || llmResponse.Image.Data[0].B64JSON != "" {
		t.Fatalf("normalized image response shape is invalid")
	}
}

func TestLuchikeyImageJobsFailedJobDoesNotRepeatCreateOrLeakProviderError(t *testing.T) {
	recorder := &recordingImageJobExecutor{do: func(call int, _ *httpclient.Request) (*httpclient.Response, error) {
		if call == 0 {
			return imageJobResponse(http.StatusAccepted, `{"ok":true,"data":{"id":"relay_failed","status":"queued"}}`), nil
		}
		return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"id":"relay_failed","status":"failed","error":{"message":"provider-private-error-canary"}}}`), nil
	}}
	outbound, createRequest := testLuchikeyImageJobsOutbound(t, "https://image.luchikey.test", "oct-126-failed")
	_, err := outbound.CustomizeExecutor(recorder).Do(t.Context(), createRequest)
	if err == nil {
		t.Fatal("failed provider job returned no error")
	}
	assertSafeImageJobError(t, err, http.StatusBadGateway, "provider-private-error-canary")
	assertExactlyOneImageJobCreate(t, recorder.calls)
}

func TestLuchikeyImageJobsRecoversAmbiguousCreateWithoutRepeatingCreate(t *testing.T) {
	const (
		baseURL     = "https://image.luchikey.test"
		clientJobID = "oct-126-recovered"
		jobID       = "relay_recovered"
	)
	recorder := &recordingImageJobExecutor{do: func(call int, request *httpclient.Request) (*httpclient.Response, error) {
		switch call {
		case 0:
			return nil, &httpclient.Error{StatusCode: http.StatusBadGateway, Body: []byte(`{"error":"provider-private-create-error"}`)}
		case 1:
			if request.Method != http.MethodGet || request.URL != baseURL+luchikeyImageJobRecoveryPath+"?client_job_id="+clientJobID {
				t.Fatalf("recovery request = %s %s", request.Method, request.URL)
			}
			return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"jobs":[{"id":"relay_recovered","client_job_id":"oct-126-recovered","status":"running"}],"data":[{"id":"relay_recovered","client_job_id":"oct-126-recovered","status":"running"}]}}`), nil
		case 2:
			if request.Method != http.MethodGet || request.URL != baseURL+luchikeyImageJobPollPath+jobID {
				t.Fatalf("poll request = %s %s", request.Method, request.URL)
			}
			return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"id":"relay_recovered","client_job_id":"oct-126-recovered","status":"succeeded","result":{"data":[{"url":"/api/relay/image-jobs/relay_recovered/files/output.png?token=signed-canary"}]}}}`), nil
		default:
			t.Fatalf("unexpected provider request %d", call)
			return nil, errors.New("unexpected provider request")
		}
	}}
	outbound, createRequest := testLuchikeyImageJobsOutbound(t, baseURL, clientJobID)
	response, err := outbound.CustomizeExecutor(recorder).Do(t.Context(), createRequest)
	if err != nil {
		t.Fatalf("recover ambiguous create: %v", err)
	}
	if response == nil || response.StatusCode != http.StatusOK {
		t.Fatal("recovered image response is missing")
	}
	assertExactlyOneImageJobCreate(t, recorder.calls)
}

func TestLuchikeyImageJobsMissingRecoveryDoesNotRepeatCreate(t *testing.T) {
	recorder := &recordingImageJobExecutor{do: func(call int, _ *httpclient.Request) (*httpclient.Response, error) {
		if call == 0 {
			return nil, &httpclient.Error{StatusCode: http.StatusBadGateway, Body: []byte(`{"error":"provider-private-create-error"}`)}
		}
		return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"jobs":[],"data":[],"items":[],"total":0}}`), nil
	}}
	outbound, createRequest := testLuchikeyImageJobsOutbound(t, "https://image.luchikey.test", "oct-126-not-created")
	_, err := outbound.CustomizeExecutor(recorder).Do(t.Context(), createRequest)
	if err == nil {
		t.Fatal("missing recovered Job returned no error")
	}
	assertSafeImageJobError(t, err, http.StatusBadGateway, "provider-private-create-error")
	assertExactlyOneImageJobCreate(t, recorder.calls)
	if len(recorder.calls) != 2 || recorder.calls[1].Method != http.MethodGet {
		t.Fatal("ambiguous create was not followed by exactly one read-only recovery query")
	}
}

func TestLuchikeyImageJobsTimeoutDoesNotRepeatCreate(t *testing.T) {
	recorder := &recordingImageJobExecutor{do: func(call int, _ *httpclient.Request) (*httpclient.Response, error) {
		if call == 0 {
			return imageJobResponse(http.StatusAccepted, `{"ok":true,"data":{"id":"relay_timeout","status":"queued"}}`), nil
		}
		return imageJobResponse(http.StatusOK, `{"ok":true,"data":{"id":"relay_timeout","status":"running"}}`), nil
	}}
	outbound, createRequest := testLuchikeyImageJobsOutbound(t, "https://image.luchikey.test", "oct-126-timeout")
	outbound.maxWait = 12 * time.Millisecond
	_, err := outbound.CustomizeExecutor(recorder).Do(t.Context(), createRequest)
	if err == nil {
		t.Fatal("timed out provider job returned no error")
	}
	assertSafeImageJobError(t, err, http.StatusGatewayTimeout, "")
	assertExactlyOneImageJobCreate(t, recorder.calls)
	if len(recorder.calls) < 2 {
		t.Fatal("provider job timed out before any poll")
	}
}

func TestLuchikeyImageJobPrivateTransportRejectsOverride(t *testing.T) {
	_, request := testLuchikeyImageJobsOutbound(t, "https://image.luchikey.test", "oct-126-private")
	var body map[string]any
	if err := json.Unmarshal(request.Body, &body); err != nil {
		t.Fatalf("decode private transport: %v", err)
	}
	body["quality"] = "hd"
	body["response_format"] = "url"
	request.Body, _ = json.Marshal(body)
	if err := restoreHoneyImageResponseFormat(request, honeyImageModel); err == nil {
		t.Fatal("modified provider-private transport was accepted")
	}
}

func TestOpenAIImageOutboundPackyPathRemainsSynchronous(t *testing.T) {
	one := int64(1)
	request := &llm.Request{
		Model:       "gpt-image-2",
		RequestType: llm.RequestTypeImage,
		APIFormat:   llm.APIFormatOpenAIImageGeneration,
		Image: &llm.ImageRequest{
			Prompt:         "non-private test prompt",
			N:              &one,
			ResponseFormat: "url",
		},
	}
	outbound, err := newOutbound(llm.APIFormatOpenAIImageGeneration, request, "https://www.packyapi.test/v1", "test-key", "oct-126-packy")
	if err != nil {
		t.Fatalf("create Packy-compatible outbound: %v", err)
	}
	outboundRequest, err := outbound.TransformRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("transform Packy-compatible request: %v", err)
	}
	if err := restoreHoneyImageResponseFormat(outboundRequest, honeyImageModel); err != nil {
		t.Fatalf("apply Packy-compatible image boundary: %v", err)
	}
	if outboundRequest.URL != "https://www.packyapi.test/v1/images/generations" || outboundRequest.Method != http.MethodPost {
		t.Fatalf("Packy-compatible path regressed: %s %s", outboundRequest.Method, outboundRequest.URL)
	}
	var body map[string]any
	if err := json.Unmarshal(outboundRequest.Body, &body); err != nil {
		t.Fatalf("decode Packy-compatible body: %v", err)
	}
	if body["n"] != float64(1) || body["response_format"] != "url" {
		t.Fatalf("Packy-compatible verified fields are missing")
	}
	for _, privateField := range []string{"ratio", "client_job_id", "count"} {
		if _, ok := body[privateField]; ok {
			t.Fatalf("Luchikey private field %q leaked into Packy path", privateField)
		}
	}
}

func TestLuchikeyImageJobPayloadRemainsMetadataOnlyInRelayLog(t *testing.T) {
	one := int64(1)
	request := &llm.Request{
		Model:       honeyImageModel,
		RequestType: llm.RequestTypeImage,
		APIFormat:   llm.APIFormatOpenAIImageGeneration,
		Image:       &llm.ImageRequest{Prompt: "provider-private-prompt-canary", N: &one},
	}
	metrics := newRelayMetrics(7, request, request.APIFormat, time.Now())
	metrics.RequestID = "oct-126-metadata"
	metrics.ResultCode = http.StatusOK
	metrics.captureResponse([]byte(`{"data":[{"url":"https://image.luchikey.test/private.png?token=signed-canary"}]}`))
	relayLog := metrics.buildRelayLog(
		errors.New("provider-private-error-canary"),
		50*time.Millisecond,
		[]model.ChannelAttempt{{ChannelID: 3, ChannelName: "private-luchikey-channel", Msg: "provider-private-error-canary"}},
		3,
		"private-luchikey-channel",
	)
	if err := op.ValidateImageRelayLogAudit(relayLog); err != nil {
		t.Fatalf("metadata-only Luchikey relay log failed audit: %v", err)
	}
	encoded, err := json.Marshal(relayLog)
	if err != nil {
		t.Fatalf("encode relay log: %v", err)
	}
	for _, forbidden := range []string{"provider-private-prompt-canary", "signed-canary", "provider-private-error-canary", "private-luchikey-channel"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("metadata-only relay log leaked provider-private content")
		}
	}
	// Numeric provider-private fields use zero as their empty value. This is a
	// type-aware assertion; zero must not be mistaken for leaked content.
	if relayLog.ChannelId != 0 || relayLog.InputTokens != 0 || relayLog.OutputTokens != 0 || relayLog.Cost != 0 {
		t.Fatalf("numeric provider-private fields are populated")
	}
}

func testLuchikeyImageJobsOutbound(t *testing.T, baseURL, clientJobID string) (*luchikeyImageJobsOutbound, *httpclient.Request) {
	t.Helper()
	rawOutbound, err := newLuchikeyImageJobsOutbound(baseURL, "test-key", clientJobID)
	if err != nil {
		t.Fatalf("create Luchikey image jobs outbound: %v", err)
	}
	outbound := rawOutbound.(*luchikeyImageJobsOutbound)
	outbound.pollInterval = time.Millisecond
	outbound.maxWait = 100 * time.Millisecond
	one := int64(1)
	request, err := outbound.TransformRequest(t.Context(), &llm.Request{
		Model:       "gpt-image-2",
		RequestType: llm.RequestTypeImage,
		APIFormat:   llm.APIFormatOpenAIImageGeneration,
		Image: &llm.ImageRequest{
			Prompt:         "non-private test prompt",
			N:              &one,
			ResponseFormat: "url",
		},
	})
	if err != nil {
		t.Fatalf("transform Luchikey image job request: %v", err)
	}
	request, err = httpclient.FinalizeAuthHeaders(request)
	if err != nil {
		t.Fatalf("finalize Luchikey auth: %v", err)
	}
	if err := restoreHoneyImageResponseFormat(request, honeyImageModel); err != nil {
		t.Fatalf("enforce private Luchikey transport: %v", err)
	}
	return outbound, request
}

func assertLuchikeyPrivateCreateBody(t *testing.T, body []byte, clientJobID string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode image job body: %v", err)
	}
	if len(fields) != 6 {
		t.Fatalf("private image job fields = %d, want 6", len(fields))
	}
	var request luchikeyImageJobCreateRequest
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("decode private image job request: %v", err)
	}
	if request.Model != "gpt-image-2" || request.Prompt == "" || request.Ratio != luchikeyImageJobRatio ||
		request.Quality != luchikeyImageJobQuality || request.Count != luchikeyImageJobCount || request.ClientJobID != clientJobID {
		t.Fatalf("private image job transport mapping is invalid")
	}
	for _, publicField := range []string{"n", "response_format", "size"} {
		if _, ok := fields[publicField]; ok {
			t.Fatalf("public field %q leaked into private Job transport", publicField)
		}
	}
}

func assertExactlyOneImageJobCreate(t *testing.T, calls []*httpclient.Request) {
	t.Helper()
	creates := 0
	for _, call := range calls {
		if call.Method == http.MethodPost {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("image job create requests = %d, want exactly one", creates)
	}
}

func assertSafeImageJobError(t *testing.T, err error, wantStatus int, forbidden string) {
	t.Helper()
	var httpErr *httpclient.Error
	if !errors.As(err, &httpErr) || httpErr.StatusCode != wantStatus {
		t.Fatalf("image job error = %v, want safe HTTP %d", err, wantStatus)
	}
	if forbidden != "" && (strings.Contains(err.Error(), forbidden) || strings.Contains(string(httpErr.Body), forbidden)) {
		t.Fatal("provider-private error leaked through adapter")
	}
}

func imageJobResponse(statusCode int, body string) *httpclient.Response {
	return &httpclient.Response{StatusCode: statusCode, Headers: make(http.Header), Body: []byte(body)}
}

type recordingImageJobExecutor struct {
	calls []*httpclient.Request
	do    func(call int, request *httpclient.Request) (*httpclient.Response, error)
}

func (e *recordingImageJobExecutor) Do(_ context.Context, request *httpclient.Request) (*httpclient.Response, error) {
	clone := *request
	clone.Headers = request.Headers.Clone()
	clone.Body = append([]byte(nil), request.Body...)
	e.calls = append(e.calls, &clone)
	return e.do(len(e.calls)-1, request)
}

func (e *recordingImageJobExecutor) DoStream(context.Context, *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	return nil, errors.New("unexpected streaming provider request")
}

var _ pipeline.Executor = (*recordingImageJobExecutor)(nil)
