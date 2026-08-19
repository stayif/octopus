package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/billing"
	"github.com/bestruirui/octopus/internal/db"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/settlement"
	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/llm"
	"gorm.io/gorm"
)

const xud161Model = "honey-xud161-test"

type xud161BillingClient struct {
	mu          sync.Mutex
	admissions  map[string]int
	charges     map[string]int
	chargeCalls map[string]int
	chargeGate  map[string]*xud161ProviderGate
}

func newXUD161BillingClient() *xud161BillingClient {
	return &xud161BillingClient{
		admissions:  make(map[string]int),
		charges:     make(map[string]int),
		chargeCalls: make(map[string]int),
		chargeGate:  make(map[string]*xud161ProviderGate),
	}
}

func (client *xud161BillingClient) Admit(_ context.Context, request billing.AdmissionRequest) (billing.Admission, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.admissions[request.BillingEventID]++
	return billing.Admission{
		ReceiptID: "receipt-" + request.BillingEventID[len(request.BillingEventID)-8:],
		Status:    "ALLOWED",
	}, nil
}

func (client *xud161BillingClient) Charge(_ context.Context, request billing.ChargeRequest) (billing.Charge, error) {
	client.mu.Lock()
	client.chargeCalls[request.BillingEventID]++
	client.charges[request.BillingEventID] = 1
	gate := client.chargeGate[request.BillingEventID]
	client.mu.Unlock()
	if gate != nil {
		gate.once.Do(func() { close(gate.started) })
		<-gate.release
	}
	return billing.Charge{
		ReceiptID:        request.ReceiptID,
		ChargeMicrounits: request.ChargeMicrounits,
	}, nil
}

func (client *xud161BillingClient) gateCharge(billingEventID string) *xud161ProviderGate {
	client.mu.Lock()
	defer client.mu.Unlock()
	gate := &xud161ProviderGate{started: make(chan struct{}), release: make(chan struct{})}
	client.chargeGate[billingEventID] = gate
	return gate
}

func (client *xud161BillingClient) counts(billingEventID string) (int, int) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.admissions[billingEventID], client.charges[billingEventID]
}

func (client *xud161BillingClient) chargeCallCount(billingEventID string) int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.chargeCalls[billingEventID]
}

type xud161ProviderGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type xud161Provider struct {
	mu     sync.Mutex
	calls  map[string]int
	gates  map[string]*xud161ProviderGate
	result string
}

func newXUD161Provider() *xud161Provider {
	return &xud161Provider{
		calls:  make(map[string]int),
		gates:  make(map[string]*xud161ProviderGate),
		result: "xud161-durable-result",
	}
}

func (provider *xud161Provider) gate(tag string) *xud161ProviderGate {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	gate := &xud161ProviderGate{started: make(chan struct{}), release: make(chan struct{})}
	provider.gates[tag] = gate
	return gate
}

func (provider *xud161Provider) callCount(tag string) int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls[tag]
}

func (provider *xud161Provider) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	for _, header := range []string{
		honeyAttemptModeHeader,
		honeyBillingIDHeader,
		honeyGenerationIDHeader,
		honeyRequestDigestHeader,
	} {
		if request.Header.Get(header) != "" {
			http.Error(w, "Honey execution header leaked upstream", http.StatusBadRequest)
			return
		}
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(w, "read request", http.StatusBadRequest)
		return
	}
	tag := xud161ProviderTag(body)
	provider.mu.Lock()
	provider.calls[tag]++
	gate := provider.gates[tag]
	provider.mu.Unlock()
	if gate != nil {
		gate.once.Do(func() { close(gate.started) })
		<-gate.release
	}
	if tag == "provider-failure" {
		http.Error(w, "provider failure", http.StatusBadGateway)
		return
	}
	var payload struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &payload)
	if payload.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-xud161\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":%q}}]}\n\n", xud161Model, provider.result)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-xud161\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", xud161Model)
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-xud161\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":%q,\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n", xud161Model)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w,
		`{"id":"chatcmpl-xud161","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		xud161Model, provider.result)
}

func xud161ProviderTag(body []byte) string {
	for _, tag := range []string{
		"lost-response",
		"concurrent",
		"accepted-window",
		"stream-result",
		"commit-before-delivery",
		"provider-failure",
		"persist-failure",
		"restart-pending",
		"limit-amount",
		"limit-count",
		"limit-age",
	} {
		if bytes.Contains(body, []byte(tag)) {
			return tag
		}
	}
	return "other"
}

type xud161Identity struct {
	generationID   string
	billingEventID string
	requestDigest  string
}

func xud161ID(hexDigit byte, digestDigit byte) xud161Identity {
	return xud161Identity{
		generationID:   "main:" + strings.Repeat(string(hexDigit), 64),
		billingEventID: "chat:" + strings.Repeat(string(hexDigit), 64),
		requestDigest:  strings.Repeat(string(digestDigit), 64),
	}
}

func xud161Body(tag string, stream bool) []byte {
	body, _ := json.Marshal(map[string]any{
		"model": xud161Model,
		"messages": []map[string]string{{
			"role":    "user",
			"content": tag,
		}},
		"stream": stream,
	})
	return body
}

func xud161Post(
	ctx context.Context,
	client *http.Client,
	url string,
	mode string,
	identity xud161Identity,
	body []byte,
	accountID string,
) (*http.Response, []byte, error) {
	return xud161PostBound(ctx, client, url, mode, identity, body, accountID, "role-xud161-a")
}

func xud161PostBound(
	ctx context.Context,
	client *http.Client,
	url string,
	mode string,
	identity xud161Identity,
	body []byte,
	accountID string,
	roleID string,
) (*http.Response, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(honeyAttemptModeHeader, mode)
	request.Header.Set(honeyGenerationIDHeader, identity.generationID)
	request.Header.Set(honeyBillingIDHeader, identity.billingEventID)
	request.Header.Set(honeyRequestDigestHeader, identity.requestDigest)
	request.Header.Set("X-XUD161-Account", accountID)
	request.Header.Set("X-XUD161-Role", roleID)
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	return response, responseBody, err
}

func TestXUD162DurableSettlementFaultWindows(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "octopus-xud161.db")
	if err := db.InitDB("sqlite", databasePath, false); err != nil {
		t.Fatalf("InitDB: %v", err)
	}

	provider := newXUD161Provider()
	providerServer := httptest.NewServer(provider)
	defer providerServer.Close()
	seedXUD161Relay(t, providerServer.URL)

	billingClient := newXUD161BillingClient()
	billing.SetDefaultClient(billingClient)
	defer billing.SetDefaultClient(nil)
	settlementService, err := settlement.NewService(billingClient, settlement.Limits{
		MaxPendingAmountMicrounits: 1_000_000,
		MaxPendingCount:            1_000,
		MaxPendingAge:              time.Hour,
	}, time.Millisecond)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	settlement.SetDefaultService(settlementService)
	settlementService.Start()
	defer func() {
		_ = settlementService.Close()
		settlement.SetDefaultService(nil)
	}()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/v1/chat/completions",
		func(c *gin.Context) {
			accountID := c.GetHeader("X-XUD161-Account")
			if accountID == "" {
				accountID = "account-xud161-a"
			}
			roleID := c.GetHeader("X-XUD161-Role")
			if roleID == "" {
				roleID = "role-xud161-a"
			}
			c.Set("billing_enabled", true)
			c.Set("billing_account_id", accountID)
			c.Set("billing_role_id", roleID)
			c.Set("api_key_id", 161)
			c.Set("supported_models", xud161Model)
		},
		Handler(llm.APIFormatOpenAIChatCompletion),
	)
	relayServer := httptest.NewServer(router)
	defer relayServer.Close()
	url := relayServer.URL + "/v1/chat/completions"

	t.Run("Provider completion survives a lost Runtime response", func(t *testing.T) {
		identity := xud161ID('a', '1')
		body := xud161Body("lost-response", false)
		gate := provider.gate("lost-response")
		requestCtx, cancel := context.WithCancel(context.Background())
		requestDone := make(chan error, 1)
		go func() {
			_, _, err := xud161Post(requestCtx, relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
			requestDone <- err
		}()

		select {
		case <-gate.started:
		case <-time.After(5 * time.Second):
			t.Fatal("Provider did not start")
		}
		cancel()
		close(gate.release)
		select {
		case <-requestDone:
		case <-time.After(5 * time.Second):
			t.Fatal("canceled Runtime request did not return")
		}

		attempt := waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		if !bytes.Contains(attempt.ResponseBody, []byte(provider.result)) {
			t.Fatalf("durable response does not contain Provider result: %s", attempt.ResponseBody)
		}
		response, replayBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
		if err != nil {
			t.Fatalf("replay request: %v", err)
		}
		if response.StatusCode != http.StatusOK || response.Header.Get(honeyGenerationReplayed) != "true" {
			t.Fatalf("replay status=%d headers=%v body=%s", response.StatusCode, response.Header, replayBody)
		}
		if !bytes.Equal(replayBody, attempt.ResponseBody) {
			t.Fatal("replayed response differs from durable Provider result")
		}
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("lost-response") != 1 || admissions != 1 || charges != 1 {
			t.Fatalf("provider=%d admissions=%d charges=%d", provider.callCount("lost-response"), admissions, charges)
		}
	})

	t.Run("concurrent duplicate runs and charges once then replays", func(t *testing.T) {
		identity := xud161ID('b', '2')
		body := xud161Body("concurrent", false)
		gate := provider.gate("concurrent")
		type requestResult struct {
			response *http.Response
			body     []byte
			err      error
		}
		firstDone := make(chan requestResult, 1)
		go func() {
			response, responseBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
			firstDone <- requestResult{response: response, body: responseBody, err: err}
		}()
		select {
		case <-gate.started:
		case <-time.After(5 * time.Second):
			t.Fatal("Provider did not start")
		}

		duplicate, duplicateBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
		if err != nil {
			t.Fatalf("concurrent duplicate: %v", err)
		}
		if duplicate.StatusCode != http.StatusConflict || duplicate.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationRunning) {
			t.Fatalf("duplicate status=%d state=%q body=%s", duplicate.StatusCode, duplicate.Header.Get(honeyGenerationState), duplicateBody)
		}
		close(gate.release)
		var first requestResult
		select {
		case first = <-firstDone:
		case <-time.After(5 * time.Second):
			t.Fatal("first request did not finish")
		}
		if first.err != nil || first.response.StatusCode != http.StatusOK {
			t.Fatalf("first response=%v err=%v body=%s", first.response, first.err, first.body)
		}

		for _, mode := range []string{honeyAttemptModeStart, honeyAttemptModeResolveOnly} {
			replay, replayBody, replayErr := xud161Post(context.Background(), relayServer.Client(), url, mode, identity, body, "account-xud161-a")
			if replayErr != nil || replay.StatusCode != http.StatusOK || replay.Header.Get(honeyGenerationReplayed) != "true" {
				t.Fatalf("mode=%s replay=%v err=%v body=%s", mode, replay, replayErr, replayBody)
			}
			if !bytes.Equal(replayBody, first.body) {
				t.Fatalf("mode=%s replay body changed", mode)
			}
		}
		// Runtime rebuilds dynamic Memory and Emotion prompt context before a
		// RESOLVE_ONLY request. That can change the Provider body even though the
		// stable Honey request identity is unchanged. Resolution never calls the
		// Provider, so it must replay the durable result by stable identity while
		// START continues to bind the exact Provider body below.
		resolvePrompt := "concurrent with refreshed runtime context"
		resolveBody := xud161Body(resolvePrompt, false)
		resolved, resolvedBody, resolveErr := xud161Post(
			context.Background(),
			relayServer.Client(),
			url,
			honeyAttemptModeResolveOnly,
			identity,
			resolveBody,
			"account-xud161-a",
		)
		if resolveErr != nil || resolved.StatusCode != http.StatusOK || resolved.Header.Get(honeyGenerationReplayed) != "true" {
			t.Fatalf("changed-body resolve=%v err=%v body=%s", resolved, resolveErr, resolvedBody)
		}
		if !bytes.Equal(resolvedBody, first.body) {
			t.Fatal("changed-body RESOLVE_ONLY did not replay the durable result")
		}
		waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("concurrent") != 1 || provider.callCount(resolvePrompt) != 0 || admissions != 1 || charges != 1 {
			t.Fatalf(
				"provider=%d resolve_provider=%d admissions=%d charges=%d",
				provider.callCount("concurrent"),
				provider.callCount(resolvePrompt),
				admissions,
				charges,
			)
		}
	})

	t.Run("streaming Provider result is committed before delivery and replays byte-for-byte", func(t *testing.T) {
		identity := xud161ID('f', '6')
		body := xud161Body("stream-result", true)
		response, firstBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
		if err != nil {
			t.Fatalf("stream request: %v", err)
		}
		if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") || !bytes.Contains(firstBody, []byte(provider.result)) {
			t.Fatalf("stream status=%d content-type=%q body=%s", response.StatusCode, response.Header.Get("Content-Type"), firstBody)
		}
		attempt := waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		if !bytes.Equal(attempt.ResponseBody, firstBody) {
			t.Fatal("delivered stream differs from the durable result")
		}
		replay, replayBody, replayErr := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeResolveOnly, identity, body, "account-xud161-a")
		if replayErr != nil || replay.StatusCode != http.StatusOK || replay.Header.Get(honeyGenerationReplayed) != "true" {
			t.Fatalf("stream replay=%v err=%v body=%s", replay, replayErr, replayBody)
		}
		if !bytes.Equal(replayBody, firstBody) {
			t.Fatal("replayed stream differs byte-for-byte")
		}
		waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("stream-result") != 1 || admissions != 1 || charges != 1 {
			t.Fatalf("provider=%d admissions=%d charges=%d", provider.callCount("stream-result"), admissions, charges)
		}
	})

	t.Run("Runtime receives the durable result while Core charge is pending", func(t *testing.T) {
		identity := xud161ID('9', '7')
		body := xud161Body("commit-before-delivery", false)
		chargeGate := billingClient.gateCharge(identity.billingEventID)
		type requestResult struct {
			response *http.Response
			body     []byte
			err      error
		}
		done := make(chan requestResult, 1)
		go func() {
			response, responseBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
			done <- requestResult{response: response, body: responseBody, err: err}
		}()
		select {
		case <-chargeGate.started:
		case <-time.After(5 * time.Second):
			t.Fatal("billing charge did not start")
		}
		attempt, err := loadHoneyGeneration(context.Background(), identity.generationID)
		if err != nil || attempt.Status != dbmodel.HoneyGenerationPendingCharge {
			t.Fatalf("attempt while charge is pending=%+v err=%v", attempt, err)
		}
		var result requestResult
		select {
		case result = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Runtime did not receive durable result while charge was pending")
		}
		if result.err != nil || result.response.StatusCode != http.StatusOK ||
			result.response.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationPendingCharge) {
			t.Fatalf("pending response=%v err=%v body=%s", result.response, result.err, result.body)
		}
		replay, replayBody, replayErr := xud161Post(
			context.Background(), relayServer.Client(), url, honeyAttemptModeResolveOnly,
			identity, body, "account-xud161-a",
		)
		if replayErr != nil || replay.StatusCode != http.StatusOK || !bytes.Equal(replayBody, result.body) {
			t.Fatalf("pending replay=%v err=%v body=%s", replay, replayErr, replayBody)
		}
		close(chargeGate.release)
		waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("commit-before-delivery") != 1 || admissions != 1 || charges != 1 {
			t.Fatalf("provider=%d admissions=%d charges=%d", provider.callCount("commit-before-delivery"), admissions, charges)
		}
	})

	t.Run("identity conflicts fail closed before Provider and billing", func(t *testing.T) {
		identity := xud161ID('b', '2')
		stableBody := xud161Body("concurrent", false)
		beforeProvider := provider.callCount("concurrent")
		beforeAdmissions, beforeCharges := billingClient.counts(identity.billingEventID)
		cases := []struct {
			name     string
			identity xud161Identity
			body     []byte
			account  string
			role     string
		}{
			{name: "account", identity: identity, body: stableBody, account: "account-xud161-b", role: "role-xud161-a"},
			{name: "role", identity: identity, body: stableBody, account: "account-xud161-a", role: "role-xud161-b"},
			{name: "body", identity: identity, body: xud161Body("concurrent changed", false), account: "account-xud161-a", role: "role-xud161-a"},
			{name: "request digest", identity: xud161Identity{generationID: identity.generationID, billingEventID: identity.billingEventID, requestDigest: strings.Repeat("3", 64)}, body: stableBody, account: "account-xud161-a", role: "role-xud161-a"},
			{name: "billing reused by another generation", identity: xud161Identity{generationID: "main:" + strings.Repeat("c", 64), billingEventID: identity.billingEventID, requestDigest: identity.requestDigest}, body: stableBody, account: "account-xud161-a", role: "role-xud161-a"},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				response, responseBody, err := xud161PostBound(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, testCase.identity, testCase.body, testCase.account, testCase.role)
				if err != nil {
					t.Fatalf("request: %v", err)
				}
				if response.StatusCode != http.StatusConflict || response.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationAmbiguous) {
					t.Fatalf("status=%d state=%q body=%s", response.StatusCode, response.Header.Get(honeyGenerationState), responseBody)
				}
			})
		}
		afterAdmissions, afterCharges := billingClient.counts(identity.billingEventID)
		if provider.callCount("concurrent") != beforeProvider || afterAdmissions != beforeAdmissions || afterCharges != beforeCharges {
			t.Fatalf("conflict caused side effects: provider=%d admissions=%d charges=%d", provider.callCount("concurrent"), afterAdmissions, afterCharges)
		}
	})

	t.Run("restart converts unknown Provider result to stable ambiguous", func(t *testing.T) {
		identity := xud161ID('d', '4')
		body := xud161Body("restart-unknown", false)
		bodyDigest := sha256.Sum256(body)
		running := dbmodel.HoneyGenerationAttempt{
			GenerationID:       identity.generationID,
			BillingEventID:     identity.billingEventID,
			AccountID:          "account-xud161-a",
			RoleID:             "role-xud161-a",
			APIKeyID:           161,
			RouteFormat:        string(llm.APIFormatOpenAIChatCompletion),
			RequestModel:       xud161Model,
			RequestDigest:      identity.requestDigest,
			ProviderBodyDigest: hex.EncodeToString(bodyDigest[:]),
			Status:             dbmodel.HoneyGenerationRunning,
		}
		if err := db.GetDB().Create(&running).Error; err != nil {
			t.Fatalf("create RUNNING attempt: %v", err)
		}
		if err := db.RecoverHoneyGenerationAttempts(); err != nil {
			t.Fatalf("recover attempts: %v", err)
		}
		response, responseBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeResolveOnly, identity, body, "account-xud161-a")
		if err != nil {
			t.Fatalf("resolve ambiguous: %v", err)
		}
		if response.StatusCode != http.StatusConflict || response.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationAmbiguous) {
			t.Fatalf("status=%d state=%q body=%s", response.StatusCode, response.Header.Get(honeyGenerationState), responseBody)
		}
		if provider.callCount("other") != 0 {
			t.Fatalf("ambiguous attempt called Provider %d times", provider.callCount("other"))
		}
		admissions, charges := billingClient.counts(identity.billingEventID)
		if admissions != 0 || charges != 0 {
			t.Fatalf("ambiguous resolve billed: admissions=%d charges=%d", admissions, charges)
		}
	})

	t.Run("ACCEPTED attempt can be safely resumed once", func(t *testing.T) {
		identity := xud161ID('e', '5')
		body := xud161Body("accepted-window", false)
		bodyDigest := sha256.Sum256(body)
		accepted := dbmodel.HoneyGenerationAttempt{
			GenerationID:       identity.generationID,
			BillingEventID:     identity.billingEventID,
			AccountID:          "account-xud161-a",
			RoleID:             "role-xud161-a",
			APIKeyID:           161,
			RouteFormat:        string(llm.APIFormatOpenAIChatCompletion),
			RequestModel:       xud161Model,
			RequestDigest:      identity.requestDigest,
			ProviderBodyDigest: hex.EncodeToString(bodyDigest[:]),
			Status:             dbmodel.HoneyGenerationAccepted,
		}
		if err := db.GetDB().Create(&accepted).Error; err != nil {
			t.Fatalf("create ACCEPTED attempt: %v", err)
		}
		response, responseBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("resume response=%v err=%v body=%s", response, err, responseBody)
		}
		waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("accepted-window") != 1 || admissions != 1 || charges != 1 {
			t.Fatalf("provider=%d admissions=%d charges=%d", provider.callCount("accepted-window"), admissions, charges)
		}
	})

	t.Run("Provider failure never fails over or retries", func(t *testing.T) {
		identity := xud161ID('8', '8')
		body := xud161Body("provider-failure", false)
		response, responseBody, err := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeStart, identity, body, "account-xud161-a")
		if err != nil {
			t.Fatalf("failed Provider request: %v", err)
		}
		if response.StatusCode != http.StatusConflict || response.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationAmbiguous) {
			t.Fatalf("status=%d state=%q body=%s", response.StatusCode, response.Header.Get(honeyGenerationState), responseBody)
		}
		attempt := waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationAmbiguous)
		if attempt.ResponseBody != nil {
			t.Fatal("ambiguous Provider failure retained a replayable result")
		}
		resolve, resolveBody, resolveErr := xud161Post(context.Background(), relayServer.Client(), url, honeyAttemptModeResolveOnly, identity, body, "account-xud161-a")
		if resolveErr != nil || resolve.StatusCode != http.StatusConflict || resolve.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationAmbiguous) {
			t.Fatalf("resolve=%v err=%v body=%s", resolve, resolveErr, resolveBody)
		}
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("provider-failure") != 1 || admissions != 1 || charges != 0 {
			t.Fatalf("provider=%d admissions=%d charges=%d", provider.callCount("provider-failure"), admissions, charges)
		}
	})

	t.Run("outbox persistence failure never returns the Provider result", func(t *testing.T) {
		identity := xud161ID('0', '0')
		body := xud161Body("persist-failure", false)
		callbackName := "xud162:fail-pending-charge-create"
		if err := db.GetDB().Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
			if _, ok := tx.Statement.Dest.(*dbmodel.HoneyChargeOutbox); ok {
				tx.AddError(fmt.Errorf("injected pending charge persistence failure"))
			}
		}); err != nil {
			t.Fatalf("register persistence fault: %v", err)
		}

		response, responseBody, err := xud161Post(
			context.Background(), relayServer.Client(), url,
			honeyAttemptModeStart, identity, body, "account-xud161-a",
		)
		if removeErr := db.GetDB().Callback().Create().Remove(callbackName); removeErr != nil {
			t.Fatalf("remove persistence fault: %v", removeErr)
		}
		if err != nil || response.StatusCode != http.StatusConflict ||
			response.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationAmbiguous) {
			t.Fatalf("response=%v err=%v body=%s", response, err, responseBody)
		}
		if bytes.Contains(responseBody, []byte(provider.result)) {
			t.Fatal("Provider result escaped before the local persistence barrier")
		}
		attempt := waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationAmbiguous)
		if len(attempt.ResponseBody) != 0 {
			t.Fatal("failed persistence retained a replayable Provider result")
		}
		var outboxCount int64
		if err := db.GetDB().Model(&dbmodel.HoneyChargeOutbox{}).
			Where("generation_id = ?", identity.generationID).Count(&outboxCount).Error; err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		admissions, charges := billingClient.counts(identity.billingEventID)
		if provider.callCount("persist-failure") != 1 || admissions != 1 || charges != 0 || outboxCount != 0 {
			t.Fatalf("provider=%d admissions=%d charges=%d outbox=%d",
				provider.callCount("persist-failure"), admissions, charges, outboxCount)
		}
	})

	t.Run("Honey relay log remains metadata only", func(t *testing.T) {
		metrics := &RelayMetrics{
			HoneyGeneration: true,
			RequestModel:    xud161Model,
			InternalRequest: &llm.Request{},
			InternalResponse: []byte(
				"assistant-private-canary",
			),
		}
		logRecord := metrics.buildRelayLog(nil, time.Second, nil, 0, "")
		if logRecord.RequestContent != "" || logRecord.ResponseContent != "" || strings.Contains(fmt.Sprintf("%+v", logRecord), "assistant-private-canary") {
			t.Fatal("Honey generation chat content leaked into relay log")
		}
	})

	t.Run("PENDING_CHARGE survives restart and settles without another Provider call", func(t *testing.T) {
		if err := settlementService.Close(); err != nil {
			t.Fatalf("stop settlement worker: %v", err)
		}
		identity := xud161ID('7', '7')
		body := xud161Body("restart-pending", false)
		pausedService, err := settlement.NewService(billingClient, settlement.Limits{
			MaxPendingAmountMicrounits: 1_000_000,
			MaxPendingCount:            1_000,
			MaxPendingAge:              time.Hour,
		}, time.Hour)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		settlement.SetDefaultService(pausedService)
		response, firstBody, err := xud161Post(
			context.Background(), relayServer.Client(), url,
			honeyAttemptModeStart, identity, body, "account-xud161-a",
		)
		if err != nil || response.StatusCode != http.StatusOK ||
			response.Header.Get(honeyGenerationState) != string(dbmodel.HoneyGenerationPendingCharge) {
			t.Fatalf("pending response=%v err=%v body=%s", response, err, firstBody)
		}
		if provider.callCount("restart-pending") != 1 {
			t.Fatalf("provider calls before restart=%d", provider.callCount("restart-pending"))
		}
		_, chargesBefore := billingClient.counts(identity.billingEventID)
		if chargesBefore != 0 {
			t.Fatalf("charges before restart=%d", chargesBefore)
		}
		callbackName := "xud162:fail-local-settlement-ack"
		if err := db.GetDB().Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
			if _, ok := tx.Statement.Model.(*dbmodel.HoneyChargeOutbox); ok {
				tx.AddError(fmt.Errorf("injected crash after Core charge ACK"))
			}
		}); err != nil {
			t.Fatalf("register settlement ACK fault: %v", err)
		}
		processed, settleErr := pausedService.RunOnce(context.Background())
		if !processed || settleErr == nil {
			t.Fatalf("pre-crash settlement processed=%t err=%v", processed, settleErr)
		}
		if removeErr := db.GetDB().Callback().Update().Remove(callbackName); removeErr != nil {
			t.Fatalf("remove settlement ACK fault: %v", removeErr)
		}
		attemptBeforeRestart := waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationPendingCharge)
		if !bytes.Equal(attemptBeforeRestart.ResponseBody, firstBody) {
			t.Fatal("pending result changed after failed local settlement ACK")
		}
		_, uniqueChargesAfterACK := billingClient.counts(identity.billingEventID)
		if uniqueChargesAfterACK != 1 || billingClient.chargeCallCount(identity.billingEventID) != 1 {
			t.Fatalf("unique charges=%d charge calls=%d after first ACK",
				uniqueChargesAfterACK, billingClient.chargeCallCount(identity.billingEventID))
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close database for restart: %v", err)
		}
		if err := db.InitDB("sqlite", databasePath, false); err != nil {
			t.Fatalf("restart database: %v", err)
		}
		restartedService, err := settlement.NewService(billingClient, settlement.Limits{
			MaxPendingAmountMicrounits: 1_000_000,
			MaxPendingCount:            1_000,
			MaxPendingAge:              time.Hour,
		}, time.Hour)
		if err != nil {
			t.Fatalf("NewService after restart: %v", err)
		}
		settlement.SetDefaultService(restartedService)
		processed, settleErr = restartedService.RunOnce(context.Background())
		if !processed || settleErr != nil {
			t.Fatalf("restart settlement processed=%t err=%v", processed, settleErr)
		}
		waitXUD161Status(t, identity.generationID, dbmodel.HoneyGenerationCompleted)
		replay, replayBody, replayErr := xud161Post(
			context.Background(), relayServer.Client(), url,
			honeyAttemptModeResolveOnly, identity, body, "account-xud161-a",
		)
		if replayErr != nil || replay.StatusCode != http.StatusOK || !bytes.Equal(replayBody, firstBody) {
			t.Fatalf("restart replay=%v err=%v body=%s", replay, replayErr, replayBody)
		}
		_, chargesAfter := billingClient.counts(identity.billingEventID)
		if provider.callCount("restart-pending") != 1 || chargesAfter != 1 ||
			billingClient.chargeCallCount(identity.billingEventID) != 2 {
			t.Fatalf("provider=%d unique_charges=%d charge_calls=%d",
				provider.callCount("restart-pending"), chargesAfter,
				billingClient.chargeCallCount(identity.billingEventID))
		}
	})

	t.Run("amount count and age limits pause only new Provider calls", func(t *testing.T) {
		cases := []struct {
			name          string
			reason        settlement.GateReason
			account       string
			existing      xud161Identity
			candidate     xud161Identity
			tag           string
			amount        int64
			createdAt     time.Time
			limits        settlement.Limits
			existingReply []byte
		}{
			{
				name: "amount", reason: settlement.GateAmount, account: "account-xud162-limit-amount",
				existing: xud161ID('1', '1'), candidate: xud161ID('4', '4'), tag: "limit-amount",
				amount: 10, createdAt: time.Now().UTC(),
				limits:        settlement.Limits{MaxPendingAmountMicrounits: 10, MaxPendingCount: 100, MaxPendingAge: time.Hour},
				existingReply: []byte(`{"result":"durable-amount"}`),
			},
			{
				name: "count", reason: settlement.GateCount, account: "account-xud162-limit-count",
				existing: xud161ID('2', '2'), candidate: xud161ID('5', '5'), tag: "limit-count",
				amount: 1, createdAt: time.Now().UTC(),
				limits:        settlement.Limits{MaxPendingAmountMicrounits: 100, MaxPendingCount: 1, MaxPendingAge: time.Hour},
				existingReply: []byte(`{"result":"durable-count"}`),
			},
			{
				name: "age", reason: settlement.GateAge, account: "account-xud162-limit-age",
				existing: xud161ID('3', '3'), candidate: xud161ID('6', '6'), tag: "limit-age",
				amount: 1, createdAt: time.Now().UTC().Add(-2 * time.Second),
				limits:        settlement.Limits{MaxPendingAmountMicrounits: 100, MaxPendingCount: 100, MaxPendingAge: time.Second},
				existingReply: []byte(`{"result":"durable-age"}`),
			},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				seedXUD162PendingCharge(
					t, testCase.existing, testCase.account, "role-xud161-a",
					testCase.amount, testCase.createdAt, testCase.existingReply,
				)
				service, err := settlement.NewService(billingClient, testCase.limits, time.Hour)
				if err != nil {
					t.Fatalf("NewService: %v", err)
				}
				settlement.SetDefaultService(service)
				if err := service.CheckProviderGate(
					context.Background(), testCase.account+"-other", time.Now().UTC(),
				); err != nil {
					t.Fatalf("another account was blocked by %s pending charges: %v", testCase.account, err)
				}

				replay, replayBody, replayErr := xud161Post(
					context.Background(), relayServer.Client(), url,
					honeyAttemptModeResolveOnly, testCase.existing,
					xud161Body("existing-"+testCase.name, false), testCase.account,
				)
				if replayErr != nil || replay.StatusCode != http.StatusOK ||
					!bytes.Equal(replayBody, testCase.existingReply) {
					t.Fatalf("pending replay=%v err=%v body=%s", replay, replayErr, replayBody)
				}

				candidateBody := xud161Body(testCase.tag, false)
				blocked, blockedBody, blockedErr := xud161Post(
					context.Background(), relayServer.Client(), url,
					honeyAttemptModeStart, testCase.candidate, candidateBody, testCase.account,
				)
				if blockedErr != nil || blocked.StatusCode != http.StatusTooManyRequests ||
					blocked.Header.Get("X-Honey-Settlement-Gate") != string(testCase.reason) {
					t.Fatalf("blocked=%v err=%v body=%s", blocked, blockedErr, blockedBody)
				}
				if provider.callCount(testCase.tag) != 0 {
					t.Fatalf("blocked call reached Provider %d times", provider.callCount(testCase.tag))
				}

				processed, settleErr := service.RunOnce(context.Background())
				if !processed || settleErr != nil {
					t.Fatalf("settlement while gated processed=%t err=%v", processed, settleErr)
				}
				allowed, allowedBody, allowedErr := xud161Post(
					context.Background(), relayServer.Client(), url,
					honeyAttemptModeStart, testCase.candidate, candidateBody, testCase.account,
				)
				if allowedErr != nil || allowed.StatusCode != http.StatusOK {
					t.Fatalf("allowed=%v err=%v body=%s", allowed, allowedErr, allowedBody)
				}
				if provider.callCount(testCase.tag) != 1 {
					t.Fatalf("Provider calls after recovery=%d", provider.callCount(testCase.tag))
				}
				processed, settleErr = service.RunOnce(context.Background())
				if !processed || settleErr != nil {
					t.Fatalf("candidate settlement processed=%t err=%v", processed, settleErr)
				}
			})
		}
	})
}

func seedXUD162PendingCharge(
	t *testing.T,
	identity xud161Identity,
	accountID string,
	roleID string,
	amount int64,
	createdAt time.Time,
	responseBody []byte,
) {
	t.Helper()
	completedAt := createdAt
	attempt := dbmodel.HoneyGenerationAttempt{
		GenerationID:        identity.generationID,
		BillingEventID:      identity.billingEventID,
		AccountID:           accountID,
		RoleID:              roleID,
		APIKeyID:            161,
		RouteFormat:         string(llm.APIFormatOpenAIChatCompletion),
		RequestModel:        xud161Model,
		RequestDigest:       identity.requestDigest,
		ProviderBodyDigest:  strings.Repeat("a", 64),
		Status:              dbmodel.HoneyGenerationPendingCharge,
		ReceiptID:           "receipt-" + identity.billingEventID[len(identity.billingEventID)-8:],
		ResponseStatus:      http.StatusOK,
		ResponseContentType: "application/json",
		ResponseBody:        append([]byte(nil), responseBody...),
		CompletedAt:         &completedAt,
		CreatedAt:           createdAt,
		UpdatedAt:           createdAt,
	}
	outbox := dbmodel.HoneyChargeOutbox{
		GenerationID:     identity.generationID,
		BillingEventID:   identity.billingEventID,
		AccountID:        accountID,
		RoleID:           roleID,
		APIKeyID:         161,
		ReceiptID:        attempt.ReceiptID,
		ExternalModel:    xud161Model,
		PricingVersion:   "xud161-v1",
		ChargeKind:       string(billing.ChargeKindChatTokens),
		InputTokens:      3,
		OutputTokens:     2,
		ChargeMicrounits: amount,
		ProviderRef:      "channel-161",
		Status:           dbmodel.HoneyChargePending,
		CreatedAt:        createdAt,
		UpdatedAt:        createdAt,
	}
	if err := db.GetDB().Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&attempt).Error; err != nil {
			return err
		}
		return tx.Create(&outbox).Error
	}); err != nil {
		t.Fatalf("seed pending charge: %v", err)
	}
}

func seedXUD161Relay(t *testing.T, providerURL string) {
	t.Helper()
	ctx := context.Background()
	channel := dbmodel.Channel{
		ID:      161,
		Name:    "xud161-provider",
		Type:    llm.APIFormatOpenAIChatCompletion,
		Enabled: true,
		BaseUrls: []dbmodel.BaseUrl{{
			URL: providerURL,
		}},
		Model: xud161Model,
	}
	key := dbmodel.ChannelKey{
		ID:         161,
		ChannelID:  channel.ID,
		Enabled:    true,
		ChannelKey: "xud161-test-provider-key",
	}
	group := dbmodel.Group{
		ID:   161,
		Name: xud161Model,
		Mode: dbmodel.GroupModeFailover,
	}
	item := dbmodel.GroupItem{
		ID:        161,
		GroupID:   group.ID,
		ChannelID: channel.ID,
		ModelName: xud161Model,
		Priority:  1,
		Weight:    1,
	}
	secondChannel := dbmodel.Channel{
		ID:      162,
		Name:    "xud161-provider-failover",
		Type:    llm.APIFormatOpenAIChatCompletion,
		Enabled: true,
		BaseUrls: []dbmodel.BaseUrl{{
			URL: providerURL,
		}},
		Model: xud161Model,
	}
	secondKey := dbmodel.ChannelKey{
		ID:         162,
		ChannelID:  secondChannel.ID,
		Enabled:    true,
		ChannelKey: "xud161-test-provider-key-2",
	}
	secondItem := dbmodel.GroupItem{
		ID:        162,
		GroupID:   group.ID,
		ChannelID: secondChannel.ID,
		ModelName: xud161Model,
		Priority:  2,
		Weight:    1,
	}
	info := dbmodel.LLMInfo{
		Name: xud161Model,
		UserBillingPrice: dbmodel.UserBillingPrice{
			PricingVersion:             "xud161-v1",
			InputMicrounitsPerMillion:  1000,
			OutputMicrounitsPerMillion: 2000,
		},
	}
	seeds := []struct {
		name  string
		value any
	}{
		{name: "channel", value: &channel},
		{name: "key", value: &key},
		{name: "second channel", value: &secondChannel},
		{name: "second key", value: &secondKey},
		{name: "group", value: &group},
		{name: "item", value: &item},
		{name: "second item", value: &secondItem},
		{name: "model", value: &info},
	}
	for _, seed := range seeds {
		if err := db.GetDB().WithContext(ctx).Create(seed.value).Error; err != nil {
			t.Fatalf("seed %s: %v", seed.name, err)
		}
	}
	if err := op.InitCache(); err != nil {
		t.Fatalf("InitCache: %v", err)
	}
}

func waitXUD161Status(t *testing.T, generationID string, status dbmodel.HoneyGenerationStatus) dbmodel.HoneyGenerationAttempt {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		attempt, err := loadHoneyGeneration(context.Background(), generationID)
		if err == nil && attempt.Status == status {
			return attempt
		}
		time.Sleep(10 * time.Millisecond)
	}
	attempt, err := loadHoneyGeneration(context.Background(), generationID)
	t.Fatalf("generation %s did not reach %s: attempt=%+v err=%v", generationID, status, attempt, err)
	return dbmodel.HoneyGenerationAttempt{}
}
