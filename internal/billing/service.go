package billing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const priceDivisor int64 = 1_000_000

type Price struct {
	Model                          string
	Version                        string
	InputMicrounitsPerMillion      int64
	OutputMicrounitsPerMillion     int64
	CacheReadMicrounitsPerMillion  int64
	CacheWriteMicrounitsPerMillion int64
	GenerationMicrounitsPerImage   int64
}

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
}

type ChargeKind string

const (
	ChargeKindChatTokens      ChargeKind = "CHAT_TOKENS"
	ChargeKindImageGeneration ChargeKind = "IMAGE_GENERATION"
)

type AdmissionRequest struct {
	AccountID      string `json:"accountId"`
	APIKeyID       int    `json:"apiKeyId"`
	BillingEventID string `json:"billingEventId"`
}

type Admission struct {
	ReceiptID string `json:"receiptId"`
	Status    string `json:"status"`
}

type ChargeRequest struct {
	AccountID        string
	APIKeyID         int
	BillingEventID   string
	ReceiptID        string
	ExternalModel    string
	PricingVersion   string
	ChargeKind       ChargeKind
	Usage            Usage
	GenerationCount  int64
	ChargeMicrounits int64
	ProviderRef      string
}

func (request ChargeRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		AccountID        string     `json:"accountId"`
		APIKeyID         int        `json:"apiKeyId"`
		BillingEventID   string     `json:"billingEventId"`
		ReceiptID        string     `json:"receiptId"`
		ExternalModel    string     `json:"externalModel"`
		PricingVersion   string     `json:"pricingVersion"`
		ChargeKind       ChargeKind `json:"chargeKind"`
		InputTokens      int64      `json:"inputTokens"`
		OutputTokens     int64      `json:"outputTokens"`
		CacheReadTokens  int64      `json:"cacheReadTokens"`
		CacheWriteTokens int64      `json:"cacheWriteTokens"`
		GenerationCount  int64      `json:"generationCount"`
		ChargeMicrounits int64      `json:"chargeMicrounits"`
		ProviderRef      string     `json:"providerRef"`
	}{
		AccountID:        request.AccountID,
		APIKeyID:         request.APIKeyID,
		BillingEventID:   request.BillingEventID,
		ReceiptID:        request.ReceiptID,
		ExternalModel:    request.ExternalModel,
		PricingVersion:   request.PricingVersion,
		ChargeKind:       request.ChargeKind,
		InputTokens:      request.Usage.InputTokens,
		OutputTokens:     request.Usage.OutputTokens,
		CacheReadTokens:  request.Usage.CacheReadTokens,
		CacheWriteTokens: request.Usage.CacheWriteTokens,
		GenerationCount:  request.GenerationCount,
		ChargeMicrounits: request.ChargeMicrounits,
		ProviderRef:      request.ProviderRef,
	})
}

type Charge struct {
	ReceiptID              string `json:"receiptId"`
	ChargeMicrounits       int64  `json:"chargeMicrounits"`
	BalanceAfterMicrounits int64  `json:"balanceAfterMicrounits"`
}

type ReserveRequest struct {
	AccountID           string `json:"accountId"`
	APIKeyID            int    `json:"apiKeyId"`
	RequestID           string `json:"requestId"`
	MaxChargeMicrounits int64  `json:"maxChargeMicrounits"`
}

type Reservation struct {
	ReservationID      string `json:"reservationId"`
	ReservedMicrounits int64  `json:"reservedMicrounits"`
	Status             string `json:"status"`
}

type SettlementRequest struct {
	ReservationID    string
	ReceiptID        string
	ExternalModel    string
	PricingVersion   string
	Usage            Usage
	ChargeMicrounits int64
	ProviderRef      string
}

func (request SettlementRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ReservationID    string `json:"reservationId"`
		ReceiptID        string `json:"receiptId"`
		ExternalModel    string `json:"externalModel"`
		PricingVersion   string `json:"pricingVersion"`
		InputTokens      int64  `json:"inputTokens"`
		OutputTokens     int64  `json:"outputTokens"`
		CacheReadTokens  int64  `json:"cacheReadTokens"`
		CacheWriteTokens int64  `json:"cacheWriteTokens"`
		ChargeMicrounits int64  `json:"chargeMicrounits"`
		ProviderRef      string `json:"providerRef"`
	}{
		ReservationID:    request.ReservationID,
		ReceiptID:        request.ReceiptID,
		ExternalModel:    request.ExternalModel,
		PricingVersion:   request.PricingVersion,
		InputTokens:      request.Usage.InputTokens,
		OutputTokens:     request.Usage.OutputTokens,
		CacheReadTokens:  request.Usage.CacheReadTokens,
		CacheWriteTokens: request.Usage.CacheWriteTokens,
		ChargeMicrounits: request.ChargeMicrounits,
		ProviderRef:      request.ProviderRef,
	})
}

type Settlement struct {
	ReceiptID              string `json:"receiptId"`
	ChargeMicrounits       int64  `json:"chargeMicrounits"`
	BalanceAfterMicrounits int64  `json:"balanceAfterMicrounits"`
}

type Client interface {
	Admit(context.Context, AdmissionRequest) (Admission, error)
	Charge(context.Context, ChargeRequest) (Charge, error)
}

var (
	defaultClientLock sync.RWMutex
	defaultClient     Client
)

func SetDefaultClient(client Client) {
	defaultClientLock.Lock()
	defer defaultClientLock.Unlock()
	defaultClient = client
}

func DefaultClient() Client {
	defaultClientLock.RLock()
	defer defaultClientLock.RUnlock()
	return defaultClient
}

func validatePriceIdentity(price Price) error {
	if strings.TrimSpace(price.Model) == "" || strings.TrimSpace(price.Version) == "" {
		return fmt.Errorf("billing model and pricing version are required")
	}
	return nil
}

func validateTokenPrice(price Price) error {
	if err := validatePriceIdentity(price); err != nil {
		return err
	}
	if price.GenerationMicrounitsPerImage != 0 {
		return fmt.Errorf("generation price cannot be used for token billing")
	}
	rates := []int64{
		price.InputMicrounitsPerMillion,
		price.OutputMicrounitsPerMillion,
		price.CacheReadMicrounitsPerMillion,
		price.CacheWriteMicrounitsPerMillion,
	}
	var nonzero bool
	for _, rate := range rates {
		if rate < 0 {
			return fmt.Errorf("billing rate cannot be negative")
		}
		nonzero = nonzero || rate > 0
	}
	if !nonzero {
		return fmt.Errorf("at least one billing rate is required")
	}
	return nil
}

func validateGenerationPrice(price Price) error {
	if err := validatePriceIdentity(price); err != nil {
		return err
	}
	if price.GenerationMicrounitsPerImage <= 0 {
		return fmt.Errorf("generation price must be positive")
	}
	if price.InputMicrounitsPerMillion != 0 || price.OutputMicrounitsPerMillion != 0 ||
		price.CacheReadMicrounitsPerMillion != 0 || price.CacheWriteMicrounitsPerMillion != 0 {
		return fmt.Errorf("generation price cannot contain token rates")
	}
	return nil
}

func addProduct(total *int64, count, rate int64) error {
	if count < 0 || rate < 0 {
		return fmt.Errorf("billing values cannot be negative")
	}
	if count != 0 && rate > math.MaxInt64/count {
		return fmt.Errorf("billing amount overflow")
	}
	product := count * rate
	if product > math.MaxInt64-*total {
		return fmt.Errorf("billing amount overflow")
	}
	*total += product
	return nil
}

func CalculateCharge(price Price, usage Usage) (int64, error) {
	if err := validateTokenPrice(price); err != nil {
		return 0, err
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.CacheReadTokens < 0 || usage.CacheWriteTokens < 0 {
		return 0, fmt.Errorf("token usage cannot be negative")
	}
	if usage.InputTokens == 0 && usage.OutputTokens == 0 {
		return 0, fmt.Errorf("token usage is missing")
	}
	ordinaryInput := usage.InputTokens - usage.CacheReadTokens - usage.CacheWriteTokens
	if ordinaryInput < 0 {
		return 0, fmt.Errorf("cache token usage exceeds input usage")
	}
	var numerator int64
	for _, item := range [][2]int64{
		{ordinaryInput, price.InputMicrounitsPerMillion},
		{usage.OutputTokens, price.OutputMicrounitsPerMillion},
		{usage.CacheReadTokens, price.CacheReadMicrounitsPerMillion},
		{usage.CacheWriteTokens, price.CacheWriteMicrounitsPerMillion},
	} {
		if err := addProduct(&numerator, item[0], item[1]); err != nil {
			return 0, err
		}
	}
	if numerator == 0 {
		return 0, nil
	}
	return (numerator + priceDivisor - 1) / priceDivisor, nil
}

func CalculateGenerationCharge(price Price, generationCount int64) (int64, error) {
	if err := validateGenerationPrice(price); err != nil {
		return 0, err
	}
	if generationCount != 1 {
		return 0, fmt.Errorf("first release requires exactly one generation")
	}
	return price.GenerationMicrounitsPerImage, nil
}

func MaximumCharge(price Price, requestBytes int, maxOutputTokens int64) (int64, error) {
	if err := validateTokenPrice(price); err != nil {
		return 0, err
	}
	if requestBytes <= 0 || maxOutputTokens <= 0 {
		return 0, fmt.Errorf("request size and output-token bound are required")
	}
	inputRate := max(
		price.InputMicrounitsPerMillion,
		price.CacheReadMicrounitsPerMillion,
		price.CacheWriteMicrounitsPerMillion,
	)
	var numerator int64
	if err := addProduct(&numerator, int64(requestBytes), inputRate); err != nil {
		return 0, err
	}
	if err := addProduct(&numerator, maxOutputTokens, price.OutputMicrounitsPerMillion); err != nil {
		return 0, err
	}
	if numerator == 0 {
		return 0, fmt.Errorf("maximum charge is zero")
	}
	return (numerator + priceDivisor - 1) / priceDivisor, nil
}

type HTTPClient struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

type HTTPStatusError struct {
	StatusCode int
}

func (err *HTTPStatusError) Error() string {
	return fmt.Sprintf("billing request returned HTTP %d", err.StatusCode)
}

func StatusCode(err error) (int, bool) {
	var statusError *HTTPStatusError
	if !errors.As(err, &statusError) {
		return 0, false
	}
	return statusError.StatusCode, true
}

func NewHTTPClient(baseURL, serviceToken string, client *http.Client) (*HTTPClient, error) {
	endpoint, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return nil, fmt.Errorf("billing base URL is invalid")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("billing base URL contains forbidden components")
	}
	if len(serviceToken) < 32 || serviceToken != strings.TrimSpace(serviceToken) || strings.ContainsAny(serviceToken, "\r\n") {
		return nil, fmt.Errorf("billing service token is invalid")
	}
	if client == nil {
		client = http.DefaultClient
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	return &HTTPClient{baseURL: endpoint, token: serviceToken, client: client}, nil
}

func (client *HTTPClient) post(ctx context.Context, path string, request any, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode billing request: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		endpoint := *client.baseURL
		endpoint.Path = client.baseURL.Path + path
		httpRequest, createErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
		if createErr != nil {
			return fmt.Errorf("create billing request: %w", createErr)
		}
		httpRequest.Header.Set("Authorization", "Bearer "+client.token)
		httpRequest.Header.Set("Content-Type", "application/json")
		httpResponse, requestErr := client.client.Do(httpRequest)
		if requestErr != nil {
			lastErr = fmt.Errorf("billing request failed: %w", requestErr)
			if ctx.Err() != nil {
				return lastErr
			}
			continue
		}
		if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
			io.Copy(io.Discard, io.LimitReader(httpResponse.Body, 64*1024))
			httpResponse.Body.Close()
			lastErr = &HTTPStatusError{StatusCode: httpResponse.StatusCode}
			if httpResponse.StatusCode >= 500 {
				continue
			}
			return lastErr
		}
		decoder := json.NewDecoder(io.LimitReader(httpResponse.Body, 64*1024))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(response)
		httpResponse.Body.Close()
		if decodeErr == nil {
			return nil
		}
		lastErr = fmt.Errorf("decode billing response: %w", decodeErr)
	}
	return lastErr
}

func (client *HTTPClient) Admit(ctx context.Context, request AdmissionRequest) (Admission, error) {
	var response Admission
	if err := client.post(ctx, "/internal/v1/billing/admissions", request, &response); err != nil {
		return Admission{}, err
	}
	if response.ReceiptID == "" || (response.Status != "ALLOWED" && response.Status != "REPLAYED") {
		return Admission{}, fmt.Errorf("billing admission response is invalid")
	}
	return response, nil
}

func (client *HTTPClient) Charge(ctx context.Context, request ChargeRequest) (Charge, error) {
	var response Charge
	if err := client.post(ctx, "/internal/v1/billing/charges", request, &response); err != nil {
		return Charge{}, err
	}
	if response.ReceiptID != request.ReceiptID || response.ChargeMicrounits != request.ChargeMicrounits || response.BalanceAfterMicrounits < 0 {
		return Charge{}, fmt.Errorf("billing charge response is invalid")
	}
	return response, nil
}

func (client *HTTPClient) Reserve(ctx context.Context, request ReserveRequest) (Reservation, error) {
	var response Reservation
	if err := client.post(ctx, "/internal/v1/billing/reservations", request, &response); err != nil {
		return Reservation{}, err
	}
	if response.ReservationID == "" || response.Status != "RESERVED" || response.ReservedMicrounits != request.MaxChargeMicrounits {
		return Reservation{}, fmt.Errorf("billing reservation response is invalid")
	}
	return response, nil
}

func (client *HTTPClient) Settle(ctx context.Context, request SettlementRequest) (Settlement, error) {
	var response Settlement
	if err := client.post(ctx, "/internal/v1/billing/settlements", request, &response); err != nil {
		return Settlement{}, err
	}
	if response.ReceiptID != request.ReceiptID || response.ChargeMicrounits != request.ChargeMicrounits || response.BalanceAfterMicrounits < 0 {
		return Settlement{}, fmt.Errorf("billing settlement response is invalid")
	}
	return response, nil
}

func (client *HTTPClient) Cancel(ctx context.Context, reservationID string) (Reservation, error) {
	var response Reservation
	if err := client.post(ctx, "/internal/v1/billing/cancellations", map[string]string{
		"reservationId": reservationID,
	}, &response); err != nil {
		return Reservation{}, err
	}
	if response.ReservationID != reservationID || response.Status != "CANCELED" {
		return Reservation{}, fmt.Errorf("billing cancellation response is invalid")
	}
	return response, nil
}
