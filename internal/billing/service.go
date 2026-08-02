package billing

import (
	"bytes"
	"context"
	"encoding/json"
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
}

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
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
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Settle(context.Context, SettlementRequest) (Settlement, error)
	Cancel(context.Context, string) (Reservation, error)
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

func validatePrice(price Price) error {
	if strings.TrimSpace(price.Model) == "" || strings.TrimSpace(price.Version) == "" {
		return fmt.Errorf("billing model and pricing version are required")
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
	if err := validatePrice(price); err != nil {
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

func MaximumCharge(price Price, requestBytes int, maxOutputTokens int64) (int64, error) {
	if err := validatePrice(price); err != nil {
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
			lastErr = fmt.Errorf("billing request returned HTTP %d", httpResponse.StatusCode)
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
