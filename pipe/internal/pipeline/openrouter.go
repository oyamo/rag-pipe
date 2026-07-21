package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/oyamo/rag-pipe/pipe/internal/domain"
)

const (
	DefaultOpenRouterBaseURL   = "https://openrouter.ai/api/v1/embeddings"
	DefaultTimeout             = 30 * time.Second
	DefaultDialTimeout         = 10 * time.Second
	DefaultKeepAlive           = 30 * time.Second
	DefaultIdleConnTimeout     = 90 * time.Second
	DefaultMaxIdleConns        = 100
	DefaultMaxIdleConnsPerHost = 25
	DefaultBufferCapacity      = 4096
	EncodingFormatFloat        = "float"
	ContentTypeJSON            = "application/json"
	ContentTypeText            = "text"
)

var (
	sharedTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   DefaultDialTimeout,
			KeepAlive: DefaultKeepAlive,
		}).DialContext,
		MaxIdleConns:          DefaultMaxIdleConns,
		MaxIdleConnsPerHost:   DefaultMaxIdleConnsPerHost,
		IdleConnTimeout:       DefaultIdleConnTimeout,
		TLSHandshakeTimeout:   DefaultDialTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	bufferPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, DefaultBufferCapacity))
		},
	}
)

type OpenRouterClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func NewOpenRouterClient(apiKey, baseURL string) *OpenRouterClient {
	if baseURL == "" {
		baseURL = DefaultOpenRouterBaseURL
	}
	return &OpenRouterClient{
		apiKey:  apiKey,
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: sharedTransport,
			Timeout:   DefaultTimeout,
		},
	}
}

func (c *OpenRouterClient) CreateEmbeddings(ctx context.Context, model string, inputs []domain.MultimodalInput) ([]domain.EmbeddingDataItem, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	reqPayload := domain.EmbeddingRequest{
		Model:          model,
		Input:          inputs,
		EncodingFormat: EncodingFormatFloat,
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(reqPayload); err != nil {
		return nil, fmt.Errorf("failed to encode embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", ContentTypeJSON)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openrouter embedding HTTP request failed: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		var errResp domain.EmbeddingResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil && errResp.Error != nil {
			return nil, fmt.Errorf("openrouter API error (status %d): %s", resp.StatusCode, errResp.Error.Message)
		}
		return nil, fmt.Errorf("openrouter API request failed with status code: %d", resp.StatusCode)
	}

	var resPayload domain.EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&resPayload); err != nil {
		return nil, fmt.Errorf("failed to decode openrouter response: %w", err)
	}

	if resPayload.Error != nil {
		return nil, fmt.Errorf("openrouter returned error: %s", resPayload.Error.Message)
	}

	return resPayload.Data, nil
}
