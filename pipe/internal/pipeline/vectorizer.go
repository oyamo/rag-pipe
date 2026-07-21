package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

const (
	DefaultMaxRetries     = 3
	DefaultRetryBackoffMs = 100
)

type VectorizationScheduler struct {
	dimension    int
	modelVersion string
	maxBatchSize int
	maxTokens    int
	timeout      time.Duration
	client       *OpenRouterClient
}

func NewVectorizationScheduler(dimension int, modelVersion string, client *OpenRouterClient) *VectorizationScheduler {
	return &VectorizationScheduler{
		dimension:    dimension,
		modelVersion: modelVersion,
		maxBatchSize: 64,
		maxTokens:    8192,
		timeout:      400 * time.Millisecond,
		client:       client,
	}
}

func (v *VectorizationScheduler) GenerateBatchEmbeddings(ctx context.Context, chunks []domain.Chunk) ([]domain.Vector, error) {
	_, span := otel.Tracer("pipeline.vectorizer").Start(ctx, "VectorizationScheduler.GenerateBatchEmbeddings")
	defer span.End()

	if len(chunks) == 0 {
		return nil, nil
	}

	inputs := make([]domain.MultimodalInput, len(chunks))
	for i, chunk := range chunks {
		inputs[i] = domain.MultimodalInput{
			Content: []domain.MultimodalContentItem{
				{
					Type: ContentTypeText,
					Text: chunk.Content,
				},
			},
		}
	}

	var dataItems []domain.EmbeddingDataItem
	var err error

	for attempt := 1; attempt <= DefaultMaxRetries; attempt++ {
		if v.client != nil {
			dataItems, err = v.client.CreateEmbeddings(ctx, v.modelVersion, inputs)
		} else {
			err = fmt.Errorf("openrouter client is not configured")
		}

		if err == nil && len(dataItems) == len(chunks) {
			break
		}

		if attempt == DefaultMaxRetries {
			span.RecordError(err)
			return nil, fmt.Errorf("batch embedding generation failed after %d retries: %w", DefaultMaxRetries, err)
		}

		backoff := time.Duration(DefaultRetryBackoffMs*attempt) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	var vectors []domain.Vector
	for i, item := range dataItems {
		chunk := chunks[i]
		vecID, err := uuid.NewV7()
		if err != nil {
			vecID = uuid.New()
		}

		dim := len(item.Embedding)
		if dim == 0 {
			dim = v.dimension
		}

		vector := domain.Vector{
			ID:           vecID,
			Hash:         chunk.Hash,
			Embedding:    item.Embedding,
			ModelVersion: v.modelVersion,
			Dimensions:   dim,
			CreatedAt:    time.Now().UTC(),
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}
