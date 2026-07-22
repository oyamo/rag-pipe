package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

const (
	DefaultMaxRetries     = 3
	DefaultRetryBackoffMs = 100
	SubBatchSize          = 16
	MaxConcurrentCalls    = 8
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
	if len(chunks) == 0 {
		return nil, nil
	}

	_, span := otel.Tracer("pipeline.vectorizer").Start(ctx, "VectorizationScheduler.GenerateBatchEmbeddings")
	defer span.End()

	if len(chunks) <= SubBatchSize {
		return v.processSubBatch(ctx, chunks)
	}

	subBatches := (len(chunks) + SubBatchSize - 1) / SubBatchSize
	results := make([][]domain.Vector, subBatches)
	errChan := make(chan error, subBatches)
	sem := make(chan struct{}, MaxConcurrentCalls)
	var wg sync.WaitGroup

	for i := 0; i < subBatches; i++ {
		start := i * SubBatchSize
		end := min(start+SubBatchSize, len(chunks))

		wg.Add(1)
		go func(index int, batch []domain.Chunk) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			vecs, err := v.processSubBatch(ctx, batch)
			if err != nil {
				errChan <- err
				return
			}
			results[index] = vecs
		}(i, chunks[start:end])
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		span.RecordError(err)
		return nil, err
	}

	var finalVectors []domain.Vector
	for _, batchVecs := range results {
		finalVectors = append(finalVectors, batchVecs...)
	}

	return finalVectors, nil
}

func (v *VectorizationScheduler) processSubBatch(ctx context.Context, chunks []domain.Chunk) ([]domain.Vector, error) {
	if len(chunks) == 0 {
		return nil, nil
	}

	if v.client == nil {
		return nil, fmt.Errorf("openrouter client is not configured")
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
		dataItems, err = v.client.CreateEmbeddings(ctx, v.modelVersion, inputs)
		if err == nil && len(dataItems) == len(chunks) {
			break
		}

		if attempt == DefaultMaxRetries {
			return nil, fmt.Errorf("batch embedding failed: %w", err)
		}

		backoff := time.Duration(DefaultRetryBackoffMs*attempt) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	vectors := make([]domain.Vector, len(dataItems))
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

		vectors[i] = domain.Vector{
			ID:           vecID,
			Hash:         chunk.Hash,
			Embedding:    item.Embedding,
			ModelVersion: v.modelVersion,
			Dimensions:   dim,
			CreatedAt:    time.Now().UTC(),
		}
	}

	return vectors, nil
}
