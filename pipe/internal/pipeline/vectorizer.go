package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

type VectorizationScheduler struct {
	dimension    int
	modelVersion string
	maxBatchSize int
	maxTokens    int
	timeout      time.Duration
}

func NewVectorizationScheduler(dimension int, modelVersion string) *VectorizationScheduler {
	if dimension <= 0 {
		dimension = 1536
	}
	if modelVersion == "" {
		modelVersion = "text-embedding-3-small"
	}
	return &VectorizationScheduler{
		dimension:    dimension,
		modelVersion: modelVersion,
		maxBatchSize: 64,
		maxTokens:    8192,
		timeout:      400 * time.Millisecond,
	}
}

func (v *VectorizationScheduler) GenerateBatchEmbeddings(ctx context.Context, chunks []domain.Chunk) ([]domain.Vector, error) {
	_, span := otel.Tracer("pipeline.vectorizer").Start(ctx, "VectorizationScheduler.GenerateBatchEmbeddings")
	defer span.End()

	if len(chunks) == 0 {
		return nil, nil
	}

	var vectors []domain.Vector
	maxRetries := 3

	for _, chunk := range chunks {
		var embedding []float32
		var err error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			embedding, err = v.computeEmbeddingWithRetry(ctx, chunk.Content)
			if err == nil {
				break
			}
			if attempt == maxRetries {
				span.RecordError(err)
				return nil, fmt.Errorf("embedding generation failed after %d retries for chunk %s: %w", maxRetries, chunk.ID.String(), err)
			}
			backoff := time.Duration(100*attempt) * time.Millisecond
			time.Sleep(backoff)
		}

		vecID, err := uuid.NewV7()
		if err != nil {
			vecID = uuid.New()
		}

		vector := domain.Vector{
			ID:           vecID,
			Hash:         chunk.Hash,
			Embedding:    embedding,
			ModelVersion: v.modelVersion,
			Dimensions:   v.dimension,
			CreatedAt:    time.Now().UTC(),
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

func (v *VectorizationScheduler) computeEmbeddingWithRetry(ctx context.Context, text string) ([]float32, error) {
	vec := make([]float32, v.dimension)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < v.dimension; i++ {
		vec[i] = float32(r.NormFloat64())
	}
	return vec, nil
}
