package service

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/oyamo/rag-pipe/inference/internal/domain"
	"github.com/oyamo/rag-pipe/inference/internal/repository"
	"go.opentelemetry.io/otel"
)

type InferenceService struct {
	repo         *repository.InferenceRepository
	dimension    int
	modelVersion string
}

func NewInferenceService(repo *repository.InferenceRepository, dimension int, modelVersion string) *InferenceService {
	return &InferenceService{
		repo:         repo,
		dimension:    dimension,
		modelVersion: modelVersion,
	}
}

func (s *InferenceService) QueryRAG(ctx context.Context, req *domain.QueryRequest) (*domain.QueryResponse, error) {
	startTime := time.Now()
	tracer := otel.Tracer("service.inference")
	ctx, span := tracer.Start(ctx, "InferenceService.QueryRAG")
	defer span.End()

	trimmed := strings.TrimSpace(req.Query)
	if trimmed == "" {
		return nil, fmt.Errorf("query text cannot be empty")
	}

	queryVec := s.generateQueryEmbedding(trimmed)

	results, err := s.repo.SearchSimilarVectors(ctx, queryVec, req.MinSimilarity, req.TopK, req.TenantID)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("inference repository query error: %w", err)
	}

	latencyMs := time.Since(startTime).Milliseconds()

	return &domain.QueryResponse{
		Query:     trimmed,
		Total:     len(results),
		LatencyMs: latencyMs,
		Results:   results,
	}, nil
}

func (s *InferenceService) generateQueryEmbedding(text string) []float32 {
	r := rand.New(rand.NewSource(int64(len(text))))
	vec := make([]float32, s.dimension)
	var norm float64

	for i := 0; i < s.dimension; i++ {
		val := float32(r.NormFloat64())
		vec[i] = val
		norm += float64(val * val)
	}

	if norm > 0 {
		scale := float32(1.0 / (norm * norm))
		for i := 0; i < s.dimension; i++ {
			vec[i] *= scale
		}
	}

	return vec
}
