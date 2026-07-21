package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oyamo/rag-pipe/inference/internal/client"
	"github.com/oyamo/rag-pipe/inference/internal/domain"
	"github.com/oyamo/rag-pipe/inference/internal/repository"
	"go.opentelemetry.io/otel"
)

const (
	DefaultMinSimilarity = 0.18
	QueryPrefix          = "search_query: "
)

type InferenceService struct {
	repo         *repository.InferenceRepository
	dimension    int
	modelVersion string
	client       *client.OpenRouterClient
}

func NewInferenceService(repo *repository.InferenceRepository, dimension int, modelVersion string, openRouterClient *client.OpenRouterClient) *InferenceService {
	return &InferenceService{
		repo:         repo,
		dimension:    dimension,
		modelVersion: modelVersion,
		client:       openRouterClient,
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

	minSimilarity := req.MinSimilarity
	if minSimilarity <= 0.0 {
		minSimilarity = DefaultMinSimilarity
	}

	queryVec, err := s.generateQueryEmbedding(ctx, trimmed)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to generate query embedding: %w", err)
	}

	results, err := s.repo.SearchSimilarVectors(ctx, queryVec, minSimilarity, req.TopK, req.TenantID)
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

func (s *InferenceService) generateQueryEmbedding(ctx context.Context, text string) ([]float32, error) {
	if s.client == nil {
		return nil, fmt.Errorf("openrouter client is not configured")
	}

	queryInputText := text
	if !strings.HasPrefix(strings.ToLower(text), "search_query:") {
		queryInputText = QueryPrefix + text
	}

	inputs := []domain.MultimodalInput{
		{
			Content: []domain.MultimodalContentItem{
				{
					Type: client.ContentTypeText,
					Text: queryInputText,
				},
			},
		},
	}

	dataItems, err := s.client.CreateEmbeddings(ctx, s.modelVersion, inputs)
	if err != nil {
		return nil, fmt.Errorf("openrouter embedding call failed: %w", err)
	}

	if len(dataItems) == 0 {
		return nil, fmt.Errorf("openrouter returned no embedding data")
	}

	return dataItems[0].Embedding, nil
}
