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
	"go.opentelemetry.io/otel/attribute"
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

	// Sub-span 1: Generate Query Embedding (OpenRouter API)
	embCtx, embSpan := tracer.Start(ctx, "InferenceService.GenerateQueryEmbedding")
	t0 := time.Now()
	queryVec, err := s.generateQueryEmbedding(embCtx, trimmed)
	embDuration := time.Since(t0).Milliseconds()
	embSpan.SetAttributes(attribute.Int64("openrouter.embedding_duration_ms", embDuration))
	embSpan.End()

	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to generate query embedding: %w", err)
	}

	// Sub-span 2: Search Vector Database (Postgres pgvector)
	dbCtx, dbSpan := tracer.Start(ctx, "InferenceService.SearchSimilarVectors")
	t0 = time.Now()
	results, err := s.repo.SearchSimilarVectors(dbCtx, queryVec, minSimilarity, req.TopK, req.TenantID)
	dbDuration := time.Since(t0).Milliseconds()
	dbSpan.SetAttributes(attribute.Int64("pgvector.search_duration_ms", dbDuration), attribute.Int("pgvector.matches_found", len(results)))
	dbSpan.End()

	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("inference repository query error: %w", err)
	}

	latencyMs := time.Since(startTime).Milliseconds()
	span.SetAttributes(
		attribute.Int64("rag.total_latency_ms", latencyMs),
		attribute.Int64("rag.embedding_latency_ms", embDuration),
		attribute.Int64("rag.vector_search_latency_ms", dbDuration),
	)

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
