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
	"go.opentelemetry.io/otel/metric"
)

const (
	DefaultMinSimilarity = 0.18
	QueryPrefix          = "search_query: "
)

type InferenceService struct {
	repo           *repository.InferenceRepository
	dimension      int
	modelVersion   string
	client         *client.OpenRouterClient
	queryCounter   metric.Int64Counter
	latencyHisto   metric.Float64Histogram
}

func NewInferenceService(repo *repository.InferenceRepository, dimension int, modelVersion string, openRouterClient *client.OpenRouterClient) *InferenceService {
	meter := otel.GetMeterProvider().Meter("inference-service")
	qCounter, _ := meter.Int64Counter("rag_queries_total", metric.WithDescription("Total RAG queries executed"))
	latHisto, _ := meter.Float64Histogram("rag_query_duration_seconds", metric.WithDescription("RAG query latency in seconds"), metric.WithUnit("s"))

	return &InferenceService{
		repo:           repo,
		dimension:      dimension,
		modelVersion:   modelVersion,
		client:         openRouterClient,
		queryCounter:   qCounter,
		latencyHisto:   latHisto,
	}
}

func (s *InferenceService) QueryRAG(ctx context.Context, req *domain.QueryRequest) (*domain.QueryResponse, error) {
	startTime := time.Now()
	tracer := otel.Tracer("service.inference")
	ctx, span := tracer.Start(ctx, "InferenceService.QueryRAG")
	defer span.End()

	defer func() {
		durationSec := time.Since(startTime).Seconds()
		if s.latencyHisto != nil {
			s.latencyHisto.Record(ctx, durationSec, metric.WithAttributes(attribute.String("service", "inference-service")))
		}
	}()

	trimmed := strings.TrimSpace(req.Query)
	if trimmed == "" {
		if s.queryCounter != nil {
			s.queryCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error")))
		}
		return nil, fmt.Errorf("query text cannot be empty")
	}

	minSimilarity := req.MinSimilarity
	if minSimilarity <= 0.0 {
		minSimilarity = DefaultMinSimilarity
	}

	embCtx, embSpan := tracer.Start(ctx, "InferenceService.GenerateQueryEmbedding")
	t0 := time.Now()
	queryVec, err := s.generateQueryEmbedding(embCtx, trimmed)
	embDuration := time.Since(t0).Milliseconds()
	embSpan.SetAttributes(attribute.Int64("openrouter.embedding_duration_ms", embDuration))
	embSpan.End()

	if err != nil {
		span.RecordError(err)
		if s.queryCounter != nil {
			s.queryCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "embedding_error")))
		}
		return nil, fmt.Errorf("failed to generate query embedding: %w", err)
	}

	dbCtx, dbSpan := tracer.Start(ctx, "InferenceService.SearchSimilarVectors")
	t0 = time.Now()
	results, err := s.repo.SearchSimilarVectors(dbCtx, queryVec, minSimilarity, req.TopK, req.TenantID)
	dbDuration := time.Since(t0).Milliseconds()
	dbSpan.SetAttributes(attribute.Int64("pgvector.search_duration_ms", dbDuration), attribute.Int("pgvector.matches_found", len(results)))
	dbSpan.End()

	if err != nil {
		span.RecordError(err)
		if s.queryCounter != nil {
			s.queryCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "db_error")))
		}
		return nil, fmt.Errorf("inference repository query error: %w", err)
	}

	if s.queryCounter != nil {
		s.queryCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
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
