package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/lib/pq"
	"github.com/oyamo/rag-pipe/inference/internal/domain"
	"go.opentelemetry.io/otel"
)

type InferenceRepository struct {
	db *sql.DB
}

func NewInferenceRepository(db *sql.DB) *InferenceRepository {
	return &InferenceRepository{db: db}
}

func (r *InferenceRepository) SearchSimilarVectors(ctx context.Context, queryVec []float32, minSimilarity float64, topK int, tenantID string) ([]domain.QueryResultItem, error) {
	tracer := otel.Tracer("repository.inference")
	ctx, span := tracer.Start(ctx, "InferenceRepository.SearchSimilarVectors")
	defer span.End()

	if topK <= 0 {
		topK = 10
	}

	if minSimilarity < 0.0 {
		minSimilarity = 0.0
	}

	query := `
		SELECT 
			c.id, 
			c.document_id, 
			c.vector_id, 
			c.chunk_index, 
			c.content, 
			c.metadata, 
			v.model_version, 
			c.created_at, 
			(1 - (v.embedding <=> $1)) AS similarity
		FROM chunks c
		JOIN vectors v ON c.vector_id = v.id
		WHERE (1 - (v.embedding <=> $1)) >= $2
	`

	args := []interface{}{pq.Array(queryVec), minSimilarity}
	paramIdx := 3

	if tenantID != "" {
		query += fmt.Sprintf(" AND c.metadata->>'tenant_id' = $%d", paramIdx)
		args = append(args, tenantID)
		paramIdx++
	}

	query += fmt.Sprintf(" ORDER BY v.embedding <=> $1 LIMIT $%d", paramIdx)
	args = append(args, topK)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("vector similarity query failed: %w", err)
	}
	defer rows.Close()

	var results []domain.QueryResultItem

	for rows.Next() {
		var item domain.QueryResultItem
		var metaBytes []byte

		err := rows.Scan(
			&item.ChunkID,
			&item.DocumentID,
			&item.VectorID,
			&item.Index,
			&item.Content,
			&metaBytes,
			&item.ModelVersion,
			&item.CreatedAt,
			&item.Similarity,
		)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("row scan error: %w", err)
		}

		item.Metadata = json.RawMessage(metaBytes)
		results = append(results, item)
	}

	err = rows.Err()
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return results, nil
}
