package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type QueryRequest struct {
	Query         string                 `json:"query"`
	TopK          int                    `json:"top_k"`
	MinSimilarity float64                `json:"min_similarity"`
	TenantID      string                 `json:"tenant_id,omitempty"`
	Filter        map[string]interface{} `json:"filter,omitempty"`
}

type QueryResultItem struct {
	ChunkID      uuid.UUID       `json:"chunk_id"`
	DocumentID   uuid.UUID       `json:"document_id"`
	VectorID     uuid.UUID       `json:"vector_id"`
	Index        int             `json:"index"`
	Content      string          `json:"content"`
	Similarity   float64         `json:"similarity"`
	ModelVersion string          `json:"model_version"`
	Metadata     json.RawMessage `json:"metadata"`
	CreatedAt    time.Time       `json:"created_at"`
}

type QueryResponse struct {
	Query      string            `json:"query"`
	Total      int               `json:"total"`
	LatencyMs  int64             `json:"latency_ms"`
	Results    []QueryResultItem `json:"results"`
}
