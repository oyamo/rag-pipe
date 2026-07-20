package domain

import (
	"time"

	"github.com/google/uuid"
)

type IngestionEvent struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	DocumentID  string    `json:"document_id"`
	Name        string    `json:"name"`
	FileKey     string    `json:"file_key"`
	FileSize    int64     `json:"file_size"`
	ContentType string    `json:"content_type"`
	Timestamp   time.Time `json:"timestamp"`
}

type ChunkMetadata struct {
	DocumentID          string    `json:"document_id"`
	TenantID            string    `json:"tenant_id,omitempty"`
	StartPage           int       `json:"start_page"`
	EndPage             int       `json:"end_page"`
	SectionHeading      string    `json:"section_heading,omitempty"`
	Language            string    `json:"language"`
	ParserVersion       string    `json:"parser_version"`
	ExtractionTimestamp time.Time `json:"extraction_timestamp"`
	StartCharOffset     int       `json:"start_char_offset"`
	EndCharOffset       int       `json:"end_char_offset"`
	TokenCount          int       `json:"token_count"`
}

type Chunk struct {
	ID         uuid.UUID     `json:"id"`
	DocumentID uuid.UUID     `json:"document_id"`
	VectorID   uuid.UUID     `json:"vector_id"`
	Index      int           `json:"index"`
	Content    string        `json:"content"`
	Hash       string        `json:"hash"`
	Metadata   ChunkMetadata `json:"metadata"`
	CreatedAt  time.Time     `json:"created_at"`
}

type Vector struct {
	ID           uuid.UUID `json:"id"`
	Hash         string    `json:"hash"`
	Embedding    []float32 `json:"embedding"`
	ModelVersion string    `json:"model_version"`
	Dimensions   int       `json:"dimensions"`
	CreatedAt    time.Time `json:"created_at"`
}
