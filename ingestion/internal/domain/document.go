package domain

import (
	"time"

	"github.com/google/uuid"
)

type Document struct {
	ID          uuid.UUID `json:"id"`
	TenantID    string    `json:"tenant_id,omitempty"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	FileKey     string    `json:"file_key"`
	FileSize    int64     `json:"file_size"`
	ContentType string    `json:"content_type"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type DocumentCreatedEvent struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	DocumentID  string    `json:"document_id"`
	TenantID    string    `json:"tenant_id,omitempty"`
	Name        string    `json:"name"`
	FileKey     string    `json:"file_key"`
	FileSize    int64     `json:"file_size"`
	ContentType string    `json:"content_type"`
	Timestamp   time.Time `json:"timestamp"`
	TraceParent string    `json:"traceparent,omitempty"`
	TraceState  string    `json:"tracestate,omitempty"`
}
