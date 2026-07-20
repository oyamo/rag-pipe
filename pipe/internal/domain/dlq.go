package domain

import (
	"time"
)

type DLQMessage struct {
	EventID         string    `json:"event_id"`
	DocumentID      string    `json:"document_id"`
	FileKey         string    `json:"file_key"`
	AttemptCount    uint64    `json:"attempt_count"`
	ErrorReason     string    `json:"error_reason"`
	FailedTimestamp time.Time `json:"failed_timestamp"`
	RawPayload      string    `json:"raw_payload"`
}
