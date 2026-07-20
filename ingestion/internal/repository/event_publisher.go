package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/oyamo/rag-pipe/ingestion/internal/domain"
	"go.opentelemetry.io/otel"
)

type EventPublisher struct {
	conn  *nats.Conn
	topic string
}

func NewEventPublisher(natsURL, topic string) (*EventPublisher, error) {
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	return &EventPublisher{
		conn:  conn,
		topic: topic,
	}, nil
}

func (p *EventPublisher) PublishDocumentCreated(ctx context.Context, event *domain.DocumentCreatedEvent) error {
	tracer := otel.Tracer("repository.publisher")
	_, span := tracer.Start(ctx, "EventPublisher.PublishDocumentCreated")
	defer span.End()

	data, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = p.conn.Publish(p.topic, data)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to publish nats message: %w", err)
	}

	return nil
}

func (p *EventPublisher) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}
