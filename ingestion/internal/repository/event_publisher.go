package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/oyamo/rag-pipe/ingestion/internal/domain"
	"go.opentelemetry.io/otel"
)

type natsHeaderCarrier struct {
	header nats.Header
}

func (c *natsHeaderCarrier) Get(key string) string {
	values := c.header[key]
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

func (c *natsHeaderCarrier) Set(key string, value string) {
	if c.header == nil {
		c.header = make(nats.Header)
	}
	c.header[key] = []string{value}
}

func (c *natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.header))
	for k := range c.header {
		keys = append(keys, k)
	}
	return keys
}

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
	ctx, span := tracer.Start(ctx, "EventPublisher.PublishDocumentCreated")
	defer span.End()

	data, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := nats.Header{}
	carrier := &natsHeaderCarrier{header: headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	msg := &nats.Msg{
		Subject: p.topic,
		Data:    data,
		Header:  headers,
	}

	err = p.conn.PublishMsg(msg)
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
