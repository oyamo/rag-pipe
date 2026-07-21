package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

type DLQPublisher struct {
	js        nats.JetStreamContext
	dlqSubject string
}

func NewDLQPublisher(js nats.JetStreamContext, dlqSubject string) *DLQPublisher {
	return &DLQPublisher{
		js:        js,
		dlqSubject: dlqSubject,
	}
}

func (p *DLQPublisher) PublishDLQ(ctx context.Context, msg *domain.DLQMessage) error {
	tracer := otel.Tracer("repository.dlq")
	ctx, span := tracer.Start(ctx, "DLQPublisher.PublishDLQ")
	defer span.End()

	data, err := json.Marshal(msg)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal dlq message: %w", err)
	}

	headers := nats.Header{}
	carrier := &natsHeaderCarrier{header: headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	natsMsg := &nats.Msg{
		Subject: p.dlqSubject,
		Data:    data,
		Header:  headers,
	}

	_, err = p.js.PublishMsg(natsMsg)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to publish to dlq subject %s: %w", p.dlqSubject, err)
	}

	return nil
}
