package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type natsHeaderCarrier struct {
	header nats.Header
}

type EventSubscriber struct {
	conn              nats.JetStreamContext
	sub               *nats.Subscription
	dlqPublisher      *DLQPublisher
	maxDeliveries     uint64
	workerConcurrency int
	msgChan           chan *nats.Msg
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



func NewEventSubscriber(natsURL, streamName, subjectName, consumerGroup string, dlqPublisher *DLQPublisher, maxDeliveries uint64, workerConcurrency int) (*EventSubscriber, error) {
	if workerConcurrency <= 0 {
		workerConcurrency = 1
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create jetstream context: %w", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subjectName},
	})
	if err != nil {
		slog.Info("jetstream add stream status", "info", err.Error())
	}

	return &EventSubscriber{
		conn:              js,
		dlqPublisher:      dlqPublisher,
		maxDeliveries:     maxDeliveries,
		workerConcurrency: workerConcurrency,
		msgChan:           make(chan *nats.Msg, workerConcurrency*2),
	}, nil
}

func (s *EventSubscriber) GetJetStreamContext() nats.JetStreamContext {
	return s.conn
}

func (s *EventSubscriber) Subscribe(subjectName, consumerGroup string, handlerFunc func(ctx context.Context, event *domain.IngestionEvent) error) error {
	for i := 0; i < s.workerConcurrency; i++ {
		go s.workerLoop(handlerFunc)
	}

	sub, err := s.conn.QueueSubscribe(subjectName, consumerGroup, func(msg *nats.Msg) {
		s.msgChan <- msg
	}, nats.ManualAck(), nats.AckWait(300*time.Second))

	if err != nil {
		return fmt.Errorf("failed to queue subscribe to jetstream: %w", err)
	}

	s.sub = sub
	return nil
}

func (s *EventSubscriber) workerLoop(handlerFunc func(ctx context.Context, event *domain.IngestionEvent) error) {
	for msg := range s.msgChan {
		var event domain.IngestionEvent
		unmarshalErr := json.Unmarshal(msg.Data, &event)

		ctx := context.Background()
		if event.TraceParent != "" {
			mapCarrier := propagation.MapCarrier{
				"traceparent": event.TraceParent,
				"tracestate":  event.TraceState,
			}
			ctx = otel.GetTextMapPropagator().Extract(ctx, mapCarrier)
		} else if msg.Header != nil {
			carrier := &natsHeaderCarrier{header: msg.Header}
			ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
		}

		tracer := otel.Tracer("repository.subscriber")
		ctx, span := tracer.Start(ctx, "EventSubscriber.WorkerProcess")

		meta, metaErr := msg.Metadata()
		var numDelivered uint64 = 1
		if metaErr == nil && meta != nil {
			numDelivered = meta.NumDelivered
		}

		if unmarshalErr != nil {
			span.RecordError(unmarshalErr)
			slog.Error("failed to unmarshal ingestion event", "error", unmarshalErr, "attempt", numDelivered)

			if numDelivered >= s.maxDeliveries && s.dlqPublisher != nil {
				dlqMsg := &domain.DLQMessage{
					EventID:         "",
					DocumentID:      "",
					FileKey:         "",
					AttemptCount:    numDelivered,
					ErrorReason:     fmt.Sprintf("unmarshal error: %v", unmarshalErr),
					FailedTimestamp: time.Now().UTC(),
					RawPayload:      string(msg.Data),
				}
				s.dlqPublisher.PublishDLQ(ctx, dlqMsg)
				msg.Ack()
				span.End()
				continue
			}

			backoff := time.Duration(numDelivered*2) * time.Second
			msg.NakWithDelay(backoff)
			span.End()
			continue
		}

		procErr := handlerFunc(ctx, &event)
		if procErr != nil {
			span.RecordError(procErr)
			slog.Error("pipeline worker failed to process document event",
				"document_id", event.DocumentID,
				"attempt", numDelivered,
				"max_deliveries", s.maxDeliveries,
				"error", procErr,
			)

			if numDelivered >= s.maxDeliveries && s.dlqPublisher != nil {
				dlqMsg := &domain.DLQMessage{
					EventID:         event.EventID,
					DocumentID:      event.DocumentID,
					FileKey:         event.FileKey,
					AttemptCount:    numDelivered,
					ErrorReason:     procErr.Error(),
					FailedTimestamp: time.Now().UTC(),
					RawPayload:      string(msg.Data),
				}

				dlqErr := s.dlqPublisher.PublishDLQ(ctx, dlqMsg)
				if dlqErr != nil {
					slog.Error("failed to publish to dead letter queue", "error", dlqErr)
				} else {
					slog.Warn("max delivery retries reached; moved document event to DLQ",
						"document_id", event.DocumentID,
						"attempts", numDelivered,
					)
				}

				msg.Ack()
				span.End()
				continue
			}

			backoff := time.Duration(numDelivered*2) * time.Second
			msg.NakWithDelay(backoff)
			span.End()
			continue
		}

		msg.Ack()
		span.End()
	}
}

func (s *EventSubscriber) Unsubscribe() {
	if s.sub != nil {
		s.sub.Unsubscribe()
	}
	close(s.msgChan)
}
