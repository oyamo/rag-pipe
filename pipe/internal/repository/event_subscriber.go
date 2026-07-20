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
)

type EventSubscriber struct {
	conn              nats.JetStreamContext
	sub               *nats.Subscription
	dlqPublisher      *DLQPublisher
	maxDeliveries     uint64
	workerConcurrency int
	msgChan           chan *nats.Msg
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
		tracer := otel.Tracer("repository.subscriber")
		ctx, span := tracer.Start(context.Background(), "EventSubscriber.WorkerProcess")

		meta, metaErr := msg.Metadata()
		var numDelivered uint64 = 1
		if metaErr == nil && meta != nil {
			numDelivered = meta.NumDelivered
		}

		var event domain.IngestionEvent
		unmarshalErr := json.Unmarshal(msg.Data, &event)
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
