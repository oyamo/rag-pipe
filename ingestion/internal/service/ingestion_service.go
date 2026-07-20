package service

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/ingestion/internal/domain"
	"github.com/oyamo/rag-pipe/ingestion/internal/repository"
	"go.opentelemetry.io/otel"
)

type IngestionService struct {
	docRepo   *repository.DocumentRepository
	storage   *repository.StorageRepository
	publisher *repository.EventPublisher
}

func NewIngestionService(docRepo *repository.DocumentRepository, storage *repository.StorageRepository, publisher *repository.EventPublisher) *IngestionService {
	return &IngestionService{
		docRepo:   docRepo,
		storage:   storage,
		publisher: publisher,
	}
}

func (s *IngestionService) ProcessAndIngest(ctx context.Context, name, description, contentType string, fileSize int64, fileReader io.Reader) (*domain.Document, error) {
	tracer := otel.Tracer("service.ingestion")
	ctx, span := tracer.Start(ctx, "IngestionService.ProcessAndIngest")
	defer span.End()

	docID, err := uuid.NewV7()
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to generate uuid v7 for document: %w", err)
	}

	objectName := fmt.Sprintf("documents/%s.pdf", docID.String())

	fileKey, err := s.storage.UploadFile(ctx, objectName, fileReader, fileSize, contentType)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("storage upload failed: %w", err)
	}

	now := time.Now().UTC()
	doc := &domain.Document{
		ID:          docID,
		TenantID:    "default",
		Name:        name,
		Description: description,
		FileKey:     fileKey,
		FileSize:    fileSize,
		ContentType: contentType,
		Status:      "INGESTED",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	err = s.docRepo.Save(ctx, doc)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("database save failed: %w", err)
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to generate uuid v7 for event: %w", err)
	}

	event := &domain.DocumentCreatedEvent{
		EventID:     eventID.String(),
		EventType:   "document.created",
		DocumentID:  doc.ID.String(),
		TenantID:    doc.TenantID,
		Name:        doc.Name,
		FileKey:     doc.FileKey,
		FileSize:    doc.FileSize,
		ContentType: doc.ContentType,
		Timestamp:   now,
	}

	err = s.publisher.PublishDocumentCreated(ctx, event)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("event publish failed: %w", err)
	}

	return doc, nil
}
