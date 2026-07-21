package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/ingestion/internal/domain"
	"github.com/oyamo/rag-pipe/ingestion/internal/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

	// 1. Read file stream & compute SHA-256 Checksum
	buf := new(bytes.Buffer)
	hasher := sha256.New()
	multiWriter := io.MultiWriter(buf, hasher)

	_, err := io.Copy(multiWriter, fileReader)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to read file stream for SHA-256 checksum: %w", err)
	}

	checksumHex := fmt.Sprintf("%x", hasher.Sum(nil))
	span.SetAttributes(attribute.String("document.checksum", checksumHex))

	// 2. Check duplicate document by SHA-256 Checksum in DB
	existingDoc, err := s.docRepo.FindByChecksum(ctx, "default", checksumHex)
	if err != nil {
		slog.WarnContext(ctx, "checksum lookup query warning", "error", err)
	} else if existingDoc != nil {
		slog.InfoContext(ctx, "duplicate document detected by SHA-256 checksum, skipping ingestion",
			"checksum", checksumHex,
			"existing_doc_id", existingDoc.ID.String(),
			"existing_file_key", existingDoc.FileKey,
		)
		span.SetAttributes(
			attribute.Bool("document.duplicate", true),
			attribute.String("document.existing_id", existingDoc.ID.String()),
		)
		return existingDoc, nil
	}

	// 3. Generate new Document ID & upload to Storage
	docID, err := uuid.NewV7()
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to generate uuid v7 for document: %w", err)
	}

	objectName := fmt.Sprintf("documents/%s.pdf", docID.String())

	fileKey, err := s.storage.UploadFile(ctx, objectName, buf, int64(buf.Len()), contentType)
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
		FileSize:    int64(buf.Len()),
		ContentType: contentType,
		Checksum:    checksumHex,
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
