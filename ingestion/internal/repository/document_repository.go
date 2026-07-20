package repository

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/oyamo/rag-pipe/ingestion/internal/domain"
	"go.opentelemetry.io/otel"
)

type DocumentRepository struct {
	db *sql.DB
}

func NewDocumentRepository(db *sql.DB) *DocumentRepository {
	return &DocumentRepository{db: db}
}

func (r *DocumentRepository) Save(ctx context.Context, doc *domain.Document) error {
	tracer := otel.Tracer("repository.document")
	ctx, span := tracer.Start(ctx, "DocumentRepository.Save")
	defer span.End()

	query := `
		INSERT INTO documents (id, tenant_id, name, description, file_key, file_size, content_type, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`

	_, err := r.db.ExecContext(
		ctx,
		query,
		doc.ID,
		doc.TenantID,
		doc.Name,
		doc.Description,
		doc.FileKey,
		doc.FileSize,
		doc.ContentType,
		doc.Status,
		doc.CreatedAt,
		doc.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to insert document: %w", err)
	}

	return nil
}

func (r *DocumentRepository) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	tracer := otel.Tracer("repository.document")
	ctx, span := tracer.Start(ctx, "DocumentRepository.GetByID")
	defer span.End()

	query := `
		SELECT id, tenant_id, name, description, file_key, file_size, content_type, status, created_at, updated_at
		FROM documents
		WHERE id = $1
	`

	doc := &domain.Document{}
	var tenantID sql.NullString

	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&doc.ID,
		&tenantID,
		&doc.Name,
		&doc.Description,
		&doc.FileKey,
		&doc.FileSize,
		&doc.ContentType,
		&doc.Status,
		&doc.CreatedAt,
		&doc.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query document: %w", err)
	}

	if tenantID.Valid {
		doc.TenantID = tenantID.String
	}

	return doc, nil
}
