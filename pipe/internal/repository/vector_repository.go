package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

type VectorRepository struct {
	db *sql.DB
}

func NewVectorRepository(db *sql.DB) *VectorRepository {
	return &VectorRepository{db: db}
}

func (r *VectorRepository) FindVectorIDByHash(ctx context.Context, hash string) (uuid.UUID, error) {
	tracer := otel.Tracer("repository.vector")
	ctx, span := tracer.Start(ctx, "VectorRepository.FindVectorIDByHash")
	defer span.End()

	query := `SELECT id FROM vectors WHERE hash = $1 LIMIT 1`

	var id uuid.UUID
	err := r.db.QueryRowContext(ctx, query, hash).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return uuid.Nil, nil
		}
		span.RecordError(err)
		return uuid.Nil, fmt.Errorf("failed to query vector by hash: %w", err)
	}

	return id, nil
}

func formatVector(embedding []float32) string {
	s := make([]string, len(embedding))
	for i, v := range embedding {
		s[i] = strconv.FormatFloat(float64(v), 'f', -1, 32)
	}
	return "[" + strings.Join(s, ",") + "]"
}

func (r *VectorRepository) SaveBulk(ctx context.Context, vectors []domain.Vector, chunks []domain.Chunk) error {
	tracer := otel.Tracer("repository.vector")
	ctx, span := tracer.Start(ctx, "VectorRepository.SaveBulk")
	defer span.End()

	if len(vectors) == 0 && len(chunks) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	batchSize := 400

	hashToOriginalID := make(map[string]uuid.UUID)
	for _, vec := range vectors {
		hashToOriginalID[vec.Hash] = vec.ID
	}

	if len(vectors) > 0 {
		for i := 0; i < len(vectors); i += batchSize {
			end := i + batchSize
			if end > len(vectors) {
				end = len(vectors)
			}

			batch := vectors[i:end]
			vectorValueStrings := make([]string, 0, len(batch))
			vectorArgs := make([]interface{}, 0, len(batch)*6)

			for idx, vec := range batch {
				baseIdx := idx * 6
				vectorValueStrings = append(vectorValueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6))
				vectorArgs = append(vectorArgs, vec.ID, vec.Hash, formatVector(vec.Embedding), vec.ModelVersion, vec.Dimensions, vec.CreatedAt)
			}

			vectorQuery := fmt.Sprintf(`
				INSERT INTO vectors (id, hash, embedding, model_version, dimensions, created_at)
				VALUES %s
				ON CONFLICT (hash) DO UPDATE SET
					embedding = EXCLUDED.embedding,
					model_version = EXCLUDED.model_version,
					dimensions = EXCLUDED.dimensions,
					created_at = EXCLUDED.created_at
				RETURNING id, hash
			`, strings.Join(vectorValueStrings, ","))

			rows, err := tx.QueryContext(ctx, vectorQuery, vectorArgs...)
			if err != nil {
				span.RecordError(err)
				return fmt.Errorf("failed to bulk insert vectors batch: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var actualID uuid.UUID
				var hash string
				if err := rows.Scan(&actualID, &hash); err != nil {
					span.RecordError(err)
					return fmt.Errorf("failed to scan returned vector id: %w", err)
				}
				if originalID, ok := hashToOriginalID[hash]; ok && originalID != actualID {
					hashToOriginalID[hash] = actualID
				}
			}
			if err := rows.Err(); err != nil {
				span.RecordError(err)
				return fmt.Errorf("error iterating vector rows: %w", err)
			}
		}
	}

	vectorIDMap := make(map[uuid.UUID]uuid.UUID)
	for hash, actualID := range hashToOriginalID {
		for _, vec := range vectors {
			if vec.Hash == hash {
				vectorIDMap[vec.ID] = actualID
				break
			}
		}
	}

	if len(chunks) > 0 {
		for i := 0; i < len(chunks); i += batchSize {
			end := i + batchSize
			if end > len(chunks) {
				end = len(chunks)
			}

			batch := chunks[i:end]
			chunkValueStrings := make([]string, 0, len(batch))
			chunkArgs := make([]interface{}, 0, len(batch)*7)

			for idx, chk := range batch {
				if actualID, ok := vectorIDMap[chk.VectorID]; ok {
					chk.VectorID = actualID
				}

				metaJSON, err := json.Marshal(chk.Metadata)
				if err != nil {
					span.RecordError(err)
					return fmt.Errorf("failed to marshal chunk metadata: %w", err)
				}

				baseIdx := idx * 7
				chunkValueStrings = append(chunkValueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)", baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))
				chunkArgs = append(chunkArgs, chk.ID, chk.DocumentID, chk.VectorID, chk.Index, chk.Content, metaJSON, chk.CreatedAt)
			}

			chunkQuery := fmt.Sprintf(`
				INSERT INTO chunks (id, document_id, vector_id, chunk_index, content, metadata, created_at)
				VALUES %s
			`, strings.Join(chunkValueStrings, ","))

			_, err = tx.ExecContext(ctx, chunkQuery, chunkArgs...)
			if err != nil {
				span.RecordError(err)
				return fmt.Errorf("failed to bulk insert chunks batch: %w", err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
