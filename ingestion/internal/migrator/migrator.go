package migrator

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
)

type MigrationFile struct {
	Version int
	Name    string
	SQL     string
}

type Migrator struct {
	db *sql.DB
}

func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{db: db}
}

func (m *Migrator) Run(ctx context.Context, fs embed.FS, dir string) error {
	tracer := otel.Tracer("migrator")
	ctx, span := tracer.Start(ctx, "Migrator.Run")
	defer span.End()

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);
	`
	_, err := m.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(dir)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var files []MigrationFile

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		parts := strings.SplitN(entry.Name(), "_", 2)
		if len(parts) < 2 {
			continue
		}

		ver, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		filePath := filepath.Join(dir, entry.Name())
		content, err := fs.ReadFile(filePath)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to read migration file %s: %w", entry.Name(), err)
		}

		files = append(files, MigrationFile{
			Version: ver,
			Name:    entry.Name(),
			SQL:     string(content),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Version < files[j].Version
	})

	for _, f := range files {
		var count int
		checkSQL := `SELECT COUNT(1) FROM schema_migrations WHERE version = $1`
		err := m.db.QueryRowContext(ctx, checkSQL, f.Version).Scan(&count)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to check migration status for version %d: %w", f.Version, err)
		}

		if count > 0 {
			continue
		}

		slog.Info("applying database migration", "version", f.Version, "name", f.Name)

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to start migration transaction: %w", err)
		}

		_, err = tx.ExecContext(ctx, f.SQL)
		if err != nil {
			tx.Rollback()
			span.RecordError(err)
			return fmt.Errorf("failed to execute migration %s: %w", f.Name, err)
		}

		recordSQL := `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`
		_, err = tx.ExecContext(ctx, recordSQL, f.Version, f.Name)
		if err != nil {
			tx.Rollback()
			span.RecordError(err)
			return fmt.Errorf("failed to record migration %s: %w", f.Name, err)
		}

		err = tx.Commit()
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to commit migration transaction: %w", err)
		}

		slog.Info("migration applied successfully", "version", f.Version, "name", f.Name)
	}

	return nil
}
