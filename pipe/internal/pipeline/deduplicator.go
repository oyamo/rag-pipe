package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"github.com/oyamo/rag-pipe/pipe/internal/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/text/unicode/norm"
)

type ChunkDeduplicator struct {
	vectorRepo *repository.VectorRepository
	redisRepo  *repository.RedisRepository
	cache      map[string]uuid.UUID
	cacheOrder []string
	capacity   int
	mu         sync.RWMutex
}

func NewChunkDeduplicator(vectorRepo *repository.VectorRepository, redisRepo *repository.RedisRepository, cacheCapacity int) *ChunkDeduplicator {
	if cacheCapacity <= 0 {
		cacheCapacity = 10000
	}
	return &ChunkDeduplicator{
		vectorRepo: vectorRepo,
		redisRepo:  redisRepo,
		cache:      make(map[string]uuid.UUID),
		cacheOrder: make([]string, 0, cacheCapacity),
		capacity:   cacheCapacity,
	}
}

func (d *ChunkDeduplicator) CanonicalizeHash(text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}

	normalized := norm.NFKC.String(text)
	normalized = strings.TrimSpace(normalized)
	normalized = strings.ToLower(normalized)

	words := strings.Fields(normalized)
	canonical := strings.Join(words, " ")

	hash := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(hash[:])
}

func (d *ChunkDeduplicator) DeduplicateChunkBatch(ctx context.Context, chunks []*domain.Chunk) (map[string]uuid.UUID, error) {
	if len(chunks) == 0 {
		return nil, nil
	}

	tracer := otel.Tracer("pipeline.deduplicator")
	ctx, span := tracer.Start(ctx, "ChunkDeduplicator.DeduplicateChunkBatch")
	defer span.End()

	foundMap := make(map[string]uuid.UUID, len(chunks))
	var missingHashes []string

	d.mu.RLock()
	for _, chk := range chunks {
		hash := d.CanonicalizeHash(chk.Content)
		chk.Hash = hash

		if vectorID, exists := d.cache[hash]; exists {
			foundMap[hash] = vectorID
		} else {
			missingHashes = append(missingHashes, hash)
		}
	}
	d.mu.RUnlock()

	if len(missingHashes) == 0 {
		return foundMap, nil
	}

	if d.redisRepo != nil {
		redisFound, err := d.redisRepo.GetChunkVectorIDsBatch(ctx, missingHashes)
		if err == nil && len(redisFound) > 0 {
			remainingHashes := make([]string, 0, len(missingHashes))
			for _, h := range missingHashes {
				if vectorID, exists := redisFound[h]; exists {
					foundMap[h] = vectorID
					d.addToCache(h, vectorID)
				} else {
					remainingHashes = append(remainingHashes, h)
				}
			}
			missingHashes = remainingHashes
		}
	}

	if len(missingHashes) > 0 && d.vectorRepo != nil {
		dbFound, err := d.vectorRepo.FindVectorIDsByHashesBatch(ctx, missingHashes)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to batch lookup hashes in database: %w", err)
		}
		if len(dbFound) > 0 {
			toCacheInRedis := make(map[string]uuid.UUID, len(dbFound))
			for hash, existingVectorID := range dbFound {
				foundMap[hash] = existingVectorID
				d.addToCache(hash, existingVectorID)
				toCacheInRedis[hash] = existingVectorID
			}
			if d.redisRepo != nil {
				_ = d.redisRepo.SetChunkVectorIDsBatch(ctx, toCacheInRedis, 7*24*time.Hour)
			}
		}
	}

	return foundMap, nil
}

func (d *ChunkDeduplicator) DeduplicateChunk(ctx context.Context, chunk *domain.Chunk) (bool, uuid.UUID, string, error) {
	if chunk == nil {
		return false, uuid.Nil, "", nil
	}

	resMap, err := d.DeduplicateChunkBatch(ctx, []*domain.Chunk{chunk})
	if err != nil {
		return false, uuid.Nil, chunk.Hash, err
	}

	vectorID, found := resMap[chunk.Hash]
	return found, vectorID, chunk.Hash, nil
}

func (d *ChunkDeduplicator) RegisterVectorHash(hash string, vectorID uuid.UUID) {
	if hash == "" || vectorID == uuid.Nil {
		return
	}
	d.addToCache(hash, vectorID)
	if d.redisRepo != nil {
		_ = d.redisRepo.SetChunkVectorIDsBatch(context.Background(), map[string]uuid.UUID{hash: vectorID}, 7*24*time.Hour)
	}
}

func (d *ChunkDeduplicator) WarmupCache(ctx context.Context, limit int) error {
	if limit <= 0 || d.vectorRepo == nil {
		return nil
	}

	tracer := otel.Tracer("pipeline.deduplicator")
	ctx, span := tracer.Start(ctx, "ChunkDeduplicator.WarmupCache")
	defer span.End()

	t0 := time.Now()
	recentHashes, err := d.vectorRepo.FetchRecentVectorHashes(ctx, limit)
	if err != nil {
		span.RecordError(err)
		slog.WarnContext(ctx, "failed to fetch recent vector hashes for cache warmup", "error", err)
		return err
	}

	if len(recentHashes) == 0 {
		return nil
	}

	for hash, vectorID := range recentHashes {
		d.addToCache(hash, vectorID)
	}

	if d.redisRepo != nil {
		_ = d.redisRepo.SetChunkVectorIDsBatch(ctx, recentHashes, 7*24*time.Hour)
	}

	duration := time.Since(t0).Milliseconds()
	span.SetAttributes(
		attribute.Int("warmup.hashes_loaded", len(recentHashes)),
		attribute.Int64("warmup.duration_ms", duration),
	)

	slog.InfoContext(ctx, "cache warmup completed successfully", "hashes_loaded", len(recentHashes), "duration_ms", duration)
	return nil
}

func (d *ChunkDeduplicator) addToCache(hash string, vectorID uuid.UUID) {
	if hash == "" || vectorID == uuid.Nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.cache[hash]; exists {
		return
	}

	if len(d.cache) >= d.capacity {
		evictHash := d.cacheOrder[0]
		d.cacheOrder = d.cacheOrder[1:]
		delete(d.cache, evictHash)
	}

	d.cache[hash] = vectorID
	d.cacheOrder = append(d.cacheOrder, hash)
}
