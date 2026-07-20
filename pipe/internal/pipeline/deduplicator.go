package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"github.com/oyamo/rag-pipe/pipe/internal/repository"
	"go.opentelemetry.io/otel"
	"golang.org/x/text/unicode/norm"
)

type ChunkDeduplicator struct {
	vectorRepo *repository.VectorRepository
	cache      map[string]uuid.UUID
	cacheOrder []string
	capacity   int
	mu         sync.RWMutex
}

func NewChunkDeduplicator(vectorRepo *repository.VectorRepository, cacheCapacity int) *ChunkDeduplicator {
	if cacheCapacity <= 0 {
		cacheCapacity = 10000
	}
	return &ChunkDeduplicator{
		vectorRepo: vectorRepo,
		cache:      make(map[string]uuid.UUID),
		cacheOrder: make([]string, 0, cacheCapacity),
		capacity:   cacheCapacity,
	}
}

func (d *ChunkDeduplicator) CanonicalizeHash(text string) string {
	normalized := norm.NFKC.String(text)
	normalized = strings.TrimSpace(normalized)
	normalized = strings.ToLower(normalized)
	
	words := strings.Fields(normalized)
	canonical := strings.Join(words, " ")

	hash := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(hash[:])
}

func (d *ChunkDeduplicator) DeduplicateChunk(ctx context.Context, chunk *domain.Chunk) (bool, uuid.UUID, string, error) {
	_, span := otel.Tracer("pipeline.deduplicator").Start(ctx, "ChunkDeduplicator.DeduplicateChunk")
	defer span.End()

	hash := d.CanonicalizeHash(chunk.Content)
	chunk.Hash = hash

	d.mu.RLock()
	cachedVectorID, found := d.cache[hash]
	d.mu.RUnlock()

	if found {
		return true, cachedVectorID, hash, nil
	}

	existingVectorID, err := d.vectorRepo.FindVectorIDByHash(ctx, hash)
	if err != nil {
		span.RecordError(err)
		return false, uuid.Nil, hash, fmt.Errorf("failed to lookup hash in database: %w", err)
	}

	if existingVectorID != uuid.Nil {
		d.addToCache(hash, existingVectorID)
		return true, existingVectorID, hash, nil
	}

	return false, uuid.Nil, hash, nil
}

func (d *ChunkDeduplicator) RegisterVectorHash(hash string, vectorID uuid.UUID) {
	d.addToCache(hash, vectorID)
}

func (d *ChunkDeduplicator) addToCache(hash string, vectorID uuid.UUID) {
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
