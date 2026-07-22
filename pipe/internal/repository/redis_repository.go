package repository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisRepository(redisURL, password string, db int) (*RedisRepository, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		opts = &redis.Options{
			Addr:     redisURL,
			Password: password,
			DB:       db,
		}
	}

	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 1 * time.Second
	opts.WriteTimeout = 1 * time.Second
	opts.PoolSize = 50
	opts.MinIdleConns = 10

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = client.Ping(ctx).Err()
	if err != nil {
		slog.Warn("redis connection ping failed, fallback to postgres L3", "error", err)
	}

	return &RedisRepository{client: client}, nil
}

func (r *RedisRepository) GetChunkVectorIDsBatch(ctx context.Context, hashes []string) (map[string]uuid.UUID, error) {
	if len(hashes) == 0 || r.client == nil {
		return nil, nil
	}

	tracer := otel.Tracer("repository.redis")
	ctx, span := tracer.Start(ctx, "RedisRepository.GetChunkVectorIDsBatch")
	defer span.End()

	span.SetAttributes(attribute.Int("redis.keys_count", len(hashes)))

	keys := make([]string, len(hashes))
	for i, h := range hashes {
		keys[i] = fmt.Sprintf("chunk:hash:%s", h)
	}

	t0 := time.Now()
	vals, err := r.client.MGet(ctx, keys...).Result()
	duration := time.Since(t0).Milliseconds()
	span.SetAttributes(attribute.Int64("redis.duration_ms", duration))

	if err != nil {
		span.RecordError(err)
		slog.WarnContext(ctx, "redis batch mget failed", "error", err)
		return nil, nil
	}

	result := make(map[string]uuid.UUID, len(hashes))
	hits := 0
	for i, val := range vals {
		if val == nil {
			continue
		}
		strVal, ok := val.(string)
		if !ok || strVal == "" {
			continue
		}
		parsedID, err := uuid.Parse(strVal)
		if err == nil && parsedID != uuid.Nil {
			result[hashes[i]] = parsedID
			hits++
		}
	}

	misses := len(hashes) - hits
	span.SetAttributes(
		attribute.Int("redis.hits_count", hits),
		attribute.Int("redis.misses_count", misses),
	)

	return result, nil
}

func (r *RedisRepository) SetChunkVectorIDsBatch(ctx context.Context, hashToVectorID map[string]uuid.UUID, ttl time.Duration) error {
	if len(hashToVectorID) == 0 || r.client == nil {
		return nil
	}

	tracer := otel.Tracer("repository.redis")
	ctx, span := tracer.Start(ctx, "RedisRepository.SetChunkVectorIDsBatch")
	defer span.End()

	span.SetAttributes(attribute.Int("redis.keys_count", len(hashToVectorID)))

	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}

	pipe := r.client.Pipeline()
	for hash, vectorID := range hashToVectorID {
		key := fmt.Sprintf("chunk:hash:%s", hash)
		pipe.Set(ctx, key, vectorID.String(), ttl)
	}

	t0 := time.Now()
	_, err := pipe.Exec(ctx)
	duration := time.Since(t0).Milliseconds()
	span.SetAttributes(attribute.Int64("redis.duration_ms", duration))

	if err != nil {
		span.RecordError(err)
		slog.WarnContext(ctx, "redis pipeline batch set failed", "error", err)
	}

	return nil
}

func (r *RedisRepository) AcquireDocLock(ctx context.Context, docID string, ttl time.Duration) (bool, error) {
	if docID == "" || r.client == nil {
		return true, nil
	}

	tracer := otel.Tracer("repository.redis")
	ctx, span := tracer.Start(ctx, "RedisRepository.AcquireDocLock")
	defer span.End()

	span.SetAttributes(attribute.String("redis.doc_id", docID))

	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	lockKey := fmt.Sprintf("lock:doc:%s", docID)
	acquired, err := r.client.SetNX(ctx, lockKey, "locked", ttl).Result()
	span.SetAttributes(attribute.Bool("redis.lock_acquired", acquired))

	if err != nil {
		span.RecordError(err)
		slog.WarnContext(ctx, "failed to acquire doc lock from redis", "doc_id", docID, "error", err)
		return true, nil
	}

	return acquired, nil
}

func (r *RedisRepository) ReleaseDocLock(ctx context.Context, docID string) error {
	if docID == "" || r.client == nil {
		return nil
	}

	tracer := otel.Tracer("repository.redis")
	ctx, span := tracer.Start(ctx, "RedisRepository.ReleaseDocLock")
	defer span.End()

	span.SetAttributes(attribute.String("redis.doc_id", docID))

	lockKey := fmt.Sprintf("lock:doc:%s", docID)
	return r.client.Del(ctx, lockKey).Err()
}

func (r *RedisRepository) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
