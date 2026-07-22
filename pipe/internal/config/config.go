package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port                  string
	DatabaseDSN           string
	MinioEndpoint         string
	MinioAccessKey        string
	MinioSecretKey        string
	MinioBucket           string
	MinioUseSSL           bool
	NatsURL               string
	NatsStream            string
	NatsSubject           string
	NatsConsumer          string
	NatsDLQSubject        string
	NatsMaxDeliveries     uint64
	WorkerConcurrency     int
	EmbeddingDimension    int
	EmbeddingModelVersion string
	ChunkStrategy         string
	OpenRouterAPIKey      string
	OpenRouterBaseURL     string
	OTelServiceName       string
	OTelCollectorURL      string
}

func loadDotEnv() {
	files := []string{".env", "../.env", "../../.env"}
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				v = strings.Trim(v, `"'`)
				if os.Getenv(k) == "" {
					_ = os.Setenv(k, v)
				}
			}
		}
		_ = f.Close()
	}
}

func getRequiredEnv(key string) (string, error) {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return "", fmt.Errorf("missing required environment variable: %s", key)
	}
	return val, nil
}

func getOptionalEnv(key, defaultVal string) string {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return defaultVal
	}
	return val
}

func LoadConfig() (*Config, error) {
	loadDotEnv()

	port, err := getRequiredEnv("PORT")
	if err != nil {
		return nil, err
	}

	dsn, err := getRequiredEnv("DATABASE_DSN")
	if err != nil {
		return nil, err
	}

	minioEndpoint, err := getRequiredEnv("MINIO_ENDPOINT")
	if err != nil {
		return nil, err
	}

	minioAccessKey, err := getRequiredEnv("MINIO_ACCESS_KEY")
	if err != nil {
		return nil, err
	}

	minioSecretKey, err := getRequiredEnv("MINIO_SECRET_KEY")
	if err != nil {
		return nil, err
	}

	minioBucket, err := getRequiredEnv("MINIO_BUCKET")
	if err != nil {
		return nil, err
	}

	minioUseSSLStr, err := getRequiredEnv("MINIO_USE_SSL")
	if err != nil {
		return nil, err
	}
	minioUseSSL, err := strconv.ParseBool(minioUseSSLStr)
	if err != nil {
		return nil, fmt.Errorf("invalid boolean for MINIO_USE_SSL: %w", err)
	}

	natsURL, err := getRequiredEnv("NATS_URL")
	if err != nil {
		return nil, err
	}

	natsStream, err := getRequiredEnv("NATS_STREAM")
	if err != nil {
		return nil, err
	}

	natsSubject, err := getRequiredEnv("NATS_SUBJECT")
	if err != nil {
		return nil, err
	}

	natsConsumer, err := getRequiredEnv("NATS_CONSUMER")
	if err != nil {
		return nil, err
	}

	natsDLQSubject, err := getRequiredEnv("NATS_DLQ_SUBJECT")
	if err != nil {
		return nil, err
	}

	maxDeliveriesStr, err := getRequiredEnv("NATS_MAX_DELIVERIES")
	if err != nil {
		return nil, err
	}
	natsMaxDeliveries, err := strconv.ParseUint(maxDeliveriesStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid uint for NATS_MAX_DELIVERIES: %w", err)
	}

	workerConcStr, err := getRequiredEnv("WORKER_CONCURRENCY")
	if err != nil {
		return nil, err
	}
	workerConcurrency, err := strconv.Atoi(workerConcStr)
	if err != nil {
		return nil, fmt.Errorf("invalid int for WORKER_CONCURRENCY: %w", err)
	}

	dimStr, err := getRequiredEnv("EMBEDDING_DIMENSION")
	if err != nil {
		return nil, err
	}
	embeddingDimension, err := strconv.Atoi(dimStr)
	if err != nil {
		return nil, fmt.Errorf("invalid int for EMBEDDING_DIMENSION: %w", err)
	}

	embeddingModelVersion, err := getRequiredEnv("EMBEDDING_MODEL_VERSION")
	if err != nil {
		return nil, err
	}

	chunkStrategy := getOptionalEnv("CHUNK_STRATEGY", "paragraph")

	openRouterAPIKey, _ := getRequiredEnv("OPENROUTER_API_KEY")
	openRouterBaseURL := getOptionalEnv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1/embeddings")

	otelServiceName, err := getRequiredEnv("OTEL_SERVICE_NAME")
	if err != nil {
		return nil, err
	}

	otelCollectorURL, err := getRequiredEnv("OTEL_COLLECTOR_URL")
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:                  port,
		DatabaseDSN:           dsn,
		MinioEndpoint:         minioEndpoint,
		MinioAccessKey:        minioAccessKey,
		MinioSecretKey:        minioSecretKey,
		MinioBucket:           minioBucket,
		MinioUseSSL:           minioUseSSL,
		NatsURL:               natsURL,
		NatsStream:            natsStream,
		NatsSubject:           natsSubject,
		NatsConsumer:          natsConsumer,
		NatsDLQSubject:        natsDLQSubject,
		NatsMaxDeliveries:     natsMaxDeliveries,
		WorkerConcurrency:     workerConcurrency,
		EmbeddingDimension:    embeddingDimension,
		EmbeddingModelVersion: embeddingModelVersion,
		ChunkStrategy:         chunkStrategy,
		OpenRouterAPIKey:      openRouterAPIKey,
		OpenRouterBaseURL:     openRouterBaseURL,
		OTelServiceName:       otelServiceName,
		OTelCollectorURL:      otelCollectorURL,
	}, nil
}
