package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port                  string
	DatabaseDSN           string
	EmbeddingDimension    int
	EmbeddingModelVersion string
	OTelServiceName       string
}

func getRequiredEnv(key string) (string, error) {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return "", fmt.Errorf("missing required environment variable: %s", key)
	}
	return val, nil
}

func LoadConfig() (*Config, error) {
	port, err := getRequiredEnv("PORT")
	if err != nil {
		return nil, err
	}

	dsn, err := getRequiredEnv("DATABASE_DSN")
	if err != nil {
		return nil, err
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

	otelServiceName, err := getRequiredEnv("OTEL_SERVICE_NAME")
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:                  port,
		DatabaseDSN:           dsn,
		EmbeddingDimension:    embeddingDimension,
		EmbeddingModelVersion: embeddingModelVersion,
		OTelServiceName:       otelServiceName,
	}, nil
}
