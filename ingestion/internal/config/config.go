package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port             string
	DatabaseDSN      string
	MinioEndpoint    string
	MinioAccessKey   string
	MinioSecretKey   string
	MinioBucket      string
	MinioUseSSL      bool
	NatsURL          string
	NatsTopic        string
	OTelServiceName  string
	OTelCollectorURL string
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

	natsTopic, err := getRequiredEnv("NATS_TOPIC")
	if err != nil {
		return nil, err
	}

	otelServiceName, err := getRequiredEnv("OTEL_SERVICE_NAME")
	if err != nil {
		return nil, err
	}

	otelCollectorURL, err := getRequiredEnv("OTEL_COLLECTOR_URL")
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:             port,
		DatabaseDSN:      dsn,
		MinioEndpoint:    minioEndpoint,
		MinioAccessKey:   minioAccessKey,
		MinioSecretKey:   minioSecretKey,
		MinioBucket:      minioBucket,
		MinioUseSSL:      minioUseSSL,
		NatsURL:          natsURL,
		NatsTopic:        natsTopic,
		OTelServiceName:  otelServiceName,
		OTelCollectorURL: otelCollectorURL,
	}, nil
}
