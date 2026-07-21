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
	EmbeddingDimension    int
	EmbeddingModelVersion string
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
		EmbeddingDimension:    embeddingDimension,
		EmbeddingModelVersion: embeddingModelVersion,
		OpenRouterAPIKey:      openRouterAPIKey,
		OpenRouterBaseURL:     openRouterBaseURL,
		OTelServiceName:       otelServiceName,
		OTelCollectorURL:      otelCollectorURL,
	}, nil
}
