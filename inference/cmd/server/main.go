package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/oyamo/rag-pipe/inference/internal/client"
	"github.com/oyamo/rag-pipe/inference/internal/config"
	"github.com/oyamo/rag-pipe/inference/internal/handler"
	"github.com/oyamo/rag-pipe/inference/internal/repository"
	"github.com/oyamo/rag-pipe/inference/internal/service"
	"github.com/oyamo/rag-pipe/inference/internal/telemetry"
)

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stdout, nil)
	logger := slog.New(telemetry.NewTracingHandler(jsonHandler))
	slog.SetDefault(logger)

	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	telProvider, err := telemetry.InitTelemetry(cfg.OTelServiceName, cfg.OTelCollectorURL)
	if err != nil {
		slog.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}

	db, err := sql.Open("postgres", cfg.DatabaseDSN)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	err = db.Ping()
	if err != nil {
		slog.Warn("database ping failed", "error", err)
	}

	openRouterClient := client.NewOpenRouterClient(cfg.OpenRouterAPIKey, cfg.OpenRouterBaseURL)
	inferRepo := repository.NewInferenceRepository(db)
	inferService := service.NewInferenceService(inferRepo, cfg.EmbeddingDimension, cfg.EmbeddingModelVersion, openRouterClient)
	inferHandler := handler.NewInferenceHandler(inferService)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"inference"}`))
	})
	inferHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("Inference RAG service listening", "port", cfg.Port)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stopSignal
	slog.Info("Shutting down inference RAG service gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	if db != nil {
		db.Close()
	}

	if telProvider != nil {
		telProvider.Shutdown(ctx)
	}

	slog.Info("Inference service stopped.")
}
