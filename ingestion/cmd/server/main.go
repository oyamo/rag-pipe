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
	"github.com/oyamo/rag-pipe/ingestion/internal/config"
	"github.com/oyamo/rag-pipe/ingestion/internal/handler"
	"github.com/oyamo/rag-pipe/ingestion/internal/repository"
	"github.com/oyamo/rag-pipe/ingestion/internal/service"
	"github.com/oyamo/rag-pipe/ingestion/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	telProvider, err := telemetry.InitTelemetry(cfg.OTelServiceName)
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

	storageRepo, err := repository.NewStorageRepository(
		cfg.MinioEndpoint,
		cfg.MinioAccessKey,
		cfg.MinioSecretKey,
		cfg.MinioBucket,
		cfg.MinioUseSSL,
	)
	if err != nil {
		slog.Error("failed to initialize storage repository", "error", err)
		os.Exit(1)
	}

	publisher, err := repository.NewEventPublisher(cfg.NatsURL, cfg.NatsTopic)
	if err != nil {
		slog.Warn("failed to connect to nats", "error", err)
	}

	docRepo := repository.NewDocumentRepository(db)
	ingestService := service.NewIngestionService(docRepo, storageRepo, publisher)
	docHandler := handler.NewDocumentHandler(ingestService)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"ingestion"}`))
	})
	docHandler.RegisterRoutes(mux)

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
		slog.Info("Ingestion service listening", "port", cfg.Port)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stopSignal
	slog.Info("Shutting down ingestion service gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	if db != nil {
		db.Close()
	}

	if publisher != nil {
		publisher.Close()
	}

	if telProvider != nil {
		telProvider.Shutdown(ctx)
	}

	slog.Info("Ingestion service stopped.")
}
