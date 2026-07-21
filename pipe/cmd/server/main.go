package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/oyamo/rag-pipe/pipe/internal/config"
	"github.com/oyamo/rag-pipe/pipe/internal/nlp"
	"github.com/oyamo/rag-pipe/pipe/internal/pipeline"
	"github.com/oyamo/rag-pipe/pipe/internal/repository"
	"github.com/oyamo/rag-pipe/pipe/internal/telemetry"
	"github.com/oyamo/rag-pipe/pipe/internal/worker"
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

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
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

	vectorRepo := repository.NewVectorRepository(db)

	extractor := pipeline.NewPopplerExtractor()
	normalizer := pipeline.NewTextNormalizer()
	qualityFilter := pipeline.NewQualityFilter()
	segmenter := pipeline.NewDocumentSegmenter(450, 80)
	deduplicator := pipeline.NewChunkDeduplicator(vectorRepo, 10000)
	openRouterClient := pipeline.NewOpenRouterClient(cfg.OpenRouterAPIKey, cfg.OpenRouterBaseURL)
	vectorizer := pipeline.NewVectorizationScheduler(cfg.EmbeddingDimension, cfg.EmbeddingModelVersion, openRouterClient)
	nlpPipeline := nlp.NewNLPPipeline(nil, nil, 64, 16)

	pipelineWorker := worker.NewPipelineWorker(
		storageRepo,
		vectorRepo,
		extractor,
		normalizer,
		qualityFilter,
		segmenter,
		deduplicator,
		vectorizer,
		nlpPipeline,
	)

	subscriber, err := repository.NewEventSubscriber(
		cfg.NatsURL,
		cfg.NatsStream,
		cfg.NatsSubject,
		cfg.NatsConsumer,
		nil,
		cfg.NatsMaxDeliveries,
		cfg.WorkerConcurrency,
	)
	if err != nil {
		slog.Warn("failed to initialize jetstream event subscriber", "error", err)
	}

	if subscriber != nil {
		jsContext := subscriber.GetJetStreamContext()
		dlqPublisher := repository.NewDLQPublisher(jsContext, cfg.NatsDLQSubject)

		subscriber, err = repository.NewEventSubscriber(
			cfg.NatsURL,
			cfg.NatsStream,
			cfg.NatsSubject,
			cfg.NatsConsumer,
			dlqPublisher,
			cfg.NatsMaxDeliveries,
			cfg.WorkerConcurrency,
		)
		if err != nil {
			slog.Error("failed to re-initialize subscriber with dlq publisher", "error", err)
			os.Exit(1)
		}

		err = subscriber.Subscribe(cfg.NatsSubject, cfg.NatsConsumer, pipelineWorker.ProcessDocument)
		if err != nil {
			slog.Error("failed to subscribe to jetstream subject", "error", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"pipe"}`))
	})

	mux.HandleFunc("GET /pdf-check", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path, err := exec.LookPath("pdftotext")
		if err != nil {
			slog.Error("pdftotext binary lookup failed", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf(`{"status":"error","message":"pdftotext not found: %v"}`, err)))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"status":"ok","pdftotext_path":"%s"}`, path)))
	})

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
		slog.Info("Pipe worker service listening", "port", cfg.Port, "concurrency", cfg.WorkerConcurrency)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stopSignal
	slog.Info("Shutting down pipe worker service gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if subscriber != nil {
		subscriber.Unsubscribe()
	}

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

	slog.Info("Pipe service stopped.")
}
