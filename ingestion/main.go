package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	telProvider, err := telemetry.InitTelemetry(cfg.OTelServiceName)
	if err != nil {
		log.Fatalf("failed to initialize telemetry: %v", err)
	}

	db, err := sql.Open("postgres", cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Printf("warning: database ping failed: %v", err)
	}

	storageRepo, err := repository.NewStorageRepository(
		cfg.MinioEndpoint,
		cfg.MinioAccessKey,
		cfg.MinioSecretKey,
		cfg.MinioBucket,
		cfg.MinioUseSSL,
	)
	if err != nil {
		log.Fatalf("failed to initialize storage repository: %v", err)
	}

	publisher, err := repository.NewEventPublisher(cfg.NatsURL, cfg.NatsTopic)
	if err != nil {
		log.Printf("warning: failed to connect to nats: %v", err)
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
		log.Printf("Ingestion service listening on port %s...", cfg.Port)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-stopSignal
	log.Println("Shutting down ingestion service gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("server shutdown error: %v", err)
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

	log.Println("Ingestion service stopped.")
}
