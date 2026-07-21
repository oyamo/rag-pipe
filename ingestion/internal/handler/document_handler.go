package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/oyamo/rag-pipe/ingestion/internal/service"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

type DocumentHandler struct {
	service        *service.IngestionService
	requestCounter metric.Int64Counter
	latencyHisto   metric.Float64Histogram
}

func NewDocumentHandler(service *service.IngestionService) *DocumentHandler {
	meter := otel.GetMeterProvider().Meter("ingestion-service")
	reqCounter, _ := meter.Int64Counter("http_requests_total", metric.WithDescription("Total HTTP requests processed"))
	latHisto, _ := meter.Float64Histogram("http_request_duration_seconds", metric.WithDescription("HTTP request latency in seconds"), metric.WithUnit("s"))

	return &DocumentHandler{
		service:        service,
		requestCounter: reqCounter,
		latencyHisto:   latHisto,
	}
}

func (h *DocumentHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/documents", h.UploadDocument)
}

func (h *DocumentHandler) UploadDocument(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	tracer := otel.Tracer("handler.document")
	ctx, span := tracer.Start(ctx, "DocumentHandler.UploadDocument")
	defer span.End()

	defer func() {
		durationSec := time.Since(t0).Seconds()
		if h.latencyHisto != nil {
			h.latencyHisto.Record(ctx, durationSec, metric.WithAttributes(attribute.String("handler", "UploadDocument")))
		}
	}()

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		span.RecordError(err)
		slog.Error("failed to parse multipart form", "error", err)
		h.recordMetrics(ctx, http.StatusBadRequest)
		h.writeErrorResponse(w, http.StatusBadRequest, "failed to parse multipart form")
		return
	}

	name := r.FormValue("name")
	if name == "" {
		slog.Warn("document upload validation failed: missing name")
		h.recordMetrics(ctx, http.StatusBadRequest)
		h.writeErrorResponse(w, http.StatusBadRequest, "name is required")
		return
	}

	description := r.FormValue("description")

	file, header, err := r.FormFile("file")
	if err != nil {
		span.RecordError(err)
		slog.Warn("document upload validation failed: missing file", "error", err)
		h.recordMetrics(ctx, http.StatusBadRequest)
		h.writeErrorResponse(w, http.StatusBadRequest, "file field is required")
		return
	}
	defer file.Close()

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/pdf"
	}

	doc, err := h.service.ProcessAndIngest(ctx, name, description, contentType, header.Size, file)
	if err != nil {
		span.RecordError(err)
		slog.Error("document ingestion failed", "name", name, "error", err)
		h.recordMetrics(ctx, http.StatusInternalServerError)
		h.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.recordMetrics(ctx, http.StatusCreated)
	slog.Info("document ingested successfully", "document_id", doc.ID.String(), "file_key", doc.FileKey)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(doc)
}

func (h *DocumentHandler) recordMetrics(ctx context.Context, statusCode int) {
	if h.requestCounter != nil {
		h.requestCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("handler", "UploadDocument"),
			attribute.Int("status_code", statusCode),
		))
	}
}

func (h *DocumentHandler) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
