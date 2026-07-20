package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/oyamo/rag-pipe/ingestion/internal/service"
	"go.opentelemetry.io/otel"
)

type DocumentHandler struct {
	service *service.IngestionService
}

func NewDocumentHandler(service *service.IngestionService) *DocumentHandler {
	return &DocumentHandler{service: service}
}

func (h *DocumentHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/documents", h.UploadDocument)
}

func (h *DocumentHandler) UploadDocument(w http.ResponseWriter, r *http.Request) {
	tracer := otel.Tracer("handler.document")
	ctx, span := tracer.Start(r.Context(), "DocumentHandler.UploadDocument")
	defer span.End()

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		span.RecordError(err)
		slog.Error("failed to parse multipart form", "error", err)
		h.writeErrorResponse(w, http.StatusBadRequest, "failed to parse multipart form")
		return
	}

	name := r.FormValue("name")
	if name == "" {
		slog.Warn("document upload validation failed: missing name")
		h.writeErrorResponse(w, http.StatusBadRequest, "name is required")
		return
	}

	description := r.FormValue("description")

	file, header, err := r.FormFile("file")
	if err != nil {
		span.RecordError(err)
		slog.Warn("document upload validation failed: missing file", "error", err)
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
		h.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.Info("document ingested successfully", "document_id", doc.ID.String(), "file_key", doc.FileKey)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(doc)
}

func (h *DocumentHandler) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
