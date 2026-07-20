package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/oyamo/rag-pipe/inference/internal/domain"
	"github.com/oyamo/rag-pipe/inference/internal/service"
)

type InferenceHandler struct {
	service *service.InferenceService
}

func NewInferenceHandler(service *service.InferenceService) *InferenceHandler {
	return &InferenceHandler{service: service}
}

func (h *InferenceHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/query", h.HandleQuery)
}

func (h *InferenceHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	var req domain.QueryRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		slog.Error("failed to decode query request payload", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status":"error","message":"invalid JSON payload"}`))
		return
	}

	if req.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status":"error","message":"field 'query' is required"}`))
		return
	}

	resp, err := h.service.QueryRAG(ctx, &req)
	if err != nil {
		slog.Error("inference RAG query execution failed", "query", req.Query, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"status":"error","message":"query processing failed"}`))
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
