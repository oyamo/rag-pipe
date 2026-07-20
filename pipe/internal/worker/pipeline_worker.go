package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"github.com/oyamo/rag-pipe/pipe/internal/nlp"
	"github.com/oyamo/rag-pipe/pipe/internal/pipeline"
	"github.com/oyamo/rag-pipe/pipe/internal/repository"
	"go.opentelemetry.io/otel"
)

type PipelineWorker struct {
	storage       *repository.StorageRepository
	vectorRepo    *repository.VectorRepository
	extractor     *pipeline.PopplerExtractor
	normalizer    *pipeline.TextNormalizer
	qualityFilter *pipeline.QualityFilter
	segmenter     *pipeline.DocumentSegmenter
	deduplicator  *pipeline.ChunkDeduplicator
	vectorizer    *pipeline.VectorizationScheduler
	nlpPipeline   *nlp.NLPPipeline
}

func NewPipelineWorker(
	storage *repository.StorageRepository,
	vectorRepo *repository.VectorRepository,
	extractor *pipeline.PopplerExtractor,
	normalizer *pipeline.TextNormalizer,
	qualityFilter *pipeline.QualityFilter,
	segmenter *pipeline.DocumentSegmenter,
	deduplicator *pipeline.ChunkDeduplicator,
	vectorizer *pipeline.VectorizationScheduler,
	nlpPipeline *nlp.NLPPipeline,
) *PipelineWorker {
	return &PipelineWorker{
		storage:       storage,
		vectorRepo:    vectorRepo,
		extractor:     extractor,
		normalizer:    normalizer,
		qualityFilter: qualityFilter,
		segmenter:     segmenter,
		deduplicator:  deduplicator,
		vectorizer:    vectorizer,
		nlpPipeline:   nlpPipeline,
	}
}

func (w *PipelineWorker) ProcessDocument(ctx context.Context, event *domain.IngestionEvent) error {
	tracer := otel.Tracer("worker.pipeline")
	ctx, span := tracer.Start(ctx, "PipelineWorker.ProcessDocument")
	defer span.End()

	slog.Info("starting pipeline processing for document", "document_id", event.DocumentID, "file_key", event.FileKey)

	tmpFilePath, cleanup, err := w.storage.DownloadTempFile(ctx, event.FileKey)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to download document from storage: %w", err)
	}
	defer cleanup()

	lineChan := make(chan pipeline.ExtractedLine, 500)
	var extractErr error

	go func() {
		extractErr = w.extractor.ExtractTextStream(ctx, tmpFilePath, lineChan)
	}()

	var extractedLines []pipeline.ExtractedLine
	for l := range lineChan {
		extractedLines = append(extractedLines, l)
	}

	if extractErr != nil {
		span.RecordError(extractErr)
		return fmt.Errorf("extraction error: %w", extractErr)
	}

	if len(extractedLines) == 0 {
		slog.Warn("no text extracted from document", "document_id", event.DocumentID)
		return nil
	}

	normalizedLines := w.normalizer.NormalizeLines(ctx, extractedLines)

	rawChunks, err := w.segmenter.SegmentDocument(ctx, event.DocumentID, normalizedLines)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("segmentation error: %w", err)
	}

	var enrichedChunks []domain.Chunk
	for i := range rawChunks {
		chk := rawChunks[i]
		if w.nlpPipeline.EnrichChunk(ctx, &chk) {
			enrichedChunks = append(enrichedChunks, chk)
		}
	}

	if len(enrichedChunks) == 0 {
		slog.Warn("all chunks filtered out by nlp quality pipeline", "document_id", event.DocumentID)
		return nil
	}

	var chunksToEmbed []domain.Chunk
	var finalChunks []domain.Chunk
	dedupCount := 0

	for i := range enrichedChunks {
		chk := enrichedChunks[i]
		isDuplicate, existingVecID, hash, err := w.deduplicator.DeduplicateChunk(ctx, &chk)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("deduplication error: %w", err)
		}

		chk.Hash = hash

		if isDuplicate {
			chk.VectorID = existingVecID
			finalChunks = append(finalChunks, chk)
			dedupCount++
		} else {
			chunksToEmbed = append(chunksToEmbed, chk)
		}
	}

	var newVectors []domain.Vector
	if len(chunksToEmbed) > 0 {
		newVectors, err = w.vectorizer.GenerateBatchEmbeddings(ctx, chunksToEmbed)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("vectorization error: %w", err)
		}

		for i := range chunksToEmbed {
			chunksToEmbed[i].VectorID = newVectors[i].ID
			w.deduplicator.RegisterVectorHash(chunksToEmbed[i].Hash, newVectors[i].ID)
			w.nlpPipeline.RegisterChunkVector(chunksToEmbed[i].Content, newVectors[i].ID)
			finalChunks = append(finalChunks, chunksToEmbed[i])
		}
	}

	err = w.vectorRepo.SaveBulk(ctx, newVectors, finalChunks)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("bulk database save error: %w", err)
	}

	slog.Info("document pipeline processing completed successfully",
		"document_id", event.DocumentID,
		"total_chunks", len(finalChunks),
		"deduplicated_chunks", dedupCount,
		"new_vectors", len(newVectors),
	)

	return nil
}
