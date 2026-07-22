package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"github.com/oyamo/rag-pipe/pipe/internal/nlp"
	"github.com/oyamo/rag-pipe/pipe/internal/pipeline"
	"github.com/oyamo/rag-pipe/pipe/internal/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	if event == nil || event.DocumentID == "" {
		return nil
	}

	tracer := otel.Tracer("worker.pipeline")
	ctx, span := tracer.Start(ctx, "PipelineWorker.ProcessDocument")
	defer span.End()

	slog.InfoContext(ctx, "starting pipeline processing", "document_id", event.DocumentID, "file_key", event.FileKey)

	downloadCtx, downloadSpan := tracer.Start(ctx, "PipelineWorker.FetchStorageStream")
	t0 := time.Now()
	objReader, err := w.storage.FetchObjectReader(downloadCtx, event.FileKey)
	downloadDuration := time.Since(t0).Milliseconds()
	downloadSpan.SetAttributes(attribute.Int64("download.duration_ms", downloadDuration))
	downloadSpan.End()

	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to fetch document stream: %w", err)
	}
	defer objReader.Close()

	extractCtx, extractSpan := tracer.Start(ctx, "PipelineWorker.PDFTextExtraction")
	t0 = time.Now()
	lineChan := make(chan pipeline.ExtractedLine, 500)
	var extractErr error

	go func() {
		extractErr = w.extractor.ExtractFromReader(extractCtx, objReader, lineChan)
	}()

	var extractedLines []pipeline.ExtractedLine
	for l := range lineChan {
		extractedLines = append(extractedLines, l)
	}
	extractDuration := time.Since(t0).Milliseconds()
	extractSpan.SetAttributes(attribute.Int64("extraction.duration_ms", extractDuration), attribute.Int("extraction.line_count", len(extractedLines)))
	extractSpan.End()

	if extractErr != nil {
		span.RecordError(extractErr)
		return fmt.Errorf("extraction error: %w", extractErr)
	}

	if len(extractedLines) == 0 {
		slog.WarnContext(ctx, "no text extracted from document", "document_id", event.DocumentID)
		return nil
	}

	normalizedLines := w.normalizer.NormalizeLines(ctx, extractedLines)

	segmentCtx, segmentSpan := tracer.Start(ctx, "PipelineWorker.DocumentSegmentation")
	t0 = time.Now()
	rawChunks, err := w.segmenter.SegmentDocument(segmentCtx, event.DocumentID, normalizedLines)
	segmentDuration := time.Since(t0).Milliseconds()
	segmentSpan.SetAttributes(attribute.Int64("segmentation.duration_ms", segmentDuration), attribute.Int("segmentation.chunk_count", len(rawChunks)))
	segmentSpan.End()

	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("segmentation error: %w", err)
	}

	if len(rawChunks) == 0 {
		return nil
	}

	nlpCtx, nlpSpan := tracer.Start(ctx, "PipelineWorker.NLPProfilingParallel")
	t0 = time.Now()
	enrichedChunks := w.nlpPipeline.EnrichChunksParallel(nlpCtx, rawChunks)
	nlpDuration := time.Since(t0).Milliseconds()
	nlpSpan.SetAttributes(attribute.Int64("nlp.duration_ms", nlpDuration), attribute.Int("nlp.enriched_chunks", len(enrichedChunks)))
	nlpSpan.End()

	if len(enrichedChunks) == 0 {
		slog.WarnContext(ctx, "all chunks filtered out by nlp quality pipeline", "document_id", event.DocumentID)
		return nil
	}

	dedupCtx, dedupSpan := tracer.Start(ctx, "PipelineWorker.ChunkDeduplication")
	t0 = time.Now()
	var chunksToEmbed []domain.Chunk
	var finalChunks []domain.Chunk
	dedupCount := 0

	for i := range enrichedChunks {
		chk := enrichedChunks[i]
		isDuplicate, existingVecID, hash, err := w.deduplicator.DeduplicateChunk(dedupCtx, &chk)
		if err != nil {
			dedupSpan.End()
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
	dedupDuration := time.Since(t0).Milliseconds()
	dedupSpan.SetAttributes(attribute.Int64("dedup.duration_ms", dedupDuration), attribute.Int("dedup.duplicates_found", dedupCount))
	dedupSpan.End()

	var newVectors []domain.Vector
	if len(chunksToEmbed) > 0 {
		vecCtx, vecSpan := tracer.Start(ctx, "PipelineWorker.GenerateOpenRouterEmbeddingsParallel")
		t0 = time.Now()
		newVectors, err = w.vectorizer.GenerateBatchEmbeddings(vecCtx, chunksToEmbed)
		vecDuration := time.Since(t0).Milliseconds()
		vecSpan.SetAttributes(attribute.Int64("vectorization.duration_ms", vecDuration), attribute.Int("vectorization.vectors_generated", len(newVectors)))
		vecSpan.End()

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

	dbCtx, dbSpan := tracer.Start(ctx, "PipelineWorker.BulkDatabaseSave")
	t0 = time.Now()
	err = w.vectorRepo.SaveBulk(dbCtx, newVectors, finalChunks)
	dbDuration := time.Since(t0).Milliseconds()
	dbSpan.SetAttributes(attribute.Int64("db.save_duration_ms", dbDuration), attribute.Int("db.rows_saved", len(finalChunks)))
	dbSpan.End()

	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("bulk database save error: %w", err)
	}

	slog.InfoContext(ctx, "document pipeline processing completed successfully",
		"document_id", event.DocumentID,
		"total_chunks", len(finalChunks),
		"deduplicated_chunks", dedupCount,
		"new_vectors", len(newVectors),
		"download_ms", downloadDuration,
		"extraction_ms", extractDuration,
		"segmentation_ms", segmentDuration,
		"nlp_ms", nlpDuration,
		"dedup_ms", dedupDuration,
		"db_save_ms", dbDuration,
	)

	return nil
}
