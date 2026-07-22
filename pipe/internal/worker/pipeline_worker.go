package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

	// Deduplicate raw chunks via Redis batch MGET in 1 roundtrip (< 1ms)
	dedupCtx, dedupSpan := tracer.Start(ctx, "PipelineWorker.ChunkDeduplication")
	t0 = time.Now()

	chunkPtrs := make([]*domain.Chunk, len(rawChunks))
	for i := range rawChunks {
		chunkPtrs[i] = &rawChunks[i]
	}

	dedupMap, err := w.deduplicator.DeduplicateChunkBatch(dedupCtx, chunkPtrs)
	if err != nil {
		dedupSpan.End()
		span.RecordError(err)
		return fmt.Errorf("deduplication batch error: %w", err)
	}

	var chunksToEmbed []domain.Chunk
	var finalChunks []domain.Chunk
	dedupCount := 0

	for _, chk := range rawChunks {
		if vectorID, isDuplicate := dedupMap[chk.Hash]; isDuplicate {
			chk.VectorID = vectorID
			finalChunks = append(finalChunks, chk)
			dedupCount++
		} else {
			chunksToEmbed = append(chunksToEmbed, chk)
		}
	}
	dedupDuration := time.Since(t0).Milliseconds()
	dedupSpan.SetAttributes(attribute.Int64("dedup.duration_ms", dedupDuration), attribute.Int("dedup.duplicates_found", dedupCount))
	dedupSpan.End()

	var (
		enrichedChunks []domain.Chunk
		newVectors     []domain.Vector
		vecErr         error
		nlpDuration    int64
		vecDuration    int64
		wg             sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		nlpCtx, nlpSpan := tracer.Start(ctx, "PipelineWorker.NLPProfilingParallel")
		tNLP := time.Now()
		enrichedChunks = w.nlpPipeline.EnrichChunksParallel(nlpCtx, chunksToEmbed)
		nlpDuration = time.Since(tNLP).Milliseconds()
		nlpSpan.SetAttributes(attribute.Int64("nlp.duration_ms", nlpDuration), attribute.Int("nlp.enriched_chunks", len(enrichedChunks)))
		nlpSpan.End()
	}()

	if len(chunksToEmbed) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vecCtx, vecSpan := tracer.Start(ctx, "PipelineWorker.GenerateOpenRouterEmbeddingsParallel")
			tVec := time.Now()
			newVectors, vecErr = w.vectorizer.GenerateBatchEmbeddings(vecCtx, chunksToEmbed)
			vecDuration = time.Since(tVec).Milliseconds()
			vecSpan.SetAttributes(attribute.Int64("vectorization.duration_ms", vecDuration), attribute.Int("vectorization.vectors_generated", len(newVectors)))
			vecSpan.End()
		}()
	}

	wg.Wait()

	if vecErr != nil {
		span.RecordError(vecErr)
		return fmt.Errorf("vectorization error: %w", vecErr)
	}

	if len(enrichedChunks) > 0 {
		for i := range enrichedChunks {
			if i < len(newVectors) {
				enrichedChunks[i].VectorID = newVectors[i].ID
				w.deduplicator.RegisterVectorHash(enrichedChunks[i].Hash, newVectors[i].ID)
				w.nlpPipeline.RegisterChunkVector(enrichedChunks[i].Content, newVectors[i].ID)
			}
			finalChunks = append(finalChunks, enrichedChunks[i])
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
