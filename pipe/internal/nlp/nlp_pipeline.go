package nlp

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

type NLPPipeline struct {
	normalizer     *UnicodeNormalizer
	profiler       *LanguageProfiler
	sentenceSplit  *SentenceSplitter
	structureParse *StructureParser
	qualityFilter  *QualityAndNoiseFilter
	kwExtractor    *KeywordExtractor
	entityExtract  *EntityExtractor
	lsh            *MinHashLSH
}

func NewNLPPipeline(stopWords map[string]bool, honorifics map[string]bool, lshHashes, lshBands int) *NLPPipeline {
	return &NLPPipeline{
		normalizer:     NewUnicodeNormalizer(),
		profiler:       NewLanguageProfiler(),
		sentenceSplit:  NewSentenceSplitter(honorifics),
		structureParse: NewStructureParser(),
		qualityFilter:  NewQualityAndNoiseFilter(stopWords),
		kwExtractor:    NewKeywordExtractor(stopWords),
		entityExtract:  NewEntityExtractor(),
		lsh:            NewMinHashLSH(lshHashes, lshBands),
	}
}

func (p *NLPPipeline) EnrichChunksParallel(ctx context.Context, rawChunks []domain.Chunk) []domain.Chunk {
	if len(rawChunks) == 0 {
		return nil
	}

	tracer := otel.Tracer("nlp.pipeline")
	ctx, span := tracer.Start(ctx, "NLPPipeline.EnrichChunksParallel")
	defer span.End()

	results := make([]*domain.Chunk, len(rawChunks))
	var wg sync.WaitGroup

	for i := range rawChunks {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			chk := rawChunks[idx]
			if p.EnrichChunk(ctx, &chk) {
				results[idx] = &chk
			}
		}(i)
	}

	wg.Wait()

	var enriched []domain.Chunk
	for _, res := range results {
		if res != nil {
			enriched = append(enriched, *res)
		}
	}

	return enriched
}

func (p *NLPPipeline) EnrichChunk(ctx context.Context, chunk *domain.Chunk) bool {
	normalizedText := p.normalizer.NormalizeText(ctx, chunk.Content)
	if normalizedText == "" {
		return false
	}
	chunk.Content = normalizedText

	qualityScore := p.qualityFilter.CalculateQualityScore(ctx, normalizedText)
	if qualityScore < 0.25 {
		return false
	}

	docProfile := p.profiler.ProfileDocument(ctx, normalizedText)
	keywords := p.kwExtractor.ExtractKeywords(ctx, normalizedText, 8)

	entities, err := p.entityExtract.ExtractEntities(ctx, normalizedText)
	if err != nil {
		slog.Warn("statistical ner extraction failed", "error", err)
	}

	var entStrings []string
	for _, ent := range entities {
		entStrings = append(entStrings, fmt.Sprintf("%s:%s", ent.Category, ent.Text))
	}

	var kwStrings []string
	for _, kw := range keywords {
		kwStrings = append(kwStrings, kw.Keyword)
	}

	chunk.Metadata.Language = docProfile.Language
	chunk.Metadata.TokenCount = docProfile.TokenCount
	chunk.Metadata.Entities = entStrings
	chunk.Metadata.Keywords = kwStrings

	sig := p.lsh.ComputeSignature(normalizedText)
	isNearDup, existingVecID := p.lsh.FindNearDuplicate(ctx, sig)
	if isNearDup {
		chunk.VectorID = existingVecID
	}

	return true
}

func (p *NLPPipeline) RegisterChunkVector(text string, vectorID uuid.UUID) {
	sig := p.lsh.ComputeSignature(text)
	p.lsh.IndexSignature(sig, vectorID)
}

func (p *NLPPipeline) GetUnicodeNormalizer() *UnicodeNormalizer {
	return p.normalizer
}
