package nlp

import (
	"context"
	"log/slog"

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

func (p *NLPPipeline) EnrichChunk(ctx context.Context, chunk *domain.Chunk) bool {
	tracer := otel.Tracer("nlp.pipeline")
	ctx, span := tracer.Start(ctx, "NLPPipeline.EnrichChunk")
	defer span.End()

	normalizedText := p.normalizer.NormalizeText(ctx, chunk.Content)
	if normalizedText == "" {
		return false
	}
	chunk.Content = normalizedText

	qualityScore := p.qualityFilter.CalculateQualityScore(ctx, normalizedText)
	if qualityScore < 0.25 {
		slog.Debug("chunk rejected by nlp quality score", "chunk_id", chunk.ID.String(), "score", qualityScore)
		return false
	}

	docProfile := p.profiler.ProfileDocument(ctx, normalizedText)
	keywords := p.kwExtractor.ExtractKeywords(ctx, normalizedText, 8)

	entities, err := p.entityExtract.ExtractEntities(ctx, normalizedText)
	if err != nil {
		slog.Warn("statistical ner extraction failed", "error", err)
	}

	_ = entities

	var kwStrings []string
	for _, kw := range keywords {
		kwStrings = append(kwStrings, kw.Keyword)
	}

	chunk.Metadata.Language = docProfile.Language
	chunk.Metadata.TokenCount = docProfile.TokenCount

	sig := p.lsh.ComputeSignature(normalizedText)
	isNearDup, existingVecID := p.lsh.FindNearDuplicate(ctx, sig)
	if isNearDup {
		slog.Debug("chunk identified as near-duplicate by minhash lsh", "chunk_id", chunk.ID.String(), "existing_vector_id", existingVecID.String())
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
