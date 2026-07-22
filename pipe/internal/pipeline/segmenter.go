package pipeline

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"github.com/tiktoken-go/tokenizer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type ChunkStrategy string

const (
	StrategyExact          ChunkStrategy = "exact"
	StrategyParagraph      ChunkStrategy = "paragraph"
	StrategySentence       ChunkStrategy = "sentence"
	StrategyCharacterBased ChunkStrategy = "character-based"
)

type textUnit struct {
	text      string
	startPage int
	endPage   int
}

type DocumentSegmenter struct {
	targetTokenBudget int
	overlapTokens     int
	strategy          ChunkStrategy
	enc               tokenizer.Codec
	encOnce           sync.Once
}

func NewDocumentSegmenter(targetTokenBudget, overlapTokens int, strategy string) *DocumentSegmenter {
	if targetTokenBudget <= 0 {
		targetTokenBudget = 450
	}
	if overlapTokens <= 0 {
		overlapTokens = 80
	}

	strat := ChunkStrategy(strings.ToLower(strategy))
	switch strat {
	case StrategyExact, StrategyParagraph, StrategySentence, StrategyCharacterBased:
	default:
		strat = StrategyParagraph
	}

	return &DocumentSegmenter{
		targetTokenBudget: targetTokenBudget,
		overlapTokens:     overlapTokens,
		strategy:          strat,
	}
}

func (s *DocumentSegmenter) getEncoder() tokenizer.Codec {
	s.encOnce.Do(func() {
		enc, err := tokenizer.Get(tokenizer.Cl100kBase)
		if err != nil {
			slog.Warn("failed to load embedded cl100k_base tokenizer codec", "error", err)
			return
		}
		s.enc = enc
	})
	return s.enc
}

func (s *DocumentSegmenter) CountTokens(text string) int {
	if strings.TrimSpace(text) == "" {
		return 0
	}

	enc := s.getEncoder()
	if enc == nil {
		return len(strings.Fields(text))
	}

	ids, _, err := enc.Encode(text)
	if err != nil {
		slog.Warn("tiktoken encoding failed, falling back to word count", "error", err)
		return len(strings.Fields(text))
	}

	return len(ids)
}

func (s *DocumentSegmenter) SegmentDocument(ctx context.Context, docID string, lines []ExtractedLine) ([]domain.Chunk, error) {
	if len(lines) == 0 {
		return nil, nil
	}

	_, span := otel.Tracer("pipeline.segmenter").Start(ctx, "DocumentSegmenter.SegmentDocument")
	defer span.End()

	span.SetAttributes(
		attribute.String("segmenter.strategy", string(s.strategy)),
		attribute.Int("segmenter.target_tokens", s.targetTokenBudget),
		attribute.Int("segmenter.overlap_tokens", s.overlapTokens),
	)

	docUUID, err := uuid.Parse(docID)
	if err != nil {
		docUUID = uuid.New()
	}

	switch s.strategy {
	case StrategyExact:
		return s.segmentExactTokens(docUUID, docID, lines)
	case StrategySentence:
		return s.segmentUnits(docUUID, docID, s.extractSentences(lines), "tiktoken-sentence-1.0")
	case StrategyCharacterBased:
		return s.segmentCharacterBased(docUUID, docID, lines)
	case StrategyParagraph:
		fallthrough
	default:
		return s.segmentUnits(docUUID, docID, s.extractParagraphs(lines), "tiktoken-paragraph-1.0")
	}
}

// 1. Exact BPE Token-Aware Chunking (tiktoken-go/tokenizer)
func (s *DocumentSegmenter) segmentExactTokens(docUUID uuid.UUID, docID string, lines []ExtractedLine) ([]domain.Chunk, error) {
	var sb strings.Builder
	for _, l := range lines {
		if txt := strings.TrimSpace(l.Text); txt != "" {
			sb.WriteString(txt)
			sb.WriteString(" ")
		}
	}

	fullText := sb.String()
	if strings.TrimSpace(fullText) == "" {
		return nil, nil
	}

	enc := s.getEncoder()
	if enc == nil {
		return s.segmentUnits(docUUID, docID, s.extractParagraphs(lines), "paragraph-1.0")
	}

	ids, _, err := enc.Encode(fullText)
	if err != nil || len(ids) == 0 {
		return s.segmentUnits(docUUID, docID, s.extractParagraphs(lines), "paragraph-1.0")
	}

	step := s.calcStep(s.targetTokenBudget, s.overlapTokens)
	var chunks []domain.Chunk

	for i, idx := 0, 0; i < len(ids); i += step {
		end := min(i+s.targetTokenBudget, len(ids))
		chunkIDs := ids[i:end]

		chunkText, err := enc.Decode(chunkIDs)
		if err != nil {
			chunkText = fullText
		}

		chunks = append(chunks, s.buildChunk(
			docUUID, docID, idx, chunkText,
			lines[0].PageNumber, lines[len(lines)-1].PageNumber,
			0, 0, "tiktoken-exact-1.0",
		))
		idx++
		if end == len(ids) {
			break
		}
	}
	return chunks, nil
}

// 2. Generic Unit-Based Chunking (Paragraphs & Sentences)
func (s *DocumentSegmenter) segmentUnits(docUUID uuid.UUID, docID string, units []textUnit, version string) ([]domain.Chunk, error) {
	if len(units) == 0 {
		return nil, nil
	}

	var chunks []domain.Chunk
	var buffer []textUnit
	bufferTokens, chunkIndex, offset := 0, 0, 0

	for i := 0; i < len(units); i++ {
		u := units[i]
		uToks := s.CountTokens(u.text)

		buffer = append(buffer, u)
		bufferTokens += uToks

		if bufferTokens < s.targetTokenBudget && i < len(units)-1 {
			continue
		}

		var sb strings.Builder
		for j, bu := range buffer {
			if j > 0 {
				sb.WriteString("\n\n")
			}
			sb.WriteString(bu.text)
		}

		content := sb.String()
		contentLen := len(content)

		chunks = append(chunks, s.buildChunk(
			docUUID, docID, chunkIndex, content,
			buffer[0].startPage, buffer[len(buffer)-1].endPage,
			offset, offset+contentLen, version,
		))

		chunkIndex++
		offset += contentLen
		buffer, bufferTokens = s.calcOverlapBuffer(buffer)
	}

	return chunks, nil
}

// 3. Character-Based Window Chunking
func (s *DocumentSegmenter) segmentCharacterBased(docUUID uuid.UUID, docID string, lines []ExtractedLine) ([]domain.Chunk, error) {
	var sb strings.Builder
	for _, l := range lines {
		if txt := strings.TrimSpace(l.Text); txt != "" {
			sb.WriteString(txt)
			sb.WriteString("\n")
		}
	}

	fullText := sb.String()
	if strings.TrimSpace(fullText) == "" {
		return nil, nil
	}

	charWindowSize := s.targetTokenBudget * 4
	step := s.calcStep(charWindowSize, s.overlapTokens*4)
	runes := []rune(fullText)
	var chunks []domain.Chunk

	for i, idx := 0, 0; i < len(runes); i += step {
		end := min(i+charWindowSize, len(runes))
		chunkText := string(runes[i:end])

		chunks = append(chunks, s.buildChunk(
			docUUID, docID, idx, chunkText,
			lines[0].PageNumber, lines[len(lines)-1].PageNumber,
			i, end, "character-based-1.0",
		))
		idx++
		if end == len(runes) {
			break
		}
	}

	return chunks, nil
}

// Helper Functions
func (s *DocumentSegmenter) extractParagraphs(lines []ExtractedLine) []textUnit {
	var units []textUnit
	var current []string
	startPage, endPage := lines[0].PageNumber, lines[0].PageNumber

	for _, l := range lines {
		trimmed := strings.TrimSpace(l.Text)
		if trimmed == "" {
			if len(current) > 0 {
				units = append(units, textUnit{text: strings.Join(current, " "), startPage: startPage, endPage: endPage})
				current = nil
			}
			continue
		}
		if len(current) == 0 {
			startPage = l.PageNumber
		}
		endPage = l.PageNumber
		current = append(current, trimmed)
	}

	if len(current) > 0 {
		units = append(units, textUnit{text: strings.Join(current, " "), startPage: startPage, endPage: endPage})
	}
	return units
}

func (s *DocumentSegmenter) extractSentences(lines []ExtractedLine) []textUnit {
	var units []textUnit
	replacer := strings.NewReplacer(". ", ".\n", "! ", "!\n", "? ", "?\n")

	for _, l := range lines {
		trimmed := strings.TrimSpace(l.Text)
		if trimmed == "" {
			continue
		}
		for _, st := range strings.Split(replacer.Replace(trimmed), "\n") {
			if stTrimmed := strings.TrimSpace(st); stTrimmed != "" {
				units = append(units, textUnit{text: stTrimmed, startPage: l.PageNumber, endPage: l.PageNumber})
			}
		}
	}
	return units
}

func (s *DocumentSegmenter) calcOverlapBuffer(buffer []textUnit) ([]textUnit, int) {
	overlapTokens := 0
	var newBuf []textUnit
	for i := len(buffer) - 1; i >= 0; i-- {
		toks := s.CountTokens(buffer[i].text)
		if overlapTokens+toks <= s.overlapTokens {
			newBuf = append([]textUnit{buffer[i]}, newBuf...)
			overlapTokens += toks
		} else {
			break
		}
	}
	return newBuf, overlapTokens
}

func (s *DocumentSegmenter) calcStep(budget, overlap int) int {
	step := budget - overlap
	if step <= 0 {
		return budget / 2
	}
	return step
}

func (s *DocumentSegmenter) buildChunk(docUUID uuid.UUID, docID string, index int, content string, startPage, endPage, startOffset, endOffset int, version string) domain.Chunk {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	return domain.Chunk{
		ID:         id,
		DocumentID: docUUID,
		Index:      index,
		Content:    content,
		Metadata: domain.ChunkMetadata{
			DocumentID:          docID,
			StartPage:           startPage,
			EndPage:             endPage,
			Language:            "en",
			ParserVersion:       version,
			ExtractionTimestamp: time.Now().UTC(),
			StartCharOffset:     startOffset,
			EndCharOffset:       endOffset,
			TokenCount:          s.CountTokens(content),
		},
		CreatedAt: time.Now().UTC(),
	}
}
