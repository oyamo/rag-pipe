package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/oyamo/rag-pipe/pipe/internal/domain"
	"go.opentelemetry.io/otel"
)

type DocumentSegmenter struct {
	targetTokenBudget int
	overlapTokens     int
}

func NewDocumentSegmenter(targetTokenBudget, overlapTokens int) *DocumentSegmenter {
	if targetTokenBudget <= 0 {
		targetTokenBudget = 450
	}
	if overlapTokens <= 0 {
		overlapTokens = 80
	}
	return &DocumentSegmenter{
		targetTokenBudget: targetTokenBudget,
		overlapTokens:     overlapTokens,
	}
}

func (s *DocumentSegmenter) EstimateTokenCount(text string) int {
	words := strings.Fields(text)
	return int(float64(len(words)) * 1.3)
}

func (s *DocumentSegmenter) SegmentDocument(ctx context.Context, docID string, lines []ExtractedLine) ([]domain.Chunk, error) {
	_, span := otel.Tracer("pipeline.segmenter").Start(ctx, "DocumentSegmenter.SegmentDocument")
	defer span.End()

	if len(lines) == 0 {
		return nil, nil
	}

	docUUID, err := uuid.Parse(docID)
	if err != nil {
		docUUID = uuid.New()
	}

	var paragraphs []paragraph
	var currentLines []string
	startPage := lines[0].PageNumber
	endPage := startPage

	for _, l := range lines {
		trimmed := strings.TrimSpace(l.Text)
		if trimmed == "" {
			if len(currentLines) > 0 {
				pText := strings.Join(currentLines, " ")
				paragraphs = append(paragraphs, paragraph{
					text:      pText,
					startPage: startPage,
					endPage:   endPage,
				})
				currentLines = nil
			}
			continue
		}
		if len(currentLines) == 0 {
			startPage = l.PageNumber
		}
		endPage = l.PageNumber
		currentLines = append(currentLines, trimmed)
	}

	if len(currentLines) > 0 {
		pText := strings.Join(currentLines, " ")
		paragraphs = append(paragraphs, paragraph{
			text:      pText,
			startPage: startPage,
			endPage:   endPage,
		})
	}

	var chunks []domain.Chunk
	chunkIndex := 0
	currentCharOffset := 0

	var buffer []paragraph
	bufferTokens := 0
	pIndex := 0

	for pIndex < len(paragraphs) {
		p := paragraphs[pIndex]
		pTokens := s.EstimateTokenCount(p.text)

		buffer = append(buffer, p)
		bufferTokens += pTokens

		if bufferTokens >= s.targetTokenBudget || pIndex == len(paragraphs)-1 {
			var chunkTextBuilder strings.Builder
			chunkStartPage := buffer[0].startPage
			chunkEndPage := buffer[len(buffer)-1].endPage

			for i, bp := range buffer {
				if i > 0 {
					chunkTextBuilder.WriteString("\n\n")
				}
				chunkTextBuilder.WriteString(bp.text)
			}

			chunkContent := chunkTextBuilder.String()
			tokenCount := s.EstimateTokenCount(chunkContent)
			contentLen := len(chunkContent)

			chunkID, err := uuid.NewV7()
			if err != nil {
				chunkID = uuid.New()
			}

			chunk := domain.Chunk{
				ID:         chunkID,
				DocumentID: docUUID,
				Index:      chunkIndex,
				Content:    chunkContent,
				Metadata: domain.ChunkMetadata{
					DocumentID:          docID,
					StartPage:           chunkStartPage,
					EndPage:             chunkEndPage,
					Language:            "en",
					ParserVersion:       "poppler-pdftotext-1.0",
					ExtractionTimestamp: time.Now().UTC(),
					StartCharOffset:     currentCharOffset,
					EndCharOffset:       currentCharOffset + contentLen,
					TokenCount:          tokenCount,
				},
				CreatedAt: time.Now().UTC(),
			}

			chunks = append(chunks, chunk)
			chunkIndex++
			currentCharOffset += contentLen

			overlapBufferTokens := 0
			var newBuffer []paragraph
			for i := len(buffer) - 1; i >= 0; i-- {
				toks := s.EstimateTokenCount(buffer[i].text)
				if overlapBufferTokens+toks <= s.overlapTokens {
					newBuffer = append([]paragraph{buffer[i]}, newBuffer...)
					overlapBufferTokens += toks
				} else {
					break
				}
			}

			buffer = newBuffer
			bufferTokens = overlapBufferTokens
		}

		pIndex++
	}

	return chunks, nil
}

type paragraph struct {
	text      string
	startPage int
	endPage   int
}
