package pipeline

import (
	"context"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel"
	"golang.org/x/text/unicode/norm"
)

type TextNormalizer struct{}

func NewTextNormalizer() *TextNormalizer {
	return &TextNormalizer{}
}

func (n *TextNormalizer) NormalizeLines(ctx context.Context, inputLines []ExtractedLine) []ExtractedLine {
	_, span := otel.Tracer("pipeline.normalizer").Start(ctx, "TextNormalizer.NormalizeLines")
	defer span.End()

	if len(inputLines) == 0 {
		return inputLines
	}

	pageLineCounts := make(map[string]int)
	totalPages := 0

	for _, item := range inputLines {
		if item.PageNumber > totalPages {
			totalPages = item.PageNumber
		}

		trimmed := strings.TrimSpace(item.Text)
		if len(trimmed) <= 5 || len(trimmed) >= 120 {
			continue
		}

		pageLineCounts[trimmed]++
	}

	recurringHeaders := make(map[string]bool)
	if totalPages > 1 {
		threshold := totalPages / 2
		if threshold < 2 {
			threshold = 2
		}

		for line, count := range pageLineCounts {
			if count >= threshold {
				recurringHeaders[line] = true
			}
		}
	}

	var normalized []ExtractedLine
	for _, item := range inputLines {
		trimmed := strings.TrimSpace(item.Text)
		if recurringHeaders[trimmed] {
			continue
		}

		cleaned := norm.NFKC.String(item.Text)

		var sb strings.Builder
		for _, r := range cleaned {
			if unicode.IsControl(r) && r != '\n' && r != '\t' {
				continue
			}
			sb.WriteRune(r)
		}

		words := strings.Fields(sb.String())
		normalized = append(normalized, ExtractedLine{
			Text:       strings.Join(words, " "),
			PageNumber: item.PageNumber,
		})
	}

	return normalized
}

func (n *TextNormalizer) CleanChunkText(text string) string {
	cleaned := norm.NFKC.String(text)

	var sb strings.Builder
	for _, r := range cleaned {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			continue
		}
		sb.WriteRune(r)
	}

	lines := strings.Split(sb.String(), "\n")
	var nonBlankLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		words := strings.Fields(trimmed)
		nonBlankLines = append(nonBlankLines, strings.Join(words, " "))
	}

	return strings.Join(nonBlankLines, "\n")
}
