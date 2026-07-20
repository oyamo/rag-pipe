package pipeline

import (
	"context"
	"math"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel"
)

type QualityFilter struct{}

func NewQualityFilter() *QualityFilter {
	return &QualityFilter{}
}

func (q *QualityFilter) IsHighQualityChunk(ctx context.Context, text string) bool {
	_, span := otel.Tracer("pipeline.quality_filter").Start(ctx, "QualityFilter.IsHighQualityChunk")
	defer span.End()

	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 20 {
		return false
	}

	alphaCount := 0
	digitCount := 0
	spaceCount := 0
	totalChars := len([]rune(trimmed))

	for _, r := range trimmed {
		if unicode.IsLetter(r) {
			alphaCount++
		} else if unicode.IsDigit(r) {
			digitCount++
		} else if unicode.IsSpace(r) {
			spaceCount++
		}
	}

	if totalChars == 0 {
		return false
	}

	alphaRatio := float64(alphaCount) / float64(totalChars)
	digitRatio := float64(digitCount) / float64(totalChars)

	if digitRatio > 0.4 && alphaRatio > 0.15 {
		return true
	}

	if alphaRatio < 0.35 {
		return false
	}

	entropy := q.calculateEntropy(trimmed)
	if entropy < 2.5 || entropy > 6.8 {
		return false
	}

	words := strings.Fields(trimmed)
	if len(words) < 5 {
		return false
	}

	lowerText := strings.ToLower(trimmed)
	if strings.Contains(lowerText, "all rights reserved") && len(words) < 15 {
		return false
	}
	if strings.Contains(lowerText, "confidential and proprietary") && len(words) < 15 {
		return false
	}

	return true
}

func (q *QualityFilter) calculateEntropy(s string) float64 {
	if len(s) == 0 {
		return 0.0
	}

	freq := make(map[rune]int)
	for _, r := range s {
		freq[r]++
	}

	total := float64(len([]rune(s)))
	entropy := 0.0

	for _, count := range freq {
		p := float64(count) / total
		entropy -= p * math.Log2(p)
	}

	return entropy
}
