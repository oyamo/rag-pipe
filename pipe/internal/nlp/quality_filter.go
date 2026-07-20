package nlp

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"math"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel"
)

type QualityAndNoiseFilter struct {
	stopWords map[string]bool
}

func NewQualityAndNoiseFilter(stopWords map[string]bool) *QualityAndNoiseFilter {
	return &QualityAndNoiseFilter{
		stopWords: stopWords,
	}
}

func (f *QualityAndNoiseFilter) CalculateQualityScore(ctx context.Context, text string) float64 {
	_, span := otel.Tracer("nlp.quality_filter").Start(ctx, "QualityAndNoiseFilter.CalculateQualityScore")
	defer span.End()

	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 20 {
		return 0.0
	}

	words := strings.Fields(trimmed)
	if len(words) < 4 {
		return 0.0
	}

	alphaCount := 0
	stopWordCount := 0

	for _, w := range words {
		cleaned := strings.ToLower(strings.Trim(w, ".,!?;:\"'()[]{}"))
		if f.stopWords != nil && f.stopWords[cleaned] {
			stopWordCount++
		}
	}

	for _, r := range trimmed {
		if unicode.IsLetter(r) {
			alphaCount++
		}
	}

	totalRunes := float64(len([]rune(trimmed)))
	if totalRunes == 0 {
		return 0.0
	}

	alphaRatio := float64(alphaCount) / totalRunes
	stopWordRatio := 0.0
	if len(words) > 0 {
		stopWordRatio = float64(stopWordCount) / float64(len(words))
	}
	entropy := f.calculateEntropy(trimmed)

	if alphaRatio < 0.35 {
		return 0.1
	}

	if entropy < 2.2 || entropy > 6.8 {
		return 0.2
	}

	score := (alphaRatio * 0.4) + (stopWordRatio * 0.4) + ((entropy / 7.0) * 0.2)
	if score > 1.0 {
		score = 1.0
	}

	return score
}

func (f *QualityAndNoiseFilter) FilterBoilerplateParagraphs(ctx context.Context, paragraphs []string, thresholdRatio float64) []string {
	_, span := otel.Tracer("nlp.quality_filter").Start(ctx, "QualityAndNoiseFilter.FilterBoilerplateParagraphs")
	defer span.End()

	if len(paragraphs) == 0 {
		return nil
	}

	hashCounts := make(map[string]int)
	for _, p := range paragraphs {
		trimmed := strings.TrimSpace(p)
		if len(trimmed) <= 10 {
			continue
		}
		hash := f.hashParagraph(trimmed)
		hashCounts[hash]++
	}

	maxAllowedOccurrences := int(float64(len(paragraphs)) * thresholdRatio)
	if maxAllowedOccurrences < 2 {
		maxAllowedOccurrences = 2
	}

	var filtered []string
	for _, p := range paragraphs {
		trimmed := strings.TrimSpace(p)
		if len(trimmed) > 10 {
			hash := f.hashParagraph(trimmed)
			if hashCounts[hash] >= maxAllowedOccurrences {
				continue
			}
		}
		filtered = append(filtered, p)
	}

	return filtered
}

func (f *QualityAndNoiseFilter) hashParagraph(p string) string {
	cleaned := strings.ToLower(strings.Join(strings.Fields(p), " "))
	h := md5.Sum([]byte(cleaned))
	return hex.EncodeToString(h[:])
}

func (f *QualityAndNoiseFilter) calculateEntropy(s string) float64 {
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
