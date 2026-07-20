package nlp

import (
	"context"
	"strings"
	"unicode"

	"github.com/pemistahl/lingua-go"
	"go.opentelemetry.io/otel"
)

type DocumentProfile struct {
	Language         string  `json:"language"`
	Confidence       float64 `json:"confidence"`
	SentenceCount    int     `json:"sentence_count"`
	AvgSentenceLen   float64 `json:"avg_sentence_len"`
	LexicalDiversity float64 `json:"lexical_diversity"`
	DigitRatio       float64 `json:"digit_ratio"`
	UppercaseRatio   float64 `json:"uppercase_ratio"`
	TokenCount       int     `json:"token_count"`
}

type LanguageProfiler struct {
	detector lingua.LanguageDetector
}

func NewLanguageProfiler() *LanguageProfiler {
	detector := lingua.NewLanguageDetectorBuilder().
		FromAllLanguages().
		Build()

	return &LanguageProfiler{
		detector: detector,
	}
}

func (p *LanguageProfiler) ProfileDocument(ctx context.Context, text string) *DocumentProfile {
	_, span := otel.Tracer("nlp.profiler").Start(ctx, "LanguageProfiler.ProfileDocument")
	defer span.End()

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return &DocumentProfile{
			Language:   "en",
			Confidence: 0.0,
		}
	}

	langStr := "en"
	confidence := 0.0
	detectedLang, exists := p.detector.DetectLanguageOf(trimmed)
	if exists {
		langStr = strings.ToLower(detectedLang.IsoCode639_1().String())
		confidence = p.detector.ComputeLanguageConfidence(trimmed, detectedLang)
	}

	words := strings.Fields(trimmed)
	tokenCount := int(float64(len(words)) * 1.3)
	uniqueWords := make(map[string]bool)
	digitChars := 0
	upperChars := 0
	totalChars := 0

	for _, w := range words {
		cleaned := strings.ToLower(w)
		uniqueWords[cleaned] = true
	}

	for _, r := range trimmed {
		totalChars++
		if unicode.IsDigit(r) {
			digitChars++
		}
		if unicode.IsUpper(r) {
			upperChars++
		}
	}

	lexicalDiversity := 0.0
	if len(words) > 0 {
		lexicalDiversity = float64(len(uniqueWords)) / float64(len(words))
	}

	digitRatio := 0.0
	uppercaseRatio := 0.0
	if totalChars > 0 {
		digitRatio = float64(digitChars) / float64(totalChars)
		uppercaseRatio = float64(upperChars) / float64(totalChars)
	}

	sentences := strings.Split(trimmed, ".")
	sentenceCount := len(sentences)
	avgSentenceLen := 0.0
	if sentenceCount > 0 {
		avgSentenceLen = float64(len(words)) / float64(sentenceCount)
	}

	return &DocumentProfile{
		Language:         langStr,
		Confidence:       confidence,
		SentenceCount:    sentenceCount,
		AvgSentenceLen:   avgSentenceLen,
		LexicalDiversity: lexicalDiversity,
		DigitRatio:       digitRatio,
		UppercaseRatio:   uppercaseRatio,
		TokenCount:       tokenCount,
	}
}
