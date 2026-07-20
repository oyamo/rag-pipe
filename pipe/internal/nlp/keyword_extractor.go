package nlp

import (
	"context"
	"sort"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel"
)

type KeywordScore struct {
	Keyword string  `json:"keyword"`
	Score   float64 `json:"score"`
}

type KeywordExtractor struct {
	stopWords map[string]bool
}

func NewKeywordExtractor(customStopWords map[string]bool) *KeywordExtractor {
	return &KeywordExtractor{
		stopWords: customStopWords,
	}
}

func (k *KeywordExtractor) ExtractKeywords(ctx context.Context, text string, topN int) []KeywordScore {
	_, span := otel.Tracer("nlp.keyword_extractor").Start(ctx, "KeywordExtractor.ExtractKeywords")
	defer span.End()

	if topN <= 0 {
		return nil
	}

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}

	sentences := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == '.' || r == '!' || r == '?' || r == ';' || r == '\n' || r == ','
	})

	var candidatePhrases []string
	for _, sent := range sentences {
		words := strings.Fields(sent)
		var currentPhrase []string

		for _, w := range words {
			cleaned := strings.ToLower(strings.TrimFunc(w, func(r rune) bool {
				return unicode.IsPunct(r) || unicode.IsSymbol(r)
			}))

			if cleaned == "" {
				continue
			}

			isStop := len(cleaned) <= 2
			if k.stopWords != nil && k.stopWords[cleaned] {
				isStop = true
			}

			if isStop {
				if len(currentPhrase) > 0 {
					candidatePhrases = append(candidatePhrases, strings.Join(currentPhrase, " "))
					currentPhrase = nil
				}
				continue
			}

			currentPhrase = append(currentPhrase, cleaned)
		}

		if len(currentPhrase) > 0 {
			candidatePhrases = append(candidatePhrases, strings.Join(currentPhrase, " "))
		}
	}

	wordFreq := make(map[string]int)
	wordDegree := make(map[string]int)

	for _, phrase := range candidatePhrases {
		words := strings.Fields(phrase)
		degree := len(words) - 1

		for _, w := range words {
			wordFreq[w]++
			wordDegree[w] += degree
		}
	}

	wordScores := make(map[string]float64)
	for w, freq := range wordFreq {
		if freq == 0 {
			continue
		}
		wordScores[w] = float64(wordDegree[w]+freq) / float64(freq)
	}

	phraseScores := make(map[string]float64)
	for _, phrase := range candidatePhrases {
		words := strings.Fields(phrase)
		score := 0.0
		for _, w := range words {
			score += wordScores[w]
		}
		phraseScores[phrase] = score
	}

	var results []KeywordScore
	for phrase, score := range phraseScores {
		results = append(results, KeywordScore{
			Keyword: phrase,
			Score:   score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > topN {
		results = results[:topN]
	}

	return results
}
