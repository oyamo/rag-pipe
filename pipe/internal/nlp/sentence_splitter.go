package nlp

import (
	"context"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel"
)

type SentenceSplitter struct {
	honorifics map[string]bool
}

func NewSentenceSplitter(customHonorifics map[string]bool) *SentenceSplitter {
	return &SentenceSplitter{
		honorifics: customHonorifics,
	}
}

func (s *SentenceSplitter) SplitSentences(ctx context.Context, text string) []string {
	_, span := otel.Tracer("nlp.sentence_splitter").Start(ctx, "SentenceSplitter.SplitSentences")
	defer span.End()

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}

	runes := []rune(trimmed)
	length := len(runes)
	if length == 0 {
		return nil
	}

	var sentences []string
	start := 0

	for i := 0; i < length; i++ {
		r := runes[i]
		if r != '.' && r != '!' && r != '?' {
			continue
		}

		if i+1 < length && !unicode.IsSpace(runes[i+1]) {
			continue
		}

		segment := strings.TrimSpace(string(runes[start : i+1]))
		words := strings.Fields(segment)
		if len(words) > 0 && s.honorifics != nil {
			lastWord := strings.ToLower(strings.Trim(words[len(words)-1], ".!?,\""))
			if s.honorifics[lastWord] {
				continue
			}
		}

		sentences = append(sentences, segment)
		start = i + 1
	}

	if start < length {
		remainder := strings.TrimSpace(string(runes[start:]))
		if remainder != "" {
			sentences = append(sentences, remainder)
		}
	}

	return sentences
}
