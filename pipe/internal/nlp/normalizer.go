package nlp

import (
	"context"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel"
	"golang.org/x/text/unicode/norm"
)

type UnicodeNormalizer struct{}

func NewUnicodeNormalizer() *UnicodeNormalizer {
	return &UnicodeNormalizer{}
}

func (n *UnicodeNormalizer) NormalizeText(ctx context.Context, text string) string {
	_, span := otel.Tracer("nlp.normalizer").Start(ctx, "UnicodeNormalizer.NormalizeText")
	defer span.End()

	if text == "" {
		return ""
	}

	normalized := norm.NFKC.String(text)

	var sb strings.Builder
	for _, r := range normalized {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			continue
		}
		sb.WriteRune(r)
	}

	lines := strings.Split(sb.String(), "\n")
	var cleanedLines []string

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimFunc(line, unicode.IsSpace)
		if trimmed == "" {
			continue
		}

		if strings.HasSuffix(trimmed, "-") && i+1 < len(lines) {
			nextLine := strings.TrimFunc(lines[i+1], unicode.IsSpace)
			if nextLine != "" {
				trimmed = strings.TrimSuffix(trimmed, "-") + nextLine
				i++
			}
		}

		words := strings.Fields(trimmed)
		cleanedLines = append(cleanedLines, strings.Join(words, " "))
	}

	return strings.Join(cleanedLines, "\n")
}
