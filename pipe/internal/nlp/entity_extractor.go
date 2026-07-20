package nlp

import (
	"context"
	"fmt"
	"strings"

	"github.com/jdkato/prose/v2"
	"go.opentelemetry.io/otel"
)

type Entity struct {
	Text     string `json:"text"`
	Category string `json:"category"`
}

type EntityExtractor struct{}

func NewEntityExtractor() *EntityExtractor {
	return &EntityExtractor{}
}

func (e *EntityExtractor) ExtractEntities(ctx context.Context, text string) ([]Entity, error) {
	tracer := otel.Tracer("nlp.entity_extractor")
	_, span := tracer.Start(ctx, "EntityExtractor.ExtractEntities")
	defer span.End()

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, nil
	}

	doc, err := prose.NewDocument(trimmed)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("prose statistical ner parsing failed: %w", err)
	}

	entityMap := make(map[string]string)

	for _, ent := range doc.Entities() {
		cleanText := strings.TrimSpace(ent.Text)
		if cleanText != "" {
			entityMap[cleanText] = ent.Label
		}
	}

	var entities []Entity
	for txt, label := range entityMap {
		entities = append(entities, Entity{
			Text:     txt,
			Category: label,
		})
	}

	return entities, nil
}
