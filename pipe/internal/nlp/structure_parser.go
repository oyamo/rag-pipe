package nlp

import (
	"context"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"go.opentelemetry.io/otel"
)

type ElementType string

const (
	ElementHeading      ElementType = "HEADING"
	ElementParagraph    ElementType = "PARAGRAPH"
	ElementBulletList   ElementType = "BULLET_LIST"
	ElementNumberedList ElementType = "NUMBERED_LIST"
	ElementTableRow     ElementType = "TABLE_ROW"
	ElementCodeBlock    ElementType = "CODE_BLOCK"
	ElementFootnote     ElementType = "FOOTNOTE"
)

type StructuredElement struct {
	Type           ElementType `json:"type"`
	Text           string      `json:"text"`
	SectionHeading string      `json:"section_heading,omitempty"`
	HeadingLevel   int         `json:"heading_level,omitempty"`
	LineNumber     int         `json:"line_number"`
}

type StructureParser struct {
	markdown goldmark.Markdown
}

func NewStructureParser() *StructureParser {
	return &StructureParser{
		markdown: goldmark.New(),
	}
}

func (p *StructureParser) ParseDocumentAST(ctx context.Context, fullText string) []StructuredElement {
	_, span := otel.Tracer("nlp.structure_parser").Start(ctx, "StructureParser.ParseDocumentAST")
	defer span.End()

	trimmed := strings.TrimSpace(fullText)
	if trimmed == "" {
		return nil
	}

	src := []byte(trimmed)
	reader := text.NewReader(src)
	doc := p.markdown.Parser().Parse(reader)

	var elements []StructuredElement
	currentHeading := ""
	lineCounter := 1

	ast.Walk(doc, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}

		headingNode, isHeading := node.(*ast.Heading)
		if isHeading {
			content := string(headingNode.Text(src))
			if content == "" {
				return ast.WalkContinue, nil
			}

			currentHeading = content
			elements = append(elements, StructuredElement{
				Type:           ElementHeading,
				Text:           content,
				SectionHeading: currentHeading,
				HeadingLevel:   headingNode.Level,
				LineNumber:     lineCounter,
			})
			lineCounter++
			return ast.WalkContinue, nil
		}

		codeNode, isCode := node.(*ast.FencedCodeBlock)
		if isCode {
			var lines []string
			for i := 0; i < codeNode.Lines().Len(); i++ {
				lineSeg := codeNode.Lines().At(i)
				lines = append(lines, string(lineSeg.Value(src)))
			}
			codeText := strings.Join(lines, "")
			if codeText == "" {
				return ast.WalkContinue, nil
			}

			elements = append(elements, StructuredElement{
				Type:           ElementCodeBlock,
				Text:           codeText,
				SectionHeading: currentHeading,
				LineNumber:     lineCounter,
			})
			lineCounter += len(lines)
			return ast.WalkContinue, nil
		}

		listItemNode, isListItem := node.(*ast.ListItem)
		if isListItem {
			content := string(listItemNode.Text(src))
			if content == "" {
				return ast.WalkContinue, nil
			}

			eType := ElementBulletList
			if listItemNode.Parent() != nil {
				listNode, isList := listItemNode.Parent().(*ast.List)
				if isList && listNode.IsOrdered() {
					eType = ElementNumberedList
				}
			}

			elements = append(elements, StructuredElement{
				Type:           eType,
				Text:           content,
				SectionHeading: currentHeading,
				LineNumber:     lineCounter,
			})
			lineCounter++
			return ast.WalkContinue, nil
		}

		paraNode, isPara := node.(*ast.Paragraph)
		if isPara {
			content := string(paraNode.Text(src))
			if content == "" {
				return ast.WalkContinue, nil
			}
			if paraNode.Parent() == nil || paraNode.Parent().Kind() != ast.KindDocument {
				return ast.WalkContinue, nil
			}

			elements = append(elements, StructuredElement{
				Type:           ElementParagraph,
				Text:           content,
				SectionHeading: currentHeading,
				LineNumber:     lineCounter,
			})
			lineCounter++
			return ast.WalkContinue, nil
		}

		return ast.WalkContinue, nil
	})

	if len(elements) > 0 {
		return elements
	}

	lines := strings.Split(trimmed, "\n")
	for idx, line := range lines {
		lTrim := strings.TrimSpace(line)
		if lTrim == "" {
			continue
		}

		elements = append(elements, StructuredElement{
			Type:           ElementParagraph,
			Text:           lTrim,
			SectionHeading: currentHeading,
			LineNumber:     idx + 1,
		})
	}

	return elements
}
