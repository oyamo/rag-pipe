package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"go.opentelemetry.io/otel"
)

type ExtractedLine struct {
	Text       string
	PageNumber int
}

type PopplerExtractor struct{}

func NewPopplerExtractor() *PopplerExtractor {
	return &PopplerExtractor{}
}

func (e *PopplerExtractor) ExtractTextStream(ctx context.Context, pdfFilePath string, lineChan chan<- ExtractedLine) error {
	tracer := otel.Tracer("pipeline.extractor")
	ctx, span := tracer.Start(ctx, "PopplerExtractor.ExtractTextStream")
	defer span.End()
	defer close(lineChan)

	cmd := exec.CommandContext(ctx, "pdftotext", "-layout", pdfFilePath, "-")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create stdout pipe for pdftotext: %w", err)
	}

	err = cmd.Start()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to start pdftotext command: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	currentPage := 1

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "\f") {
			parts := strings.Split(line, "\f")
			for i, p := range parts {
				if strings.TrimSpace(p) != "" {
					lineChan <- ExtractedLine{Text: p, PageNumber: currentPage}
				}
				if i < len(parts)-1 {
					currentPage++
				}
			}
			continue
		}

		lineChan <- ExtractedLine{Text: line, PageNumber: currentPage}
	}

	err = scanner.Err()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("error reading pdftotext stdout: %w", err)
	}

	err = cmd.Wait()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("pdftotext execution failed: %w", err)
	}

	return nil
}

func (e *PopplerExtractor) ExtractFromReader(ctx context.Context, reader io.Reader, lineChan chan<- ExtractedLine) error {
	tracer := otel.Tracer("pipeline.extractor")
	ctx, span := tracer.Start(ctx, "PopplerExtractor.ExtractFromReader")
	defer span.End()
	defer close(lineChan)

	cmd := exec.CommandContext(ctx, "pdftotext", "-layout", "-", "-")
	cmd.Stdin = reader

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	err = cmd.Start()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to start pdftotext: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	currentPage := 1

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "\f") {
			parts := strings.Split(line, "\f")
			for i, p := range parts {
				if strings.TrimSpace(p) != "" {
					lineChan <- ExtractedLine{Text: p, PageNumber: currentPage}
				}
				if i < len(parts)-1 {
					currentPage++
				}
			}
			continue
		}

		lineChan <- ExtractedLine{Text: line, PageNumber: currentPage}
	}

	err = scanner.Err()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("scanner error: %w", err)
	}

	err = cmd.Wait()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("pdftotext wait error: %w", err)
	}

	return nil
}
