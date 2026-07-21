package telemetry

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

type TelemetryProvider struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	LoggerProvider *sdklog.LoggerProvider
	Tracer         trace.Tracer
	Meter          metric.Meter
}

type TracingHandler struct {
	slog.Handler
}

func NewTracingHandler(h slog.Handler) slog.Handler {
	return &TracingHandler{Handler: h}
}

func (h *TracingHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func InitTelemetry(serviceName, collectorURL string) (*TelemetryProvider, error) {
	ctx := context.Background()
	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// 1. Traces (OTLP HTTP)
	traceExporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpoint(collectorURL),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)
	otel.SetTracerProvider(tp)

	metricExporter, err := otlpmetrichttp.New(
		ctx,
		otlpmetrichttp.WithEndpoint(collectorURL),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	otel.SetMeterProvider(mp)

	logExporter, err := otlploghttp.New(
		ctx,
		otlploghttp.WithEndpoint(collectorURL),
		otlploghttp.WithInsecure(),
	)
	var lp *sdklog.LoggerProvider
	if err == nil {
		lp = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		)
		global.SetLoggerProvider(lp)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	tracer := tp.Tracer(serviceName)
	meter := mp.Meter(serviceName)

	return &TelemetryProvider{
		TracerProvider: tp,
		MeterProvider:  mp,
		LoggerProvider: lp,
		Tracer:         tracer,
		Meter:          meter,
	}, nil
}

func (t *TelemetryProvider) Shutdown(ctx context.Context) error {
	if t.TracerProvider != nil {
		_ = t.TracerProvider.Shutdown(ctx)
	}
	if t.MeterProvider != nil {
		_ = t.MeterProvider.Shutdown(ctx)
	}
	if t.LoggerProvider != nil {
		_ = t.LoggerProvider.Shutdown(ctx)
	}
	return nil
}
