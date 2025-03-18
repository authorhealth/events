package main

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func TracerProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	// Create exporter.
	f, err := os.OpenFile("traces.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening traces.jsonl: %w", err)
	}

	exporter, err := stdouttrace.New(stdouttrace.WithWriter(f))
	if err != nil {
		return nil, fmt.Errorf("stdouttrace.New: %w", err)
	}

	// Identify your application using resource detection
	res, err := resource.New(ctx,
		// Use the default detectors
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("resource.New: %w", err)
	}

	// Create tracer provider with the exporter.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	return tp, nil
}
