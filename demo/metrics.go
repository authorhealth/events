package main

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func MeterProvider(ctx context.Context, serviceName string) (*sdkmetric.MeterProvider, error) {
	// Create exporter.
	f, err := os.OpenFile("metrics.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening metrics.jsonl: %w", err)
	}

	exporter, err := stdoutmetric.New(stdoutmetric.WithWriter(f))
	if err != nil {
		return nil, fmt.Errorf("stdoutmetric.New: %w", err)
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

	// Create meter provider with the exporter.
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
		sdkmetric.WithResource(res),
	)

	return mp, nil
}
