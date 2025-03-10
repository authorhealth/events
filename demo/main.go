package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/authorhealth/events"
	"github.com/lmittmann/tint"
	"go.opentelemetry.io/otel"
)

const (
	applicationEventProducerInterval = 1 * time.Second

	domainEventProducerInterval = 1 * time.Second

	eventProcessorInterval   = 5 * time.Second
	eventProcessorLimit      = 50
	eventProcessorNumWorkers = 5

	reporterInterval = 1 * time.Second

	shutdownTimeout = 10 * time.Second
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			// Level:      slog.LevelDebug,
			TimeFormat: time.TimeOnly,
		}),
	))

	meterProvider, err := MeterProvider(ctx, "demo")
	if err != nil {
		slog.Error("error constructing meter provider", events.Err(err))
	}

	otel.SetMeterProvider(meterProvider)

	tracerProvider, err := TracerProvider(ctx, "demo")
	if err != nil {
		slog.Error("error constructing tracer provider", events.Err(err))
	}

	otel.SetTracerProvider(tracerProvider)

	eventRepo := NewEventRepository()

	configMap := events.ConfigMap{}
	configMap.AddHandlers(ApplicationEventName,
		ApplicationEventHandler(),
		FailingEventHandler(),
	)

	configMap.AddHandlers(DomainEventName,
		DomainEventHandler(),
	)

	applicationEventProducer := NewApplicationEventProducer(eventRepo)
	go func() {
		slog.Info("starting application event producer", "interval", applicationEventProducerInterval)
		applicationEventProducer.Start(ctx, applicationEventProducerInterval)
	}()

	domainEventProducer := NewDomainEventProducer(eventRepo)
	go func() {
		slog.Info("starting domain event producer", "interval", domainEventProducerInterval)
		domainEventProducer.Start(ctx, domainEventProducerInterval)
	}()

	reporter := NewReporter(eventRepo)
	go func() {
		slog.Info("starting reporter", "interval", reporterInterval)
		reporter.Start(ctx, reporterInterval)
	}()

	eventProcessor, err := events.NewProcessor(
		eventRepo,
		configMap,
		nil,
		"demo",
		eventProcessorNumWorkers,
	)
	if err != nil {
		slog.Error("error constructing event processor", events.Err(err))
		os.Exit(1)
	}

	go func() {
		slog.Info("starting event processor", "interval", eventProcessorInterval, "limit", eventProcessorLimit)

		err := eventProcessor.Start(ctx, eventProcessorInterval, eventProcessorLimit)
		if err != nil {
			slog.Error("error starting event processor", events.Err(err))
			os.Exit(1)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigs

	slog.Debug("received signal", "signal", sig)

	slog.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, shutdownTimeout)
	defer shutdownCancel()

	slog.Info("shutting down meter provider")
	err = meterProvider.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down meter provider", events.Err(err))
	}

	slog.Info("shutting down tracer provider")
	err = tracerProvider.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down tracer provider", events.Err(err))
	}

	slog.Info("shutting down application event producer")
	err = applicationEventProducer.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down application event producer", events.Err(err))
	}

	slog.Info("shutting down reporter")
	err = reporter.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down reporter", events.Err(err))
	}

	slog.Info("shutting down domain event producer")
	err = domainEventProducer.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down domain event producer", events.Err(err))
	}

	slog.Info("shutting down event processor")
	err = eventProcessor.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down event processor", events.Err(err))
	}
}
