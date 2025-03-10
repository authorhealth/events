package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/authorhealth/events/v2"
	"github.com/lmittmann/tint"
	"go.opentelemetry.io/otel"
)

const (
	applicationEventProducerInterval = 1 * time.Second

	domainEventProducerInterval = 1 * time.Second

	eventProcessorLimit      = 50
	eventProcessorNumWorkers = 1

	eventExecutorLimit      = 50
	eventExecutorNumWorkers = 5

	eventSchedulerInterval = 5 * time.Second

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
	handlerRequestRepo := NewHandlerRequestRepository()

	configMap := events.NewConfigMap(
		events.WithEvent(ApplicationEventName,
			events.WithHandler(ApplicationEventHandler()),
			events.WithHandler(FailingEventHandler()),
		),
		events.WithEvent(DomainEventName,
			events.WithHandler(DomainEventHandler()),
		),
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

	reporter := NewReporter(eventRepo, handlerRequestRepo)
	go func() {
		slog.Info("starting reporter", "interval", reporterInterval)
		reporter.Start(ctx, reporterInterval)
	}()

	eventProcessor, err := events.NewProcessor(
		eventRepo,
		handlerRequestRepo,
		configMap,
		nil,
		"demo",
		eventProcessorNumWorkers,
	)
	if err != nil {
		slog.Error("error constructing event processor", events.Err(err))
		os.Exit(1)
	}

	eventExecutor, err := events.NewExecutor(
		handlerRequestRepo,
		configMap,
		nil,
		"demo",
		eventExecutorNumWorkers,
	)
	if err != nil {
		slog.Error("error constructing event executor", events.Err(err))
		os.Exit(1)
	}

	eventScheduler := events.NewScheduler(
		eventExecutor,
		eventProcessor,
	)

	go func() {
		slog.Info("starting event executor", "interval", eventSchedulerInterval, "processorLimit", eventProcessorLimit, "executorLimit", eventExecutorLimit)

		err := eventScheduler.Start(ctx, eventSchedulerInterval, eventProcessorLimit, eventExecutorLimit)
		if err != nil {
			slog.Error("error starting event executor", events.Err(err))
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

	slog.Info("shutting down event scheduler")
	err = eventScheduler.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down event scheduler", events.Err(err))
	}
}
