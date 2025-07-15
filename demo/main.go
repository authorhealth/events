package main

import (
	"context"
	"flag"
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
	eventExecutorLimit          = 50
	eventExecutorNumWorkers     = 5
	eventProcessorLimit         = 50
	eventProcessorNumWorkers    = 1
	eventProducerInterval       = 1 * time.Second
	eventSchedulerInterval      = 5 * time.Second
	eventSystemReporterInterval = 1 * time.Second
	shutdownTimeout             = 10 * time.Second
)

var concurrent bool

func init() {
	flag.BoolVar(&concurrent, "concurrent", false, "if true, uses concurrent scheduler and queue executors")
}

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

	db := NewDatabase()
	store := NewStore(db)

	domainQueueName := events.ExecutorQueueName("domain")

	configMap := events.NewConfigMap(
		events.WithEvent(ApplicationEventName,
			events.WithHandler(ApplicationEventHandler()),
			events.WithHandler(FailingEventHandler()),
		),
		events.WithEvent(DomainEventName,
			events.WithHandler(DomainEventHandler(), events.WithQueue(domainQueueName)),
			events.WithHandler(FailingEventHandler(), events.WithQueue(domainQueueName)),
		),
	)

	eventProducer := NewEventProducer(store.Events())
	go func() {
		slog.Info("starting event producer", "interval", eventProducerInterval)
		eventProducer.Start(ctx, eventProducerInterval)
	}()

	eventSystemReporter := NewEventSystemReporter(store)
	go func() {
		slog.Info("starting event system reporter", "interval", eventSystemReporterInterval)
		eventSystemReporter.Start(
			ctx,
			eventSystemReporterInterval,
			[]events.ExecutorQueueName{
				events.DefaultExecutorQueueName,
				domainQueueName,
			},
		)
	}()

	eventProcessor, err := events.NewDefaultProcessor(
		store,
		configMap,
		nil,
		"demo",
		eventProcessorNumWorkers,
		eventProcessorLimit,
	)
	if err != nil {
		slog.Error("error constructing event processor", events.Err(err))
		os.Exit(1)
	}

	var eventScheduler events.Scheduler
	if concurrent {
		defaultQueueExecutor, err := events.NewQueueExecutor(
			store,
			configMap,
			nil,
			"demo",
			eventExecutorNumWorkers,
			eventExecutorLimit,
			events.DefaultExecutorQueueName,
		)
		if err != nil {
			slog.Error("error constructing default queue event executor", events.Err(err))
			os.Exit(1)
		}

		domainQueueExecutor, err := events.NewQueueExecutor(
			store,
			configMap,
			nil,
			"demo",
			eventExecutorNumWorkers,
			eventExecutorLimit,
			domainQueueName,
		)
		if err != nil {
			slog.Error("error constructing domain queue event executor", events.Err(err))
			os.Exit(1)
		}

		eventScheduler, err = events.NewConcurrentScheduler(
			eventProcessor,
			[]events.Executor{
				defaultQueueExecutor,
				domainQueueExecutor,
			},
			"demo",
			eventSchedulerInterval,
		)
		if err != nil {
			slog.Error("error constructing event scheduler", events.Err(err))
			os.Exit(1)
		}
	} else {
		eventExecutor, err := events.NewDefaultExecutor(
			store,
			configMap,
			nil,
			"demo",
			eventExecutorNumWorkers,
			eventExecutorLimit,
		)
		if err != nil {
			slog.Error("error constructing event executor", events.Err(err))
			os.Exit(1)
		}

		eventScheduler, err = events.NewCooperativeScheduler(
			eventProcessor,
			eventExecutor,
			"demo",
			eventSchedulerInterval,
		)
		if err != nil {
			slog.Error("error constructing event scheduler", events.Err(err))
			os.Exit(1)
		}
	}

	go func() {
		slog.Info("starting event scheduler", "interval", eventSchedulerInterval, "processorLimit", eventProcessorLimit, "executorLimit", eventExecutorLimit)

		err := eventScheduler.Start(ctx)
		if err != nil {
			slog.Error("error starting event scheduler", events.Err(err))
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

	slog.Info("shutting down event producer")
	err = eventProducer.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down event producer", events.Err(err))
	}

	slog.Info("shutting down event system reporter")
	err = eventSystemReporter.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down event system reporter", events.Err(err))
	}

	slog.Info("shutting down event scheduler")
	err = eventScheduler.Shutdown(shutdownCtx)
	if err != nil {
		slog.Warn("error shutting down event scheduler", events.Err(err))
	}
}
