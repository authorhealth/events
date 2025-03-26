package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	MaxEventErrors = 5

	defaultNumProcessorWorkers = 2
)

var ErrRetryable = errors.New("event failed to process but is retryable")

type BackoffFunc func(event *Event) time.Time

func DefaultBackoffFunc(event *Event) time.Time {
	duration := time.Second * time.Duration(event.Errors*2) * time.Duration(rand.IntN(45)+15)
	backoffUntil := time.Now().Add(duration)

	return backoffUntil
}

var _ BackoffFunc = DefaultBackoffFunc

type BeforeProcessHook func(context.Context, *Event) (context.Context, *Event, error)

type ProcessorStatus string

const (
	ProcessorStatusNotStarted ProcessorStatus = "notStarted"
	ProcessorStatusRunning    ProcessorStatus = "running"
	ProcessorStatusPaused     ProcessorStatus = "paused"
	ProcessorStatusShutdown   ProcessorStatus = "shutdown"
)

func (ps ProcessorStatus) String() string {
	return string(ps)
}

func (ps ProcessorStatus) operational() bool {
	return ps == ProcessorStatusRunning || ps == ProcessorStatusPaused
}

type processorEvent string

const (
	processorEventPaused   processorEvent = "paused"
	processorEventResumed  processorEvent = "resumed"
	processorEventShutdown processorEvent = "shutdown"
	processorEventStarted  processorEvent = "started"
)

type Processor struct {
	beforeProcessHook          BeforeProcessHook
	configMap                  ConfigMap
	deadCountGauge             metric.Int64ObservableGauge
	done                       chan bool
	failureCounter             metric.Int64Counter
	meter                      metric.Meter
	meterCallbackRegistrations []metric.Registration
	numProcessorWorkers        int
	repo                       Repository
	shutdown                   chan bool
	status                     ProcessorStatus
	statusRWMutex              sync.RWMutex
	statusUpDownCounter        metric.Int64UpDownCounter
	successCounter             metric.Int64Counter
	telemetryPrefix            string
	timeHistogram              metric.Float64Histogram
	tracer                     trace.Tracer
	unprocessedCountGauge      metric.Int64ObservableGauge
	unprocessedMaxAgeGauge     metric.Float64ObservableGauge
}

func NewProcessor(
	repo Repository,
	conf ConfigMap,
	beforeProcessHook BeforeProcessHook,
	telemetryPrefix string,
	numProcessorWorkers int,
) (*Processor, error) {
	if numProcessorWorkers <= 0 {
		numProcessorWorkers = defaultNumProcessorWorkers
	}

	p := &Processor{
		beforeProcessHook:   beforeProcessHook,
		configMap:           conf,
		done:                make(chan bool, 1),
		meter:               otel.GetMeterProvider().Meter("github.com/authorhealth/events"),
		numProcessorWorkers: numProcessorWorkers,
		repo:                repo,
		status:              ProcessorStatusNotStarted,
		telemetryPrefix:     telemetryPrefix,
		shutdown:            make(chan bool, 1),
		tracer:              otel.GetTracerProvider().Tracer("github.com/authorhealth/events"),
	}

	var err error
	p.successCounter, err = p.meter.Int64Counter(
		p.applyTelemetryPrefix("events.processed.successes"),
		metric.WithDescription("The number of successfully processed events."),
		metric.WithUnit("{success}"))
	if err != nil {
		return nil, fmt.Errorf("constructing success counter: %w", err)
	}

	p.failureCounter, err = p.meter.Int64Counter(
		p.applyTelemetryPrefix("events.processed.failures"),
		metric.WithDescription("The number of events that failed to process."),
		metric.WithUnit("{failure}"))
	if err != nil {
		return nil, fmt.Errorf("constructing failure counter: %w", err)
	}

	p.timeHistogram, err = p.meter.Float64Histogram(
		p.applyTelemetryPrefix("events.processing_time"),
		metric.WithDescription("The time spent processing an event."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60, 120, 300))
	if err != nil {
		return nil, fmt.Errorf("constructing time histogram: %w", err)
	}

	p.unprocessedCountGauge, err = p.meter.Int64ObservableGauge(
		p.applyTelemetryPrefix("events.unprocessed.count"),
		metric.WithDescription("The number of unprocessed events in the queue."),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("constructing unprocessed event count gauge: %w", err)
	}

	p.unprocessedMaxAgeGauge, err = p.meter.Float64ObservableGauge(
		p.applyTelemetryPrefix("events.unprocessed.max_age"),
		metric.WithDescription("The age of the oldest unprocessed event in the queue."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("constructing max unprocessed event age gauge: %w", err)
	}

	p.deadCountGauge, err = p.meter.Int64ObservableGauge(
		p.applyTelemetryPrefix("events.dead.count"),
		metric.WithDescription("The number of dead events in the queue."),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("constructing dead event count gauge: %w", err)
	}

	p.statusUpDownCounter, err = p.meter.Int64UpDownCounter(
		p.applyTelemetryPrefix("events.processor.status"),
		metric.WithDescription("Operational status for the processor: 1 (true) or 0 (false) for each of the possible states"),
	)
	if err != nil {
		return nil, fmt.Errorf("constructing processor status up/down counter: %w", err)
	}

	return p, nil
}

func (p *Processor) Start(ctx context.Context, interval time.Duration, limit int) error {
	if p.Status() != ProcessorStatusNotStarted {
		return errors.New("processor is already started")
	}

	defer func() {
		p.done <- true
	}()

	err := p.registerMeterCallbacks()
	if err != nil {
		return fmt.Errorf("registering meter callbacks: %w", err)
	}

	p.handleProcessorEvent(ctx, processorEventStarted)

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			if !p.Paused() {
				p.processEvents(ctx, limit)
			}

		case <-p.shutdown:
			return nil
		}
	}
}

func (p *Processor) beforeProcess(ctx context.Context, event *Event) (context.Context, *Event, error) {
	if p.beforeProcessHook == nil {
		return ctx, event, nil
	}

	return p.beforeProcessHook(ctx, event)
}

func (p *Processor) applyTelemetryPrefix(k string) string {
	if len(p.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", p.telemetryPrefix, k)
	}

	return k
}

func (p *Processor) processEvents(ctx context.Context, limit int) {
	ctx, span := p.tracer.Start(ctx, "process events")
	defer span.End()

	events, err := p.repo.FindUnprocessed(ctx, limit)
	if err != nil {
		slog.ErrorContext(ctx, "error finding unprocessed events", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error finding unprocessed events")
		return
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	workers := make(chan struct{}, p.numProcessorWorkers)

	for _, event := range events {
		select {
		case <-p.shutdown:
			return

		default:
			workers <- struct{}{}

			event := event
			wg.Add(1)
			go func() {
				defer func() {
					<-workers
					wg.Done()
				}()

				p.processEvent(ctx, event)
			}()
		}
	}
}

func (p *Processor) processEvent(ctx context.Context, event *Event) {
	traceAttrs := []attribute.KeyValue{
		attribute.String(p.applyTelemetryPrefix("event.id"), event.ID),
		attribute.String(p.applyTelemetryPrefix("event.type"), event.Name.String()),
		attribute.String(p.applyTelemetryPrefix("correlation_id"), event.CorrelationID),
	}

	if event.EntityID != nil {
		entityID := event.EntityID
		traceAttrs = append(traceAttrs, attribute.String(p.applyTelemetryPrefix("event.entity.id"), *entityID))
	}

	ctx, span := p.tracer.Start(
		ctx,
		fmt.Sprintf("process %s event", event.Name),
		trace.WithAttributes(traceAttrs...),
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithNewRoot())
	defer span.End()

	logger := slog.With(
		"eventCorrelationId", event.CorrelationID,
		"eventId", event.ID,
		"eventName", event.Name.String(),
	)

	logger.InfoContext(ctx, fmt.Sprintf("processing %s event", event.Name))

	ctx, event, err := p.beforeProcess(ctx, event)
	if err != nil {
		logger.ErrorContext(ctx, "error running before process", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running before process")
		return
	}

	err = p.repo.Transaction(ctx, func(txRepo Repository) error {
		event, err := txRepo.FindByIDForUpdate(ctx, event.ID, true)
		if err != nil {
			// Locked rows are skipped, so do not error on not found.
			if errors.Is(err, ErrNotFound) {
				return nil
			}

			return fmt.Errorf("finding event by id for update: %w", err)
		}

		if event.ProcessedAt != nil {
			return nil
		}

		config := p.configMap[event.Name]
		if config == nil {
			config = &Config{}
		}

		if len(config.Handlers) == 0 {
			logger.WarnContext(ctx, fmt.Sprintf("no handlers mapped for %s event", event.Name))
		}

		metricAttrs := []attribute.KeyValue{
			attribute.String(p.applyTelemetryPrefix("event.type"), event.Name.String()),
		}

		err = event.Process(ctx, config)
		if err != nil {
			isRetryable := errors.Is(err, ErrRetryable)
			metricAttrs = append(metricAttrs,
				attribute.Bool(p.applyTelemetryPrefix("event.retryable"), isRetryable),
				attribute.Int(p.applyTelemetryPrefix("event.errors"), event.Errors))
			var unprocessedHandlerNames []string
			for handlerName, handlerResult := range event.UnprocessedHandlers() {
				unprocessedHandlerNames = append(unprocessedHandlerNames, handlerName.String())
				handlerLogger := logger.With("handlerName", handlerName)
				var logLevel slog.Level
				if isRetryable {
					logLevel = slog.LevelWarn
				} else {
					logLevel = slog.LevelError
				}

				handlerLogger.Log(
					ctx,
					logLevel,
					fmt.Sprintf("error while processing %s event with %s handler", event.Name, handlerName),
					Err(handlerResult.LastError),
					"lastProcessAttmptAt", handlerResult.LastProcessAttemptAt,
				)

				if !isRetryable {
					span.RecordError(fmt.Errorf("processing %s event with \"%s\" handler: %w", event.Name, handlerName, handlerResult.LastError))
				}
			}

			metricAttrs = append(metricAttrs,
				attribute.StringSlice(p.applyTelemetryPrefix("event.unprocessed_handlers"), unprocessedHandlerNames))
			p.failureCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))

			if !isRetryable {
				logger.ErrorContext(ctx, fmt.Sprintf("%s event process error", event.Name), Err(err))
				span.RecordError(err)
				span.SetStatus(codes.Error, "event process error")
			}
		} else {
			p.successCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		}

		err = txRepo.Update(ctx, event)
		if err != nil {
			return fmt.Errorf("updating %s event: %w", event.Name, err)
		}

		if event.ProcessedAt != nil {
			p.timeHistogram.Record(ctx, event.ProcessedAt.Sub(event.Timestamp).Seconds(), metric.WithAttributes(metricAttrs...))
		}

		return nil
	})
	if err != nil {
		logger.ErrorContext(ctx, "error running transaction", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running transaction")
	}
}

func (p *Processor) Shutdown(ctx context.Context) error {
	if !p.Status().operational() {
		return errors.New("processor is not running")
	}

	p.shutdown <- true

	p.handleProcessorEvent(ctx, processorEventShutdown)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-p.done:
			err := p.unregisterMeterCallbacks()
			if err != nil {
				return fmt.Errorf("unregistering meter callbacks: %w", err)
			}

			return nil
		}
	}
}

func (p *Processor) registerMeterCallbacks() error {
	var registrations []metric.Registration
	registration, err := p.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := p.repo.CountUnprocessed(ctx)
		if err != nil {
			return fmt.Errorf("counting unprocessed events: %w", err)
		}

		o.ObserveInt64(p.unprocessedCountGauge, int64(count))

		return nil
	}, p.unprocessedCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for unprocessed event count guage: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = p.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		oldest, err := p.repo.FindOldestUnprocessed(ctx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				o.ObserveFloat64(p.unprocessedMaxAgeGauge, 0)

				return nil
			}

			return fmt.Errorf("finding oldest unprocessed event: %w", err)
		}

		o.ObserveFloat64(p.unprocessedMaxAgeGauge, time.Since(oldest.Timestamp).Seconds())

		return nil
	}, p.unprocessedMaxAgeGauge)
	if err != nil {
		return fmt.Errorf("registering callback for max unprocessed event age gauge: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = p.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := p.repo.CountDead(ctx)
		if err != nil {
			return fmt.Errorf("counting dead events: %w", err)
		}

		o.ObserveInt64(p.deadCountGauge, int64(count))

		return nil
	}, p.deadCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for dead event count guage: %w", err)
	}

	registrations = append(registrations, registration)

	p.meterCallbackRegistrations = registrations

	return nil
}

func (p *Processor) unregisterMeterCallbacks() error {
	for _, registration := range p.meterCallbackRegistrations {
		err := registration.Unregister()
		if err != nil {
			return fmt.Errorf("unregistering meter callback: %w", err)
		}
	}

	p.meterCallbackRegistrations = nil

	return nil
}

func (p *Processor) Pause(ctx context.Context) {
	p.handleProcessorEvent(ctx, processorEventPaused)
}

func (p *Processor) Paused() bool {
	return p.Status() == ProcessorStatusPaused
}

func (p *Processor) Resume(ctx context.Context) {
	p.handleProcessorEvent(ctx, processorEventResumed)
}

func (p *Processor) Status() ProcessorStatus {
	p.statusRWMutex.RLock()
	defer p.statusRWMutex.RUnlock()

	return p.status
}

func (p *Processor) handleProcessorEvent(ctx context.Context, event processorEvent) {
	p.statusRWMutex.Lock()
	defer p.statusRWMutex.Unlock()

	nextStatus := p.status

	switch p.status {
	case ProcessorStatusNotStarted:
		switch event {
		case processorEventStarted:
			nextStatus = ProcessorStatusRunning
		}

	case ProcessorStatusRunning:
		switch event {
		case processorEventPaused:
			nextStatus = ProcessorStatusPaused

		case processorEventShutdown:
			nextStatus = ProcessorStatusShutdown
		}

	case ProcessorStatusPaused:
		switch event {
		case processorEventResumed:
			nextStatus = ProcessorStatusRunning

		case processorEventShutdown:
			nextStatus = ProcessorStatusShutdown
		}
	}

	if nextStatus == p.status {
		return
	}

	previousStatus := p.status
	p.status = nextStatus

	// Keep track of the number of operational event processors and their state (i.e., running or paused).
	if previousStatus.operational() {
		p.statusUpDownCounter.Add(
			ctx,
			-1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(
						p.applyTelemetryPrefix("events.processor.state"),
						previousStatus.String(),
					),
				),
			),
		)
	}

	if p.status.operational() {
		p.statusUpDownCounter.Add(
			ctx,
			1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(
						p.applyTelemetryPrefix("events.processor.state"),
						p.status.String(),
					),
				),
			),
		)
	}
}
