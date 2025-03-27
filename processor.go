package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultNumProcessorWorkers = 1
)

var ErrRetryable = errors.New("event failed to process but is retryable")

// BeforeProcessHook is a function that is called before an event is processed.
// It can be used to modify the given context or event before it is processed.
// If an error is returned, the event will not be processed.
type BeforeProcessHook func(context.Context, *Event) (context.Context, *Event, error)

// Processor provides a way to process events from an EventRepository.
// It uses a configurable number of worker goroutines to process events concurrently.
// Before processing, a BeforeProcessHook can be used to modify the event or context.
// The Processor also provides metrics for monitoring the processing of events.
//
// The Processor starts by retrieving unprocessed events from the Repository.
// For each event, it acquires a worker from a pool of workers. If no worker is available, it waits until one becomes available.
// Once a worker is acquired, the event is processed. After processing, the worker is released back to the pool.
//
// The processing of an event involves creating handler execution requests for the event based on the set of handlers defined in the ConfigMap.
// If the handler execution requests are created successfully, the event is marked as processed.
type Processor struct {
	beforeProcessHook          BeforeProcessHook
	configMap                  ConfigMap
	failureCounter             metric.Int64Counter
	meter                      metric.Meter
	meterCallbackRegistrations []metric.Registration
	numProcessorWorkers        int
	shutdown                   chan bool
	store                      Storer
	successCounter             metric.Int64Counter
	telemetryPrefix            string
	timeHistogram              metric.Float64Histogram
	tracer                     trace.Tracer
	unprocessedCountGauge      metric.Int64ObservableGauge
	unprocessedMaxAgeGauge     metric.Float64ObservableGauge
}

// NewProcessor creates a new event processor.
//
// repo is the repository used to store and retrieve events.
//
// conf is a map of event names to configurations. If an event is encountered
// during processing with an event name that is not present in the map, a
// warning will be logged, and the event will be marked as processed.
//
// beforeProcessHook is an optional hook that is called before each event
// is processed.
//
// telemetryPrefix is used to prefix all metric names. This can be used to
// prevent metric name collisions between different applications.
//
// numProcessorWorkers configures the number of worker goroutines that process
// events. If numProcessorWorkers is <= 0, it defaults to 2.
func NewProcessor(
	store Storer,
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
		store:               store,
		meter:               otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		numProcessorWorkers: numProcessorWorkers,
		shutdown:            make(chan bool, 1),
		telemetryPrefix:     telemetryPrefix,
		tracer:              otel.GetTracerProvider().Tracer("github.com/authorhealth/events/v2"),
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

	return p, nil
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

	events, err := p.store.Events().FindUnprocessed(ctx, limit)
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

	if event.EntityID != "" {
		traceAttrs = append(traceAttrs, attribute.String(p.applyTelemetryPrefix("event.entity.id"), event.EntityID))
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

	logger.DebugContext(ctx, fmt.Sprintf("processing %s event", event.Name))

	ctx, event, err := p.beforeProcess(ctx, event)
	if err != nil {
		logger.ErrorContext(ctx, "error running before process", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running before process")
		return
	}

	metricAttrs := []attribute.KeyValue{
		attribute.String(p.applyTelemetryPrefix("event.type"), event.Name.String()),
	}

	err = p.store.Transaction(ctx, func(txStore Storer) error {
		event, err := txStore.Events().FindByIDForUpdate(ctx, event.ID, true)
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

		handlerConfigMap := p.configMap[event.Name]
		if handlerConfigMap == nil {
			handlerConfigMap = HandlerConfigMap{}
		}

		if len(handlerConfigMap) == 0 {
			logger.WarnContext(ctx, fmt.Sprintf("no handlers mapped for %s event", event.Name))
		}

		var requests []*HandlerRequest
		for handlerName, handlerConfig := range handlerConfigMap {
			request, err := NewHandlerRequest(
				event,
				handlerName,
				handlerConfig.MaxErrors,
				handlerConfig.Priority,
			)

			if err != nil {
				return fmt.Errorf("new handler request: %w", err)
			}

			requests = append(requests, request)
		}

		for _, request := range requests {
			err := retry.Do(func() error {
				return txStore.HandlerRequests().Create(ctx, request)
			})
			if err != nil {
				return fmt.Errorf("creating handler request: %w", err)
			}
		}

		event.ProcessedAt = Ptr(time.Now())

		err = retry.Do(func() error {
			return txStore.Events().Update(ctx, event)
		})
		if err != nil {
			return fmt.Errorf("updating %s event: %w", event.Name, err)
		}

		p.successCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		p.timeHistogram.Record(ctx, event.ProcessedAt.Sub(event.Timestamp).Seconds(), metric.WithAttributes(metricAttrs...))

		return nil
	})
	if err != nil {
		p.failureCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		logger.ErrorContext(ctx, "error processing event", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error processing event")
	}
}

func (p *Processor) registerMeterCallbacks() error {
	var registrations []metric.Registration
	registration, err := p.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := p.store.Events().CountUnprocessed(ctx)
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
		oldest, err := p.store.Events().FindOldestUnprocessed(ctx)
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
