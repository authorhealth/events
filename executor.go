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
	defaultNumExecutorWorkers = 2
)

// BackoffFunc is a function that determines the time at which the given request
// should be retried after it has failed to execute. The time should be in
// the future.
type BackoffFunc func(request *HandlerRequest) time.Time

// DefaultBackoffFunc is an implementation of BackoffFunc that uses an
// exponential backoff strategy with randomized jitter.
func DefaultBackoffFunc(request *HandlerRequest) time.Time {
	duration := time.Second * time.Duration(request.Errors*2) * time.Duration(rand.IntN(45)+15)
	backoffUntil := time.Now().Add(duration)

	return backoffUntil
}

var _ BackoffFunc = DefaultBackoffFunc

var missingHandlerFunc HandlerFunc = func(ctx context.Context, hr *HandlerRequest) error {
	return errors.New("handler request has no configured handler")
}

// BeforeExecuteHook is a function that is called before a handler request is executed.
// It can be used to modify the given context or request before it is executed.
// If an error is returned, the request will not be executed.
type BeforeExecuteHook func(context.Context, *HandlerRequest) (context.Context, *HandlerRequest, error)

// Executor provides a standard interface for executing requests.
type Executor interface {
	executeRequests(ctx context.Context)
	registerMeterCallbacks() error
	shutdown()
	unregisterMeterCallbacks() error
}

// DefaultExecutor provides a way to execute requests from a Storer.
// It uses a configurable number of worker goroutines to execute requests concurrently.
// Before execution, a BeforeExecuteHook can be used to modify the request or context.
// The DefaultExecutor also provides metrics for monitoring the execution of requests.
//
// The DefaultExecutor starts by retrieving unexecuted requests from the Storer.
// For each request, it acquires a worker from a pool of workers. If no worker is available, it waits until one becomes available.
// Once a worker is acquired, the request is executed. After execution, the worker is released back to the pool.
//
// The execution of a handler request involves running a set of handlers defined in the ConfigMap.
// If any handler returns an error, the request is marked as failed.
// If all handlers complete successfully, the request is marked as executed.
type DefaultExecutor struct {
	beforeExecuteHook          BeforeExecuteHook
	configMap                  ConfigMap
	deadCountGauge             metric.Int64ObservableGauge
	failureCounter             metric.Int64Counter
	limit                      int
	meter                      metric.Meter
	meterCallbackRegistrations []metric.Registration
	numExecutorWorkers         int
	shutdownChan               chan bool
	store                      Storer
	successCounter             metric.Int64Counter
	telemetryPrefix            string
	timeHistogram              metric.Float64Histogram
	tracer                     trace.Tracer
	unexecutedCountGauge       metric.Int64ObservableGauge
	unexecutedMaxAgeGauge      metric.Float64ObservableGauge
}

var _ Executor = (*DefaultExecutor)(nil)

// NewDefaultExecutor creates a new DefaultExecutor.
//
// store is the Storer used to store and retrieve requests.
//
// conf is a map of request names to configurations. If a handler request is
// encountered during execution with a handler request name that is not present
// in the map, a warning will be logged, and the request will be marked
// as executed.
//
// beforeExecuteHook is an optional hook that is called before each request
// is executed.
//
// telemetryPrefix is used to prefix all metric names. This can be used to
// prevent metric name collisions between different applications.
//
// numExecutorWorkers configures the number of worker goroutines that execute
// requests. If numExecutorWorkers is <= 0, it defaults to 2.
func NewDefaultExecutor(
	store Storer,
	conf ConfigMap,
	beforeExecuteHook BeforeExecuteHook,
	telemetryPrefix string,
	numExecutorWorkers int,
	limit int,
) (*DefaultExecutor, error) {
	if numExecutorWorkers <= 0 {
		numExecutorWorkers = defaultNumExecutorWorkers
	}

	e := &DefaultExecutor{
		beforeExecuteHook:  beforeExecuteHook,
		configMap:          conf,
		limit:              limit,
		meter:              otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		numExecutorWorkers: numExecutorWorkers,
		store:              store,
		telemetryPrefix:    telemetryPrefix,
		shutdownChan:       make(chan bool, 1),
		tracer:             otel.GetTracerProvider().Tracer("github.com/authorhealth/events/v2"),
	}

	var err error
	e.successCounter, err = e.meter.Int64Counter(
		e.applyTelemetryPrefix("requests.executed.successes"),
		metric.WithDescription("The number of successfully executed requests."),
		metric.WithUnit("{success}"))
	if err != nil {
		return nil, fmt.Errorf("constructing success counter: %w", err)
	}

	e.failureCounter, err = e.meter.Int64Counter(
		e.applyTelemetryPrefix("requests.executed.failures"),
		metric.WithDescription("The number of requests that failed to execute."),
		metric.WithUnit("{failure}"))
	if err != nil {
		return nil, fmt.Errorf("constructing failure counter: %w", err)
	}

	e.timeHistogram, err = e.meter.Float64Histogram(
		e.applyTelemetryPrefix("requests.execution_time"),
		metric.WithDescription("The time spent executing a handler request."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60, 120, 300))
	if err != nil {
		return nil, fmt.Errorf("constructing time histogram: %w", err)
	}

	e.unexecutedCountGauge, err = e.meter.Int64ObservableGauge(
		e.applyTelemetryPrefix("requests.unexecuted.count"),
		metric.WithDescription("The number of unexecuted requests in the queue."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("constructing unexecuted request count gauge: %w", err)
	}

	e.unexecutedMaxAgeGauge, err = e.meter.Float64ObservableGauge(
		e.applyTelemetryPrefix("requests.unexecuted.max_age"),
		metric.WithDescription("The age of the oldest unexecuted request in the queue."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("constructing max unexecuted request age gauge: %w", err)
	}

	e.deadCountGauge, err = e.meter.Int64ObservableGauge(
		e.applyTelemetryPrefix("requests.dead.count"),
		metric.WithDescription("The number of dead requests in the queue."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("constructing dead request count gauge: %w", err)
	}

	return e, nil
}

func (e *DefaultExecutor) beforeExecute(ctx context.Context, request *HandlerRequest) (context.Context, *HandlerRequest, error) {
	if e.beforeExecuteHook == nil {
		return ctx, request, nil
	}

	return e.beforeExecuteHook(ctx, request)
}

func (e *DefaultExecutor) applyTelemetryPrefix(k string) string {
	if len(e.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", e.telemetryPrefix, k)
	}

	return k
}

func (e *DefaultExecutor) executeRequests(ctx context.Context) {
	ctx, span := e.tracer.Start(ctx, "execute requests")
	defer span.End()

	requests, err := e.store.HandlerRequests().FindUnexecuted(ctx, e.limit)
	if err != nil {
		slog.ErrorContext(ctx, "error finding unexecuted requests", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error finding unexecuted requests")
		return
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	workers := make(chan struct{}, e.numExecutorWorkers)

	for _, request := range requests {
		select {
		case <-e.shutdownChan:
			return

		default:
			workers <- struct{}{}

			request := request
			wg.Add(1)
			go func() {
				defer func() {
					<-workers
					wg.Done()
				}()

				e.executeRequest(ctx, request)
			}()
		}
	}
}

func (e *DefaultExecutor) executeRequest(ctx context.Context, request *HandlerRequest) {
	traceAttrs := []attribute.KeyValue{
		attribute.String(e.applyTelemetryPrefix("request.id"), request.ID),
		attribute.String(e.applyTelemetryPrefix("request.event.id"), request.EventID),
		attribute.Stringer(e.applyTelemetryPrefix("request.event.type"), request.EventName),
		attribute.Stringer(e.applyTelemetryPrefix("request.type"), request.HandlerName),
		attribute.String(e.applyTelemetryPrefix("correlation_id"), request.CorrelationID),
	}

	if request.EventEntityID != "" {
		traceAttrs = append(traceAttrs, attribute.String(e.applyTelemetryPrefix("request.event.entity.id"), request.EventEntityID))
	}

	ctx, span := e.tracer.Start(
		ctx,
		fmt.Sprintf("execute %s request", request.HandlerName),
		trace.WithAttributes(traceAttrs...),
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithNewRoot())
	defer span.End()

	logger := slog.With(
		"requestCorrelationId", request.CorrelationID,
		"requestId", request.ID,
		"requestHandlerName", request.HandlerName.String(),
	)

	ctx, request, err := e.beforeExecute(ctx, request)
	if err != nil {
		logger.ErrorContext(ctx, "error running before execute", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running before execute")
		return
	}

	logger.DebugContext(ctx, fmt.Sprintf("executing %s request", request.HandlerName))

	err = e.store.Transaction(ctx, func(txStore Storer) error {
		request, err := txStore.HandlerRequests().FindByIDForUpdate(ctx, request.ID, true)
		if err != nil {
			// Locked rows are skipped, so do not error on not found.
			if errors.Is(err, ErrNotFound) {
				return nil
			}

			return fmt.Errorf("finding request by id for update: %w", err)
		}

		if request.CompletedAt != nil {
			return nil
		}

		config := e.configMap[request.EventName][request.HandlerName]
		if config == nil {
			config = &HandlerConfig{
				Handler: NewHandler(request.HandlerName, e.telemetryPrefix, missingHandlerFunc),
			}
		}

		metricAttrs := []attribute.KeyValue{
			attribute.Stringer(e.applyTelemetryPrefix("request.event.type"), request.EventName),
			attribute.Stringer(e.applyTelemetryPrefix("request.type"), request.HandlerName),
		}

		err = request.execute(ctx, config)
		if err != nil {
			isRetryable := errors.Is(err, ErrRetryable)
			metricAttrs = append(metricAttrs,
				attribute.Bool(e.applyTelemetryPrefix("request.retryable"), isRetryable),
				attribute.Int(e.applyTelemetryPrefix("request.errors"), request.Errors),
			)

			var logLevel slog.Level
			if isRetryable {
				logLevel = slog.LevelWarn
			} else {
				logLevel = slog.LevelError
			}

			logger.Log(
				ctx,
				logLevel,
				"error while executing handler request",
				Err(request.LastError),
				"lastAttemptAt", request.LastAttemptAt,
			)

			if !isRetryable {
				span.RecordError(request.LastError)
				span.SetStatus(codes.Error, "error while executing handler request")
			}

			e.failureCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		} else {
			e.successCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		}

		err = txStore.HandlerRequests().Update(ctx, request)
		if err != nil {
			return fmt.Errorf("updating %s request: %w", request.HandlerName, err)
		}

		if request.CompletedAt != nil {
			e.timeHistogram.Record(ctx, request.CompletedAt.Sub(request.EventTimestamp).Seconds(), metric.WithAttributes(metricAttrs...))
		}

		return nil
	})
	if err != nil {
		logger.ErrorContext(ctx, "error running transaction", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running transaction")
	}
}

func (e *DefaultExecutor) shutdown() {
	e.shutdownChan <- true
}

func (e *DefaultExecutor) registerMeterCallbacks() error {
	var registrations []metric.Registration
	registration, err := e.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := e.store.HandlerRequests().CountUnexecuted(ctx)
		if err != nil {
			return fmt.Errorf("counting unexecuted requests: %w", err)
		}

		o.ObserveInt64(e.unexecutedCountGauge, int64(count))

		return nil
	}, e.unexecutedCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for unexecuted request count guage: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = e.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		oldest, err := e.store.HandlerRequests().FindOldestUnexecuted(ctx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				o.ObserveFloat64(e.unexecutedMaxAgeGauge, 0)

				return nil
			}

			return fmt.Errorf("finding oldest unexecuted request: %w", err)
		}

		o.ObserveFloat64(e.unexecutedMaxAgeGauge, time.Since(oldest.EventTimestamp).Seconds())

		return nil
	}, e.unexecutedMaxAgeGauge)
	if err != nil {
		return fmt.Errorf("registering callback for max unexecuted request age gauge: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = e.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := e.store.HandlerRequests().CountDead(ctx)
		if err != nil {
			return fmt.Errorf("counting dead requests: %w", err)
		}

		o.ObserveInt64(e.deadCountGauge, int64(count))

		return nil
	}, e.deadCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for dead request count guage: %w", err)
	}

	registrations = append(registrations, registration)

	e.meterCallbackRegistrations = registrations

	return nil
}

func (e *DefaultExecutor) unregisterMeterCallbacks() error {
	for _, registration := range e.meterCallbackRegistrations {
		err := registration.Unregister()
		if err != nil {
			return fmt.Errorf("unregistering meter callback: %w", err)
		}
	}

	e.meterCallbackRegistrations = nil

	return nil
}
