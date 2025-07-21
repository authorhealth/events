package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
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

type ExecutorQueueName string

const DefaultExecutorQueueName ExecutorQueueName = "default"

func (eqn ExecutorQueueName) String() string {
	return string(eqn)
}

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
	*executorStatsObserver
	*executorWorkerPool

	limit int
	store Storer
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
	observer, err := newExecutorStatsObserver(
		store,
		telemetryPrefix,
	)
	if err != nil {
		return nil, err
	}

	if numExecutorWorkers <= 0 {
		numExecutorWorkers = defaultNumExecutorWorkers
	}

	pool, err := newExecutorWorkerPool(
		store,
		conf,
		beforeExecuteHook,
		telemetryPrefix,
		numExecutorWorkers,
		DefaultExecutorQueueName,
	)
	if err != nil {
		return nil, err
	}

	e := &DefaultExecutor{
		executorStatsObserver: observer,
		executorWorkerPool:    pool,
		limit:                 limit,
		store:                 store,
	}

	return e, nil
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

	e.executorWorkerPool.executeRequests(ctx, requests)
}

// QueueExecutor provides a way to execute requests for an executor queue from a Storer.
// It uses a configurable number of worker goroutines to execute requests concurrently.
// Before execution, a BeforeExecuteHook can be used to modify the request or context.
// The QueueExecutor also provides metrics for monitoring the execution of requests.
//
// The QueueExecutor starts by retrieving unexecuted requests for the executor queue from the Storer.
// For each request, it acquires a worker from a pool of workers. If no worker is available, it waits until one becomes available.
// Once a worker is acquired, the request is executed. After execution, the worker is released back to the pool.
//
// The execution of a handler request involves running a set of handlers defined in the ConfigMap.
// If any handler returns an error, the request is marked as failed.
// If all handlers complete successfully, the request is marked as executed.
type QueueExecutor struct {
	*executorQueueStatsObserver
	*executorWorkerPool

	limit     int
	queueName ExecutorQueueName
	store     Storer
}

var _ Executor = (*QueueExecutor)(nil)

// NewQueueExecutor creates a new QueueExecutor.
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
//
// queueName is the name of the queue to execute requests from.
func NewQueueExecutor(
	store Storer,
	conf ConfigMap,
	beforeExecuteHook BeforeExecuteHook,
	telemetryPrefix string,
	numExecutorWorkers int,
	limit int,
	queueName ExecutorQueueName,
) (*QueueExecutor, error) {
	observer, err := newExecutorQueueStatsObserver(
		store,
		telemetryPrefix,
		queueName,
	)
	if err != nil {
		return nil, err
	}

	if numExecutorWorkers <= 0 {
		numExecutorWorkers = defaultNumExecutorWorkers
	}

	pool, err := newExecutorWorkerPool(
		store,
		conf,
		beforeExecuteHook,
		telemetryPrefix,
		numExecutorWorkers,
		DefaultExecutorQueueName,
	)
	if err != nil {
		return nil, err
	}

	e := &QueueExecutor{
		executorQueueStatsObserver: observer,
		executorWorkerPool:         pool,
		limit:                      limit,
		queueName:                  queueName,
		store:                      store,
	}

	return e, nil
}

func (e *QueueExecutor) executeRequests(ctx context.Context) {
	ctx, span := e.tracer.Start(ctx, "execute requests")
	defer span.End()

	requests, err := e.store.HandlerRequests().FindUnexecutedByQueue(ctx, e.queueName, e.limit)
	if err != nil {
		slog.ErrorContext(ctx, "error finding unexecuted requests by queue", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error finding unexecuted requests")
		return
	}

	e.executorWorkerPool.executeRequests(ctx, requests)
}

type executorWorkerPool struct {
	beforeExecuteHook  BeforeExecuteHook
	configMap          ConfigMap
	executingRequests  atomic.Bool
	failureCounter     metric.Int64Counter
	meter              metric.Meter
	numExecutorWorkers int
	queueName          ExecutorQueueName
	shutdownChan       chan bool
	store              Storer
	successCounter     metric.Int64Counter
	telemetryPrefix    string
	timeHistogram      metric.Float64Histogram
	tracer             trace.Tracer
}

func newExecutorWorkerPool(
	store Storer,
	conf ConfigMap,
	beforeExecuteHook BeforeExecuteHook,
	telemetryPrefix string,
	numExecutorWorkers int,
	queueName ExecutorQueueName,
) (*executorWorkerPool, error) {
	ewp := &executorWorkerPool{
		beforeExecuteHook:  beforeExecuteHook,
		configMap:          conf,
		meter:              otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		numExecutorWorkers: numExecutorWorkers,
		queueName:          queueName,
		store:              store,
		telemetryPrefix:    telemetryPrefix,
		shutdownChan:       make(chan bool, 1),
		tracer:             otel.GetTracerProvider().Tracer("github.com/authorhealth/events/v2"),
	}

	var err error
	ewp.successCounter, err = ewp.meter.Int64Counter(
		ewp.applyTelemetryPrefix("requests.executed.successes"),
		metric.WithDescription("The number of successfully executed requests."),
		metric.WithUnit("{success}"))
	if err != nil {
		return nil, fmt.Errorf("constructing success counter: %w", err)
	}

	ewp.failureCounter, err = ewp.meter.Int64Counter(
		ewp.applyTelemetryPrefix("requests.executed.failures"),
		metric.WithDescription("The number of requests that failed to execute."),
		metric.WithUnit("{failure}"))
	if err != nil {
		return nil, fmt.Errorf("constructing failure counter: %w", err)
	}

	ewp.timeHistogram, err = ewp.meter.Float64Histogram(
		ewp.applyTelemetryPrefix("requests.execution_time"),
		metric.WithDescription("The time spent executing a handler request."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60, 120, 300))
	if err != nil {
		return nil, fmt.Errorf("constructing time histogram: %w", err)
	}

	return ewp, nil
}

func (ewp *executorWorkerPool) beforeExecute(ctx context.Context, request *HandlerRequest) (context.Context, *HandlerRequest, error) {
	if ewp.beforeExecuteHook == nil {
		return ctx, request, nil
	}

	return ewp.beforeExecuteHook(ctx, request)
}

func (ewp *executorWorkerPool) applyTelemetryPrefix(k string) string {
	if len(ewp.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", ewp.telemetryPrefix, k)
	}

	return k
}

func (ewp *executorWorkerPool) executeRequests(ctx context.Context, requests []*HandlerRequest) bool {
	if !ewp.executingRequests.CompareAndSwap(false, true) {
		return false
	}

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		ewp.executingRequests.Store(false)
	}()

	workers := make(chan bool, ewp.numExecutorWorkers)

	for _, request := range requests {
		select {
		case <-ewp.shutdownChan:
			return true

		default:
			workers <- true

			request := request
			wg.Add(1)
			go func() {
				defer func() {
					<-workers
					wg.Done()
				}()

				ewp.executeRequest(ctx, request)
			}()
		}
	}

	return true
}

func (ewp *executorWorkerPool) executeRequest(ctx context.Context, request *HandlerRequest) {
	traceAttrs := []attribute.KeyValue{
		attribute.String(ewp.applyTelemetryPrefix("request.id"), request.ID),
		attribute.String(ewp.applyTelemetryPrefix("request.event.id"), request.EventID),
		attribute.Stringer(ewp.applyTelemetryPrefix("request.event.type"), request.EventName),
		attribute.Stringer(ewp.applyTelemetryPrefix("request.type"), request.HandlerName),
		attribute.String(ewp.applyTelemetryPrefix("correlation_id"), request.CorrelationID),
		attribute.Stringer(ewp.applyTelemetryPrefix("request.queue.name"), ewp.queueName),
	}

	if request.EventEntityID != "" {
		traceAttrs = append(traceAttrs, attribute.String(ewp.applyTelemetryPrefix("request.event.entity.id"), request.EventEntityID))
	}

	ctx, span := ewp.tracer.Start(
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

	logger.DebugContext(ctx, fmt.Sprintf("executing %s request", request.HandlerName))

	ctx, request, err := ewp.beforeExecute(ctx, request)
	if err != nil {
		logger.ErrorContext(ctx, "error running before execute", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running before execute")
		return
	}

	err = ewp.store.Transaction(ctx, func(txStore Storer) error {
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

		config := ewp.configMap[request.EventName][request.HandlerName]
		if config == nil {
			config = &HandlerConfig{
				Handler: NewHandler(request.HandlerName, ewp.telemetryPrefix, missingHandlerFunc),
			}
		}

		metricAttrs := []attribute.KeyValue{
			attribute.Stringer(ewp.applyTelemetryPrefix("request.event.type"), request.EventName),
			attribute.Stringer(ewp.applyTelemetryPrefix("request.type"), request.HandlerName),
		}

		err = request.execute(ctx, config)
		if err != nil {
			isRetryable := errors.Is(err, ErrRetryable)
			metricAttrs = append(metricAttrs,
				attribute.Bool(ewp.applyTelemetryPrefix("request.retryable"), isRetryable),
				attribute.Int(ewp.applyTelemetryPrefix("request.errors"), request.Errors),
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

			ewp.failureCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		} else {
			ewp.successCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		}

		err = txStore.HandlerRequests().Update(ctx, request)
		if err != nil {
			return fmt.Errorf("updating %s request: %w", request.HandlerName, err)
		}

		if request.CompletedAt != nil {
			ewp.timeHistogram.Record(ctx, request.CompletedAt.Sub(request.EventTimestamp).Seconds(), metric.WithAttributes(metricAttrs...))
		}

		return nil
	})
	if err != nil {
		logger.ErrorContext(ctx, "error running transaction", Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "error running transaction")
	}
}

func (ewp *executorWorkerPool) shutdown() {
	ewp.shutdownChan <- true
}

type executorStatsObserver struct {
	deadCountGauge             metric.Int64ObservableGauge
	meter                      metric.Meter
	meterCallbackRegistrations []metric.Registration
	store                      Storer
	telemetryPrefix            string
	unexecutedCountGauge       metric.Int64ObservableGauge
	unexecutedMaxAgeGauge      metric.Float64ObservableGauge
}

func newExecutorStatsObserver(
	store Storer,
	telemetryPrefix string,
) (*executorStatsObserver, error) {
	eso := &executorStatsObserver{
		meter:           otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		store:           store,
		telemetryPrefix: telemetryPrefix,
	}

	var err error
	eso.unexecutedCountGauge, err = eso.meter.Int64ObservableGauge(
		eso.applyTelemetryPrefix("requests.unexecuted.count"),
		metric.WithDescription("The number of unexecuted requests in the queue."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("constructing unexecuted request count gauge: %w", err)
	}

	eso.unexecutedMaxAgeGauge, err = eso.meter.Float64ObservableGauge(
		eso.applyTelemetryPrefix("requests.unexecuted.max_age"),
		metric.WithDescription("The age of the oldest unexecuted request in the queue."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("constructing max unexecuted request age gauge: %w", err)
	}

	eso.deadCountGauge, err = eso.meter.Int64ObservableGauge(
		eso.applyTelemetryPrefix("requests.dead.count"),
		metric.WithDescription("The number of dead requests in the queue."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("constructing dead request count gauge: %w", err)
	}

	return eso, nil
}

func (eso *executorStatsObserver) applyTelemetryPrefix(k string) string {
	if len(eso.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", eso.telemetryPrefix, k)
	}

	return k
}

func (eso *executorStatsObserver) registerMeterCallbacks() error {
	var registrations []metric.Registration
	registration, err := eso.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := eso.store.HandlerRequests().CountUnexecuted(ctx)
		if err != nil {
			return fmt.Errorf("counting unexecuted requests: %w", err)
		}

		o.ObserveInt64(eso.unexecutedCountGauge, int64(count))

		return nil
	}, eso.unexecutedCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for unexecuted request count guage: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = eso.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		oldest, err := eso.store.HandlerRequests().FindOldestUnexecuted(ctx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				o.ObserveFloat64(eso.unexecutedMaxAgeGauge, 0)

				return nil
			}

			return fmt.Errorf("finding oldest unexecuted request: %w", err)
		}

		o.ObserveFloat64(eso.unexecutedMaxAgeGauge, time.Since(oldest.EventTimestamp).Seconds())

		return nil
	}, eso.unexecutedMaxAgeGauge)
	if err != nil {
		return fmt.Errorf("registering callback for max unexecuted request age gauge: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = eso.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := eso.store.HandlerRequests().CountDead(ctx)
		if err != nil {
			return fmt.Errorf("counting dead requests: %w", err)
		}

		o.ObserveInt64(eso.deadCountGauge, int64(count))

		return nil
	}, eso.deadCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for dead request count guage: %w", err)
	}

	registrations = append(registrations, registration)

	eso.meterCallbackRegistrations = registrations

	return nil
}

func (eso *executorStatsObserver) unregisterMeterCallbacks() error {
	for _, registration := range eso.meterCallbackRegistrations {
		err := registration.Unregister()
		if err != nil {
			return fmt.Errorf("unregistering meter callback: %w", err)
		}
	}

	eso.meterCallbackRegistrations = nil

	return nil
}

type executorQueueStatsObserver struct {
	deadCountGauge             metric.Int64ObservableGauge
	meter                      metric.Meter
	meterCallbackRegistrations []metric.Registration
	queueName                  ExecutorQueueName
	store                      Storer
	telemetryPrefix            string
	unexecutedCountGauge       metric.Int64ObservableGauge
	unexecutedMaxAgeGauge      metric.Float64ObservableGauge
}

func newExecutorQueueStatsObserver(
	store Storer,
	telemetryPrefix string,
	queueName ExecutorQueueName,
) (*executorQueueStatsObserver, error) {
	eqso := &executorQueueStatsObserver{
		meter:           otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		queueName:       queueName,
		store:           store,
		telemetryPrefix: telemetryPrefix,
	}

	var err error
	eqso.unexecutedCountGauge, err = eqso.meter.Int64ObservableGauge(
		eqso.applyTelemetryPrefix("requests.unexecuted.count"),
		metric.WithDescription("The number of unexecuted requests in the queue."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("constructing unexecuted request count gauge: %w", err)
	}

	eqso.unexecutedMaxAgeGauge, err = eqso.meter.Float64ObservableGauge(
		eqso.applyTelemetryPrefix("requests.unexecuted.max_age"),
		metric.WithDescription("The age of the oldest unexecuted request in the queue."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("constructing max unexecuted request age gauge: %w", err)
	}

	eqso.deadCountGauge, err = eqso.meter.Int64ObservableGauge(
		eqso.applyTelemetryPrefix("requests.dead.count"),
		metric.WithDescription("The number of dead requests in the queue."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("constructing dead request count gauge: %w", err)
	}

	return eqso, nil
}

func (eqso *executorQueueStatsObserver) applyTelemetryPrefix(k string) string {
	if len(eqso.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", eqso.telemetryPrefix, k)
	}

	return k
}

func (eqso *executorQueueStatsObserver) registerMeterCallbacks() error {
	var registrations []metric.Registration
	registration, err := eqso.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := eqso.store.HandlerRequests().CountUnexecutedByQueue(ctx, eqso.queueName)
		if err != nil {
			return fmt.Errorf("counting unexecuted requests by queue: %w", err)
		}

		o.ObserveInt64(
			eqso.unexecutedCountGauge,
			int64(count),
			metric.WithAttributes(
				attribute.Stringer(eqso.applyTelemetryPrefix("request.queue.name"), eqso.queueName),
			),
		)

		return nil
	}, eqso.unexecutedCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for unexecuted request count guage: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = eqso.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		oldest, err := eqso.store.HandlerRequests().FindOldestUnexecutedByQueue(ctx, eqso.queueName)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				o.ObserveFloat64(
					eqso.unexecutedMaxAgeGauge,
					0,
					metric.WithAttributes(
						attribute.Stringer(eqso.applyTelemetryPrefix("request.queue.name"), eqso.queueName),
					),
				)

				return nil
			}

			return fmt.Errorf("finding oldest unexecuted request by queue: %w", err)
		}

		o.ObserveFloat64(
			eqso.unexecutedMaxAgeGauge,
			time.Since(oldest.EventTimestamp).Seconds(),
			metric.WithAttributes(
				attribute.Stringer(eqso.applyTelemetryPrefix("request.queue.name"), eqso.queueName),
			),
		)

		return nil
	}, eqso.unexecutedMaxAgeGauge)
	if err != nil {
		return fmt.Errorf("registering callback for max unexecuted request age gauge: %w", err)
	}

	registrations = append(registrations, registration)

	registration, err = eqso.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		count, err := eqso.store.HandlerRequests().CountDeadByQueue(ctx, eqso.queueName)
		if err != nil {
			return fmt.Errorf("counting dead requests by queue: %w", err)
		}

		o.ObserveInt64(
			eqso.deadCountGauge,
			int64(count),
			metric.WithAttributes(
				attribute.Stringer(eqso.applyTelemetryPrefix("request.queue.name"), eqso.queueName),
			),
		)

		return nil
	}, eqso.deadCountGauge)
	if err != nil {
		return fmt.Errorf("registering callback for dead request count guage: %w", err)
	}

	registrations = append(registrations, registration)

	eqso.meterCallbackRegistrations = registrations

	return nil
}

func (eqso *executorQueueStatsObserver) unregisterMeterCallbacks() error {
	for _, registration := range eqso.meterCallbackRegistrations {
		err := registration.Unregister()
		if err != nil {
			return fmt.Errorf("unregistering meter callback: %w", err)
		}
	}

	eqso.meterCallbackRegistrations = nil

	return nil
}
