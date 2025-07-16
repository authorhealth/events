package events

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type HandlerFunc func(context.Context, *HandlerRequest) error

type HandlerName string

func (n HandlerName) String() string {
	return string(n)
}

type HandlerPanicError struct {
	panicMsg any
	stack    string
}

var _ error = (*HandlerPanicError)(nil)

func NewHandlerPanicError(panicMsg any, stack string) *HandlerPanicError {
	return &HandlerPanicError{
		panicMsg: panicMsg,
		stack:    stack,
	}
}

func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("recovered panic while executing handler request: %v\n%s", e.panicMsg, e.stack)
}

func (e *HandlerPanicError) Stack() string {
	return e.stack
}

type Handler struct {
	executionCounter metric.Int64Counter
	executionTime    metric.Float64Histogram
	f                HandlerFunc
	name             HandlerName
	telemetryPrefix  string
	tracer           trace.Tracer
}

func NewHandler(name HandlerName, telemetryPrefix string, f HandlerFunc) *Handler {
	h, err := NewHandlerErr(name, telemetryPrefix, f)
	if err != nil {
		panic(err)
	}

	return h
}

func NewHandlerErr(name HandlerName, telemetryPrefix string, f HandlerFunc) (*Handler, error) {
	h := &Handler{
		f:               f,
		name:            name,
		telemetryPrefix: telemetryPrefix,
		tracer:          otel.GetTracerProvider().Tracer("github.com/authorhealth/events/v2"),
	}

	meter := otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2")

	var err error
	h.executionCounter, err = meter.Int64Counter(
		h.applyTelemetryPrefix("event_handlers.executions"),
		metric.WithDescription("The number of event handler executions."),
		metric.WithUnit("{execution}"))
	if err != nil {
		return nil, fmt.Errorf("constructing execution counter: %w", err)
	}

	h.executionTime, err = meter.Float64Histogram(
		h.applyTelemetryPrefix("event_handlers.execution_time"),
		metric.WithDescription("The time spent executing an event handler."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10))
	if err != nil {
		return nil, fmt.Errorf("constructing execuction time instrument: %w", err)
	}

	return h, nil
}

func (h *Handler) Name() HandlerName {
	return h.name
}

func (h *Handler) Do(ctx context.Context, request *HandlerRequest) (err error) {
	ctx, span := h.tracer.Start(ctx, fmt.Sprintf("%s handler", h.name))
	defer span.End()

	defer func() {
		if rvr := recover(); rvr != nil {
			err = NewHandlerPanicError(rvr, string(debug.Stack()))
			span.SetAttributes(attribute.String(h.applyTelemetryPrefix("request.event.panic"), fmt.Sprintf("%v", rvr)))
			span.SetStatus(codes.Error, "recovered panic while executing handler request")
		}
	}()

	metricAttrs := []attribute.KeyValue{
		attribute.Stringer(h.applyTelemetryPrefix("event_handler.name"), h.name),
		attribute.Stringer(h.applyTelemetryPrefix("event.type"), request.EventName),
	}

	start := time.Now()
	err = h.f(ctx, request)
	duration := time.Since(start)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "error handling event")
	}

	metricAttrs = append(metricAttrs, attribute.Bool(h.applyTelemetryPrefix("event_handler.error"), err != nil))
	h.executionCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	h.executionTime.Record(ctx, duration.Seconds(), metric.WithAttributes(metricAttrs...))

	return err
}

func (h *Handler) applyTelemetryPrefix(k string) string {
	if len(h.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", h.telemetryPrefix, k)
	}

	return k
}
