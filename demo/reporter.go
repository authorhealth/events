package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/authorhealth/events/v2"
)

type EventSystemReporter struct {
	done     chan bool
	shutdown chan bool
	store    events.Storer
}

func NewEventSystemReporter(store events.Storer) *EventSystemReporter {
	return &EventSystemReporter{
		done:     make(chan bool, 1),
		shutdown: make(chan bool, 1),
		store:    store,
	}
}

func (r *EventSystemReporter) Start(ctx context.Context, interval time.Duration) {
	defer func() {
		r.done <- true
	}()

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			r.report(ctx)

		case <-r.shutdown:
			return
		}
	}
}

func (r *EventSystemReporter) Shutdown(ctx context.Context) error {
	r.shutdown <- true

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.done:
			return nil
		}
	}
}

func (r *EventSystemReporter) report(ctx context.Context) {
	unprocessedEventCount, err := r.store.Events().CountUnprocessed(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting unprocessed events", events.Err(err))
	}

	oldestUnprocessedEvent, err := r.store.Events().FindOldestUnprocessed(ctx)
	if err != nil && !errors.Is(err, events.ErrNotFound) {
		slog.ErrorContext(ctx, "error finding oldest unprocessed event", events.Err(err))
	}

	var maxUnprocessedEventAge float64
	if oldestUnprocessedEvent != nil {
		maxUnprocessedEventAge = time.Since(oldestUnprocessedEvent.Timestamp).Seconds()
	}

	unexecutedRequestCount, err := r.store.HandlerRequests().CountUnexecuted(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting unexecuted handler requests", events.Err(err))
	}

	oldestUnexecutedRequest, err := r.store.HandlerRequests().FindOldestUnexecuted(ctx)
	if err != nil && !errors.Is(err, events.ErrNotFound) {
		slog.ErrorContext(ctx, "error finding oldest unexecuted handler request", events.Err(err))
	}

	var maxUnexecutedRequestAge float64
	if oldestUnexecutedRequest != nil {
		maxUnexecutedRequestAge = time.Since(oldestUnexecutedRequest.EventTimestamp).Seconds()
	}

	deadRequestCount, err := r.store.HandlerRequests().CountDead(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting dead handler requests", events.Err(err))
	}

	slog.InfoContext(
		ctx,
		"event system report",
		slog.Int("unprocessedEventCount", unprocessedEventCount),
		slog.Float64("maxUnprocessedEventAge", maxUnprocessedEventAge),
		slog.Int("unexecutedRequestCount", unexecutedRequestCount),
		slog.Float64("maxUnexecutedRequestAge", maxUnexecutedRequestAge),
		slog.Int("deadRequestCount", deadRequestCount),
	)
}
