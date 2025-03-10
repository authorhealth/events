package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/authorhealth/events/v2"
)

type Reporter struct {
	done               chan bool
	eventRepo          events.EventRepository
	handlerRequestRepo events.HandlerRequestRepository
	shutdown           chan bool
}

func NewReporter(eventRepo events.EventRepository, handlerRequestRepo events.HandlerRequestRepository) *Reporter {
	return &Reporter{
		done:               make(chan bool, 1),
		eventRepo:          eventRepo,
		handlerRequestRepo: handlerRequestRepo,
		shutdown:           make(chan bool, 1),
	}
}

func (r *Reporter) Start(ctx context.Context, interval time.Duration) error {
	defer func() {
		r.done <- true
	}()

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			r.report(ctx)

		case <-r.shutdown:
			return nil
		}
	}
}

func (r *Reporter) Shutdown(ctx context.Context) error {
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

func (r *Reporter) report(ctx context.Context) {
	unprocessedEventCount, err := r.eventRepo.CountUnprocessed(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting unprocessed events", events.Err(err))
	}

	oldestUnprocessedEvent, err := r.eventRepo.FindOldestUnprocessed(ctx)
	if err != nil && !errors.Is(err, events.ErrNotFound) {
		slog.ErrorContext(ctx, "error finding oldest unprocessed event", events.Err(err))
	}

	var maxUnprocessedEventAge float64
	if oldestUnprocessedEvent != nil {
		maxUnprocessedEventAge = time.Since(oldestUnprocessedEvent.Timestamp).Seconds()
	}

	unexecutedHandlerCount, err := r.handlerRequestRepo.CountUnexecuted(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting unexecuted handler requests", events.Err(err))
	}

	oldestUnexecutedHandlerRequest, err := r.handlerRequestRepo.FindOldestUnexecuted(ctx)
	if err != nil && !errors.Is(err, events.ErrNotFound) {
		slog.ErrorContext(ctx, "error finding oldest unexecuted handler request", events.Err(err))
	}

	var maxUnexecutedHandlerRequestAge float64
	if oldestUnexecutedHandlerRequest != nil {
		maxUnexecutedHandlerRequestAge = time.Since(oldestUnexecutedHandlerRequest.EventTimestamp).Seconds()
	}

	deadRequestCount, err := r.handlerRequestRepo.CountDead(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting dead handler requests", events.Err(err))
	}

	slog.InfoContext(
		ctx,
		"event system report",
		slog.Int("unprocessedEventCount", unprocessedEventCount),
		slog.Float64("maxUnprocessedEventAge", maxUnprocessedEventAge),
		slog.Int("unexecutedHandlerCount", unexecutedHandlerCount),
		slog.Float64("maxUnexecutedHandlerRequestAge", maxUnexecutedHandlerRequestAge),
		slog.Int("deadRequestCount", deadRequestCount),
	)
}
