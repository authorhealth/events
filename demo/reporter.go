package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/authorhealth/events"
)

type Reporter struct {
	done      chan bool
	eventRepo events.Repository
	shutdown  chan bool
}

func NewReporter(eventRepo events.Repository) *Reporter {
	return &Reporter{
		done:      make(chan bool, 1),
		eventRepo: eventRepo,
		shutdown:  make(chan bool, 1),
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

	deadEventCount, err := r.eventRepo.CountDead(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "error counting dead events", events.Err(err))
	}

	slog.InfoContext(
		ctx,
		"event system report",
		slog.Int("unprocessedEventCount", unprocessedEventCount),
		slog.Float64("maxUnprocessedEventAge", maxUnprocessedEventAge),
		slog.Int("deadEventCount", deadEventCount),
	)
}
