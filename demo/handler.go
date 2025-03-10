package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/authorhealth/events"
)

const (
	ApplicationEventHandlerName events.HandlerName = "myWonderfulApplicationEventHandler"
	DomainEventHandlerName      events.HandlerName = "mySpectacularDomainEventHandler"
	FailingEventHandlerName     events.HandlerName = "myBodaciousFailingEventHandler"
)

func ApplicationEventHandler() *events.Handler {
	return events.NewHandler(ApplicationEventHandlerName, "demo", func(ctx context.Context, e *events.Event) error {
		slog.InfoContext(
			ctx,
			"handling application event",
			slog.Group(
				"event",
				"data", e.Data,
				"id", e.ID,
				"name", e.Name,
				"timestamp", e.Timestamp,
			),
		)

		return nil
	})
}

func DomainEventHandler() *events.Handler {
	return events.NewHandler(DomainEventHandlerName, "demo", func(ctx context.Context, e *events.Event) error {
		slog.InfoContext(
			ctx,
			"handling domain event",
			slog.Group(
				"event",
				"data", e.Data,
				"entityId", *e.EntityID,
				"entityName", e.EntityName,
				"id", e.ID,
				"name", e.Name,
				"timestamp", e.Timestamp,
			),
		)

		return nil
	})
}

func FailingEventHandler() *events.Handler {
	return events.NewHandler(FailingEventHandlerName, "demo", func(ctx context.Context, e *events.Event) error {
		return errors.New("oh noes")
	})
}
