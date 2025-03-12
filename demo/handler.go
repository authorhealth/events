package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/authorhealth/events/v2"
)

const (
	ApplicationEventHandlerName events.HandlerName = "myWonderfulApplicationEventHandler"
	DomainEventHandlerName      events.HandlerName = "mySpectacularDomainEventHandler"
	FailingEventHandlerName     events.HandlerName = "myBodaciousFailingEventHandler"
)

func ApplicationEventHandler() *events.Handler {
	return events.NewHandler(ApplicationEventHandlerName, "demo", func(ctx context.Context, hr *events.HandlerRequest) error {
		slog.InfoContext(
			ctx,
			"handling application event",
			slog.Group(
				"event",
				"data", hr.EventData,
				"id", hr.EventID,
				"name", hr.EventName,
				"timestamp", hr.EventTimestamp,
			),
		)

		return nil
	})
}

func DomainEventHandler() *events.Handler {
	return events.NewHandler(DomainEventHandlerName, "demo", func(ctx context.Context, hr *events.HandlerRequest) error {
		slog.InfoContext(
			ctx,
			"handling domain event",
			slog.Group(
				"event",
				"data", hr.EventData,
				"entityId", hr.EventEntityID,
				"entityName", hr.EventEntityName,
				"id", hr.EventID,
				"name", hr.EventName,
				"timestamp", hr.EventTimestamp,
			),
		)

		return nil
	})
}

func FailingEventHandler() *events.Handler {
	return events.NewHandler(FailingEventHandlerName, "demo", func(ctx context.Context, hr *events.HandlerRequest) error {
		return errors.New("oh noes")
	})
}
