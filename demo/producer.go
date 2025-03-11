package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/authorhealth/events/v2"
)

const (
	ApplicationEventName events.EventName = "myCoolApplicationEvent"
	DomainEventName      events.EventName = "myRadDomainEvent"
)

type EventProducer struct {
	done      chan bool
	eventRepo events.EventRepository
	shutdown  chan bool
}

func NewEventProducer(eventRepo events.EventRepository) *EventProducer {
	return &EventProducer{
		done:      make(chan bool, 1),
		eventRepo: eventRepo,
		shutdown:  make(chan bool, 1),
	}
}

func (p *EventProducer) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			p.produceApplicationEvent(ctx)
			p.produceDomainEvent(ctx)

		case <-p.shutdown:
			p.done <- true
			return
		}
	}
}

func (p *EventProducer) Shutdown(ctx context.Context) error {
	p.shutdown <- true

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-p.done:
			return nil
		}
	}
}

func (p *EventProducer) produceApplicationEvent(ctx context.Context) {
	event, err := events.NewApplicationEvent(ApplicationEventName, map[string]any{
		"a": "alpha",
		"b": 12345,
		"c": []string{"fee", "fie", "foe", "fum"},
	})
	if err != nil {
		slog.Error("new application event", events.Err(err))
		return
	}

	err = p.eventRepo.Create(ctx, event)
	if err != nil {
		slog.Error("create application event", events.Err(err))
		return
	}
}

func (p *EventProducer) produceDomainEvent(ctx context.Context) {
	event, err := events.NewDomainEvent(
		DomainEventName,
		"gnarlyEntityID",
		"awesomeEntityName",
		map[string]any{
			"d": "delta",
			"e": 67890,
			"f": []string{"eenie", "meenie", "miney", "mo"},
		},
	)
	if err != nil {
		slog.Error("new domain event", events.Err(err))
		return
	}

	err = p.eventRepo.Create(ctx, event)
	if err != nil {
		slog.Error("create domain event", events.Err(err))
		return
	}
}
