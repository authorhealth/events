package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/authorhealth/events"
)

const (
	ApplicationEventName events.EventName = "myCoolApplicationEvent"
	DomainEventName      events.EventName = "myRadDomainEvent"
)

type ApplicationEventProducer struct {
	done      chan bool
	eventRepo events.Repository
	shutdown  chan bool
}

func NewApplicationEventProducer(eventRepo events.Repository) *ApplicationEventProducer {
	return &ApplicationEventProducer{
		done:      make(chan bool, 1),
		eventRepo: eventRepo,
		shutdown:  make(chan bool, 1),
	}
}

func (p *ApplicationEventProducer) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			p.produceEvent(ctx)

		case <-p.shutdown:
			p.done <- true
			return
		}
	}
}

func (p *ApplicationEventProducer) Shutdown(ctx context.Context) error {
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

func (p *ApplicationEventProducer) produceEvent(ctx context.Context) {
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

type DomainEventProducer struct {
	done      chan bool
	eventRepo events.Repository
	shutdown  chan bool
}

func NewDomainEventProducer(eventRepo events.Repository) *DomainEventProducer {
	return &DomainEventProducer{
		done:      make(chan bool, 1),
		eventRepo: eventRepo,
		shutdown:  make(chan bool, 1),
	}
}

func (p *DomainEventProducer) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			p.produceEvent(ctx)

		case <-p.shutdown:
			p.done <- true
			return
		}
	}
}

func (p *DomainEventProducer) Shutdown(ctx context.Context) error {
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

func (p *DomainEventProducer) produceEvent(ctx context.Context) {
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
