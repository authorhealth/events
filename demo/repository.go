package main

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/authorhealth/events"
)

type EventRepositoryItem struct {
	event                 *events.Event
	lockedByTransactionID int64
}

type EventRepository struct {
	items                map[string]*EventRepositoryItem
	mutex                *sync.RWMutex
	transactionID        int64
	transactionIDCounter *atomic.Int64
}

var _ events.Repository = (*EventRepository)(nil)

func NewEventRepository() *EventRepository {
	return &EventRepository{
		items:                map[string]*EventRepositoryItem{},
		mutex:                &sync.RWMutex{},
		transactionIDCounter: &atomic.Int64{},
	}
}

func (r *EventRepository) CountDead(ctx context.Context) (int, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "counting dead handler requests")

	var count int
	for _, item := range r.items {
		if item.event.ProcessedAt != nil {
			continue
		}

		if item.event.ArchivedAt != nil {
			continue
		}

		if item.event.Errors < events.MaxEventErrors {
			continue
		}

		count++
	}

	return count, nil
}

func (r *EventRepository) CountUnprocessed(ctx context.Context) (int, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "counting unprocessed events")

	var count int
	for _, item := range r.items {
		if item.event.ProcessedAt != nil {
			continue
		}

		if item.event.ArchivedAt != nil {
			continue
		}

		if item.event.Errors >= events.MaxEventErrors {
			continue
		}

		if item.event.BackoffUntil != nil && item.event.BackoffUntil.After(time.Now()) {
			continue
		}

		count++
	}

	return count, nil
}

func (r *EventRepository) Create(ctx context.Context, event *events.Event) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	slog.DebugContext(ctx, "creating event", "eventId", event.ID)

	if _, found := r.items[event.ID]; found {
		return errors.New("there is already an event with the same ID in the collection")
	}

	r.items[event.ID] = &EventRepositoryItem{
		event: event,
	}

	return nil
}

func (r *EventRepository) Find(ctx context.Context) iter.Seq2[*events.Event, error] {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding events")

	items := slices.Collect(maps.Values(r.items))

	return func(yield func(*events.Event, error) bool) {
		for _, item := range items {
			if !yield(item.event, nil) {
				return
			}
		}
	}
}

func (r *EventRepository) FindByID(ctx context.Context, id string) (*events.Event, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding event by ID", "id", id)

	item, found := r.items[id]
	if !found {
		return nil, events.ErrNotFound
	}

	return item.event, nil
}

func (r *EventRepository) FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*events.Event, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	slog.DebugContext(ctx, "finding event by ID for update", "id", id, "skipLocked", skipLocked, "transactionId", r.transactionID)

	item, found := r.items[id]
	if !found {
		return nil, events.ErrNotFound
	}

	if skipLocked && item.lockedByTransactionID != 0 && item.lockedByTransactionID != r.transactionID {
		return nil, events.ErrNotFound
	}

	item.lockedByTransactionID = r.transactionID

	return item.event, nil
}

func (r *EventRepository) FindDead(ctx context.Context) ([]*events.Event, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding dead events")

	var deadEvents []*events.Event
	for _, item := range r.items {
		if item.event.ProcessedAt != nil {
			continue
		}

		if item.event.ArchivedAt != nil {
			continue
		}

		if item.event.Errors < events.MaxEventErrors {
			continue
		}

		deadEvents = append(deadEvents, item.event)
	}

	return deadEvents, nil
}

func (r *EventRepository) FindOldestUnprocessed(ctx context.Context) (*events.Event, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding oldest unprocessed event")

	var oldestUnprocessedEvent *events.Event
	for _, item := range r.items {
		if item.event.ProcessedAt != nil {
			continue
		}

		if item.event.ArchivedAt != nil {
			continue
		}

		if item.event.Errors >= events.MaxEventErrors {
			continue
		}

		if item.event.BackoffUntil != nil && item.event.BackoffUntil.After(time.Now()) {
			continue
		}

		if oldestUnprocessedEvent == nil || item.event.Timestamp.Before(oldestUnprocessedEvent.Timestamp) {
			oldestUnprocessedEvent = item.event
		}
	}

	return oldestUnprocessedEvent, nil
}

func (r *EventRepository) FindUnprocessed(ctx context.Context, limit int) ([]*events.Event, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding unprocessed events", "limit", limit)

	sortedItems := slices.SortedStableFunc(maps.Values(r.items), func(a *EventRepositoryItem, b *EventRepositoryItem) int {
		return a.event.Timestamp.Compare(b.event.Timestamp)
	})

	var unprocessedEvents []*events.Event
	for _, item := range sortedItems {
		if len(unprocessedEvents) >= limit {
			break
		}

		if item.event.ProcessedAt != nil {
			continue
		}

		if item.event.ArchivedAt != nil {
			continue
		}

		if item.event.Errors >= events.MaxEventErrors {
			continue
		}

		if item.event.BackoffUntil != nil && item.event.BackoffUntil.After(time.Now()) {
			continue
		}

		unprocessedEvents = append(unprocessedEvents, item.event)
	}

	return unprocessedEvents, nil
}

func (r *EventRepository) Transaction(ctx context.Context, fn func(txRepo events.Repository) error) error {
	var txRepo *EventRepository
	if r.transactionID == 0 {
		txRepo = &EventRepository{
			items:                r.items,
			mutex:                r.mutex,
			transactionID:        r.transactionIDCounter.Add(1),
			transactionIDCounter: r.transactionIDCounter,
		}

		defer func() {
			r.mutex.Lock()
			defer r.mutex.Unlock()

			for _, item := range r.items {
				item.lockedByTransactionID = 0
			}
		}()
	} else {
		txRepo = r
	}

	slog.DebugContext(ctx, "starting event repository transaction", "transactionId", txRepo.transactionID)

	err := fn(txRepo)

	slog.DebugContext(ctx, "finished event repository transaction", "transactionId", txRepo.transactionID, "error", err)

	return err
}

func (r *EventRepository) Update(ctx context.Context, event *events.Event) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	slog.DebugContext(ctx, "updating event", "eventId", event.ID, "transactionId", r.transactionID)

	item, found := r.items[event.ID]
	if !found {
		return errors.New("there is no event with the same ID in the collection")
	}

	if item.lockedByTransactionID != 0 && item.lockedByTransactionID != r.transactionID {
		return errors.New("this event is locked by another transaction")
	}

	item.lockedByTransactionID = 0

	item.event = event

	return nil
}
