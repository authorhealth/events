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

	"github.com/authorhealth/events/v2"
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

var _ events.EventRepository = (*EventRepository)(nil)

func NewEventRepository() *EventRepository {
	return &EventRepository{
		items:                map[string]*EventRepositoryItem{},
		mutex:                &sync.RWMutex{},
		transactionIDCounter: &atomic.Int64{},
	}
}

func (r *EventRepository) CountUnprocessed(ctx context.Context) (int, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "counting unprocessed events")

	var count int
	for _, item := range r.items {
		if item.event.ProcessedAt == nil {
			count++
		}
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

func (r *EventRepository) FindOldestUnprocessed(ctx context.Context) (*events.Event, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding oldest unprocessed event")

	var oldestUnprocessedEvent *events.Event
	for _, item := range r.items {
		if item.event.ProcessedAt != nil {
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

	var unprocessedEvents []*events.Event
	for _, item := range r.items {
		if len(unprocessedEvents) >= limit {
			break
		}

		if item.event.ProcessedAt != nil {
			continue
		}

		unprocessedEvents = append(unprocessedEvents, item.event)
	}

	return unprocessedEvents, nil
}

func (r *EventRepository) Transaction(ctx context.Context, fn func(txRepo events.EventRepository) error) error {
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

type HandlerRequestRepositoryItem struct {
	handlerRequest        *events.HandlerRequest
	lockedByTransactionID int64
}

type HandlerRequestRepository struct {
	items                map[string]*HandlerRequestRepositoryItem
	mutex                *sync.RWMutex
	transactionID        int64
	transactionIDCounter *atomic.Int64
}

func NewHandlerRequestRepository() *HandlerRequestRepository {
	return &HandlerRequestRepository{
		items:                map[string]*HandlerRequestRepositoryItem{},
		mutex:                &sync.RWMutex{},
		transactionIDCounter: &atomic.Int64{},
	}
}

var _ events.HandlerRequestRepository = (*HandlerRequestRepository)(nil)

func (r *HandlerRequestRepository) CountDead(ctx context.Context) (int, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "counting dead handler requests")

	var count int
	for _, item := range r.items {
		if item.handlerRequest.CompletedAt != nil {
			continue
		}

		if item.handlerRequest.CanceledAt != nil {
			continue
		}

		if item.handlerRequest.Errors < item.handlerRequest.MaxErrors {
			continue
		}

		count++
	}

	return count, nil
}

func (r *HandlerRequestRepository) CountUnexecuted(ctx context.Context) (int, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "counting unexecuted handler requests")

	var count int
	for _, item := range r.items {
		if item.handlerRequest.CompletedAt != nil {
			continue
		}

		if item.handlerRequest.CanceledAt != nil {
			continue
		}

		if item.handlerRequest.Errors >= item.handlerRequest.MaxErrors {
			continue
		}

		if item.handlerRequest.BackoffUntil != nil && item.handlerRequest.BackoffUntil.After(time.Now()) {
			continue
		}

		count++
	}

	return count, nil
}

func (r *HandlerRequestRepository) Create(ctx context.Context, handlerRequest *events.HandlerRequest) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	slog.DebugContext(ctx, "creating handler requests", "handlerRequestId", handlerRequest.ID)

	if _, found := r.items[handlerRequest.ID]; found {
		return errors.New("there is already a request with the same ID in the collection")
	}

	r.items[handlerRequest.ID] = &HandlerRequestRepositoryItem{
		handlerRequest: handlerRequest,
	}

	return nil
}

func (r *HandlerRequestRepository) Find(ctx context.Context) iter.Seq2[*events.HandlerRequest, error] {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding handler requests")

	itemsCopy := slices.Collect(maps.Values(r.items))

	return func(yield func(*events.HandlerRequest, error) bool) {
		for _, item := range itemsCopy {
			if !yield(item.handlerRequest, nil) {
				return
			}
		}
	}
}

func (r *HandlerRequestRepository) FindByID(ctx context.Context, id string) (*events.HandlerRequest, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding handler request by ID", "id", id)

	item, found := r.items[id]
	if !found {
		return nil, events.ErrNotFound
	}

	return item.handlerRequest, nil
}

func (r *HandlerRequestRepository) FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*events.HandlerRequest, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	slog.DebugContext(ctx, "finding handler request by ID for update", "id", id, "skipLocked", skipLocked, "transactionId", r.transactionID)

	item, found := r.items[id]
	if !found {
		return nil, events.ErrNotFound
	}

	if skipLocked && item.lockedByTransactionID != 0 && item.lockedByTransactionID != r.transactionID {
		return nil, events.ErrNotFound
	}

	item.lockedByTransactionID = r.transactionID

	return item.handlerRequest, nil
}

func (r *HandlerRequestRepository) FindDead(ctx context.Context) ([]*events.HandlerRequest, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding dead handler requests")

	var deadHandlerRequests []*events.HandlerRequest
	for _, item := range r.items {
		if item.handlerRequest.CompletedAt != nil {
			continue
		}

		if item.handlerRequest.CanceledAt != nil {
			continue
		}

		if item.handlerRequest.Errors < item.handlerRequest.MaxErrors {
			continue
		}

		deadHandlerRequests = append(deadHandlerRequests, item.handlerRequest)
	}

	return deadHandlerRequests, nil
}

func (r *HandlerRequestRepository) FindOldestUnexecuted(ctx context.Context) (*events.HandlerRequest, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding oldest unexecuted handler request")

	var oldestUnexecutedHandlerRequest *events.HandlerRequest
	for _, item := range r.items {
		if item.handlerRequest.CompletedAt != nil {
			continue
		}

		if item.handlerRequest.CanceledAt != nil {
			continue
		}

		if item.handlerRequest.Errors >= item.handlerRequest.MaxErrors {
			continue
		}

		if item.handlerRequest.BackoffUntil != nil && item.handlerRequest.BackoffUntil.After(time.Now()) {
			continue
		}

		if oldestUnexecutedHandlerRequest == nil || item.handlerRequest.EventTimestamp.Before(oldestUnexecutedHandlerRequest.EventTimestamp) {
			oldestUnexecutedHandlerRequest = item.handlerRequest
		}
	}

	return oldestUnexecutedHandlerRequest, nil
}

func (r *HandlerRequestRepository) FindUnexecuted(ctx context.Context, limit int) ([]*events.HandlerRequest, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	slog.DebugContext(ctx, "finding unexecuted handler requests", "limit", limit)

	sortedItems := slices.SortedStableFunc(maps.Values(r.items), func(a *HandlerRequestRepositoryItem, b *HandlerRequestRepositoryItem) int {
		if a.handlerRequest.Priority < b.handlerRequest.Priority {
			return -1
		}

		if a.handlerRequest.Priority > b.handlerRequest.Priority {
			return 1
		}

		return a.handlerRequest.EventTimestamp.Compare(b.handlerRequest.EventTimestamp)
	})

	var unexecutedHandlerRequests []*events.HandlerRequest
	for _, item := range sortedItems {
		if len(unexecutedHandlerRequests) >= limit {
			break
		}

		if item.handlerRequest.CompletedAt != nil {
			continue
		}

		if item.handlerRequest.CanceledAt != nil {
			continue
		}

		if item.handlerRequest.Errors >= item.handlerRequest.MaxErrors {
			continue
		}

		if item.handlerRequest.BackoffUntil != nil && item.handlerRequest.BackoffUntil.After(time.Now()) {
			continue
		}

		unexecutedHandlerRequests = append(unexecutedHandlerRequests, item.handlerRequest)
	}

	return unexecutedHandlerRequests, nil
}

func (r *HandlerRequestRepository) Transaction(ctx context.Context, fn func(txRepo events.HandlerRequestRepository) error) error {
	var txRepo *HandlerRequestRepository
	if r.transactionID == 0 {
		txRepo = &HandlerRequestRepository{
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

	slog.DebugContext(ctx, "starting handler request repository transaction", "transactionId", txRepo.transactionID)

	err := fn(txRepo)

	slog.DebugContext(ctx, "finished handler request repository transaction", "transactionId", txRepo.transactionID, "error", err)

	return err
}

func (r *HandlerRequestRepository) Update(ctx context.Context, handlerRequest *events.HandlerRequest) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	slog.DebugContext(ctx, "updating event", "handlerRequestId", handlerRequest.ID, "transactionId", r.transactionID)

	item, found := r.items[handlerRequest.ID]
	if !found {
		return errors.New("there is no handler request with the same ID in the collection")
	}

	if item.lockedByTransactionID != 0 && item.lockedByTransactionID != r.transactionID {
		return errors.New("this handler request is locked by another transaction")
	}

	item.lockedByTransactionID = 0

	item.handlerRequest = handlerRequest

	return nil
}
