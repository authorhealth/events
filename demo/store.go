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
	"github.com/google/uuid"
)

type Database struct {
	eventTable               map[string]*EventTableRow
	eventTableMutex          sync.RWMutex
	handlerRequestTable      map[string]*HandlerRequestTableRow
	handlerRequestTableMutex sync.RWMutex
	transactionIDCounter     atomic.Int64
}

func NewDatabase() *Database {
	return &Database{
		eventTable:          map[string]*EventTableRow{},
		handlerRequestTable: map[string]*HandlerRequestTableRow{},
	}
}

type EventTableRow struct {
	event                 *events.Event
	lockedByTransactionID int64
}

type HandlerRequestTableRow struct {
	handlerRequest        *events.HandlerRequest
	lockedByTransactionID int64
}

type Store struct {
	db                 *Database
	eventRepo          *EventRepository
	handlerRequestRepo *HandlerRequestRepository
	transactionID      int64
}

var _ events.Storer = (*Store)(nil)

func NewStore(db *Database) *Store {
	store := &Store{
		db: db,
	}

	store.eventRepo = &EventRepository{
		db:    db,
		store: store,
	}

	store.handlerRequestRepo = &HandlerRequestRepository{
		db:    db,
		store: store,
	}

	return store
}

func (s *Store) Events() events.EventRepository {
	return s.eventRepo
}

func (s *Store) HandlerRequests() events.HandlerRequestRepository {
	return s.handlerRequestRepo
}

func (s *Store) Transaction(ctx context.Context, fn func(txStore events.Storer) error) error {
	var txStore *Store
	if s.transactionID == 0 {
		txStore = NewStore(s.db)
		txStore.transactionID = s.db.transactionIDCounter.Add(1)

		defer func() {
			s.db.eventTableMutex.Lock()

			for _, row := range s.db.eventTable {
				if row.lockedByTransactionID == txStore.transactionID {
					row.lockedByTransactionID = 0
				}
			}

			s.db.eventTableMutex.Unlock()

			s.db.handlerRequestTableMutex.Lock()

			for _, row := range s.db.handlerRequestTable {
				if row.lockedByTransactionID == txStore.transactionID {
					row.lockedByTransactionID = 0
				}
			}

			s.db.handlerRequestTableMutex.Unlock()
		}()
	} else {
		txStore = s
	}

	slog.DebugContext(ctx, "starting transaction", "transactionId", txStore.transactionID)

	err := fn(txStore)

	slog.DebugContext(ctx, "finished transaction", "transactionId", txStore.transactionID, "error", err)

	return err
}

type EventRepository struct {
	db    *Database
	store *Store
}

var _ events.EventRepository = (*EventRepository)(nil)

func (r *EventRepository) CountUnprocessed(ctx context.Context) (int, error) {
	r.db.eventTableMutex.RLock()
	defer r.db.eventTableMutex.RUnlock()

	slog.DebugContext(ctx, "counting unprocessed events")

	var count int
	for _, row := range r.db.eventTable {
		if row.event.ProcessedAt == nil {
			count++
		}
	}

	return count, nil
}

func (r *EventRepository) Create(ctx context.Context, event *events.Event) error {
	r.db.eventTableMutex.Lock()
	defer r.db.eventTableMutex.Unlock()

	slog.DebugContext(ctx, "creating event", "eventId", event.ID)

	if _, found := r.db.eventTable[event.ID]; found {
		return errors.New("there is already an event with the same ID in the table")
	}

	event.CorrelationID = uuid.Must(uuid.NewV7()).String()

	r.db.eventTable[event.ID] = &EventTableRow{
		event: event,
	}

	return nil
}

func (r *EventRepository) Find(ctx context.Context) iter.Seq2[*events.Event, error] {
	r.db.eventTableMutex.RLock()
	defer r.db.eventTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding events")

	rows := slices.Collect(maps.Values(r.db.eventTable))

	return func(yield func(*events.Event, error) bool) {
		for _, row := range rows {
			if !yield(row.event, nil) {
				return
			}
		}
	}
}

func (r *EventRepository) FindByID(ctx context.Context, id string) (*events.Event, error) {
	r.db.eventTableMutex.RLock()
	defer r.db.eventTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding event by ID", "id", id)

	row, found := r.db.eventTable[id]
	if !found {
		return nil, events.ErrNotFound
	}

	return row.event, nil
}

func (r *EventRepository) FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*events.Event, error) {
	r.db.eventTableMutex.Lock()
	defer r.db.eventTableMutex.Unlock()

	slog.DebugContext(ctx, "finding event by ID for update", "id", id, "skipLocked", skipLocked, "transactionId", r.store.transactionID)

	row, found := r.db.eventTable[id]
	if !found {
		return nil, events.ErrNotFound
	}

	if skipLocked && row.lockedByTransactionID != 0 && row.lockedByTransactionID != r.store.transactionID {
		return nil, events.ErrNotFound
	}

	row.lockedByTransactionID = r.store.transactionID

	return row.event, nil
}

func (r *EventRepository) FindOldestUnprocessed(ctx context.Context) (*events.Event, error) {
	r.db.eventTableMutex.RLock()
	defer r.db.eventTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding oldest unprocessed event")

	var oldestUnprocessedEvent *events.Event
	for _, row := range r.db.eventTable {
		if row.event.ProcessedAt != nil {
			continue
		}

		if oldestUnprocessedEvent == nil || row.event.Timestamp.Before(oldestUnprocessedEvent.Timestamp) {
			oldestUnprocessedEvent = row.event
		}
	}

	if oldestUnprocessedEvent == nil {
		return nil, events.ErrNotFound
	}

	return oldestUnprocessedEvent, nil
}

func (r *EventRepository) FindUnprocessed(ctx context.Context, limit int) ([]*events.Event, error) {
	r.db.eventTableMutex.RLock()
	defer r.db.eventTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding unprocessed events", "limit", limit)

	var unprocessedEvents []*events.Event
	for _, row := range r.db.eventTable {
		if len(unprocessedEvents) >= limit {
			break
		}

		if row.event.ProcessedAt != nil {
			continue
		}

		unprocessedEvents = append(unprocessedEvents, row.event)
	}

	return unprocessedEvents, nil
}

func (r *EventRepository) Update(ctx context.Context, event *events.Event) error {
	r.db.eventTableMutex.Lock()
	defer r.db.eventTableMutex.Unlock()

	slog.DebugContext(ctx, "updating event", "eventId", event.ID, "transactionId", r.store.transactionID)

	row, found := r.db.eventTable[event.ID]
	if !found {
		return errors.New("there is no event with the same ID in the table")
	}

	if row.lockedByTransactionID != 0 && row.lockedByTransactionID != r.store.transactionID {
		return errors.New("this event is locked by another transaction")
	}

	row.lockedByTransactionID = 0

	row.event = event

	return nil
}

type HandlerRequestRepository struct {
	db    *Database
	store *Store
}

var _ events.HandlerRequestRepository = (*HandlerRequestRepository)(nil)

func (r *HandlerRequestRepository) CountDead(ctx context.Context) (int, error) {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "counting dead handler requests")

	var count int
	for _, row := range r.db.handlerRequestTable {
		if row.handlerRequest.CompletedAt != nil {
			continue
		}

		if row.handlerRequest.CanceledAt != nil {
			continue
		}

		if row.handlerRequest.Errors < row.handlerRequest.MaxErrors {
			continue
		}

		count++
	}

	return count, nil
}

func (r *HandlerRequestRepository) CountUnexecuted(ctx context.Context) (int, error) {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "counting unexecuted handler requests")

	var count int
	for _, row := range r.db.handlerRequestTable {
		if row.handlerRequest.CompletedAt != nil {
			continue
		}

		if row.handlerRequest.CanceledAt != nil {
			continue
		}

		if row.handlerRequest.Errors >= row.handlerRequest.MaxErrors {
			continue
		}

		if row.handlerRequest.BackoffUntil != nil && row.handlerRequest.BackoffUntil.After(time.Now()) {
			continue
		}

		count++
	}

	return count, nil
}

func (r *HandlerRequestRepository) Create(ctx context.Context, handlerRequest *events.HandlerRequest) error {
	r.db.handlerRequestTableMutex.Lock()
	defer r.db.handlerRequestTableMutex.Unlock()

	slog.DebugContext(ctx, "creating handler requests", "handlerRequestId", handlerRequest.ID)

	if _, found := r.db.handlerRequestTable[handlerRequest.ID]; found {
		return errors.New("there is already a request with the same ID in the table")
	}

	r.db.handlerRequestTable[handlerRequest.ID] = &HandlerRequestTableRow{
		handlerRequest: handlerRequest,
	}

	return nil
}

func (r *HandlerRequestRepository) Find(ctx context.Context) iter.Seq2[*events.HandlerRequest, error] {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding handler requests")

	rowsCopy := slices.Collect(maps.Values(r.db.handlerRequestTable))

	return func(yield func(*events.HandlerRequest, error) bool) {
		for _, row := range rowsCopy {
			if !yield(row.handlerRequest, nil) {
				return
			}
		}
	}
}

func (r *HandlerRequestRepository) FindByID(ctx context.Context, id string) (*events.HandlerRequest, error) {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding handler request by ID", "id", id)

	row, found := r.db.handlerRequestTable[id]
	if !found {
		return nil, events.ErrNotFound
	}

	return row.handlerRequest, nil
}

func (r *HandlerRequestRepository) FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*events.HandlerRequest, error) {
	r.db.handlerRequestTableMutex.Lock()
	defer r.db.handlerRequestTableMutex.Unlock()

	slog.DebugContext(ctx, "finding handler request by ID for update", "id", id, "skipLocked", skipLocked, "transactionId", r.store.transactionID)

	row, found := r.db.handlerRequestTable[id]
	if !found {
		return nil, events.ErrNotFound
	}

	if skipLocked && row.lockedByTransactionID != 0 && row.lockedByTransactionID != r.store.transactionID {
		return nil, events.ErrNotFound
	}

	row.lockedByTransactionID = r.store.transactionID

	return row.handlerRequest, nil
}

func (r *HandlerRequestRepository) FindDead(ctx context.Context, limit int, offset int) ([]*events.HandlerRequest, error) {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding dead handler requests")

	var deadHandlerRequests []*events.HandlerRequest
	for _, row := range r.db.handlerRequestTable {
		if row.handlerRequest.CompletedAt != nil {
			continue
		}

		if row.handlerRequest.CanceledAt != nil {
			continue
		}

		if row.handlerRequest.Errors < row.handlerRequest.MaxErrors {
			continue
		}

		deadHandlerRequests = append(deadHandlerRequests, row.handlerRequest)
	}

	if offset >= len(deadHandlerRequests) {
		return nil, nil
	}

	end := min(offset+limit, len(deadHandlerRequests))

	return deadHandlerRequests[offset:end], nil
}

func (r *HandlerRequestRepository) FindOldestUnexecuted(ctx context.Context) (*events.HandlerRequest, error) {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding oldest unexecuted handler request")

	var oldestUnexecutedHandlerRequest *events.HandlerRequest
	for _, row := range r.db.handlerRequestTable {
		if row.handlerRequest.CompletedAt != nil {
			continue
		}

		if row.handlerRequest.CanceledAt != nil {
			continue
		}

		if row.handlerRequest.Errors >= row.handlerRequest.MaxErrors {
			continue
		}

		if row.handlerRequest.BackoffUntil != nil && row.handlerRequest.BackoffUntil.After(time.Now()) {
			continue
		}

		if oldestUnexecutedHandlerRequest == nil || row.handlerRequest.EventTimestamp.Before(oldestUnexecutedHandlerRequest.EventTimestamp) {
			oldestUnexecutedHandlerRequest = row.handlerRequest
		}
	}

	if oldestUnexecutedHandlerRequest == nil {
		return nil, events.ErrNotFound
	}

	return oldestUnexecutedHandlerRequest, nil
}

func (r *HandlerRequestRepository) FindUnexecuted(ctx context.Context, limit int) ([]*events.HandlerRequest, error) {
	r.db.handlerRequestTableMutex.RLock()
	defer r.db.handlerRequestTableMutex.RUnlock()

	slog.DebugContext(ctx, "finding unexecuted handler requests", "limit", limit)

	sortedItems := slices.SortedStableFunc(maps.Values(r.db.handlerRequestTable), func(a *HandlerRequestTableRow, b *HandlerRequestTableRow) int {
		if a.handlerRequest.Priority < b.handlerRequest.Priority {
			return -1
		}

		if a.handlerRequest.Priority > b.handlerRequest.Priority {
			return 1
		}

		return a.handlerRequest.EventTimestamp.Compare(b.handlerRequest.EventTimestamp)
	})

	var unexecutedHandlerRequests []*events.HandlerRequest
	for _, row := range sortedItems {
		if len(unexecutedHandlerRequests) >= limit {
			break
		}

		if row.handlerRequest.CompletedAt != nil {
			continue
		}

		if row.handlerRequest.CanceledAt != nil {
			continue
		}

		if row.handlerRequest.Errors >= row.handlerRequest.MaxErrors {
			continue
		}

		if row.handlerRequest.BackoffUntil != nil && row.handlerRequest.BackoffUntil.After(time.Now()) {
			continue
		}

		unexecutedHandlerRequests = append(unexecutedHandlerRequests, row.handlerRequest)
	}

	return unexecutedHandlerRequests, nil
}

func (r *HandlerRequestRepository) Update(ctx context.Context, handlerRequest *events.HandlerRequest) error {
	r.db.handlerRequestTableMutex.Lock()
	defer r.db.handlerRequestTableMutex.Unlock()

	slog.DebugContext(ctx, "updating handler request", "handlerRequestId", handlerRequest.ID, "transactionId", r.store.transactionID)

	row, found := r.db.handlerRequestTable[handlerRequest.ID]
	if !found {
		return errors.New("there is no handler request with the same ID in the table")
	}

	if row.lockedByTransactionID != 0 && row.lockedByTransactionID != r.store.transactionID {
		return errors.New("this handler request is locked by another transaction")
	}

	row.lockedByTransactionID = 0

	row.handlerRequest = handlerRequest

	return nil
}
