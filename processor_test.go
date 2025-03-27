package events

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestProcessor_processEvents(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	fooUpdatedEvent, err := NewApplicationEvent(fooUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)

	barUpdatedEvent, err := NewDomainEvent(barUpdatedEventName, uuid.New().String(), "bar", map[string]any{"key": "val"})
	assert.NoError(err)

	events := []*Event{
		fooUpdatedEvent,
		barUpdatedEvent,
	}

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil).Once()
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, barUpdatedEvent.ID, true).Return(barUpdatedEvent, nil).Once()
	txEventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == fooUpdatedEvent.ID &&
			assert.NotNil(e.ProcessedAt)
	})).Return(nil).Once()
	txEventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == barUpdatedEvent.ID &&
			assert.NotNil(e.ProcessedAt)
	})).Return(nil).Once()

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.EventID == fooUpdatedEvent.ID
	})).Return(nil).Once()
	txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.EventID == barUpdatedEvent.ID
	})).Return(nil).Once()

	txStore := NewMockStorer(t)
	txStore.EXPECT().Events().Return(txEventRepo)
	txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

	store := NewMockStorer(t)
	store.EXPECT().Events().Return(eventRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not be called")
		return nil
	})

	barUpdatedHandler := NewHandler(barUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not be called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
		WithEvent(barUpdatedEventName, WithHandler(barUpdatedHandler)),
	)

	p, err := NewProcessor(store, eventMap, nil, "", 2)
	assert.NoError(err)

	p.processEvents(context.Background(), limit)
}

func TestProcessor_processEvents_not_found(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        "fooUpdated",
		Data:        map[string]any{"key": "val"},
		Timestamp:   time.Now(),
		ProcessedAt: nil,
	}

	events := []*Event{
		fooUpdatedEvent,
	}

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(nil, ErrNotFound)

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()

	txStore := NewMockStorer(t)
	txStore.EXPECT().Events().Return(txEventRepo)

	store := NewMockStorer(t)
	store.EXPECT().Events().Return(eventRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not be called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	p, err := NewProcessor(store, eventMap, nil, "", 2)
	assert.NoError(err)

	p.processEvents(context.Background(), limit)
}

func TestProcessor_processEvents_already_processed(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	now := time.Now()
	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        "fooUpdated",
		Data:        map[string]any{"key": "val"},
		Timestamp:   now,
		ProcessedAt: &now,
	}

	events := []*Event{
		fooUpdatedEvent,
	}

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil)

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()

	txStore := NewMockStorer(t)
	txStore.EXPECT().Events().Return(txEventRepo)

	store := NewMockStorer(t)
	store.EXPECT().Events().Return(eventRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not be called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	p, err := NewProcessor(store, eventMap, nil, "", 2)
	assert.NoError(err)

	p.processEvents(context.Background(), limit)
}

func TestProcessor_processEvents_no_handler(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent, err := NewApplicationEvent("fooUpdated", map[string]any{"key": "val"})
	assert.NoError(err)
	fooUpdatedEvent.EntityID = entityID

	events := []*Event{
		fooUpdatedEvent,
	}

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil).Once()
	txEventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == fooUpdatedEvent.ID &&
			assert.NotNil(e.ProcessedAt)
	})).Return(nil).Once()

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()

	txStore := NewMockStorer(t)
	txStore.EXPECT().Events().Return(txEventRepo)

	store := NewMockStorer(t)
	store.EXPECT().Events().Return(eventRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	eventMap := ConfigMap{}

	p, err := NewProcessor(store, eventMap, nil, "", 2)
	assert.NoError(err)

	p.processEvents(context.Background(), limit)
}
