package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestProcessor(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent, err := NewApplicationEvent(fooUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)
	fooUpdatedEvent.EntityID = &entityID

	barUpdatedEvent, err := NewApplicationEvent(barUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)
	barUpdatedEvent.EntityID = &entityID

	events := []*Event{
		fooUpdatedEvent,
		barUpdatedEvent,
	}

	var wg sync.WaitGroup
	wg.Add(len(events))

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil).Once()
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, barUpdatedEvent.ID, true).Return(barUpdatedEvent, nil).Once()
	txEventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == fooUpdatedEvent.ID &&
			assert.NotNil(e.ProcessedAt)
	})).RunAndReturn(func(ctx context.Context, e *Event) error {
		wg.Done()
		return nil
	}).Once()
	txEventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == barUpdatedEvent.ID &&
			assert.NotNil(e.ProcessedAt)
	})).RunAndReturn(func(ctx context.Context, e *Event) error {
		wg.Done()
		return nil
	}).Once()

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()
	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.EventRepository) error")).RunAndReturn(func(ctx context.Context, f func(EventRepository) error) error {
		return f(txEventRepo)
	})

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.EventID == fooUpdatedEvent.ID
	})).Return(nil).Once()
	txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.EventID == barUpdatedEvent.ID
	})).Return(nil).Once()

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.HandlerRequestRepository) error")).RunAndReturn(func(ctx context.Context, f func(HandlerRequestRepository) error) error {
		return f(txHandlerRequestRepo)
	}).Twice()

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

	p, err := NewProcessor(eventRepo, handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)

}

func TestProcessor_not_found(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        "fooUpdated",
		EntityID:    &entityID,
		Data:        map[string]any{"key": "val"},
		Timestamp:   time.Now(),
		ProcessedAt: nil,
	}

	events := []*Event{
		fooUpdatedEvent,
	}

	var wg sync.WaitGroup
	wg.Add(len(events))

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).RunAndReturn(func(ctx context.Context, _ string, _ bool) (*Event, error) {
		wg.Done()
		return nil, ErrNotFound
	})

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()
	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.EventRepository) error")).RunAndReturn(func(ctx context.Context, f func(EventRepository) error) error {
		return f(txEventRepo)
	})

	handlerRequestRepo := NewMockHandlerRequestRepository(t)

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not be called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	p, err := NewProcessor(eventRepo, handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)
}

func TestProcessor_already_processed(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	now := time.Now()
	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        "fooUpdated",
		EntityID:    &entityID,
		Data:        map[string]any{"key": "val"},
		Timestamp:   now,
		ProcessedAt: &now,
	}

	events := []*Event{
		fooUpdatedEvent,
	}

	var wg sync.WaitGroup
	wg.Add(len(events))

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).RunAndReturn(func(ctx context.Context, _ string, _ bool) (*Event, error) {
		wg.Done()
		return fooUpdatedEvent, nil
	})

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()
	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.EventRepository) error")).RunAndReturn(func(ctx context.Context, f func(EventRepository) error) error {
		return f(txEventRepo)
	})

	handlerRequestRepo := NewMockHandlerRequestRepository(t)

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not be called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	p, err := NewProcessor(eventRepo, handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)
}

func TestProcessor_no_handler(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent, err := NewApplicationEvent("fooUpdated", map[string]any{"key": "val"})
	assert.NoError(err)
	fooUpdatedEvent.EntityID = &entityID

	events := []*Event{
		fooUpdatedEvent,
	}

	var wg sync.WaitGroup
	wg.Add(len(events))

	txEventRepo := NewMockEventRepository(t)
	txEventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil).Once()
	txEventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == fooUpdatedEvent.ID &&
			assert.NotNil(e.ProcessedAt)
	})).RunAndReturn(func(ctx context.Context, e *Event) error {
		wg.Done()
		return nil
	}).Once()

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil).Maybe()
	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.EventRepository) error")).RunAndReturn(func(ctx context.Context, f func(EventRepository) error) error {
		return f(txEventRepo)
	})

	handlerRequestRepo := NewMockHandlerRequestRepository(t)

	eventMap := ConfigMap{}

	p, err := NewProcessor(eventRepo, handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)
}
