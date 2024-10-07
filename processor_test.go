package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

var ctxMatcher = mock.MatchedBy(func(c context.Context) bool { return true })

func TestProcessor(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent, err := NewApplicationEvent("fooUpdated", map[string]any{"key": "val"})
	assert.NoError(err)
	fooUpdatedEvent.EntityID = &entityID

	barUpdatedEvent, err := NewApplicationEvent("barUpdated", map[string]any{"key": "val"})
	assert.NoError(err)
	barUpdatedEvent.EntityID = &entityID

	events := []*Event{
		fooUpdatedEvent,
		barUpdatedEvent,
	}

	eventRepo := NewMockRepository(t)

	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Repository) error")).RunAndReturn(func(ctx context.Context, f func(Repository) error) error {
		err := f(eventRepo)
		assert.NoError(err)

		return nil
	})

	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Repository) error")).RunAndReturn(func(ctx context.Context, f func(Repository) error) error {
		err := f(eventRepo)
		assert.NoError(err)

		return nil
	})

	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil)

	eventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil).Once()
	eventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, barUpdatedEvent.ID, true).Return(barUpdatedEvent, nil).Once()

	var wg sync.WaitGroup
	wg.Add(len(events))

	eventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == fooUpdatedEvent.ID
	})).RunAndReturn(func(ctx context.Context, e *Event) error {
		wg.Done()
		return nil
	}).Once()

	eventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == barUpdatedEvent.ID
	})).RunAndReturn(func(ctx context.Context, e *Event) error {
		wg.Done()
		return nil
	}).Once()
	fooSuccessHandler := NewHandler("success", "", func(ctx context.Context, e *Event) error {
		return nil
	})

	barSuccessHandler := NewHandler("success", "", func(ctx context.Context, e *Event) error {
		return nil
	})

	barFailureHandler := NewHandler("failure", "", func(ctx context.Context, e *Event) error {
		return errors.New("handler error")
	})

	eventMap := ConfigMap{}
	eventMap.AddHandlers("fooUpdated", fooSuccessHandler)
	eventMap.AddHandlers("barUpdated", barSuccessHandler, barFailureHandler)

	p, err := NewProcessor(eventRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)

	assert.NotNil(fooUpdatedEvent.ProcessedAt)

	assert.Nil(barUpdatedEvent.ProcessedAt)
	assert.NotNil(barUpdatedEvent.HandlerResults["success"].ProcessedAt)

	assert.Nil(barUpdatedEvent.HandlerResults["failure"].ProcessedAt)
	assert.Error(barUpdatedEvent.HandlerResults["failure"].LastError)
}

func TestProcessor_not_found(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent := &Event{
		ID:             uuid.New().String(),
		Type:           "fooUpdated",
		EntityID:       &entityID,
		Data:           map[string]any{"key": "val"},
		Timestamp:      time.Now(),
		ProcessedAt:    nil,
		HandlerResults: map[HandlerName]*HandlerResult{},
	}

	events := []*Event{
		fooUpdatedEvent,
	}

	eventRepo := NewMockRepository(t)

	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Repository) error")).RunAndReturn(func(ctx context.Context, f func(Repository) error) error {
		err := f(eventRepo)
		assert.NoError(err)

		return nil
	})

	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil)

	var wg sync.WaitGroup

	wg.Add(1)
	eventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).RunAndReturn(func(ctx context.Context, id string, b bool) (*Event, error) {
		wg.Done()
		return nil, ErrNotFound
	})

	handlerCalled := false
	fooSuccessHandler := NewHandler("success", "", func(ctx context.Context, e *Event) error {
		handlerCalled = true
		return nil
	})

	eventMap := ConfigMap{}
	eventMap.AddHandlers("fooUpdated", fooSuccessHandler)

	p, err := NewProcessor(eventRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)

	assert.Nil(fooUpdatedEvent.ProcessedAt)
	assert.False(handlerCalled)
}

func TestProcessor_already_processed(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	now := time.Now()
	fooUpdatedEvent := &Event{
		ID:             uuid.New().String(),
		Type:           "fooUpdated",
		EntityID:       &entityID,
		Data:           map[string]any{"key": "val"},
		Timestamp:      now,
		ProcessedAt:    &now,
		HandlerResults: map[HandlerName]*HandlerResult{},
	}

	events := []*Event{
		fooUpdatedEvent,
	}

	eventRepo := NewMockRepository(t)

	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Repository) error")).RunAndReturn(func(ctx context.Context, f func(Repository) error) error {
		err := f(eventRepo)
		assert.NoError(err)

		return nil
	})

	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil)

	var wg sync.WaitGroup

	wg.Add(1)
	eventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).RunAndReturn(func(ctx context.Context, id string, b bool) (*Event, error) {
		wg.Done()
		return fooUpdatedEvent, nil
	})

	handlerCalled := false
	fooSuccessHandler := NewHandler("success", "", func(ctx context.Context, e *Event) error {
		handlerCalled = true
		return nil
	})

	eventMap := ConfigMap{}
	eventMap.AddHandlers("fooUpdated", fooSuccessHandler)

	p, err := NewProcessor(eventRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)

	assert.False(handlerCalled)
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

	eventRepo := NewMockRepository(t)

	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Repository) error")).RunAndReturn(func(ctx context.Context, f func(Repository) error) error {
		err := f(eventRepo)
		assert.NoError(err)

		return nil
	})

	eventRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Repository) error")).RunAndReturn(func(ctx context.Context, f func(Repository) error) error {
		err := f(eventRepo)
		assert.NoError(err)

		return nil
	})

	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(events, nil).Once()
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return([]*Event{}, nil)

	eventRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedEvent.ID, true).Return(fooUpdatedEvent, nil).Once()

	var wg sync.WaitGroup
	wg.Add(len(events))

	eventRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(e *Event) bool {
		return e.ID == fooUpdatedEvent.ID
	})).RunAndReturn(func(ctx context.Context, e *Event) error {
		wg.Done()
		return nil
	}).Once()

	eventMap := ConfigMap{}

	p, err := NewProcessor(eventRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)

	assert.NotNil(fooUpdatedEvent.ProcessedAt)
}
