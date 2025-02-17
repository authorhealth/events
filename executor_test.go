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

func TestExecutor(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent, err := NewApplicationEvent(fooUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)
	fooUpdatedEvent.EntityID = &entityID

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	barUpdatedEvent, err := NewApplicationEvent(barUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)
	barUpdatedEvent.EntityID = &entityID

	barUpdatedHandlerRequest, err := NewHandlerRequest(barUpdatedEvent, barUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
		barUpdatedHandlerRequest,
	}

	var wg sync.WaitGroup
	wg.Add(len(requests))

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).Return(fooUpdatedHandlerRequest, nil).Once()
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, barUpdatedHandlerRequest.ID, true).Return(barUpdatedHandlerRequest, nil).Once()
	txHandlerRequestRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.ID == fooUpdatedHandlerRequest.ID &&
			assert.NotNil(r.CompletedAt)
	})).RunAndReturn(func(ctx context.Context, r *HandlerRequest) error {
		wg.Done()
		return nil
	}).Once()
	txHandlerRequestRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.ID == barUpdatedHandlerRequest.ID &&
			assert.Nil(r.CompletedAt) &&
			assert.Error(r.LastError)
	})).RunAndReturn(func(ctx context.Context, r *HandlerRequest) error {
		wg.Done()
		return nil
	}).Once()

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()
	handlerRequestRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.HandlerRequestRepository) error")).RunAndReturn(func(ctx context.Context, f func(HandlerRequestRepository) error) error {
		return f(txHandlerRequestRepo)
	}).Twice()

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		return nil
	})

	barUpdatedHandler := NewHandler(barUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		return errors.New("handler error")
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
		WithEvent(barUpdatedEventName, WithHandler(barUpdatedHandler)),
	)

	p, err := NewExecutor(handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)
}

func TestExecutor_not_found(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        fooUpdatedEventName,
		EntityID:    &entityID,
		Data:        map[string]any{"key": "val"},
		Timestamp:   time.Now(),
		ProcessedAt: nil,
	}

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
	}

	var wg sync.WaitGroup
	wg.Add(len(requests))

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).RunAndReturn(func(ctx context.Context, id string, b bool) (*HandlerRequest, error) {
		wg.Done()
		return nil, ErrNotFound
	})

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()
	handlerRequestRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.HandlerRequestRepository) error")).RunAndReturn(func(ctx context.Context, f func(HandlerRequestRepository) error) error {
		return f(txHandlerRequestRepo)
	}).Once()

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not have been called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	p, err := NewExecutor(handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)
}

func TestExecutor_already_executed(t *testing.T) {
	assert := assert.New(t)

	duration := 100 * time.Millisecond
	limit := 5

	entityID := uuid.New().String()

	now := time.Now()
	fooUpdatedEvent := &Event{
		ID:        uuid.New().String(),
		Name:      fooUpdatedEventName,
		EntityID:  &entityID,
		Data:      map[string]any{"key": "val"},
		Timestamp: now,
	}

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	fooUpdatedHandlerRequest.CompletedAt = &now

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
	}

	var wg sync.WaitGroup
	wg.Add(len(requests))

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).RunAndReturn(func(ctx context.Context, id string, b bool) (*HandlerRequest, error) {
		wg.Done()
		return fooUpdatedHandlerRequest, nil
	})

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()
	handlerRequestRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.HandlerRequestRepository) error")).RunAndReturn(func(ctx context.Context, f func(HandlerRequestRepository) error) error {
		return f(txHandlerRequestRepo)
	}).Once()

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not have been called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	p, err := NewExecutor(handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	go func() {
		err := p.Start(context.Background(), duration, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = p.Shutdown(context.Background())
	assert.NoError(err)
}
