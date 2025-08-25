package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestDefaultExecutor_executeRequests(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	fooUpdatedEvent, err := NewApplicationEvent(fooUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	barUpdatedEvent, err := NewDomainEvent(barUpdatedEventName, uuid.New().String(), "bar", map[string]any{"key": "val"})
	assert.NoError(err)

	barUpdatedHandlerRequest, err := NewHandlerRequest(barUpdatedEvent, barUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
		barUpdatedHandlerRequest,
	}

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).Return(fooUpdatedHandlerRequest, nil).Once()
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, barUpdatedHandlerRequest.ID, true).Return(barUpdatedHandlerRequest, nil).Once()
	txHandlerRequestRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.ID == fooUpdatedHandlerRequest.ID &&
			assert.NotNil(r.CompletedAt)
	})).Return(nil).Once()
	txHandlerRequestRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.ID == barUpdatedHandlerRequest.ID &&
			assert.Nil(r.CompletedAt) &&
			assert.Error(r.LastError)
	})).Return(nil).Once()

	txStore := NewMockStorer(t)
	txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()

	store := NewMockStorer(t)
	store.EXPECT().HandlerRequests().Return(handlerRequestRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

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

	e, err := NewDefaultExecutor(store, eventMap, nil, "", 2, limit)
	assert.NoError(err)

	e.executeRequests(context.Background())
}

func TestDefaultExecutor_executeRequests_not_found(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        fooUpdatedEventName,
		Data:        map[string]any{"key": "val"},
		Timestamp:   time.Now(),
		ProcessedAt: nil,
	}

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
	}

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).Return(nil, ErrNotFound)

	txStore := NewMockStorer(t)
	txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()

	store := NewMockStorer(t)
	store.EXPECT().HandlerRequests().Return(handlerRequestRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not have been called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	e, err := NewDefaultExecutor(store, eventMap, nil, "", 2, limit)
	assert.NoError(err)

	e.executeRequests(context.Background())
}

func TestDefaultExecutor_executeRequests_already_executed(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	now := time.Now()
	fooUpdatedEvent := &Event{
		ID:        uuid.New().String(),
		Name:      fooUpdatedEventName,
		Data:      map[string]any{"key": "val"},
		Timestamp: now,
	}

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	fooUpdatedHandlerRequest.CompletedAt = &now

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
	}

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).Return(fooUpdatedHandlerRequest, nil)

	txStore := NewMockStorer(t)
	txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()

	store := NewMockStorer(t)
	store.EXPECT().HandlerRequests().Return(handlerRequestRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	fooUpdatedHandler := NewHandler(fooUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		assert.Fail("should not have been called")
		return nil
	})

	eventMap := NewConfigMap(
		WithEvent(fooUpdatedEventName, WithHandler(fooUpdatedHandler)),
	)

	e, err := NewDefaultExecutor(store, eventMap, nil, "", 2, limit)
	assert.NoError(err)

	e.executeRequests(context.Background())
}

func TestDefaultExecutor_executeRequests_no_configured_handler(t *testing.T) {
	assert := assert.New(t)

	limit := 5

	fooUpdatedEvent := &Event{
		ID:          uuid.New().String(),
		Name:        fooUpdatedEventName,
		Data:        map[string]any{"key": "val"},
		Timestamp:   time.Now(),
		ProcessedAt: nil,
	}

	fooUpdatedHandlerRequest, err := NewHandlerRequest(fooUpdatedEvent, fooUpdatedHandlerName, defaultMaxErrors, defaultPriority)
	assert.NoError(err)

	requests := []*HandlerRequest{
		fooUpdatedHandlerRequest,
	}

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, fooUpdatedHandlerRequest.ID, true).Return(fooUpdatedHandlerRequest, nil)
	txHandlerRequestRepo.EXPECT().Update(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		return r.ID == fooUpdatedHandlerRequest.ID &&
			assert.Nil(r.CompletedAt) &&
			assert.ErrorContains(r.LastError, "handler request has no configured handler")
	})).Return(nil).Once()

	txStore := NewMockStorer(t)
	txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(requests, nil).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()

	store := NewMockStorer(t)
	store.EXPECT().HandlerRequests().Return(handlerRequestRepo)
	store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
		return f(txStore)
	})

	eventMap := NewConfigMap()

	e, err := NewDefaultExecutor(store, eventMap, nil, "", 2, limit)
	assert.NoError(err)

	e.executeRequests(context.Background())
}
