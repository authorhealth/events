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

func TestScheduler(t *testing.T) {
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
	wg.Add(len(events) * 2)

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

	var fooUpdatedHandlerRequest *HandlerRequest
	var barUpdatedHandlerRequest *HandlerRequest

	txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
	txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		if r.EventID == fooUpdatedEvent.ID {
			fooUpdatedHandlerRequest = r

			return true
		}

		return false
	})).Return(nil).Once()
	txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
		if r.EventID == barUpdatedEvent.ID {
			barUpdatedHandlerRequest = r

			return true
		}

		return false
	})).Return(nil).Once()
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, mock.MatchedBy(func(id string) bool {
		return id == fooUpdatedHandlerRequest.ID
	}), true).RunAndReturn(func(_ context.Context, _ string, _ bool) (*HandlerRequest, error) {
		return fooUpdatedHandlerRequest, nil
	}).Once()
	txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, mock.MatchedBy(func(id string) bool {
		return id == barUpdatedHandlerRequest.ID
	}), true).RunAndReturn(func(_ context.Context, _ string, _ bool) (*HandlerRequest, error) {
		return barUpdatedHandlerRequest, nil
	}).Once()
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
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).RunAndReturn(func(_ context.Context, _ int) ([]*HandlerRequest, error) {
		return []*HandlerRequest{
			fooUpdatedHandlerRequest,
			barUpdatedHandlerRequest,
		}, nil
	}).Once()
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return([]*HandlerRequest{}, nil).Maybe()
	handlerRequestRepo.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.HandlerRequestRepository) error")).RunAndReturn(func(ctx context.Context, f func(HandlerRequestRepository) error) error {
		return f(txHandlerRequestRepo)
	}).Times(4)

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

	p, err := NewProcessor(eventRepo, handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	e, err := NewExecutor(handlerRequestRepo, eventMap, nil, "", 2)
	assert.NoError(err)

	s := NewScheduler(e, p)

	go func() {
		err := s.Start(context.Background(), duration, limit, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = s.Shutdown(context.Background())
	assert.NoError(err)
}
