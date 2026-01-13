package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

func TestDefaultExecutor_executeRequests_error_reporting(t *testing.T) {
	testCases := map[string]struct {
		err                   error
		maxErrors             int
		reporter              func(t *testing.T, err error, stack []byte) ErrorReporter
		expectedErrorReported bool
		expectedStack         []byte
	}{
		"noop error reporter - handler error - retryable": {
			err:       errors.New("handler error"),
			maxErrors: 2,
		},
		"noop error reporter - handler panic error - retryable": {
			err:           NewHandlerPanicError("handler panic", []byte("the stack")),
			maxErrors:     2,
			expectedStack: []byte("the stack"),
		},
		"noop error reporter - handler error - not retryable": {
			err:       errors.New("handler error"),
			maxErrors: 1,
		},
		"noop error reporter - handler panic error - not retryable": {
			err:           NewHandlerPanicError("handler panic", []byte("the stack")),
			maxErrors:     1,
			expectedStack: []byte("the stack"),
		},
		"custom error reporter - handler error - retryable": {
			err:       errors.New("handler error"),
			maxErrors: 2,
			reporter: func(t *testing.T, err error, stack []byte) ErrorReporter {
				return NewMockErrorReporter(t)
			},
		},
		"custom error reporter - handler panic error - retryable": {
			err:       NewHandlerPanicError("handler panic", []byte("the stack")),
			maxErrors: 2,
			reporter: func(t *testing.T, err error, stack []byte) ErrorReporter {
				return NewMockErrorReporter(t)
			},
			expectedStack: []byte("the stack"),
		},
		"custom error reporter - handler error - not retryable": {
			err:       errors.New("handler error"),
			maxErrors: 1,
			reporter: func(t *testing.T, err error, stack []byte) ErrorReporter {
				reporter := NewMockErrorReporter(t)
				reporter.EXPECT().Report(err, stack).Return(true)
				return reporter
			},
			expectedErrorReported: true,
		},
		"custom error reporter - handler panic error - not retryable": {
			err:       NewHandlerPanicError("handler panic", []byte("the stack")),
			maxErrors: 1,
			reporter: func(t *testing.T, err error, stack []byte) ErrorReporter {
				reporter := NewMockErrorReporter(t)
				reporter.EXPECT().Report(err, stack).Return(true)
				return reporter
			},
			expectedErrorReported: true,
			expectedStack:         []byte("the stack"),
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			var logBuffer bytes.Buffer
			originalLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuffer, nil)))
			t.Cleanup(func() {
				slog.SetDefault(originalLogger)
			})

			limit := 5

			event, err := NewDomainEvent(barUpdatedEventName, uuid.New().String(), "bar", map[string]any{"key": "val"})
			assert.NoError(err)

			handlerRequest, err := NewHandlerRequest(event, barUpdatedHandlerName, testCase.maxErrors, defaultPriority)
			assert.NoError(err)

			requests := []*HandlerRequest{handlerRequest}

			txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
			txHandlerRequestRepo.EXPECT().FindByIDForUpdate(ctxMatcher, handlerRequest.ID, true).Return(handlerRequest, nil).Once()
			txHandlerRequestRepo.EXPECT().Update(ctxMatcher, mock.AnythingOfType("*events.HandlerRequest")).Return(nil).Once()

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

			handler := NewHandler(barUpdatedHandlerName, "", func(ctx context.Context, r *HandlerRequest) error { return testCase.err })
			eventMap := NewConfigMap(
				WithEvent(barUpdatedEventName, WithHandler(handler)),
			)

			var reporter ErrorReporter
			if testCase.reporter != nil {
				reporter = testCase.reporter(t, testCase.err, testCase.expectedStack)
			}

			e, err := NewDefaultExecutor(store, eventMap, nil, "", 2, limit, WithErrorReporter(reporter))
			assert.NoError(err)

			e.executeRequests(context.Background())

			var logEntry map[string]any
			err = json.Unmarshal(logBuffer.Bytes(), &logEntry)
			if assert.NoError(err) {
				assert.Equal(testCase.expectedErrorReported, logEntry["errorReported"])
				assert.Equal(string(testCase.expectedStack), logEntry["stack"])
			}
		})
	}
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
