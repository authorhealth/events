package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvent_Archive(t *testing.T) {
	assert := assert.New(t)

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	event.Archive()

	assert.WithinDuration(time.Now(), *event.ArchivedAt, time.Microsecond)
}

func TestEvent_Process(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	var handledEvent *Event
	handler := NewHandler("testHandler", "", func(ctx context.Context, e *Event) error {
		handledEvent = e
		return nil
	})

	err = event.Process(ctx, &Config{
		Handlers: []*Handler{
			handler,
		},
	})
	assert.NoError(err)
	assert.NotNil(event.ProcessedAt)
	assert.Nil(event.BackoffUntil)
	assert.Len(event.HandlerResults, 1)
	assert.Equal(event, handledEvent)

	result := event.HandlerResults["testHandler"]
	assert.NotZero(result.LastProcessAttemptAt)
	assert.Nil(result.LastError)
	assert.NotNil(result.ProcessedAt)
}

func TestEvent_Process_event_already_processed(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	now := time.Now()
	event.ProcessedAt = &now

	called := false
	handler := NewHandler("testHandler", "", func(ctx context.Context, e *Event) error {
		called = true
		return nil
	})

	err = event.Process(ctx, &Config{
		Handlers: []*Handler{
			handler,
		},
	})
	assert.Error(err)
	assert.False(called)
}

func TestEvent_Process_event_max_errors(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	event.Errors = MaxEventErrors

	called := false
	handler := NewHandler("testHandler", "", func(ctx context.Context, e *Event) error {
		called = true
		return nil
	})

	err = event.Process(ctx, &Config{
		Handlers: []*Handler{
			handler,
		},
	})
	assert.Error(err)
	assert.False(called)
}

func TestEvent_Process_handler_already_processed(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	event.HandlerResults = map[HandlerName]*HandlerResult{
		"testHandler": {
			ProcessedAt: Ptr(time.Now()),
		},
	}

	called := false
	handler := NewHandler("testHandler", "", func(ctx context.Context, e *Event) error {
		called = true
		return nil
	})

	err = event.Process(ctx, &Config{
		Handlers: []*Handler{
			handler,
		},
	})
	assert.NoError(err)
	assert.False(called)
}

func TestEvent_Process_error(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	handler := NewHandler("testHandler", "", func(ctx context.Context, e *Event) error {
		return errors.New("handler error")
	})

	now := time.Now()

	for i := 1; i < MaxEventErrors; i++ {
		err = event.Process(ctx, &Config{
			Handlers: []*Handler{
				handler,
			},
		})
		assert.ErrorIs(err, ErrRetryable)
		assert.Nil(event.ProcessedAt)
		assert.Equal(i, event.Errors)
		assert.Greater(*event.BackoffUntil, now)
		assert.Len(event.HandlerResults, 1)

		result := event.HandlerResults["testHandler"]
		assert.NotZero(result.LastProcessAttemptAt)
		assert.NotNil(result.LastError)
		assert.Nil(result.ProcessedAt)

		now = time.Now()
	}

	err = event.Process(ctx, &Config{
		Handlers: []*Handler{
			handler,
		},
	})
	assert.NotErrorIs(err, ErrRetryable)
	assert.Nil(event.ProcessedAt)
	assert.Equal(MaxEventErrors, event.Errors)
	assert.Nil(event.BackoffUntil)
}

func TestEvent_Reprocess(t *testing.T) {
	assert := assert.New(t)

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	event.BackoffUntil = Ptr(time.Now())
	event.Errors = 5

	event.Reprocess()

	assert.Empty(event.Errors)
	assert.Nil(event.BackoffUntil)
}

func TestEvent_UnprocessedHandlers(t *testing.T) {
	assert := assert.New(t)

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	now := time.Now()
	event.HandlerResults = map[HandlerName]*HandlerResult{
		"fooHandler": {
			LastProcessAttemptAt: now,
			LastError:            err,
			ProcessedAt:          &now,
		},
		"barHandler": {
			LastProcessAttemptAt: now,
			LastError:            err,
			ProcessedAt:          nil,
		},
	}

	unprocessed := event.UnprocessedHandlers()
	assert.Len(unprocessed, 1)
	assert.Contains(unprocessed, HandlerName("barHandler"))
}

func TestEvent_Process_custom_backoff(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	called := false
	handler := NewHandler("testHandler", "", func(ctx context.Context, e *Event) error {
		called = true
		return errors.New("test")
	})

	backoffUntil := time.Now().Add(9999 * time.Hour)

	err = event.Process(ctx, &Config{
		BackoffFn: func(event *Event) time.Time {
			return backoffUntil
		},
		Handlers: []*Handler{
			handler,
		},
	})
	assert.Error(err)
	assert.True(called)
	assert.WithinDuration(backoffUntil, *event.BackoffUntil, time.Second)
}
