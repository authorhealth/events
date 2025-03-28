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

	interval := 100 * time.Millisecond
	limit := 5

	fooUpdatedEvent, err := NewApplicationEvent(fooUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)
	fooUpdatedEvent.EntityID = uuid.New().String()

	barUpdatedEvent, err := NewApplicationEvent(barUpdatedEventName, map[string]any{"key": "val"})
	assert.NoError(err)
	barUpdatedEvent.EntityID = uuid.New().String()

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

	txStore := NewMockStorer(t)
	txStore.EXPECT().Events().Return(txEventRepo)
	txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

	store := NewMockStorer(t)
	store.EXPECT().Events().Return(eventRepo)
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

	processor, err := NewProcessor(store, eventMap, nil, "", 2)
	assert.NoError(err)

	executor, err := NewExecutor(store, eventMap, nil, "", 2)
	assert.NoError(err)

	scheduler, err := NewScheduler(executor, processor, "")
	assert.NoError(err)

	go func() {
		err := scheduler.Start(context.Background(), interval, limit, limit)
		assert.NoError(err)
	}()

	wg.Wait()

	err = scheduler.Shutdown(context.Background())
	assert.NoError(err)
}

func TestScheduler_Operational_Pause_Paused_Resume_Status(t *testing.T) {
	testCases := map[string]struct {
		pauseAfterProcessing bool
	}{
		"don't pause after processing": {},
		"pause after processing": {
			pauseAfterProcessing: true,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			// Arrange - Scheduler not started
			interval := 100 * time.Millisecond
			limit := 5

			eventsC := make(chan []*Event, 1)
			eventRepo := NewMockEventRepository(t)
			eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).RunAndReturn(func(_ context.Context, _ int) ([]*Event, error) {
				select {
				case events := <-eventsC:
					return events, nil

				default:
					return nil, nil
				}
			})

			handlerRequestC := make(chan *HandlerRequest, 2)
			handlerRequestRepo := NewMockHandlerRequestRepository(t)
			handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).RunAndReturn(func(_ context.Context, _ int) ([]*HandlerRequest, error) {
				select {
				case handlerRequest := <-handlerRequestC:
					return []*HandlerRequest{handlerRequest}, nil

				default:
					return nil, nil
				}
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

			store := NewMockStorer(t)
			store.EXPECT().Events().Return(eventRepo)
			store.EXPECT().HandlerRequests().Return(handlerRequestRepo)

			processor, err := NewProcessor(store, eventMap, nil, "", 2)
			assert.NoError(err)

			executor, err := NewExecutor(store, eventMap, nil, "", 2)
			assert.NoError(err)

			// Act/Assert - Scheduler not started
			scheduler, err := NewScheduler(executor, processor, "")

			assert.NoError(err)
			assert.False(scheduler.Operational())
			assert.False(scheduler.Paused())
			assert.Equal(SchedulerStatusNotStarted, scheduler.Status())

			// Act/Assert - Scheduler running - no events or handler requests available
			go func() {
				err := scheduler.Start(context.Background(), interval, limit, limit)
				assert.NoError(err)
			}()

			time.Sleep(3 * interval) // Sleep for a couple of ticks to ensure the scheduler has had time to run.

			assert.False(scheduler.Paused())
			assert.True(scheduler.Operational())
			assert.Equal(SchedulerStatusRunning, scheduler.Status())

			// Act/Assert - Scheduler paused - no events or handler requests available
			scheduler.Pause(context.Background())
			assert.True(scheduler.Operational())
			assert.True(scheduler.Paused())
			assert.Equal(SchedulerStatusPaused, scheduler.Status())

			// Arrange - Scheduler paused - events available
			fooUpdatedEvent, err := NewApplicationEvent(fooUpdatedEventName, map[string]any{"key": "val"})
			assert.NoError(err)
			fooUpdatedEvent.EntityID = uuid.New().String()

			barUpdatedEvent, err := NewApplicationEvent(barUpdatedEventName, map[string]any{"key": "val"})
			assert.NoError(err)
			barUpdatedEvent.EntityID = uuid.New().String()

			events := []*Event{
				fooUpdatedEvent,
				barUpdatedEvent,
			}

			// Act - Scheduler paused - events available
			time.Sleep(3 * interval) // Sleep for a couple of ticks to ensure the scheduler has had time to run.

			eventsC <- events

			time.Sleep(3 * interval) // Sleep for a couple of ticks to ensure the scheduler has had time to run.

			// Arrange - Scheduler resumed - events available
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

			var fooUpdatedHandlerRequest *HandlerRequest
			var barUpdatedHandlerRequest *HandlerRequest

			txHandlerRequestRepo := NewMockHandlerRequestRepository(t)
			txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
				if r.EventID == fooUpdatedEvent.ID {
					fooUpdatedHandlerRequest = r
					handlerRequestC <- r

					return true
				}

				return false
			})).Return(nil).Once()
			txHandlerRequestRepo.EXPECT().Create(ctxMatcher, mock.MatchedBy(func(r *HandlerRequest) bool {
				if r.EventID == barUpdatedEvent.ID {
					barUpdatedHandlerRequest = r
					handlerRequestC <- r

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

			txStore := NewMockStorer(t)
			txStore.EXPECT().Events().Return(txEventRepo)
			txStore.EXPECT().HandlerRequests().Return(txHandlerRequestRepo)

			store.EXPECT().Transaction(ctxMatcher, mock.AnythingOfType("func(events.Storer) error")).RunAndReturn(func(ctx context.Context, f func(Storer) error) error {
				return f(txStore)
			})

			// Act - Scheduler resumed - events available
			scheduler.Resume(context.Background())

			assert.True(scheduler.Operational())
			assert.False(scheduler.Paused())
			assert.Equal(SchedulerStatusRunning, scheduler.Status())

			wg.Wait()

			if testCase.pauseAfterProcessing {
				// Act/Assert - Scheduler paused
				scheduler.Pause(context.Background())

				assert.True(scheduler.Operational())
				assert.True(scheduler.Paused())
				assert.Equal(SchedulerStatusPaused, scheduler.Status())

				// Act/Assert - Scheduler paused (idempotent)
				scheduler.Pause(context.Background())

				assert.True(scheduler.Operational())
				assert.True(scheduler.Paused())
				assert.Equal(SchedulerStatusPaused, scheduler.Status())
			}

			err = scheduler.Shutdown(context.Background())
			assert.NoError(err)

			assert.False(scheduler.Operational())
			assert.False(scheduler.Paused())
			assert.Equal(SchedulerStatusShutdown, scheduler.Status())
		})
	}
}

func TestScheduler_Start_already_started(t *testing.T) {
	testCases := map[string]struct {
		paused bool
	}{
		"not paused": {},
		"paused": {
			paused: true,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			// Arrange
			interval := 100 * time.Millisecond
			limit := 5

			eventRepo := NewMockEventRepository(t)
			eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(nil, nil).Maybe()

			handlerRequestRepo := NewMockHandlerRequestRepository(t)
			handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(nil, nil).Maybe()

			store := NewMockStorer(t)
			store.EXPECT().Events().Return(eventRepo).Maybe()
			store.EXPECT().HandlerRequests().Return(handlerRequestRepo).Maybe()

			processor, err := NewProcessor(store, nil, nil, "", 2)
			assert.NoError(err)

			executor, err := NewExecutor(store, nil, nil, "", 2)
			assert.NoError(err)

			scheduler, err := NewScheduler(executor, processor, "")
			assert.NoError(err)

			if testCase.paused {
				scheduler.Pause(context.Background())
			}

			go func() {
				err := scheduler.Start(context.Background(), interval, limit, limit)
				assert.NoError(err)
			}()

			time.Sleep(10 * time.Millisecond) // Sleep for a couple of ms to ensure that the scheduler has started.

			// Act
			err = scheduler.Start(context.Background(), interval, limit, limit)

			// Assert
			assert.EqualError(err, "scheduler is already started")
		})
	}
}

func TestScheduler_Shutdown_not_running(t *testing.T) {
	testCases := map[string]struct {
		paused bool
	}{
		"not paused": {},
		"paused": {
			paused: true,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			// Arrange
			store := NewMockStorer(t)

			processor, err := NewProcessor(store, nil, nil, "", 2)
			assert.NoError(err)

			executor, err := NewExecutor(store, nil, nil, "", 2)
			assert.NoError(err)

			scheduler, err := NewScheduler(executor, processor, "")
			assert.NoError(err)

			if testCase.paused {
				scheduler.Pause(context.Background())
			}

			// Act
			err = scheduler.Shutdown(context.Background())

			// Assert
			assert.EqualError(err, "scheduler is not operational")
		})
	}
}

func TestScheduler_Shutdown_already_shut_down(t *testing.T) {
	assert := assert.New(t)

	// Arrange
	interval := 100 * time.Millisecond
	limit := 5

	eventRepo := NewMockEventRepository(t)
	eventRepo.EXPECT().FindUnprocessed(ctxMatcher, limit).Return(nil, nil).Maybe()

	handlerRequestRepo := NewMockHandlerRequestRepository(t)
	handlerRequestRepo.EXPECT().FindUnexecuted(ctxMatcher, limit).Return(nil, nil).Maybe()

	store := NewMockStorer(t)
	store.EXPECT().Events().Return(eventRepo).Maybe()
	store.EXPECT().HandlerRequests().Return(handlerRequestRepo).Maybe()

	processor, err := NewProcessor(store, nil, nil, "", 2)
	assert.NoError(err)

	executor, err := NewExecutor(store, nil, nil, "", 2)
	assert.NoError(err)

	scheduler, err := NewScheduler(executor, processor, "")
	assert.NoError(err)

	go func() {
		err := scheduler.Start(context.Background(), interval, limit, limit)
		assert.NoError(err)
	}()

	time.Sleep(10 * time.Millisecond) // Sleep for a couple of ms to ensure that the scheduler has started.

	err = scheduler.Shutdown(context.Background())
	assert.NoError(err)

	// Act
	err = scheduler.Shutdown(context.Background())

	// Assert
	assert.EqualError(err, "scheduler is not operational")
}
