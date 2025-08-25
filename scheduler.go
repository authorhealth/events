package events

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type SchedulerStatus string

const (
	SchedulerStatusNotStarted SchedulerStatus = "notStarted"
	SchedulerStatusRunning    SchedulerStatus = "running"
	SchedulerStatusPaused     SchedulerStatus = "paused"
	SchedulerStatusShutdown   SchedulerStatus = "shutdown"
)

func (ps SchedulerStatus) String() string {
	return string(ps)
}

func (ps SchedulerStatus) operational() bool {
	return ps == SchedulerStatusRunning || ps == SchedulerStatusPaused
}

type schedulerEvent string

const (
	schedulerEventPaused   schedulerEvent = "paused"
	schedulerEventResumed  schedulerEvent = "resumed"
	schedulerEventShutdown schedulerEvent = "shutdown"
	schedulerEventStarted  schedulerEvent = "started"
)

// Scheduler provides a standard interface for scheduling work for the Processor and the Executor.
//
// The Scheduler also provides a Shutdown method to gracefully stop scheduling work.
type Scheduler interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Operational() bool
	Pause(ctx context.Context)
	Paused() bool
	Resume(ctx context.Context)
	Status() SchedulerStatus
}

// CooperativeScheduler cooperatively schedules work for the Processor and the Executor.
//
// The CooperativeScheduler starts by calling the Processor to process events and create handler execution requests.
// Then, the CooperativeScheduler calls the Executor to execute handler execution requests.
// This quick succession helps minimize the gap between processing and execution.
type CooperativeScheduler struct {
	*schedulerStateMachine

	done      chan bool
	executor  Executor
	interval  time.Duration
	processor Processor
	shutdown  chan bool
}

var _ Scheduler = (*CooperativeScheduler)(nil)

func NewCooperativeScheduler(
	processor Processor,
	executor Executor,
	telemetryPrefix string,
	interval time.Duration,
) (*CooperativeScheduler, error) {
	ssm, err := newSchedulerStateMachine(telemetryPrefix)
	if err != nil {
		return nil, err
	}

	s := &CooperativeScheduler{
		done:                  make(chan bool, 1),
		executor:              executor,
		interval:              interval,
		processor:             processor,
		schedulerStateMachine: ssm,
		shutdown:              make(chan bool, 1),
	}

	return s, nil
}

func (s *CooperativeScheduler) Start(ctx context.Context) error {
	if s.Status() != SchedulerStatusNotStarted {
		return errors.New("scheduler is already started")
	}

	defer func() {
		s.done <- true
	}()

	err := s.processor.registerMeterCallbacks()
	if err != nil {
		return fmt.Errorf("registering processor meter callbacks: %w", err)
	}

	err = s.executor.registerMeterCallbacks()
	if err != nil {
		return fmt.Errorf("registering executor meter callbacks: %w", err)
	}

	s.handleProcessorEvent(ctx, schedulerEventStarted)

	ticker := time.NewTicker(s.interval)

	for {
		select {
		case <-ticker.C:
			if !s.Paused() {
				s.processor.processEvents(ctx)
				s.executor.executeRequests(ctx)
			}

		case <-s.shutdown:
			return nil
		}
	}
}

func (s *CooperativeScheduler) Shutdown(ctx context.Context) error {
	if !s.Status().operational() {
		return errors.New("scheduler is not operational")
	}

	s.shutdown <- true
	s.processor.shutdown()
	s.executor.shutdown()

	s.handleProcessorEvent(ctx, schedulerEventShutdown)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-s.done:
			err := s.processor.unregisterMeterCallbacks()
			if err != nil {
				return fmt.Errorf("unregistering processor meter callbacks: %w", err)
			}

			err = s.executor.unregisterMeterCallbacks()
			if err != nil {
				return fmt.Errorf("unregistering executor meter callbacks: %w", err)
			}

			return nil
		}
	}
}

// ConcurrentScheduler concurrently schedules work for the Processor and the Executor.
type ConcurrentScheduler struct {
	*schedulerStateMachine

	done      chan bool
	executor  Executor
	interval  time.Duration
	processor Processor
	shutdown  chan struct{}
}

var _ Scheduler = (*ConcurrentScheduler)(nil)

func NewConcurrentScheduler(
	processor Processor,
	executor Executor,
	telemetryPrefix string,
	interval time.Duration,
) (*ConcurrentScheduler, error) {
	ssm, err := newSchedulerStateMachine(telemetryPrefix)
	if err != nil {
		return nil, err
	}

	s := &ConcurrentScheduler{
		done:                  make(chan bool, 1),
		executor:              executor,
		interval:              interval,
		processor:             processor,
		schedulerStateMachine: ssm,
		shutdown:              make(chan struct{}),
	}

	return s, nil
}

func (s *ConcurrentScheduler) Start(ctx context.Context) error {
	if s.Status() != SchedulerStatusNotStarted {
		return errors.New("scheduler is already started")
	}

	defer func() {
		s.done <- true
	}()

	err := s.processor.registerMeterCallbacks()
	if err != nil {
		return fmt.Errorf("registering processor meter callbacks: %w", err)
	}

	err = s.executor.registerMeterCallbacks()
	if err != nil {
		return fmt.Errorf("registering executor meter callbacks: %w", err)
	}

	s.handleProcessorEvent(ctx, schedulerEventStarted)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(s.interval)

		for {
			select {
			case <-ticker.C:
				if !s.Paused() {
					s.processor.processEvents(ctx)
				}

			case <-s.shutdown:
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(s.interval)

		for {
			select {
			case <-ticker.C:
				if !s.Paused() {
					s.executor.executeRequests(ctx)
				}

			case <-s.shutdown:
				return
			}
		}
	}()

	wg.Wait()

	return nil
}

func (s *ConcurrentScheduler) Shutdown(ctx context.Context) error {
	if !s.Status().operational() {
		return errors.New("scheduler is not operational")
	}

	close(s.shutdown)
	s.processor.shutdown()
	s.executor.shutdown()

	s.handleProcessorEvent(ctx, schedulerEventShutdown)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-s.done:
			err := s.processor.unregisterMeterCallbacks()
			if err != nil {
				return fmt.Errorf("unregistering processor meter callbacks: %w", err)
			}

			err = s.executor.unregisterMeterCallbacks()
			if err != nil {
				return fmt.Errorf("unregistering executor meter callbacks: %w", err)
			}

			return nil
		}
	}
}

type schedulerStateMachine struct {
	meter               metric.Meter
	status              SchedulerStatus
	statusRWMutex       sync.RWMutex
	statusUpDownCounter metric.Int64UpDownCounter
	telemetryPrefix     string
}

func newSchedulerStateMachine(
	telemetryPrefix string,
) (*schedulerStateMachine, error) {
	ssm := &schedulerStateMachine{
		meter:           otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		status:          SchedulerStatusNotStarted,
		telemetryPrefix: telemetryPrefix,
	}

	var err error
	ssm.statusUpDownCounter, err = ssm.meter.Int64UpDownCounter(
		ssm.applyTelemetryPrefix("events.scheduler.status"),
		metric.WithDescription("Operational status for the scheduler: 1 (true) or 0 (false) for each of the possible states"),
	)
	if err != nil {
		return nil, fmt.Errorf("constructing scheduler status up/down counter: %w", err)
	}

	return ssm, nil
}

func (ssm *schedulerStateMachine) Operational() bool {
	return ssm.Status().operational()
}

func (ssm *schedulerStateMachine) Pause(ctx context.Context) {
	ssm.handleProcessorEvent(ctx, schedulerEventPaused)
}

func (ssm *schedulerStateMachine) Paused() bool {
	return ssm.Status() == SchedulerStatusPaused
}

func (ssm *schedulerStateMachine) Resume(ctx context.Context) {
	ssm.handleProcessorEvent(ctx, schedulerEventResumed)
}

func (ssm *schedulerStateMachine) Status() SchedulerStatus {
	ssm.statusRWMutex.RLock()
	defer ssm.statusRWMutex.RUnlock()

	return ssm.status
}

func (ssm *schedulerStateMachine) handleProcessorEvent(ctx context.Context, event schedulerEvent) {
	ssm.statusRWMutex.Lock()
	defer ssm.statusRWMutex.Unlock()

	nextStatus := ssm.status

	switch ssm.status {
	case SchedulerStatusNotStarted:
		switch event {
		case schedulerEventStarted:
			nextStatus = SchedulerStatusRunning
		}

	case SchedulerStatusRunning:
		switch event {
		case schedulerEventPaused:
			nextStatus = SchedulerStatusPaused

		case schedulerEventShutdown:
			nextStatus = SchedulerStatusShutdown
		}

	case SchedulerStatusPaused:
		switch event {
		case schedulerEventResumed:
			nextStatus = SchedulerStatusRunning

		case schedulerEventShutdown:
			nextStatus = SchedulerStatusShutdown
		}
	}

	if nextStatus == ssm.status {
		return
	}

	previousStatus := ssm.status
	ssm.status = nextStatus

	// Keep track of the number of operational schedulers and their state (i.e., running or paused).
	if previousStatus.operational() {
		ssm.statusUpDownCounter.Add(
			ctx,
			-1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(
						ssm.applyTelemetryPrefix("events.scheduler.state"),
						previousStatus.String(),
					),
				),
			),
		)
	}

	if ssm.status.operational() {
		ssm.statusUpDownCounter.Add(
			ctx,
			1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(
						ssm.applyTelemetryPrefix("events.scheduler.state"),
						ssm.status.String(),
					),
				),
			),
		)
	}
}

func (ssm *schedulerStateMachine) applyTelemetryPrefix(k string) string {
	if len(ssm.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", ssm.telemetryPrefix, k)
	}

	return k
}
