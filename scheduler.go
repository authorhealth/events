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

// Scheduler cooperatively schedules work for the Processor and the Executor.
//
// The Scheduler starts by calling the Processor to process events and create handler execution requests.
// Then, the Scheduler calls the Executor to execute handler execution requests.
// This quick succession helps minimize the gap between processing and execution.
//
// The Scheduler also provides a Shutdown method to gracefully stop scheduling work.
type Scheduler struct {
	done                chan bool
	executor            *Executor
	meter               metric.Meter
	processor           *Processor
	shutdown            chan bool
	status              SchedulerStatus
	statusRWMutex       sync.RWMutex
	statusUpDownCounter metric.Int64UpDownCounter
	telemetryPrefix     string
}

func NewScheduler(
	executor *Executor,
	processor *Processor,
	telemetryPrefix string,
) (*Scheduler, error) {
	s := &Scheduler{
		done:            make(chan bool, 1),
		meter:           otel.GetMeterProvider().Meter("github.com/authorhealth/events/v2"),
		executor:        executor,
		processor:       processor,
		shutdown:        make(chan bool, 1),
		status:          SchedulerStatusNotStarted,
		telemetryPrefix: telemetryPrefix,
	}

	var err error
	s.statusUpDownCounter, err = s.meter.Int64UpDownCounter(
		s.applyTelemetryPrefix("events.scheduler.status"),
		metric.WithDescription("Operational status for the scheduler: 1 (true) or 0 (false) for each of the possible states"),
	)
	if err != nil {
		return nil, fmt.Errorf("constructing scheduler status up/down counter: %w", err)
	}

	return s, nil
}

func (s *Scheduler) Start(
	ctx context.Context,
	interval time.Duration,
	processorLimit int,
	executorLimit int,
) error {
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

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			if !s.Paused() {
				s.processor.processEvents(ctx, processorLimit)
				s.executor.executeRequests(ctx, executorLimit)
			}

		case <-s.shutdown:
			return nil
		}
	}
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	if !s.Status().operational() {
		return errors.New("scheduler is not operational")
	}

	s.shutdown <- true
	s.processor.shutdown <- true
	s.executor.shutdown <- true

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

func (s *Scheduler) Operational() bool {
	return s.Status().operational()
}

func (s *Scheduler) Pause(ctx context.Context) {
	s.handleProcessorEvent(ctx, schedulerEventPaused)
}

func (s *Scheduler) Paused() bool {
	return s.Status() == SchedulerStatusPaused
}

func (s *Scheduler) Resume(ctx context.Context) {
	s.handleProcessorEvent(ctx, schedulerEventResumed)
}

func (s *Scheduler) Status() SchedulerStatus {
	s.statusRWMutex.RLock()
	defer s.statusRWMutex.RUnlock()

	return s.status
}

func (s *Scheduler) handleProcessorEvent(ctx context.Context, event schedulerEvent) {
	s.statusRWMutex.Lock()
	defer s.statusRWMutex.Unlock()

	nextStatus := s.status

	switch s.status {
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

	if nextStatus == s.status {
		return
	}

	previousStatus := s.status
	s.status = nextStatus

	// Keep track of the number of operational schedulers and their state (i.e., running or paused).
	if previousStatus.operational() {
		s.statusUpDownCounter.Add(
			ctx,
			-1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(
						s.applyTelemetryPrefix("events.scheduler.state"),
						previousStatus.String(),
					),
				),
			),
		)
	}

	if s.status.operational() {
		s.statusUpDownCounter.Add(
			ctx,
			1,
			metric.WithAttributeSet(
				attribute.NewSet(
					attribute.String(
						s.applyTelemetryPrefix("events.scheduler.state"),
						s.status.String(),
					),
				),
			),
		)
	}
}

func (s *Scheduler) applyTelemetryPrefix(k string) string {
	if len(s.telemetryPrefix) > 0 {
		return fmt.Sprintf("%s.%s", s.telemetryPrefix, k)
	}

	return k
}
