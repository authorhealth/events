package events

import (
	"context"
	"fmt"
	"time"
)

// Scheduler cooperatively schedules work for the Processor and the Executor.
//
// The Scheduler starts by calling the Processor to process events and create handler execution requests.
// Then, the Scheduler calls the Executor to execute handler execution requests.
// This quick succession helps minimize the gap between processing and execution.
//
// The Scheduler also provides a Shutdown method to gracefully stop scheduling work.
type Scheduler struct {
	done      chan bool
	executor  *Executor
	processor *Processor
	shutdown  chan bool
}

func NewScheduler(
	executor *Executor,
	processor *Processor,
) *Scheduler {
	return &Scheduler{
		done:      make(chan bool, 1),
		executor:  executor,
		processor: processor,
		shutdown:  make(chan bool, 1),
	}
}

func (s *Scheduler) Start(
	ctx context.Context,
	interval time.Duration,
	processorLimit int,
	executorLimit int,
) error {
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

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			s.processor.processEvents(ctx, processorLimit)
			s.executor.executeRequests(ctx, executorLimit)

		case <-s.shutdown:
			return nil
		}
	}
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.shutdown <- true
	s.processor.shutdown <- true
	s.executor.shutdown <- true

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
