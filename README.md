# events

`events` is a Go framework designed for raising, processing, and managing domain and application events. It provides robust mechanisms for event-driven architectures, including event storage, handler execution, and integrated observability.

## Features

  * **Event Definition**: Define application and domain events with associated data, entity IDs, and names.
  * **Event Persistence**: Integrates with a `Storer` interface for event and handler request persistence, including transactional operations to ensure data consistency.
  * **Configurable Handlers**: Map events to one or more handlers with options for custom queues, maximum error retries, and priority.
  * **Event Processing**: A `Processor` component handles the initial processing of raw events, creating handler requests based on configured mappings.
  * **Handler Execution**: `Executor` components manage the concurrent execution of handler requests, supporting both a default queue and custom queues. Includes configurable worker pools and an exponential backoff strategy with jitter for retries.
  * **Flexible Scheduling**: Choose between `CooperativeScheduler` (sequential processing and execution) and `ConcurrentScheduler` (parallel processing and execution) to optimize for different workloads.
  * **Observability**: Deep integration with OpenTelemetry for metrics and tracing, providing insights into event processing times, execution counts, and request states.
  * **Structured Logging**: Uses `log/slog` for structured logging throughout the framework, enhancing debuggability.

## Getting Started

To integrate the framework into your application, see the [demo/main.go](demo/main.go) file for a complete example.

### Installation

No special installation is required beyond standard Go module practices.

```bash
go get github.com/authorhealth/events/v2
```

### Basic Usage

Define your event handlers:

```go
package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/authorhealth/events/v2"
)

const (
	ApplicationEventHandlerName events.HandlerName = "myWonderfulApplicationEventHandler"
	DomainEventHandlerName      events.HandlerName = "mySpectacularDomainEventHandler"
	FailingEventHandlerName     events.HandlerName = "myBodaciousFailingEventHandler"
)

func ApplicationEventHandler() *events.Handler {
	return events.NewHandler(ApplicationEventHandlerName, "demo", func(ctx context.Context, hr *events.HandlerRequest) error {
		slog.InfoContext(ctx, "handling application event", "event", hr.EventName, "id", hr.EventID)
		return nil
	})
}

func FailingEventHandler() *events.Handler {
	return events.NewHandler(FailingEventHandlerName, "demo", func(ctx context.Context, hr *events.HandlerRequest) error {
		return errors.New("oh noes")
	})
}
```

Configure your event map and handlers:

```go
configMap := events.NewConfigMap(
    events.WithEvent(ApplicationEventName,
        events.WithHandler(ApplicationEventHandler()),
        events.WithHandler(FailingEventHandler()),
    ),
    events.WithEvent(DomainEventName,
        events.WithHandler(DomainEventHandler(), events.WithQueue(domainQueueName)),
        events.WithHandler(FailingEventHandler(), events.WithQueue(domainQueueName)),
    ),
)
```

Set up the core components (store, processor, executor, scheduler) and start them:

```go
// Example simplified setup from demo/main.go
db := NewDatabase() // In-memory database for demo
store := NewStore(db)

// Event Producer (for generating events)
eventProducer := NewEventProducer(store.Events())
go eventProducer.Start(ctx, eventProducerInterval)

// Event Processor
eventProcessor, err := events.NewDefaultProcessor(store, configMap, nil, "demo", eventProcessorNumWorkers, eventProcessorLimit)
// ... error handling ...

// Event Executor (for CooperativeScheduler)
eventExecutor, err := events.NewDefaultExecutor(store, configMap, nil, "demo", eventExecutorNumWorkers, eventExecutorLimit)
// ... error handling ...

// Event Scheduler
eventScheduler, err := events.NewCooperativeScheduler(eventProcessor, eventExecutor, "demo", eventSchedulerInterval)
// ... error handling ...

go eventScheduler.Start(ctx)
```

## Configuration

The `HandlerConfig` provides options to customize handler behavior:

  * `WithBackoffFunction(BackoffFunc)`: Customizes retry logic.
  * `WithMaxErrors(int)`: Sets the maximum number of retries for a handler.
  * `WithPriority(int)`: Assigns a priority to the handler request.
  * `WithQueue(ExecutorQueueName)`: Assigns the handler to a specific executor queue.

Defaults for `MaxErrors` is 5, `Priority` is 50.

## Observability

The framework leverages OpenTelemetry for comprehensive observability:

  * **Metrics**: Custom metrics are emitted for event processing successes/failures, processing times, request execution times, and counts of unprocessed, unexecuted, and dead requests.
  * **Tracing**: Spans are automatically created and linked for event processing and handler execution, allowing end-to-end tracing of event flow through the system.
  * **Logging**: Structured logging using `log/slog` provides detailed operational insights.

## Testing

The project includes unit tests for all core components. Mocks are generated using `mockery`.

To run tests and regenerate mocks:

```bash
# Regenerate mocks
go generate ./...

# Run tests with race detector
go test -v -race ./...
```
