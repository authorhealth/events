# events

`events` is a Go framework designed for raising, processing, and managing domain and application events. It provides robust mechanisms for event-driven architectures, including event storage, handler execution, and integrated observability.

## Features

- **Event Definition**: Define application and domain events with associated data, entity IDs, and names.
- **Event Persistence**: Integrates with a `Storer` interface for event and handler request persistence, including transactional operations to ensure data consistency.
- **Configurable Handlers**: Map events to one or more handlers with options for maximum error retries and priority.
- **Event Processing**: A `Processor` component handles the initial processing of raw events, creating handler requests based on configured mappings.
- **Handler Execution**: `Executor` components manage the concurrent execution of handler requests. Includes configurable worker pools and an exponential backoff strategy with jitter for retries.
- **Flexible Scheduling**: Choose between `CooperativeScheduler` (sequential processing and execution) and `ConcurrentScheduler` (parallel processing and execution) to optimize for different workloads.
- **Observability**: Deep integration with OpenTelemetry for metrics and tracing, providing insights into event processing times, execution counts, and request states, and optional integration with external error reporting services.
- **Structured Logging**: Uses `log/slog` for structured logging throughout the framework, enhancing debuggability.

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
        events.WithHandler(DomainEventHandler()),
        events.WithHandler(FailingEventHandler()),
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

// Event Executor
eventExecutor, err := events.NewDefaultExecutor(store, configMap, nil, "demo", eventExecutorNumWorkers, eventExecutorLimit)
// ... error handling ...

// Event Scheduler
eventScheduler, err := events.NewCooperativeScheduler(eventProcessor, eventExecutor, "demo", eventSchedulerInterval)
// ... error handling ...

go eventScheduler.Start(ctx)
```

## Configuration

The `HandlerConfig` provides options to customize handler behavior:

- `WithBackoffFunction(BackoffFunc)`: Customizes retry logic.
- `WithMaxErrors(int)`: Sets the maximum number of retries for a handler.
- `WithPriority(int)`: Assigns a priority to the handler request.

Defaults for `MaxErrors` is 5, `Priority` is 50.

## Observability

The framework leverages OpenTelemetry for comprehensive observability:

- **Metrics**: Custom metrics are emitted for event processing successes/failures, processing times, request execution times, and counts of unprocessed, unexecuted, and dead requests.
- **Tracing**: Spans are automatically created and linked for event processing and handler execution, allowing end-to-end tracing of event flow through the system.
- **Logging**: Structured logging using `log/slog` provides detailed operational insights.

### Custom Error Reporting

For non-retryable errors or panics within event handlers, you can integrate an external reporting service (e.g., Sentry, BugSnag). To do this, implement the `ErrorReporter` interface and provide it to the `DefaultExecutor` during initialization.

The `ErrorReporter` interface is defined as:

```go
type ErrorReporter interface {
	Report(err error, stack []byte) bool
}
```

-   `err`: The error that occurred.
-   `stack`: The stack trace if the error was caused by a panic (otherwise `nil`).
-   Return `true` if the error was successfully reported.

**Example:**
```go
// 1. Implement the interface
type myErrorReporter struct{}
func (r *myErrorReporter) Report(err error, stack []byte) bool {
    // Send to your error reporting service
    log.Printf("error reported: %v", err)
    return true
}

// 2. Add it as an option to the executor
eventExecutor, err := events.NewDefaultExecutor(
    store,
    configMap,
    nil,
    "demo",
    5,
    50,
    events.WithErrorReporter(&myErrorReporter{}),
)
```

## Testing

The project includes unit tests for all core components. Mocks are generated using `mockery`.

To run tests and regenerate mocks:

```bash
# Regenerate mocks
go generate ./...

# Run tests with race detector
go test -v -race ./...
```
