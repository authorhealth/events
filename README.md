# events

## Getting Started

```go
package main

import (
"context"
"log/slog"
"time"

    "github.com/authorhealth/events"

)

func main() {
var repository events.Repository

    configMap := events.ConfigMap{}
    // Add events and handlers.

    var beforeProcessHook events.BeforeProcessHook

    var telemetryPrefix string

    var numProcessorWorkers int

    eventProcessor, err := events.NewProcessor(
    	repository,
    	configMap,
    	beforeProcessHook,
    	telemetryPrefix,
    	numProcessorWorkers,
    )
    if err != nil {
    	panic("error constructing event processor")
    }

    var ctx context.Context
    var interval time.Duration
    var limit int

    go func() {
    	slog.Info("starting event processor", "interval", interval)

    	err := eventProcessor.Start(ctx, interval, limit)
    	if err != nil {
    		slog.Error("error starting event processor", "error", err)
    	}
    }()

    // ...

    slog.InfoContext(ctx, "shutting down event processor")
    err = eventProcessor.Shutdown(ctx)
    if err != nil {
    	slog.Error("error shutting down event processor", "error", err)
    }

}
```
