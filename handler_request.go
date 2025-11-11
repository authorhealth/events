package events

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type HandlerRequest struct {
	ID              string         // The request ID.
	BackoffUntil    *time.Time     // The time at which execution should be tried again.
	CanceledAt      *time.Time     // When the request was marked as canceled.
	CompletedAt     *time.Time     // When the event handler execution successfully completed.
	CorrelationID   string         // CorrelationID is a read-only field that is set by the storage layer.
	Errors          int            // Errors indicates the number of execution attempts that resulted in errors.
	EventData       map[string]any // The key/value pairs associated with the event.
	EventEntityID   string         // The event entity ID.
	EventEntityName string         // The event entity name.
	EventID         string         // The event ID.
	EventName       EventName      // Name represents the event name.
	EventTimestamp  time.Time      // When the event occurred.
	HandlerName     HandlerName    // The name of the event handler to execute.
	LastAttemptAt   *time.Time     // LastAttemptAt indicates when execution was last attempted.
	LastError       error          // LastError contains the error that occurred when execution was last attempted.
	MaxErrors       int            // The maximum error limit for the request.
	Priority        int            // The priority rank of the request.
}

func NewHandlerRequest(
	event *Event,
	handlerName HandlerName,
	maxErrors int,
	priority int,
) (*HandlerRequest, error) {
	if len(handlerName) == 0 {
		return nil, errors.New("handlerName is empty")
	}

	return &HandlerRequest{
		ID:              uuid.Must(uuid.NewV7()).String(),
		CorrelationID:   event.CorrelationID,
		EventData:       event.Data,
		EventEntityID:   event.EntityID,
		EventEntityName: event.EntityName,
		EventID:         event.ID,
		EventName:       event.Name,
		EventTimestamp:  event.Timestamp,
		HandlerName:     handlerName,
		MaxErrors:       maxErrors,
		Priority:        priority,
	}, nil
}

func (r *HandlerRequest) Cancel() {
	r.CanceledAt = Ptr(time.Now())
}

func (r *HandlerRequest) Reexecute() {
	r.BackoffUntil = nil
	r.Errors = 0
}

func (r *HandlerRequest) execute(ctx context.Context, config *HandlerConfig) error {
	if r.Errors >= r.MaxErrors {
		return errors.New("handler request has reached max errors")
	}

	if r.CompletedAt != nil {
		return errors.New("handler request is already completed")
	}

	if config.Handler == nil {
		return errors.New("handler request does not have a handler configured")
	}

	handler := config.Handler

	now := time.Now()

	err := handler.Do(ctx, r)
	if err != nil {
		r.LastAttemptAt = &now
		r.LastError = err
		r.Errors++

		if r.Errors < r.MaxErrors {
			var backoffUntil time.Time
			if config.BackoffFunction != nil {
				backoffUntil = config.BackoffFunction(r)
			} else {
				backoffUntil = DefaultBackoffFunc(r)
			}

			r.BackoffUntil = &backoffUntil

			return ErrRetryable
		}

		r.BackoffUntil = nil

		return errors.New("handler request exceeded retries")
	}

	r.LastAttemptAt = &now
	r.LastError = nil
	r.CompletedAt = &now
	r.BackoffUntil = nil

	return nil
}
