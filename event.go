package events

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	TypeDomain      EventType = "domain"
	TypeApplication EventType = "application"
)

type EventName string

func (n EventName) String() string {
	return string(n)
}

type EventType string

func (t EventType) String() string {
	return string(t)
}

type HandlerResult struct {
	LastProcessAttemptAt time.Time
	LastError            error
	ProcessedAt          *time.Time
}

type Event struct {
	ID             string
	ArchivedAt     *time.Time
	BackoffUntil   *time.Time
	CorrelationID  string // CorrelationID is a read-only field that is set by the storage layer.
	Data           map[string]any
	EntityID       *string
	EntityName     string
	Errors         int
	HandlerResults map[HandlerName]*HandlerResult
	Name           EventName
	ProcessedAt    *time.Time
	Timestamp      time.Time
	Type           EventType
}

func NewApplicationEvent(name EventName, data map[string]any) (*Event, error) {
	if len(name) == 0 {
		return nil, errors.New("name is empty")
	}

	return &Event{
		ID:             uuid.Must(uuid.NewV7()).String(),
		Type:           TypeApplication,
		Name:           name,
		Data:           data,
		Timestamp:      time.Now(),
		HandlerResults: map[HandlerName]*HandlerResult{},
	}, nil
}

func NewDomainEvent(name EventName, entityID string, entityName string, data map[string]any) (*Event, error) {
	if len(name) == 0 {
		return nil, errors.New("name is empty")
	}

	return &Event{
		ID:             uuid.Must(uuid.NewV7()).String(),
		Type:           TypeDomain,
		Name:           EventName(name),
		EntityID:       &entityID,
		EntityName:     entityName,
		Data:           data,
		Timestamp:      time.Now(),
		HandlerResults: map[HandlerName]*HandlerResult{},
	}, nil
}

func (e *Event) Archive() {
	e.ArchivedAt = Ptr(time.Now())
}

func (e *Event) Process(ctx context.Context, config *Config) error {
	if e.Errors >= MaxEventErrors {
		return errors.New("event has reached max errors")
	}

	if e.ProcessedAt != nil {
		return errors.New("event is already processed")
	}

	for _, handler := range config.Handlers {
		if result, ok := e.HandlerResults[handler.Name()]; ok {
			if result.ProcessedAt != nil {
				continue
			}
		}

		now := time.Now()

		err := handler.Do(ctx, e)
		if err != nil {
			e.HandlerResults[handler.Name()] = &HandlerResult{
				LastProcessAttemptAt: now,
				LastError:            err,
			}

		} else {
			e.HandlerResults[handler.Name()] = &HandlerResult{
				LastProcessAttemptAt: now,
				LastError:            nil,
				ProcessedAt:          &now,
			}
		}
	}

	for _, result := range e.HandlerResults {
		if result.ProcessedAt == nil {
			e.Errors++

			if e.Errors < MaxEventErrors {
				var backoffUntil time.Time
				if config.BackoffFn != nil {
					backoffUntil = config.BackoffFn(e)
				} else {
					backoffUntil = DefaultBackoffFunc(e)
				}

				e.BackoffUntil = &backoffUntil

				return ErrRetryable
			}

			e.BackoffUntil = nil

			return errors.New("event has unprocessed handler")
		}
	}

	now := time.Now()
	e.ProcessedAt = &now
	e.BackoffUntil = nil

	return nil
}

func (e *Event) Reprocess() {
	e.BackoffUntil = nil
	e.Errors = 0
}

func (e *Event) UnprocessedHandlers() map[HandlerName]*HandlerResult {
	out := make(map[HandlerName]*HandlerResult)

	for handlerName, handlerResult := range e.HandlerResults {
		if handlerResult.ProcessedAt == nil {
			out[handlerName] = handlerResult
		}
	}

	return out
}
