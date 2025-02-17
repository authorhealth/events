package events

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type EventName string

func (n EventName) String() string {
	return string(n)
}

type Event struct {
	ID            string         // The event ID.
	CorrelationID string         // A read-only field that is set by the storage layer.
	Data          map[string]any // The key/value pairs associated with the event.
	EntityID      *string        // The entity ID.
	EntityName    string         // The entity name.
	Name          EventName      // The event name.
	ProcessedAt   *time.Time     // When the event was successfully processed.
	Timestamp     time.Time      // When the event occurred.
}

func NewApplicationEvent(name EventName, data map[string]any) (*Event, error) {
	if len(name) == 0 {
		return nil, errors.New("name is empty")
	}

	return &Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Name:      name,
		Data:      data,
		Timestamp: time.Now(),
	}, nil
}

func NewDomainEvent(name EventName, entityID string, entityName string, data map[string]any) (*Event, error) {
	if len(name) == 0 {
		return nil, errors.New("name is empty")
	}

	return &Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Name:       EventName(name),
		EntityID:   &entityID,
		EntityName: entityName,
		Data:       data,
		Timestamp:  time.Now(),
	}, nil
}
