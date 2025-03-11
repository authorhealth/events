package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewApplicationEvent(t *testing.T) {
	assert := assert.New(t)
	data := map[string]any{
		"a": "alpha",
		"b": 12345,
		"c": []string{"fee", "fie", "foe", "fum"},
	}

	event, err := NewApplicationEvent(fooUpdatedEventName, data)

	assert.NoError(err)
	assert.NotEmpty(event.ID)
	assert.Empty(event.CorrelationID)
	assert.Equal(data, event.Data)
	assert.Empty(event.EntityID)
	assert.Empty(event.EntityName)
	assert.Equal(fooUpdatedEventName, event.Name)
	assert.Empty(event.ProcessedAt)
	assert.WithinDuration(time.Now(), event.Timestamp, time.Second)
}

func TestNewDomainEvent(t *testing.T) {
	assert := assert.New(t)
	entityID := "12345"
	entityName := "bar"
	data := map[string]any{
		"d": "delta",
		"e": 67890,
		"f": []string{"eenie", "meenie", "miney", "mo"},
	}

	event, err := NewDomainEvent(barUpdatedEventName, entityID, entityName, data)

	assert.NoError(err)
	assert.NotEmpty(event.ID)
	assert.Empty(event.CorrelationID)
	assert.Equal(data, event.Data)
	assert.Equal(entityID, event.EntityID)
	assert.Equal(entityName, event.EntityName)
	assert.Equal(barUpdatedEventName, event.Name)
	assert.Empty(event.ProcessedAt)
	assert.WithinDuration(time.Now(), event.Timestamp, time.Second)
}
