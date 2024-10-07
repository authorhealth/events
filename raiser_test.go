package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvents_RaiseGetClearEvent(t *testing.T) {
	assert := assert.New(t)

	event, err := NewApplicationEvent("testEvent", nil)
	assert.NoError(err)

	events := Events{}

	events.RaiseEvent(event)
	assert.Len(events.GetEvents(), 1)

	events.ClearEvents()
	assert.Len(events.GetEvents(), 0)
}
