package events

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_Name_Do(t *testing.T) {
	assert := assert.New(t)

	handlerName := HandlerName("test")
	var called bool
	h := NewHandler(handlerName, "", func(ctx context.Context, e *Event) error {
		called = true
		return nil
	})

	assert.Equal(handlerName, h.Name())

	err := h.Do(context.Background(), &Event{})
	assert.NoError(err)

	assert.True(called)
}
