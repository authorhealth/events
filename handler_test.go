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
	h := NewHandler(handlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		called = true
		return nil
	})

	assert.Equal(handlerName, h.Name())

	err := h.Do(context.Background(), &HandlerRequest{})
	assert.NoError(err)

	assert.True(called)
}

func TestHandler_Name_Do_recover_panic(t *testing.T) {
	assert := assert.New(t)

	handlerName := HandlerName("test")
	h := NewHandler(handlerName, "", func(ctx context.Context, r *HandlerRequest) error {
		panic("danger danger")
	})

	assert.Equal(handlerName, h.Name())

	panicErr := &HandlerPanicError{}
	err := h.Do(context.Background(), &HandlerRequest{})
	assert.ErrorAs(err, &panicErr)
	assert.ErrorContains(err, "recovered panic while executing handler request: danger danger")
}
