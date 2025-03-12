package events

import (
	"context"

	mock "github.com/stretchr/testify/mock"
)

var ctxMatcher = mock.MatchedBy(func(c context.Context) bool { return true })

var (
	fooUpdatedEventName = EventName("fooUpdated")
	barUpdatedEventName = EventName("barUpdated")

	fooUpdatedHandlerName = HandlerName("fooUpdatedHandler")
	barUpdatedHandlerName = HandlerName("barUpdatedHandler")
)
