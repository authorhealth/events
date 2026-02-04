package events

import (
	"context"
	"errors"
	"iter"
)

var ErrNotFound = errors.New("not found")

type Storer interface {
	Transaction(ctx context.Context, fn func(txStore Storer) error) error

	Events() EventRepository
	HandlerRequests() HandlerRequestRepository
}

type EventRepository interface {
	CountUnprocessed(ctx context.Context) (int, error)
	Create(ctx context.Context, event *Event) error
	Find(ctx context.Context) iter.Seq2[*Event, error]
	FindByID(ctx context.Context, id string) (*Event, error)
	FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*Event, error)
	FindOldestUnprocessed(ctx context.Context) (*Event, error)
	FindUnprocessed(ctx context.Context, limit int) ([]*Event, error)
	Update(ctx context.Context, event *Event) error
}

// FindDeadOptions is a struct for holding filtering options for FindDead and CountDead methods.
type FindDeadOptions struct {
	EventNames   []EventName
	HandlerNames []HandlerName
}

// DeadHandlerRequestFilter is a function type for applying filtering options.
type DeadHandlerRequestFilter func(*FindDeadOptions)

// WithEventNames returns a DeadHandlerRequestFilter that filters by event names.
func WithEventNames(names ...EventName) DeadHandlerRequestFilter {
	return func(opts *FindDeadOptions) {
		opts.EventNames = append(opts.EventNames, names...)
	}
}

// WithHandlerNames returns a DeadHandlerRequestFilter that filters by handler names.
func WithHandlerNames(names ...HandlerName) DeadHandlerRequestFilter {
	return func(opts *FindDeadOptions) {
		opts.HandlerNames = append(opts.HandlerNames, names...)
	}
}

type HandlerRequestRepository interface {
	CountDead(ctx context.Context, filters ...DeadHandlerRequestFilter) (int, error)
	CountUnexecuted(ctx context.Context) (int, error)
	Create(ctx context.Context, handlerRequest *HandlerRequest) error
	Find(ctx context.Context) iter.Seq2[*HandlerRequest, error]
	FindByID(ctx context.Context, id string) (*HandlerRequest, error)
	FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*HandlerRequest, error)
	FindDead(ctx context.Context, limit int, offset int, filters ...DeadHandlerRequestFilter) ([]*HandlerRequest, error)
	FindDeadEventAndHandlerNames(ctx context.Context) ([]EventName, []HandlerName, error)
	FindOldestUnexecuted(ctx context.Context) (*HandlerRequest, error)
	FindUnexecuted(ctx context.Context, limit int) ([]*HandlerRequest, error)
	Update(ctx context.Context, handlerRequest *HandlerRequest) error
}
