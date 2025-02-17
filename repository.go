package events

import (
	"context"
	"errors"
	"iter"
)

var ErrNotFound = errors.New("not found")

type EventRepository interface {
	Transaction(ctx context.Context, fn func(txRepo EventRepository) error) error

	CountUnprocessed(ctx context.Context) (int, error)
	Create(ctx context.Context, event *Event) error
	Find(ctx context.Context) iter.Seq2[*Event, error]
	FindByID(ctx context.Context, id string) (*Event, error)
	FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*Event, error)
	FindOldestUnprocessed(ctx context.Context) (*Event, error)
	FindUnprocessed(ctx context.Context, limit int) ([]*Event, error)
	Update(ctx context.Context, event *Event) error
}

type HandlerRequestRepository interface {
	Transaction(ctx context.Context, fn func(txRepo HandlerRequestRepository) error) error

	CountDead(ctx context.Context) (int, error)
	CountUnexecuted(ctx context.Context) (int, error)
	Create(ctx context.Context, handlerRequest *HandlerRequest) error
	Find(ctx context.Context) iter.Seq2[*HandlerRequest, error]
	FindByID(ctx context.Context, id string) (*HandlerRequest, error)
	FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*HandlerRequest, error)
	FindDead(ctx context.Context) ([]*HandlerRequest, error)
	FindOldestUnexecuted(ctx context.Context) (*HandlerRequest, error)
	FindUnexecuted(ctx context.Context, limit int) ([]*HandlerRequest, error)
	Update(ctx context.Context, handlerRequest *HandlerRequest) error
}
