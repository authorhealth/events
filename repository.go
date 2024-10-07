package events

import (
	"context"
	"errors"
	"iter"
)

var ErrNotFound = errors.New("not found")

type Repository interface {
	Transaction(ctx context.Context, fn func(txRepo Repository) error) error

	CountDead(ctx context.Context) (int, error)
	CountUnprocessed(ctx context.Context) (int, error)
	Create(ctx context.Context, event *Event) error
	Find(ctx context.Context) iter.Seq2[*Event, error]
	FindByID(ctx context.Context, id string) (*Event, error)
	FindByIDForUpdate(ctx context.Context, id string, skipLocked bool) (*Event, error)
	FindDead(ctx context.Context) ([]*Event, error)
	FindOldestUnprocessed(ctx context.Context) (*Event, error)
	FindUnprocessed(ctx context.Context, limit int) ([]*Event, error)
	Update(ctx context.Context, event *Event) error
}
