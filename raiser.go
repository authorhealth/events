package events

type EventRaiser interface {
	GetEvents() []*Event
	ClearEvents()
	RaiseEvent(event *Event)
}

type Events []*Event

var _ EventRaiser = (*Events)(nil)

func (e *Events) RaiseEvent(event *Event) {
	*e = append(*e, event)
}

func (e *Events) ClearEvents() {
	*e = nil
}

func (e *Events) GetEvents() []*Event {
	return *e
}
