package events

type Config struct {
	BackoffFn BackoffFunc
	Handlers  []*Handler
}

type ConfigMap map[EventName]*Config

func (m ConfigMap) AddHandlers(eventName EventName, handlers ...*Handler) {
	config := m[eventName]
	if config == nil {
		config = &Config{}
		m[eventName] = config
	}

	config.Handlers = append(config.Handlers, handlers...)
}

func (m ConfigMap) SetBackoffFn(eventName EventName, backoffFn BackoffFunc) {
	config := m[eventName]
	if config == nil {
		config = &Config{}
		m[eventName] = config
	}

	config.BackoffFn = backoffFn
}
