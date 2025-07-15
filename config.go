package events

const (
	defaultMaxErrors = 5
	defaultPriority  = 50
)

type HandlerConfig struct {
	BackoffFunction BackoffFunc       // The backoff function to call when execution fails.
	Handler         *Handler          // The handler for the event.
	MaxErrors       int               // The maximum error limit for the handler.
	QueueName       ExecutorQueueName // The name of the executor queue where the handler request will be executed.
	Priority        int               // The priority rank of the handler.
}

type HandlerConfigOption func(handlerConfig *HandlerConfig)

func WithBackoffFunction(backoffFunction BackoffFunc) HandlerConfigOption {
	return func(handlerConfig *HandlerConfig) {
		handlerConfig.BackoffFunction = backoffFunction
	}
}

func WithMaxErrors(maxErrors int) HandlerConfigOption {
	return func(handlerConfig *HandlerConfig) {
		handlerConfig.MaxErrors = maxErrors
	}
}

func WithPriority(priority int) HandlerConfigOption {
	return func(handlerConfig *HandlerConfig) {
		handlerConfig.Priority = priority
	}
}

func WithQueue(queueName ExecutorQueueName) HandlerConfigOption {
	return func(handlerConfig *HandlerConfig) {
		handlerConfig.QueueName = queueName
	}
}

type HandlerConfigMap map[HandlerName]*HandlerConfig

type HandlerConfigMapOption func(handlerConfigMap HandlerConfigMap)

func WithHandler(handler *Handler, options ...HandlerConfigOption) HandlerConfigMapOption {
	return func(handlerConfigMap HandlerConfigMap) {
		handlerName := handler.Name()

		handlerConfig := handlerConfigMap[handlerName]
		if handlerConfig == nil {
			handlerConfig = &HandlerConfig{
				MaxErrors: defaultMaxErrors,
				QueueName: DefaultExecutorQueueName,
				Priority:  defaultPriority,
			}
			handlerConfigMap[handlerName] = handlerConfig
		}

		handlerConfig.Handler = handler

		for _, option := range options {
			option(handlerConfig)
		}
	}
}

type ConfigMap map[EventName]HandlerConfigMap

func NewConfigMap(options ...ConfigMapOption) ConfigMap {
	configMap := ConfigMap{}
	for _, option := range options {
		option(configMap)
	}

	return configMap
}

type ConfigMapOption func(configMap ConfigMap)

func WithEvent(eventName EventName, options ...HandlerConfigMapOption) ConfigMapOption {
	return func(configMap ConfigMap) {
		handlerConfigMap := configMap[eventName]
		if handlerConfigMap == nil {
			handlerConfigMap = HandlerConfigMap{}
			configMap[eventName] = handlerConfigMap
		}

		for _, option := range options {
			option(handlerConfigMap)
		}
	}
}
