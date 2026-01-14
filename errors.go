package events

// ErrorReporter defines an interface for reporting errors to an external service,
// such as Sentry, DataDog, or BugSnag. Implementations of this interface can
// be provided to the DefaultExecutor to capture non-retryable errors or panics
// from event handlers.
type ErrorReporter interface {
	// Report sends an error for external observation.
	//
	// The `err` parameter is the error returned by the handler or recovered
	// from a panic.
	//
	// The `stack` parameter contains the stack trace if the error was from a
	// panic, otherwise it is nil.
	//
	// The returned boolean should be `true` if the error was successfully
	// reported and `false` otherwise. This result is logged for observability.
	//
	// Implementations must be safe for concurrent use by multiple executor
	// workers.
	Report(err error, stack []byte) bool
}

type NoopErrorReporter struct{}

func NewNoopErrorReporter() *NoopErrorReporter {
	return &NoopErrorReporter{}
}

var _ ErrorReporter = (*NoopErrorReporter)(nil)

func (r *NoopErrorReporter) Report(err error, stack []byte) bool {
	return false
}
