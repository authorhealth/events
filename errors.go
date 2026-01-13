package events

type ErrorReporter interface {
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
