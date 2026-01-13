package events

type ErrorReporter interface {
	Report(err error, stack []byte) bool
}
