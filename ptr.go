package events

func Ptr[T any](val T) *T {
	return &val
}
