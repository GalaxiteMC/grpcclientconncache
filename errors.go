package conncache

var _ error = &DialFailedError{}

type DialFailedError struct{}

func (b *DialFailedError) Error() string {
	return "another goroutine requested first and the dial failed"
}
