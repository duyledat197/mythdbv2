package iterator

type Iterator[V any] interface {
	Next() bool
	Entry() V
	// Seek([]byte) bool
	Close() error
}
