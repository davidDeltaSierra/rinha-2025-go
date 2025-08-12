package rest

import "sync/atomic"

type RingBuffer[T any] struct {
	data       []T
	sizeMask   uint64
	writeIndex uint64
	readIndex  uint64
}

func NewRingBuffer[T any](size int) *RingBuffer[T] {
	if size <= 0 || (size&(size-1)) != 0 {
		panic("size deve ser potência de 2")
	}
	return &RingBuffer[T]{
		data:     make([]T, size),
		sizeMask: uint64(size - 1),
	}
}

func (rb *RingBuffer[T]) Push(value T) bool {
	write := atomic.LoadUint64(&rb.writeIndex)
	read := atomic.LoadUint64(&rb.readIndex)

	if write-read == uint64(len(rb.data)) {
		return false
	}

	rb.data[write&rb.sizeMask] = value
	atomic.StoreUint64(&rb.writeIndex, write+1)
	return true
}

func (rb *RingBuffer[T]) Pop() (T, bool) {
	write := atomic.LoadUint64(&rb.writeIndex)
	read := atomic.LoadUint64(&rb.readIndex)

	if write == read {
		var zero T
		return zero, false
	}

	val := rb.data[read&rb.sizeMask]
	atomic.StoreUint64(&rb.readIndex, read+1)
	return val, true
}
