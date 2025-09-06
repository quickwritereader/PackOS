package utils

import (
	"math/bits"
	"sync"
)

var BufferSizeClass = [...]int{64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}

func SizeIndex(n int) int {
	if n <= 0 || n > 32768 {
		return -1
	}
	idx := bits.Len(uint(n))
	if idx < 7 {
		return 0
	}
	if n&(n-1) == 0 {
		return idx - 7
	}
	return idx - 6
}

type BufferPool struct {
	pools [len(BufferSizeClass)]sync.Pool
}

func NewBufferPool() *BufferPool {
	var bp BufferPool
	for i, sz := range BufferSizeClass {
		size := sz
		bp.pools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
	return &bp
}

// Acquire returns a buffer of at least n bytes.
func (bp *BufferPool) Acquire(n int) []byte {
	idx := SizeIndex(n)
	if idx < 0 {
		return make([]byte, n)
	}
	bufPtr := bp.pools[idx].Get().(*[]byte)
	return (*bufPtr)[:n]
}

func (bp *BufferPool) AcquireDefault() []byte {
	bufPtr := bp.pools[0].Get().(*[]byte)
	return *bufPtr
}

func (bp *BufferPool) AcquireZeroed(n int) []byte {
	buf := bp.Acquire(n)
	clear(buf)
	return buf
}

// Release returns the buffer to its pool if size matches a class.
func (bp *BufferPool) Release(buf []byte) {
	c := cap(buf)
	if c&(c-1) != 0 || c < 64 || c > 32768 {
		return // not a valid class
	}
	idx := bits.Len(uint(c)) - 7
	if BufferSizeClass[idx] == c {
		bp.pools[idx].Put(&buf)
	}

}
