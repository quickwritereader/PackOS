package utils

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSizeIndex(t *testing.T) {
	cases := []struct {
		n      int
		expect int
	}{
		{1, 0}, {35, 0}, {63, 0}, {64, 0}, {65, 1}, {127, 1}, {128, 1},
		{129, 2}, {255, 2}, {256, 2}, {257, 3}, {511, 3}, {512, 3},
		{1023, 4}, {1024, 4}, {2047, 5}, {2048, 5}, {4095, 6}, {4096, 6},
		{8191, 7}, {8192, 7}, {16383, 8}, {16384, 8}, {32767, 9}, {32768, 9},
		{32769, -1}, {0, -1},
	}

	for _, tc := range cases {
		idx := SizeIndex(tc.n)
		assert.Equal(t, tc.expect, idx, "SizeIndex(%d)", tc.n)

		if idx >= 0 {
			assert.LessOrEqual(t, BufferSizeClass[idx], 32768, "BufferSizeClass[%d] out of range", idx)
			assert.GreaterOrEqual(t, BufferSizeClass[idx], tc.n, "BufferSizeClass[%d] too small for n=%d", idx, tc.n)
		}
	}
}

func TestBufferPool_AcquireRelease(t *testing.T) {
	bp := NewBufferPool()

	for _, size := range BufferSizeClass {
		buf := bp.Acquire(size - 1)
		assert.GreaterOrEqual(t, cap(buf), size-1)
		assert.Equal(t, len(buf), size-1)

		buf[0] = 0xAA
		buf[len(buf)-1] = 0xBB

		bp.Release(buf)

		buf2 := bp.Acquire(size - 1)
		assert.GreaterOrEqual(t, cap(buf2), size-1)
		assert.Equal(t, len(buf2), size-1)
	}
}

func TestBufferPool_Oversized(t *testing.T) {
	bp := NewBufferPool()
	oversized := 40000

	buf := bp.Acquire(oversized)
	assert.Equal(t, len(buf), oversized)
	assert.GreaterOrEqual(t, cap(buf), oversized)

	bp.Release(buf) // should be safely ignored
}

func TestBufferPool_ExactSizeReuse(t *testing.T) {
	bp := NewBufferPool()

	for _, size := range BufferSizeClass {
		buf := bp.Acquire(size)
		assert.Equal(t, len(buf), size)
		assert.Equal(t, cap(buf), size)

		bp.Release(buf)

		buf2 := bp.Acquire(size)
		assert.Equal(t, len(buf2), size)
		assert.Equal(t, cap(buf2), size)
	}
}

var retained [][]byte

func BenchmarkGCPressureSafe(b *testing.B) {
	const count = 100_000
	const bufSize = 4096

	b.Run("Make", func(b *testing.B) {
		retained = nil
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < count; j++ {
				buf := make([]byte, bufSize)
				buf[0] = byte(j % 256) // Simulate mutation

				// Controlled retention to simulate GC pressure
				if j%100 == 0 {
					retained = append(retained, buf)
					if len(retained) > 1000 {
						retained = retained[1:]
					}
				}
			}
			runtime.GC() // Force collection
		}
	})

	b.Run("Pooled", func(b *testing.B) {
		retained = nil
		var pool = NewBufferPool()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < count; j++ {
				buf := pool.Acquire(bufSize)
				buf[0] = byte(j % 256)

				// Controlled retention — do NOT return retained buffers
				if j%100 == 0 {
					retained = append(retained, buf)
					if len(retained) > 1000 {
						retained = retained[1:]
					}
					continue // skip pool.Put for retained
				}

				pool.Release(buf)
			}
			runtime.GC()
		}
	})
}

func BenchmarkBufferPool_AcquireVariants(b *testing.B) {
	bp := NewBufferPool()
	sizes := []int{64, 4096, 8192}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Acquire_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf := bp.Acquire(size)
				_ = buf[0]
				bp.Release(buf)
			}
		})

		b.Run(fmt.Sprintf("Zeroed_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf := bp.AcquireZeroed(size)
				_ = buf[0]
				bp.Release(buf)
			}
		})
	}
}
