package packable

import (
	"testing"
	"time"
)

var sinkFlat []byte

func BenchmarkFlatFields_NewPaospPackableComputBench2(b *testing.B) {
	const count = 1000
	x := map[string]string{
		"user":  "alice",
		"role":  "admin",
		"user2": "alice",
		"role2": "admin",
		"email": "alice@example.com",
		"team":  "core",
		"zone":  "eu-west",
	}
	a0 := PackByteArray([]byte{0, 1, 0xAA})
	a1 := PackByteArray([]byte{1, 2, 0xAA})
	a2 := PackByteArray([]byte{2, 3, 0xAA})
	a3 := PackByteArray([]byte{3, 4, 0xAA})
	a4 := PackByteArray([]byte{4, 5, 0xAA})

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sinkFlat = Pack(
				PackInt16(1000),
				PackInt16(1001),
				PackInt16(1002),
				PackInt16(1003),
				PackInt16(1004),
				PackBool(true),
				PackBool(false),
				PackBool(true),
				PackBool(false),
				PackBool(true),

				PackString("label-0"),
				PackString("label-1"),
				PackString("label-2"),
				PackString("label-3"),
				PackString("label-4"),

				a0,
				a1,
				a2,
				a3,
				a4,
				PackMapStr(x),
			)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("PaospPackableComputNew: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("PaospPackableComputNew size: %d bytes", len(sinkFlat))

}
