package access

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	goccyjson "github.com/goccy/go-json"
	jsoniter "github.com/json-iterator/go"
)

type CompactPayload struct {
	I0 int16 `json:"i0"`
	I1 int16 `json:"i1"`
	I2 int16 `json:"i2"`
	I3 int16 `json:"i3"`
	I4 int16 `json:"i4"`

	F0 bool `json:"f0"`
	F1 bool `json:"f1"`
	F2 bool `json:"f2"`
	F3 bool `json:"f3"`
	F4 bool `json:"f4"`

	L0 string `json:"l0"`
	L1 string `json:"l1"`
	L2 string `json:"l2"`
	L3 string `json:"l3"`
	L4 string `json:"l4"`

	R0 []byte `json:"r0"`
	R1 []byte `json:"r1"`
	R2 []byte `json:"r2"`
	R3 []byte `json:"r3"`
	R4 []byte `json:"r4"`

	M map[string]string `json:"m"`
}

var flat = CompactPayload{
	I0: 1000, I1: 1001, I2: 1002, I3: 1003, I4: 1004,
	F0: true, F1: false, F2: true, F3: false, F4: true,
	L0: "label-0", L1: "label-1", L2: "label-2", L3: "label-3", L4: "label-4",
	R0: []byte{0, 1, 0xAA}, R1: []byte{1, 2, 0xAA}, R2: []byte{2, 3, 0xAA},
	R3: []byte{3, 4, 0xAA}, R4: []byte{4, 5, 0xAA},
	M: map[string]string{
		"user":  "alice",
		"role":  "admin",
		"user2": "alice",
		"role2": "admin",
		"email": "alice@example.com",
		"team":  "core",
		"zone":  "eu-west",
	},
}

func BenchmarkFlatFields_PaospFlatFields(b *testing.B) {
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
	a0 := []byte{0, 1, 0xAA}
	a1 := []byte{1, 2, 0xAA}
	a2 := []byte{2, 3, 0xAA}
	a3 := []byte{3, 4, 0xAA}
	a4 := []byte{4, 5, 0xAA}

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			put := NewPutAccessFromPool()

			put.AddInt16(1000)
			put.AddInt16(1001)
			put.AddInt16(1002)
			put.AddInt16(1003)
			put.AddInt16(1004)

			put.AddBool(true)
			put.AddBool(false)
			put.AddBool(true)
			put.AddBool(false)
			put.AddBool(true)

			put.AddString("label-0")
			put.AddString("label-1")
			put.AddString("label-2")
			put.AddString("label-3")
			put.AddString("label-4")

			put.AddBytes(a0)
			put.AddBytes(a1)
			put.AddBytes(a2)
			put.AddBytes(a3)
			put.AddBytes(a4)
			put.AddMapStr(x)

			sinkFlat = put.Pack()
			ReleasePutAccess(put)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("PaospFlatFields: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("PaospFlatFields size: %d bytes", len(sinkFlat))

}

func BenchmarkFlatFields_MusGenFlatFields(b *testing.B) {
	const count = 1000

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sizeX := CompactPayloadMUS.Size(flat)
			dst := make([]byte, sizeX)

			CompactPayloadMUS.Marshal(flat, dst)
			sinkFlat = dst
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("MusGenFlatFields: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("MusGenFlatFields size: %d bytes", len(sinkFlat))

}

func BenchmarkFlatFields_JsonFlatFields(b *testing.B) {
	const count = 1000
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sinkJSON, _ = json.Marshal(flat)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("JsonFlatFields: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("JsonFlatFields size:   %d bytes", len(sinkJSON))

}

var sinkFlat, sinkNested, sinkJSON, sinkManual []byte

func BenchmarkFlatFields_JsonIter(b *testing.B) {
	const count = 1000
	b.ReportAllocs()
	b.ResetTimer()

	var jsonIter = jsoniter.ConfigCompatibleWithStandardLibrary

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sinkJSON, _ = jsonIter.Marshal(flat)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("JsonIter: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("JsonIter size: %d bytes", len(sinkJSON))
}

func BenchmarkFlatFields_GoJson(b *testing.B) {
	const count = 1000
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sinkJSON, _ = goccyjson.Marshal(flat)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("GoJson: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("GoJson size: %d bytes", len(sinkJSON))
}

func BenchmarkFlatFields_MsgPack(b *testing.B) {
	const count = 1000
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sinkJSON, _ = msgpack.Marshal(flat)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("MsgPack: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("MsgPack size: %d bytes", len(sinkJSON))
}

func BenchmarkPutAccess_PackNested_JSONMarshal(b *testing.B) {
	const count = 1000 // logical iterations per benchmark tick

	type Payload struct {
		ID   int16             `json:"id"`
		Meta map[string]string `json:"meta"`
		Name string            `json:"name"`
	}

	data := Payload{
		ID: 12345,
		Meta: map[string]string{
			"user": "alice",
			"role": "admin",
		},
		Name: "gopher",
	}

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			sinkJSON, _ = json.Marshal(data)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("JSONMarshal: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("JSONMarshal size:   %d bytes", len(sinkJSON))
}

func BenchmarkPutAccess_PackNested_Paosp(b *testing.B) {
	const count = 1000 // logical iterations per benchmark tick
	x := map[string]any{
		"meta": map[string][]byte{
			"user": []byte("alice"),
			"role": []byte("admin"),
		},
		"name": "gopher",
	}
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			put := NewPutAccessFromPool()
			put.AddInt16(12345)
			put.AddMapAny(x, false)
			sinkManual = put.Pack()
			ReleasePutAccess(put)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	perPack := float64(elapsed.Nanoseconds()) / float64(b.N*count)
	opsPerSec := 1e9 / perPack
	b.Logf("PutAccessManual_Paosp: per-pack = %.2f ns/op, %.2f ops/sec", perPack, opsPerSec)
	b.Logf("PutAccessManual_Paosp size:   %d bytes", len(sinkManual))

}
