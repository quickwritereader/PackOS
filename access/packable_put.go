package access

import (
	"encoding/binary"
	"sort"

	"github.com/BranchAndLink/paosp/types"
)

type Packable interface {
	PackInto(p *PutAccess)
}

type PackInt16 int16

func (v PackInt16) PackInto(p *PutAccess) {
	p.AddInt16(int16(v))
}

func (v PackInt16) PackIntoUnsorted(p *PutAccess) {
	p.AddInt16(int16(v))
}

type PackInt32 int32

func (v PackInt32) PackInto(p *PutAccess) {
	p.AddInt32(int32(v))
}

func (v PackInt32) PackIntoUnsorted(p *PutAccess) {
	p.AddInt32(int32(v))
}

type PackFloat32 float32

func (v PackFloat32) PackInto(p *PutAccess) {
	p.AddFloat32(float32(v))
}

func (v PackFloat32) PackIntoUnsorted(p *PutAccess) {
	p.AddFloat32(float32(v))
}

type PackFloat64 float64

func (v PackFloat64) PackInto(p *PutAccess) {
	p.AddFloat64(float64(v))
}

func (v PackFloat64) PackIntoUnsorted(p *PutAccess) {
	p.AddFloat64(float64(v))
}

type PackBool bool

func (v PackBool) PackInto(p *PutAccess) {
	p.AddBool(bool(v))
}

func (v PackBool) PackIntoUnsorted(p *PutAccess) {
	p.AddBool(bool(v))
}

type PackNullableInt16 struct{ V *int16 }

func (v PackNullableInt16) PackInto(p *PutAccess) {
	p.AddNullableInt16(v.V)
}

func (v PackNullableInt16) PackIntoUnsorted(p *PutAccess) {
	p.AddNullableInt16(v.V)
}

func NullableInt16(val *int16) PackNullableInt16 {
	return PackNullableInt16{V: val}
}

type PackNullableInt32 struct{ V *int32 }

func (v PackNullableInt32) PackInto(p *PutAccess) {
	p.AddNullableInt32(v.V)
}

func (v PackNullableInt32) PackIntoUnsorted(p *PutAccess) {
	p.AddNullableInt32(v.V)
}

func NullableInt32(val *int32) PackNullableInt32 {
	return PackNullableInt32{V: val}
}

type PackNullableInt64 struct{ V *int64 }

func (v PackNullableInt64) PackInto(p *PutAccess) {
	p.AddNullableInt64(v.V)
}

func (v PackNullableInt64) PackIntoUnsorted(p *PutAccess) {
	p.AddNullableInt64(v.V)
}

func NullableInt64(val *int64) PackNullableInt64 {
	return PackNullableInt64{V: val}
}

type PackNullableFloat32 struct{ V *float32 }

func (v PackNullableFloat32) PackInto(p *PutAccess) {
	p.AddNullableFloat32(v.V)
}

func (v PackNullableFloat32) PackIntoUnsorted(p *PutAccess) {
	p.AddNullableFloat32(v.V)
}

func NullableFloat32(val *float32) PackNullableFloat32 {
	return PackNullableFloat32{V: val}
}

type PackNullableFloat64 struct{ V *float64 }

func (v PackNullableFloat64) PackInto(p *PutAccess) {
	p.AddNullableFloat64(v.V)
}

func (v PackNullableFloat64) PackIntoUnsorted(p *PutAccess) {
	p.AddNullableFloat64(v.V)
}

func NullableFloat64(val *float64) PackNullableFloat64 {
	return PackNullableFloat64{V: val}
}

type PackNullableBool struct{ V *bool }

func (v PackNullableBool) PackInto(p *PutAccess) {
	p.AddNullableBool(v.V)
}

func (v PackNullableBool) PackIntoUnsorted(p *PutAccess) {
	p.AddNullableBool(v.V)
}

func NullableBool(val *bool) PackNullableBool {
	return PackNullableBool{V: val}
}

type PackString string

func (v PackString) PackInto(p *PutAccess) {
	p.AddString(string(v))
}

func (v PackString) PackIntoUnsorted(p *PutAccess) {
	p.AddString(string(v))
}

type PackBytes []byte

func (v PackBytes) PackInto(p *PutAccess) {
	p.AddBytes([]byte(v))
}

func (v PackBytes) PackIntoUnsorted(p *PutAccess) {
	p.AddBytes([]byte(v))
}

type PackMapAny map[string]any

func (v PackMapAny) PackInto(p *PutAccess) {
	p.AddMapAnySortedKey(map[string]any(v))
}

func (v PackMapAny) PackIntoUnsorted(p *PutAccess) {
	p.AddMapAny(map[string]any(v))
}

type PackMapStr map[string]string

func (v PackMapStr) PackInto(p *PutAccess) {
	p.AddMapSortedKeyStr(map[string]string(v))
}

func (v PackMapStr) PackIntoUnsorted(p *PutAccess) {
	p.AddMapStr(map[string]string(v))
}

type PackMapBytes map[string][]byte

func (v PackMapBytes) PackInto(p *PutAccess) {
	p.AddMapSortedKey(map[string][]byte(v))
}

func (v PackMapBytes) PackIntoUnsorted(p *PutAccess) {
	p.AddMap(map[string][]byte(v))
}

type PackMapPackable map[string]Packable

func (v PackMapPackable) PackInto(p *PutAccess) {
	m := (map[string]Packable)(v)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		nested := NewPutAccessFromPool()
		for k, val := range m {
			nested.AddString(k)
			val.PackInto(nested)
		}
		p.appendAndReleaseNested(nested)
	}

}

type PackSortedMapPackable map[string]Packable

func (v PackSortedMapPackable) PackInto(p *PutAccess) {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(keys) > 0 {
		nested := NewPutAccessFromPool()
		for _, k := range keys {
			nested.AddString(k)
			v[k].PackInto(nested)
		}
		p.appendAndReleaseNested(nested)
	}
}

func PackArgs(args ...Packable) []byte {
	put := NewPutAccessFromPool()
	for _, arg := range args {
		arg.PackInto(put)
	}
	ret := put.Pack()
	ReleasePutAccess(put)
	return ret
}
