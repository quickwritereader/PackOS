package packable

import (
	"github.com/BranchAndLink/paosp/access"
	"github.com/BranchAndLink/paosp/types"
)

func PackNullableInt8(v *int8) PackableNullableInt8          { return PackableNullableInt8{V: v} }
func PackNullableUint8(v *uint8) PackableNullableUint8       { return PackableNullableUint8{V: v} }
func PackNullableInt16(v *int16) PackableNullableInt16       { return PackableNullableInt16{V: v} }
func PackNullableUint16(v *uint16) PackableNullableUint16    { return PackableNullableUint16{V: v} }
func PackNullableInt32(v *int32) PackableNullableInt32       { return PackableNullableInt32{V: v} }
func PackNullableUint32(v *uint32) PackableNullableUint32    { return PackableNullableUint32{V: v} }
func PackNullableInt64(v *int64) PackableNullableInt64       { return PackableNullableInt64{V: v} }
func PackNullableUint64(v *uint64) PackableNullableUint64    { return PackableNullableUint64{V: v} }
func PackNullableFloat32(v *float32) PackableNullableFloat32 { return PackableNullableFloat32{V: v} }
func PackNullableFloat64(v *float64) PackableNullableFloat64 { return PackableNullableFloat64{V: v} }
func PackNullableBool(v *bool) PackableNullableBool          { return PackableNullableBool{V: v} }

type PackableNullableInt8 struct{ V *int8 }

func (p PackableNullableInt8) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableInt8) ValueSize() int         { return 1 }
func (p PackableNullableInt8) Write(buf []byte, pos int) int {
	return access.WriteNullableInt8(buf, pos, p.V)
}
func (v PackableNullableInt8) PackInto(p *access.PutAccess) {
	p.AddNullableInt8(v.V)
}

type PackableNullableUint8 struct{ V *uint8 }

func (p PackableNullableUint8) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableUint8) ValueSize() int         { return 1 }
func (p PackableNullableUint8) Write(buf []byte, pos int) int {
	return access.WriteNullableUint8(buf, pos, p.V)
}
func (v PackableNullableUint8) PackInto(p *access.PutAccess) {
	p.AddNullableUint8(v.V)
}

type PackableNullableInt16 struct{ V *int16 }

func (p PackableNullableInt16) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableInt16) ValueSize() int         { return 2 }
func (p PackableNullableInt16) Write(buf []byte, pos int) int {
	return access.WriteNullableInt16(buf, pos, p.V)
}
func (v PackableNullableInt16) PackInto(p *access.PutAccess) {
	p.AddNullableInt16(v.V)
}

type PackableNullableUint16 struct{ V *uint16 }

func (p PackableNullableUint16) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableUint16) ValueSize() int         { return 2 }
func (p PackableNullableUint16) Write(buf []byte, pos int) int {
	return access.WriteNullableUint16(buf, pos, p.V)
}
func (v PackableNullableUint16) PackInto(p *access.PutAccess) {
	p.AddNullableUint16(v.V)
}

type PackableNullableInt32 struct{ V *int32 }

func (p PackableNullableInt32) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableInt32) ValueSize() int         { return 4 }
func (p PackableNullableInt32) Write(buf []byte, pos int) int {
	return access.WriteNullableInt32(buf, pos, p.V)
}
func (v PackableNullableInt32) PackInto(p *access.PutAccess) {
	p.AddNullableInt32(v.V)
}

type PackableNullableUint32 struct{ V *uint32 }

func (p PackableNullableUint32) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableUint32) ValueSize() int         { return 4 }
func (p PackableNullableUint32) Write(buf []byte, pos int) int {
	return access.WriteNullableUint32(buf, pos, p.V)
}
func (v PackableNullableUint32) PackInto(p *access.PutAccess) {
	p.AddNullableUint32(v.V)
}

type PackableNullableInt64 struct{ V *int64 }

func (p PackableNullableInt64) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableInt64) ValueSize() int         { return 8 }
func (p PackableNullableInt64) Write(buf []byte, pos int) int {
	return access.WriteNullableInt64(buf, pos, p.V)
}
func (v PackableNullableInt64) PackInto(p *access.PutAccess) {
	p.AddNullableInt64(v.V)
}

type PackableNullableUint64 struct{ V *uint64 }

func (p PackableNullableUint64) HeaderType() types.Type { return types.TypeInteger }
func (p PackableNullableUint64) ValueSize() int         { return 8 }
func (p PackableNullableUint64) Write(buf []byte, pos int) int {
	return access.WriteNullableUint64(buf, pos, p.V)
}
func (v PackableNullableUint64) PackInto(p *access.PutAccess) {
	p.AddNullableUint64(v.V)
}

type PackableNullableFloat32 struct{ V *float32 }

func (p PackableNullableFloat32) HeaderType() types.Type { return types.TypeFloating }
func (p PackableNullableFloat32) ValueSize() int         { return 4 }
func (p PackableNullableFloat32) Write(buf []byte, pos int) int {
	return access.WriteNullableFloat32(buf, pos, p.V)
}
func (v PackableNullableFloat32) PackInto(p *access.PutAccess) {
	p.AddNullableFloat32(v.V)
}

type PackableNullableFloat64 struct{ V *float64 }

func (p PackableNullableFloat64) HeaderType() types.Type { return types.TypeFloating }
func (p PackableNullableFloat64) ValueSize() int         { return 8 }
func (p PackableNullableFloat64) Write(buf []byte, pos int) int {
	return access.WriteNullableFloat64(buf, pos, p.V)
}
func (v PackableNullableFloat64) PackInto(p *access.PutAccess) {
	p.AddNullableFloat64(v.V)
}

type PackableNullableBool struct{ V *bool }

func (p PackableNullableBool) HeaderType() types.Type { return types.TypeBool }
func (p PackableNullableBool) ValueSize() int         { return 1 }
func (p PackableNullableBool) Write(buf []byte, pos int) int {
	return access.WriteNullableBool(buf, pos, p.V)
}
func (v PackableNullableBool) PackInto(p *access.PutAccess) {
	p.AddNullableBool(v.V)
}
