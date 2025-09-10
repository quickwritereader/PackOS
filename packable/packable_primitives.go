package packable

import (
	"github.com/BranchAndLink/packos/access"
	"github.com/BranchAndLink/packos/types"
)

// PackInt8 implements the Packable interface for int8.
type PackInt8 int8

func (p PackInt8) HeaderType() types.Type { return types.TypeInteger }
func (p PackInt8) ValueSize() int         { return 1 }
func (p PackInt8) Write(buf []byte, pos int) int {
	return access.WriteInt8(buf, pos, int8(p))
}
func (v PackInt8) PackInto(p *access.PutAccess) {
	p.AddInt8(int8(v))
}

// PackUint8 implements the Packable interface for uint8.
type PackUint8 uint8

func (p PackUint8) HeaderType() types.Type { return types.TypeInteger }
func (p PackUint8) ValueSize() int         { return 1 }
func (p PackUint8) Write(buf []byte, pos int) int {
	return access.WriteUint8(buf, pos, uint8(p))
}
func (v PackUint8) PackInto(p *access.PutAccess) {
	p.AddUint8(uint8(v))
}

// PackInt16 implements the Packable interface for int16.
type PackInt16 int16

func (p PackInt16) HeaderType() types.Type { return types.TypeInteger }
func (p PackInt16) ValueSize() int         { return 2 }
func (p PackInt16) Write(buf []byte, pos int) int {
	return access.WriteInt16(buf, pos, int16(p))
}
func (v PackInt16) PackInto(p *access.PutAccess) {
	p.AddInt16(int16(v))
}

// PackUint16 implements the Packable interface for uint16.
type PackUint16 uint16

func (p PackUint16) HeaderType() types.Type { return types.TypeInteger }
func (p PackUint16) ValueSize() int         { return 2 }
func (p PackUint16) Write(buf []byte, pos int) int {
	return access.WriteUint16(buf, pos, uint16(p))
}
func (v PackUint16) PackInto(p *access.PutAccess) {
	p.AddUint16(uint16(v))
}

// PackInt32 implements the Packable interface for int32.
type PackInt32 int32

func (p PackInt32) HeaderType() types.Type { return types.TypeInteger }
func (p PackInt32) ValueSize() int         { return 4 }
func (p PackInt32) Write(buf []byte, pos int) int {
	return access.WriteInt32(buf, pos, int32(p))
}
func (v PackInt32) PackInto(p *access.PutAccess) {
	p.AddInt32(int32(v))
}

// PackUint32 implements the Packable interface for uint32.
type PackUint32 uint32

func (p PackUint32) HeaderType() types.Type { return types.TypeInteger }
func (p PackUint32) ValueSize() int         { return 4 }
func (p PackUint32) Write(buf []byte, pos int) int {
	return access.WriteUint32(buf, pos, uint32(p))
}
func (v PackUint32) PackInto(p *access.PutAccess) {
	p.AddUint32(uint32(v))
}

// PackInt64 implements the Packable interface for int64.
type PackInt64 int64

func (p PackInt64) HeaderType() types.Type { return types.TypeInteger }
func (p PackInt64) ValueSize() int         { return 8 }
func (p PackInt64) Write(buf []byte, pos int) int {
	return access.WriteInt64(buf, pos, int64(p))
}
func (v PackInt64) PackInto(p *access.PutAccess) {
	p.AddInt64(int64(v))
}

// PackUint64 implements the Packable interface for uint64.
type PackUint64 uint64

func (p PackUint64) HeaderType() types.Type { return types.TypeInteger }
func (p PackUint64) ValueSize() int         { return 8 }
func (p PackUint64) Write(buf []byte, pos int) int {
	return access.WriteUint64(buf, pos, uint64(p))
}
func (v PackUint64) PackInto(p *access.PutAccess) {
	p.AddUint64(uint64(v))
}

// PackFloat32 implements the Packable interface for float32.
type PackFloat32 float32

func (p PackFloat32) HeaderType() types.Type { return types.TypeFloating }
func (p PackFloat32) ValueSize() int         { return 4 }
func (p PackFloat32) Write(buf []byte, pos int) int {
	return access.WriteFloat32(buf, pos, float32(p))
}
func (v PackFloat32) PackInto(p *access.PutAccess) {
	p.AddFloat32(float32(v))
}

// PackFloat64 implements the Packable interface for float64.
type PackFloat64 float64

func (p PackFloat64) HeaderType() types.Type { return types.TypeFloating }
func (p PackFloat64) ValueSize() int         { return 8 }
func (p PackFloat64) Write(buf []byte, pos int) int {
	return access.WriteFloat64(buf, pos, float64(p))
}
func (v PackFloat64) PackInto(p *access.PutAccess) {
	p.AddFloat64(float64(v))
}

// PackBool implements the Packable interface for bool.
type PackBool bool

func (p PackBool) HeaderType() types.Type { return types.TypeBool }
func (p PackBool) ValueSize() int         { return 1 }
func (p PackBool) Write(buf []byte, pos int) int {
	return access.WriteBool(buf, pos, bool(p))
}
func (v PackBool) PackInto(p *access.PutAccess) {
	p.AddBool(bool(v))
}

// PackString implements the Packable interface for string.
type PackString string

func (p PackString) HeaderType() types.Type { return types.TypeString }
func (p PackString) ValueSize() int         { return len(p) }
func (p PackString) Write(buf []byte, pos int) int {
	return access.WriteString(buf, pos, string(p))
}
func (v PackString) PackInto(p *access.PutAccess) {
	p.AddString(string(v))
}

// PackByteArray implements the Packable interface for *[]byte. as ref
// After benching I spotted that interface boxing some-how resulted with allocation
// thats why for []byte array we will use reference
type PackByteArrayRef struct {
	ref *[]byte
}

func (p PackByteArrayRef) HeaderType() types.Type { return types.TypeByteArray }
func (p PackByteArrayRef) ValueSize() int         { return len(*p.ref) }
func (p PackByteArrayRef) Write(buf []byte, pos int) int {
	return access.WriteBytes(buf, pos, *p.ref)
}
func (v PackByteArrayRef) PackInto(p *access.PutAccess) {
	p.AddBytes(*v.ref)
}

func PackByteArray(b []byte) PackByteArrayRef {
	return PackByteArrayRef{ref: &b}
}
