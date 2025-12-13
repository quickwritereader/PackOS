package access

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/quickwritereader/packos/types"
	"github.com/quickwritereader/packos/utils"
)

var putAccessPool = sync.Pool{
	New: func() interface{} {
		return &PutAccess{
			buf:     make([]byte, 0, 1024),
			offsets: make([]byte, 0, 1024),
		}
	},
}

func GetPutAccess() *PutAccess {
	p := putAccessPool.Get().(*PutAccess)
	p.buf = p.buf[:0]
	p.offsets = p.offsets[:0]
	p.position = 0
	return p
}

func GetPutAccessZero() *PutAccess {
	pt := putAccessPool.Get().(*PutAccess)
	clear(pt.buf)
	clear(pt.offsets)
	pt.position = 0
	return pt
}

func ReleasePutAccess(pa *PutAccess) {
	// Optionally reset fields before putting back to pool
	putAccessPool.Put(pa)
}

type PutAccess struct {
	buf      []byte // payload buffer
	offsets  []byte // header entries: offset + type tag
	position int    // current payload write position
}

// NewPutAccess initializes a new packing buffer

func NewPutAccess() *PutAccess {
	return &PutAccess{
		buf:     make([]byte, 0, 256),
		offsets: make([]byte, 0, 64),
	}
}

func NewPutAccessFromPool() *PutAccess {
	return GetPutAccess()
}

func NewPutAccessFromPoolZero() *PutAccess {
	return GetPutAccessZero()
}

func (p *PutAccess) AppendTagAndValue(tag types.Type, val []byte) {
	p.buf = append(p.buf, val...)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, tag))
	p.position = len(p.buf)
}

// AddInt16 packs an int16 value

func (p *PutAccess) AddInt16(v int16) {
	p.buf = binary.LittleEndian.AppendUint16(p.buf, uint16(v))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddInt32 packs an int32 value

func (p *PutAccess) AddInt32(v int32) {
	p.buf = binary.LittleEndian.AppendUint32(p.buf, uint32(v))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddInt64 packs an int64 value

func (p *PutAccess) AddInt64(v int64) {
	p.buf = binary.LittleEndian.AppendUint64(p.buf, uint64(v))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddUint16 packs a uint16 value.
func (p *PutAccess) AddUint16(v uint16) {
	p.buf = binary.LittleEndian.AppendUint16(p.buf, v)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddUint32 packs a uint32 value.
func (p *PutAccess) AddUint32(v uint32) {
	p.buf = binary.LittleEndian.AppendUint32(p.buf, v)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddUint64 packs a uint64 value.
func (p *PutAccess) AddUint64(v uint64) {
	p.buf = binary.LittleEndian.AppendUint64(p.buf, v)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddFloat32 packs a float32 value

func (p *PutAccess) AddFloat32(v float32) {
	p.buf = binary.LittleEndian.AppendUint32(p.buf, math.Float32bits(v))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeFloating))
	p.position = len(p.buf)
}

// AddFloat64 packs a float64 value

func (p *PutAccess) AddFloat64(v float64) {
	p.buf = binary.LittleEndian.AppendUint64(p.buf, math.Float64bits(v))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeFloating))
	p.position = len(p.buf)
}

// AddUint8 packs a boolean value as a single byte
func (p *PutAccess) AddUint8(b uint8) {

	p.buf = append(p.buf, byte(b))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddInt8 packs a boolean value as a single byte
func (p *PutAccess) AddInt8(b int8) {

	p.buf = append(p.buf, byte(b))
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	p.position = len(p.buf)
}

// AddBool packs a boolean value as a single byte
func (p *PutAccess) AddBool(b bool) {
	var bv byte
	if b {
		bv = 1
	} else {
		bv = 0
	}
	p.buf = append(p.buf, bv)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeBool))
	p.position = len(p.buf)
}

func (p *PutAccess) AddNullableInt8(v *int8) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = append(p.buf, byte(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableInt16(v *int16) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint16(p.buf, uint16(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableInt32(v *int32) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint32(p.buf, uint32(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableInt64(v *int64) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint64(p.buf, uint64(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableUint8(v *uint8) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = append(p.buf, byte(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableUint16(v *uint16) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint16(p.buf, *v)
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableUint32(v *uint32) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint32(p.buf, *v)
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableUint64(v *uint64) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeInteger))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint64(p.buf, *v)
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableFloat32(v *float32) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeFloating))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint32(p.buf, math.Float32bits(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableFloat64(v *float64) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeFloating))
	if v != nil {
		p.buf = binary.LittleEndian.AppendUint64(p.buf, math.Float64bits(*v))
		p.position = len(p.buf)
	}
}

func (p *PutAccess) AddNullableBool(v *bool) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeBool))
	if v != nil {
		b := byte(0)
		if *v {
			b = 1
		}
		p.buf = append(p.buf, b)
		p.position = len(p.buf)
	}
}

// AddBytes packs a byte slice without length prefix

func (p *PutAccess) AddBytes(b []byte) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeString))
	p.buf = append(p.buf, b...)
	p.position = len(p.buf)
}

// AddString packs a string using unsafe zero-copy conversion

func (p *PutAccess) AddString(s string) {
	b := unsafe.Slice(unsafe.StringData(s), len(s))
	p.AddBytes(b)
}

func (p *PutAccess) AddMap(m map[string][]byte) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		nested := NewPutAccessFromPool()
		for k, v := range m {
			nested.AddString(k)
			nested.AddBytes(v)
		}
		p.appendAndReleaseNested(nested)
	}

}

func (p *PutAccess) AddMapStr(m map[string]string) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		nested := NewPutAccessFromPool()
		for k, v := range m {
			nested.AddString(k)
			nested.AddString(v)
		}
		p.appendAndReleaseNested(nested)
	}

}

func (p *PutAccess) AddMapSortedKeyStr(m map[string]string) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		keys := utils.SortKeys(m)
		nested := NewPutAccessFromPool()
		for _, k := range keys {
			nested.AddString(k)
			nested.AddString(m[k])
		}
		p.appendAndReleaseNested(nested)
	}

}

func (p *PutAccess) AddMapSortedKey(m map[string][]byte) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		keys := utils.SortKeys(m)
		nested := NewPutAccessFromPool()
		for _, k := range keys {
			nested.AddString(k)
			nested.AddBytes(m[k])
		}
		p.appendAndReleaseNested(nested)
	}

}

func packAnyValue(p *PutAccess, v any) {
	switch val := v.(type) {
	case string:
		p.AddString(val)
	case []byte:
		p.AddBytes(val)
	case map[string]string:
		p.AddMapStr(val)
	case uint8:
		p.AddUint8(val)
	case uint16:
		p.AddUint16(val)
	case uint32:
		p.AddUint32(val)
	case uint64:
		p.AddUint64(val)
	case int8:
		p.AddInt8(val)
	case int16:
		p.AddInt16(val)
	case int32:
		p.AddInt32(val)
	case int64:
		p.AddInt64(val)
	case float32:
		p.AddFloat32(val)
	case float64:
		p.AddFloat64(val)
	case bool:
		p.AddBool(val)
	case map[string]any:
		p.AddMapAny(val)
	case map[string][]byte:
		p.AddMap(val)
	case Packable:
		val.PackInto(p)
	default:
		// Optional: panic or skip unsupported types
		panic(fmt.Sprintf("packAnyValue: unsupported type %T", val))
	}
}

func packAnyValueSorted(p *PutAccess, v any) {
	switch val := v.(type) {
	case string:
		p.AddString(val)
	case []byte:
		p.AddBytes(val)
	case map[string]string:
		p.AddMapSortedKeyStr(val)
	case int8:
		p.AddInt8(val)
	case int16:
		p.AddInt16(val)
	case int32:
		p.AddInt32(val)
	case int64:
		p.AddInt64(val)
	case float32:
		p.AddFloat32(val)
	case float64:
		p.AddFloat64(val)
	case bool:
		p.AddBool(val)
	case map[string]any:
		p.AddMapAny(val)
	case map[string][]byte:
		p.AddMapSortedKey(val)
	case Packable:
		val.PackInto(p)
	default:
		// Optional: panic or skip unsupported types
		panic(fmt.Sprintf("packAnyValue: unsupported type %T", val))
	}
}

func (p *PutAccess) AddMapAny(m map[string]any) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		nested := NewPutAccessFromPool()
		for k, v := range m {
			nested.AddString(k)
			packAnyValue(nested, v)
		}
		p.appendAndReleaseNested(nested)
	}

}

func (p *PutAccess) AddMapAnySortedKey(m map[string]any) {

	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeHeader(p.position, types.TypeMap))
	if len(m) > 0 {
		keys := utils.SortKeys(m)
		nested := NewPutAccessFromPool()
		for _, k := range keys {
			nested.AddString(k)
			packAnyValueSorted(nested, m[k])
		}
		p.appendAndReleaseNested(nested)
	}

}

func (p *PutAccess) appendAndReleaseNested(nested *PutAccess) {

	p.buf = nested.PackAppend(p.buf)
	ReleasePutAccess(nested)
	p.position = len(p.buf)

}

// Pack finalizes the buffer: header + payload + TypeEnd

func (p *PutAccess) Pack() []byte {
	// Append TypeEnd header for offset-derived slicing
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeEnd(p.position))
	// Compute final header size after appending TypeEnd
	headerSize := len(p.offsets)
	payloadBase := headerSize
	// Overwrite first header with absolute payload base
	hdr := types.EncodeHeader(payloadBase, types.Type(p.offsets[0]&0x07))
	p.offsets[0] = byte(hdr)
	// Allocate final buffer: headers + payload
	final := make([]byte, headerSize+len(p.buf))
	// Write headers
	copy(final, p.offsets)
	// Write payload
	copy(final[headerSize:], p.buf)
	return final
}

func (p *PutAccess) PackAppend(buf []byte) []byte {
	// Append TypeEnd header for offset-derived slicing
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeEnd(p.position))
	// Compute final header size after appending TypeEnd
	headerSize := len(p.offsets)
	payloadBase := headerSize
	// Overwrite first header with absolute payload base
	hdr := types.EncodeHeader(payloadBase, types.Type(p.offsets[0]&0x07))
	p.offsets[0] = byte(hdr)

	// Append headers
	buf = append(buf, p.offsets...)
	// Append payload
	buf = append(buf, p.buf...)
	return buf
}

// Call it before Pack. Pack adds +2
func (p *PutAccess) PackSize() int {
	headerSize := len(p.offsets)
	return headerSize + len(p.buf) + 2
}

func (p *PutAccess) PackBuff(buffer []byte) (int, error) {
	// Append TypeEnd header for offset-derived slicing
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, types.EncodeEnd(p.position))
	// Compute final header size after appending TypeEnd
	headerSize := len(p.offsets)
	payloadBase := headerSize
	// Overwrite first header with absolute payload base
	hdr := types.EncodeHeader(payloadBase, types.Type(p.offsets[0]&0x07))
	p.offsets[0] = byte(hdr)
	n := copy(buffer, p.offsets)
	// Write payload
	// Copy payload if there's room
	if headerSize < len(buffer) {
		n += copy(buffer[headerSize:], p.buf)

	}
	if n != headerSize+len(p.buf) {
		return n, errors.New("insufficient budder")
	}

	return n, nil
}

func (p *PutAccess) AddPackable(v Packable) {
	v.PackInto(p)
}
