package access

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/quickwritereader/PackOS/typetags"
)

type GetAccess struct {
	buf      []byte // full packed buffer: headers + payload
	argCount int    // number of headers (excluding TypeEnd)
	base     int    // absolute offset to payload start
}

func NewGetAccess(buf []byte) *GetAccess {
	if len(buf) < 2 {
		return nil // not enough to decode base header
	}

	base, _ := typetags.DecodeHeader(binary.LittleEndian.Uint16(buf[0:]))
	count := base/2 - 1
	if len(buf) < base {
		return nil // buffer too short for declared header count
	}

	return &GetAccess{
		buf:      buf,
		argCount: count,
		base:     base,
	}
}

// rangeAt returns absolute start and end offsets for field at pos
func (g *GetAccess) rangeAt(pos int) (tp typetags.Type, start, end int) {

	if pos >= g.argCount {
		return typetags.TypeEnd, -2, -1
	}

	h1 := binary.LittleEndian.Uint16(g.buf[pos*2:])
	h2 := binary.LittleEndian.Uint16(g.buf[(pos+1)*2:])

	start, tp = typetags.DecodeHeader(h1)
	end = typetags.DecodeOffset(h2) + g.base

	if pos > 0 {
		start += g.base
	}

	if end > len(g.buf) {
		end = -1 // force failure downstream
	}
	return
}

func (g *GetAccess) GetBool(pos int) (bool, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeBool || end-start != 1 {
		return false, errors.New("decode error")
	}
	return g.buf[start] != 0, nil
}

func (g *GetAccess) GetNullableBool(pos int) (*bool, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeBool || end-start != 1 {
		return nil, errors.New("decode error")
	}
	v := g.buf[start] != 0
	return &v, nil
}

func (g *GetAccess) GetInt8(pos int) (int8, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeInteger || end-start != 1 {
		return 0, errors.New("decode error")
	}
	return int8(g.buf[start]), nil
}

func (g *GetAccess) GetUint8(pos int) (uint8, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeInteger || end-start != 1 {
		return 0, errors.New("decode error")
	}
	return g.buf[start], nil
}

func (g *GetAccess) GetNullableInt8(pos int) (*int8, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 1 {
		return nil, errors.New("decode error")
	}
	v := int8(g.buf[start])
	return &v, nil
}

func (g *GetAccess) GetNullableUint8(pos int) (*uint8, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 1 {
		return nil, errors.New("decode error")
	}
	v := g.buf[start]
	return &v, nil
}

func (g *GetAccess) GetInt(pos int) (any, int, error) {
	tp, start, end := g.rangeAt(pos)
	size := end - start

	if tp != typetags.TypeInteger {
		return 0, 0, fmt.Errorf("GetInt decode error: not integer type")
	}

	switch size {
	case 0:
		return 0, 0, nil // nil value
	case 1:
		v := int8(g.buf[start:end][0])
		return v, 1, nil
	case 2:
		v := int16(binary.LittleEndian.Uint16(g.buf[start:end]))
		return v, 2, nil
	case 4:
		v := int32(binary.LittleEndian.Uint32(g.buf[start:end]))
		return v, 4, nil
	case 8:
		v := int64(binary.LittleEndian.Uint64(g.buf[start:end]))
		return v, 8, nil
	default:
		return 0, 0, fmt.Errorf("GetInt decode error: unsupported size %d at pos %d", size, pos)
	}
}

func (g *GetAccess) GetFloating(pos int) (any, int, error) {
	tp, start, end := g.rangeAt(pos)
	size := end - start

	if tp != typetags.TypeFloating {
		return 0, 0, fmt.Errorf("GetInt decode error: not floating type")
	}

	switch size {
	case 0:
		return 0, 0, nil // nil value
	case 4:
		bits := binary.LittleEndian.Uint32(g.buf[start:end])
		v := math.Float32frombits(bits)
		return v, 4, nil
	case 8:
		bits := binary.LittleEndian.Uint64(g.buf[start:end])
		v := math.Float64frombits(bits)
		return v, 8, nil
	default:
		return 0, 0, fmt.Errorf("GetInt decode error: unsupported size %d at pos %d", size, pos)
	}
}

// GetUint16 decodes a uint16 at position pos
func (g *GetAccess) GetUint16(pos int) (uint16, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeInteger || end-start != 2 {
		return 0, errors.New("decode error")
	}
	return binary.LittleEndian.Uint16(g.buf[start:end]), nil
}

// GetUint32 decodes a uint32 at position pos
func (g *GetAccess) GetUint32(pos int) (uint32, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeInteger || end-start != 4 {
		return 0, errors.New("decode error")
	}
	return binary.LittleEndian.Uint32(g.buf[start:end]), nil
}

// GetUint64 decodes a uint64 at position pos
func (g *GetAccess) GetUint64(pos int) (uint64, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeInteger || end-start != 8 {
		return 0, errors.New("decode error")
	}
	return binary.LittleEndian.Uint64(g.buf[start:end]), nil
}

func (g *GetAccess) GetInt16(pos int) (int16, error) {
	v, err := g.GetUint16(pos)
	return int16(v), err
}

func (g *GetAccess) GetInt32(pos int) (int32, error) {
	v, err := g.GetUint32(pos)
	return int32(v), err
}

func (g *GetAccess) GetInt64(pos int) (int64, error) {
	v, err := g.GetUint64(pos)
	return int64(v), err
}

func (g *GetAccess) GetNullableUint16(pos int) (*uint16, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 2 {
		return nil, errors.New("decode error")
	}
	v := binary.LittleEndian.Uint16(g.buf[start:end])
	return &v, nil
}

func (g *GetAccess) GetNullableUint32(pos int) (*uint32, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 4 {
		return nil, errors.New("decode error")
	}
	v := binary.LittleEndian.Uint32(g.buf[start:end])
	return &v, nil
}

func (g *GetAccess) GetNullableUint64(pos int) (*uint64, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 8 {
		return nil, errors.New("decode error")
	}
	v := binary.LittleEndian.Uint64(g.buf[start:end])
	return &v, nil
}

func (g *GetAccess) GetNullableInt16(pos int) (*int16, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 2 {
		return nil, errors.New("decode error")
	}
	v := int16(binary.LittleEndian.Uint16(g.buf[start:end]))
	return &v, nil
}

func (g *GetAccess) GetNullableInt32(pos int) (*int32, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 4 {
		return nil, errors.New("decode error")
	}
	v := int32(binary.LittleEndian.Uint32(g.buf[start:end]))
	return &v, nil
}

func (g *GetAccess) GetNullableInt64(pos int) (*int64, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeInteger || end-start != 8 {
		return nil, errors.New("decode error")
	}
	v := int64(binary.LittleEndian.Uint64(g.buf[start:end]))
	return &v, nil
}

// GetFloat32 decodes a float32 at position pos
func (g *GetAccess) GetFloat32(pos int) (float32, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeFloating || end-start != 4 {
		return 0, errors.New("decode error")
	}
	bits := binary.LittleEndian.Uint32(g.buf[start:end])
	return math.Float32frombits(bits), nil
}

// GetFloat64 decodes a float64 at position pos
func (g *GetAccess) GetFloat64(pos int) (float64, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeFloating || end-start != 8 {
		return 0, errors.New("decode error")
	}
	bits := binary.LittleEndian.Uint64(g.buf[start:end])
	return math.Float64frombits(bits), nil
}

// GetNullableFloat32 decodes a nullable float32 at position pos
func (g *GetAccess) GetNullableFloat32(pos int) (*float32, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeFloating || end-start != 4 {
		return nil, errors.New("decode error")
	}
	bits := binary.LittleEndian.Uint32(g.buf[start:end])
	v := math.Float32frombits(bits)
	return &v, nil
}

// GetNullableFloat64 decodes a nullable float64 at position pos
func (g *GetAccess) GetNullableFloat64(pos int) (*float64, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != typetags.TypeFloating || end-start != 8 {
		return nil, errors.New("decode error")
	}
	bits := binary.LittleEndian.Uint64(g.buf[start:end])
	v := math.Float64frombits(bits)
	return &v, nil
}

// GetBytes decodes a byte slice at position pos
func (g *GetAccess) GetBytes(pos int) ([]byte, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeByteArray || end < start {
		return nil, errors.New("decode error")
	}
	return g.buf[start:end], nil
}

// GetCopyBytes decodes and returns a fresh copy of the byte slice.
// Use this to avoid retention of the entire GetAccess buffer in memory,
// which prevents the garbage collector from reclaiming the large backing array.
func (g *GetAccess) GetCopyBytes(pos int) ([]byte, error) {
	data, err := g.GetBytes(pos)
	if err != nil {
		return nil, err
	}

	// Create an independent copy to break the reference to g.buf
	cp := make([]byte, len(data))
	copy(cp, data)

	return cp, nil
}

// GetString decodes a string at position pos
func (g *GetAccess) GetString(pos int) (string, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != typetags.TypeString {
		return "", errors.New("decode error")
	}
	return string(g.buf[start:end]), nil
}

// GetStringUnsafe decodes a string at position pos using unsafe.String
func (g *GetAccess) GetStringUnsafe(pos int) (string, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != typetags.TypeString || end < start {
		return "", errors.New("decode error")
	}
	return unsafe.String(&g.buf[start], end-start), nil
}

func GetAny(g *GetAccess, pos int) (any, error) {
	h := binary.LittleEndian.Uint16(g.buf[pos*2:])
	_, typ := typetags.DecodeHeader(h)

	switch typ {
	case typetags.TypeInteger:
		v, size, err := g.GetInt(pos)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, nil
		}
		return v, nil

	case typetags.TypeFloating:
		v, size, err := g.GetFloating(pos)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, nil
		}
		return v, nil
	case typetags.TypeString:
		return g.GetString(pos)

	case typetags.TypeMap:
		return g.GetMapAny(pos)

	default:
		return nil, fmt.Errorf("GetAny: unsupported type tag %d at pos %d", typ, pos)
	}
}

func (g *GetAccess) GetMapAny(pos int) (map[string]any, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != typetags.TypeMap {
		return nil, errors.New("decode error")
	}
	if end == start {
		return nil, nil // nil map
	}

	nested := NewGetAccess(g.buf[start:end])
	out := make(map[string]any, nested.argCount/2)

	for i := 0; i < nested.argCount; i += 2 {
		key, err := nested.GetString(i)
		if err != nil {
			return nil, fmt.Errorf("map key decode error at %d: %w", i, err)
		}
		val, err := GetAny(nested, i+1)
		if err != nil {
			return nil, fmt.Errorf("map value decode error at %d: %w", i+1, err)
		}
		out[key] = val
	}
	return out, nil
}

// GetMapOrderedAny decodes a map at the given position into an OrderedMapAny,
// preserving insertion order of keys.
func (g *GetAccess) GetMapOrderedAny(pos int) (*typetags.OrderedMapAny, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != typetags.TypeMap {
		return nil, errors.New("decode error")
	}
	if end == start {
		return nil, nil // nil map
	}

	nested := NewGetAccess(g.buf[start:end])
	out := typetags.NewOrderedMapAny()

	for i := 0; i < nested.argCount; i += 2 {
		key, err := nested.GetString(i)
		if err != nil {
			return nil, fmt.Errorf("ordered map key decode error at %d: %w", i, err)
		}
		val, err := GetAny(nested, i+1)
		if err != nil {
			return nil, fmt.Errorf("ordered map value decode error at %d: %w", i+1, err)
		}
		out.Set(key, val)
	}
	return out, nil
}

func (g *GetAccess) GetMapStr(pos int) (map[string]string, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != typetags.TypeMap {
		return nil, errors.New("decode error")
	}
	if end == start {
		return nil, nil // nil map
	}

	nested := NewGetAccess(g.buf[start:end])
	out := make(map[string]string, nested.argCount/2)

	for i := 0; i < nested.argCount; i += 2 {
		key, err := nested.GetString(i)
		if err != nil {
			return nil, fmt.Errorf("map key decode error at %d: %w", i, err)
		}
		out[key], err = nested.GetString(i + 1)
		if err != nil {
			return nil, fmt.Errorf("map value decode error at %d: %w", i+1, err)
		}

	}
	return out, nil
}

func (g *GetAccess) GetNestedGetAccess(pos int) (*GetAccess, typetags.Type, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || (tp != typetags.TypeMap && tp != typetags.TypeTuple) {
		return nil, tp, errors.New("decode error: it's not nested type")
	}
	if end == start {
		return nil, tp, nil // nil map
	}
	return NewGetAccess(g.buf[start:end]), tp, nil
}

// function to get type and value, which can be used for repacking or other purposes
func (g *GetAccess) GetTypeAndValue(pos int) (typetags.Type, []byte) {
	tp, start, end := g.rangeAt(pos)
	if end < start {
		return typetags.TypeInvalid, nil
	}
	return tp, g.buf[start:end]
}

type PackableTagValue struct {
	head  typetags.Type
	value []byte
}

func (px *PackableTagValue) HeaderType() typetags.Type {
	return px.head
}
func (px *PackableTagValue) ValueSize() int {
	return len(px.value)
}
func (px *PackableTagValue) Write(buf []byte, pos int) int {
	return WriteBytes(buf, pos, px.value)
}

func (px *PackableTagValue) PackInto(p *PutAccess) {
	p.AppendTagAndValue(px.head, px.value)
}

func (g *GetAccess) GetAsPackable(pos int) (Packable, error) {
	tp, value := g.GetTypeAndValue(pos)
	if value == nil && tp == typetags.TypeInvalid {
		return nil, fmt.Errorf("invalid")
	}
	return &PackableTagValue{head: tp, value: value}, nil
}
