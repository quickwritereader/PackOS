package access

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/BranchAndLink/paosp/types"
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

	base, _ := types.DecodeHeader(binary.LittleEndian.Uint16(buf[0:]))
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
func (g *GetAccess) rangeAt(pos int) (tp types.Type, start, end int) {

	if pos >= g.argCount {
		return types.TypeEnd, -2, -1
	}

	h1 := binary.LittleEndian.Uint16(g.buf[pos*2:])
	h2 := binary.LittleEndian.Uint16(g.buf[(pos+1)*2:])

	start, tp = types.DecodeHeader(h1)
	end = types.DecodeOffset(h2) + g.base

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
	if tp != types.TypeBool || end-start != 1 {
		return false, errors.New("decode error")
	}
	return g.buf[start] != 0, nil
}

func (g *GetAccess) GetInt8(pos int) (int8, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeInt8 || end-start != 1 {
		return 0, errors.New("decode error")
	}
	return int8(g.buf[start]), nil
}

func (g *GetAccess) GetUint8(pos int) (uint8, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeUint8 || end-start != 1 {
		return 0, errors.New("decode error")
	}
	return g.buf[start], nil
}

func (g *GetAccess) GetNullableBool(pos int) (*bool, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != types.TypeBool || end-start != 1 {
		return nil, errors.New("decode error")
	}
	v := g.buf[start] != 0
	return &v, nil
}

func (g *GetAccess) GetNullableInt8(pos int) (*int8, error) {
	tp, start, end := g.rangeAt(pos)
	if end-start == 0 {
		return nil, nil
	}
	if tp != types.TypeInt8 || end-start != 1 {
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
	if tp != types.TypeUint8 || end-start != 1 {
		return nil, errors.New("decode error")
	}
	v := g.buf[start]
	return &v, nil
}

func (g *GetAccess) GetInt(pos int) (int64, int, error) {
	tp, start, end := g.rangeAt(pos)
	size := end - start

	if tp != types.TypeBool && tp != types.TypeInt16 {
		return 0, 0, fmt.Errorf("GetInt decode error: not integer type")
	}

	switch size {
	case 0:
		return 0, 0, nil // nil value
	case 1:
		v := int64(g.buf[start:end][0])
		return v, 1, nil
	case 2:
		v := int64(binary.LittleEndian.Uint16(g.buf[start:end]))
		return v, 2, nil
	case 4:
		v := int64(binary.LittleEndian.Uint32(g.buf[start:end]))
		return v, 4, nil
	case 8:
		v := int64(binary.LittleEndian.Uint64(g.buf[start:end]))
		return v, 8, nil
	default:
		return 0, 0, fmt.Errorf("GetInt decode error: unsupported size %d at pos %d", size, pos)
	}
}

// GetUint16 decodes a uint16 at position pos
func (g *GetAccess) GetUint16(pos int) (uint16, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeUint16 || end-start != 2 {
		return 0, errors.New("decode error")
	}
	return binary.LittleEndian.Uint16(g.buf[start:end]), nil
}

// GetUint32 decodes a uint32 at position pos
func (g *GetAccess) GetUint32(pos int) (uint32, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeUint32 || end-start != 4 {
		return 0, errors.New("decode error")
	}
	return binary.LittleEndian.Uint32(g.buf[start:end]), nil
}

// GetUint64 decodes a uint64 at position pos
func (g *GetAccess) GetUint64(pos int) (uint64, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeUint64 || end-start != 8 {
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
	if tp != types.TypeUint16 || end-start != 2 {
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
	if tp != types.TypeUint32 || end-start != 4 {
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
	if tp != types.TypeUint64 || end-start != 8 {
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
	if tp != types.TypeInt16 || end-start != 2 {
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
	if tp != types.TypeInt32 || end-start != 4 {
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
	if tp != types.TypeInt64 || end-start != 8 {
		return nil, errors.New("decode error")
	}
	v := int64(binary.LittleEndian.Uint64(g.buf[start:end]))
	return &v, nil
}

// GetFloat32 decodes a float32 at position pos
func (g *GetAccess) GetFloat32(pos int) (float32, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeFloat32 || end-start != 4 {
		return 0, errors.New("decode error")
	}
	bits := binary.LittleEndian.Uint32(g.buf[start:end])
	return math.Float32frombits(bits), nil
}

// GetFloat64 decodes a float64 at position pos
func (g *GetAccess) GetFloat64(pos int) (float64, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeFloat64 || end-start != 8 {
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
	if tp != types.TypeFloat32 || end-start != 4 {
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
	if tp != types.TypeFloat64 || end-start != 8 {
		return nil, errors.New("decode error")
	}
	bits := binary.LittleEndian.Uint64(g.buf[start:end])
	v := math.Float64frombits(bits)
	return &v, nil
}

// GetBytes decodes a byte slice at position pos
func (g *GetAccess) GetBytes(pos int) ([]byte, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeByteArray || end < start {
		return nil, errors.New("decode error")
	}
	return g.buf[start:end], nil
}

// GetString decodes a string at position pos
func (g *GetAccess) GetString(pos int) (string, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != types.TypeString {
		return "", errors.New("decode error")
	}
	return string(g.buf[start:end]), nil
}

// GetStringUnsafe decodes a string at position pos using unsafe.String
func (g *GetAccess) GetStringUnsafe(pos int) (string, error) {
	tp, start, end := g.rangeAt(pos)
	if tp != types.TypeString || end < start {
		return "", errors.New("decode error")
	}
	return unsafe.String(&g.buf[start], end-start), nil
}

func unpackAnyValue(g *GetAccess, pos int) (any, error) {
	h := binary.LittleEndian.Uint16(g.buf[pos*2:])
	_, typ := types.DecodeHeader(h)

	switch typ {
	case types.TypeInt16:
		v, size, err := g.GetInt(pos)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, nil
		}
		return v, nil

	case types.TypeFloat32:
		return g.GetNullableFloat32(pos)

	case types.TypeFloat64:
		return g.GetNullableFloat64(pos)

	case types.TypeString:
		return g.GetString(pos)

	case types.TypeMap:
		return g.GetMapAny(pos)

	default:
		return nil, fmt.Errorf("unpackAnyValue: unsupported type tag %d at pos %d", typ, pos)
	}
}

func (g *GetAccess) GetMapAny(pos int) (map[string]any, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != types.TypeMap {
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
		val, err := unpackAnyValue(nested, i+1)
		if err != nil {
			return nil, fmt.Errorf("map value decode error at %d: %w", i+1, err)
		}
		out[key] = val
	}
	return out, nil
}

func (g *GetAccess) GetMapStr(pos int) (map[string]string, error) {
	tp, start, end := g.rangeAt(pos)
	if end < start || tp != types.TypeMap {
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
