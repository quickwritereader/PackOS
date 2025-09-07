package access

import (
	"encoding/binary"
	"math"
)

// WriteNullableInt8 writes a pointer to an int8 to the buffer.
func WriteNullableInt8(buffer []byte, pos int, v *int8) int {
	if v == nil {
		return pos
	}
	buffer[pos] = byte(*v)
	return pos + 1
}

// WriteNullableUint8 writes a pointer to a uint8 to the buffer.
func WriteNullableUint8(buffer []byte, pos int, v *uint8) int {
	if v == nil {
		return pos
	}
	buffer[pos] = *v
	return pos + 1
}

// WriteNullableInt16 writes a pointer to an int16 to the buffer.
func WriteNullableInt16(buffer []byte, pos int, v *int16) int {
	if v == nil {
		return pos
	}
	binary.LittleEndian.PutUint16(buffer[pos:], uint16(*v))
	return pos + 2
}

// WriteNullableInt32 writes a pointer to an int32 to the buffer.
func WriteNullableInt32(buffer []byte, pos int, v *int32) int {
	if v == nil {
		return pos
	}
	binary.LittleEndian.PutUint32(buffer[pos:], uint32(*v))
	return pos + 4
}

// WriteNullableInt64 writes a pointer to an int64 to the buffer.
func WriteNullableInt64(buffer []byte, pos int, v *int64) int {
	if v == nil {
		return pos
	}
	binary.LittleEndian.PutUint64(buffer[pos:], uint64(*v))
	return pos + 8
}

// WriteNullableUint16 writes a pointer to a uint16 to the buffer.
func WriteNullableUint16(buffer []byte, pos int, v *uint16) int {
	if v == nil {
		return pos
	}
	binary.LittleEndian.PutUint16(buffer[pos:], *v)
	return pos + 2
}

// WriteNullableUint32 writes a pointer to a uint32 to the buffer.
func WriteNullableUint32(buffer []byte, pos int, v *uint32) int {
	if v == nil {
		return pos
	}
	binary.LittleEndian.PutUint32(buffer[pos:], *v)
	return pos + 4
}

// WriteNullableUint64 writes a pointer to a uint64 to the buffer.
func WriteNullableUint64(buffer []byte, pos int, v *uint64) int {
	if v == nil {
		return pos
	}
	binary.LittleEndian.PutUint64(buffer[pos:], *v)
	return pos + 8
}

// WriteNullableBool writes a pointer to a boolean to the buffer.
func WriteNullableBool(buffer []byte, pos int, v *bool) int {
	if v == nil {
		return pos
	}
	b := byte(0)
	if *v {
		b = 1
	}
	buffer[pos] = b
	return pos + 1
}

// WriteNullableFloat32 writes a pointer to a float32 to the buffer.
func WriteNullableFloat32(buffer []byte, pos int, v *float32) int {
	if v == nil {
		return pos
	}
	bits := math.Float32bits(*v)
	binary.LittleEndian.PutUint32(buffer[pos:], bits)
	return pos + 4
}

// WriteNullableFloat64 writes a pointer to a float64 to the buffer.
func WriteNullableFloat64(buffer []byte, pos int, v *float64) int {
	if v == nil {
		return pos
	}
	bits := math.Float64bits(*v)
	binary.LittleEndian.PutUint64(buffer[pos:], bits)
	return pos + 8
}
