package access

import (
	"encoding/binary"
	"math"

	"github.com/quickwritereader/PackOS/typetags"
)

const HeaderTagSize = 2

// WriteTypeHeader writes a header to the buffer at pos and returns the new position.
func WriteTypeHeader(buffer []byte, pos int, encodedPos int, t typetags.Type) (newPos int) {
	header := typetags.EncodeHeader(encodedPos, t)
	binary.LittleEndian.PutUint16(buffer[pos:], header)
	return pos + 2
}

// WriteInt8 writes an int8 value to the buffer.
func WriteInt8(buffer []byte, pos int, v int8) int {
	buffer[pos] = byte(v)
	return pos + 1
}

// WriteUint8 writes a uint8 value to the buffer.
func WriteUint8(buffer []byte, pos int, v uint8) int {
	buffer[pos] = v
	return pos + 1
}

// WriteInt16 writes an int16 value to the buffer.
func WriteInt16(buffer []byte, pos int, v int16) int {
	binary.LittleEndian.PutUint16(buffer[pos:], uint16(v))
	return pos + 2
}

// WriteUint16 writes a uint16 value to the buffer.
func WriteUint16(buffer []byte, pos int, v uint16) int {
	binary.LittleEndian.PutUint16(buffer[pos:], v)
	return pos + 2
}

// WriteInt32 writes an int32 value to the buffer.
func WriteInt32(buffer []byte, pos int, v int32) int {
	binary.LittleEndian.PutUint32(buffer[pos:], uint32(v))
	return pos + 4
}

// WriteUint32 writes a uint32 value to the buffer.
func WriteUint32(buffer []byte, pos int, v uint32) int {
	binary.LittleEndian.PutUint32(buffer[pos:], v)
	return pos + 4
}

// WriteInt64 writes an int64 value to the buffer.
func WriteInt64(buffer []byte, pos int, v int64) int {
	binary.LittleEndian.PutUint64(buffer[pos:], uint64(v))
	return pos + 8
}

// WriteUint64 writes a uint64 value to the buffer.
func WriteUint64(buffer []byte, pos int, v uint64) int {
	binary.LittleEndian.PutUint64(buffer[pos:], v)
	return pos + 8
}

// WriteBool writes a boolean value as a single byte to the buffer.
func WriteBool(buffer []byte, pos int, v bool) int {
	var b byte
	if v {
		b = 1
	}
	buffer[pos] = b
	return pos + 1
}

// WriteString writes a string to the buffer.
func WriteString(buffer []byte, pos int, s string) int {
	copy(buffer[pos:], s)
	return pos + len(s)
}

// WriteBytes writes a byte slice to the buffer.
func WriteBytes(buffer []byte, pos int, b []byte) int {
	copy(buffer[pos:], b)
	return pos + len(b)
}

// WriteFloat32 writesa float32 to the buffer.
func WriteFloat32(buffer []byte, pos int, v float32) int {
	bits := math.Float32bits(v)
	binary.LittleEndian.PutUint32(buffer[pos:], bits)
	return pos + 4
}

// WriteFloat64 writesa float64 to the buffer.
func WriteFloat64(buffer []byte, pos int, v float64) int {
	bits := math.Float64bits(v)
	binary.LittleEndian.PutUint64(buffer[pos:], bits)
	return pos + 8
}
