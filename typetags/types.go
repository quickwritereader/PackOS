package typetags

import "encoding/binary"

// Type is a 3-bit tag encoded into a uint16 header
type Type uint16

// MaxScalarSize is the maximum size of a scalar value (8 bytes)
const MaxScalarSize = 8

const (
	TypeInvalid              Type = 0
	TypeEnd                  Type = 0
	TypeUnk                  Type = 0
	TypeInteger              Type = 1
	TypeExtendedTagContainer Type = 2
	TypeFloating             Type = 3
	TypeTuple                Type = 4
	TypeNull                 Type = 4
	TypeBool                 Type = 5
	TypeString               Type = 6
	TypeByteArray            Type = 6
	TypeSlice                Type = 6
	TypeMap                  Type = 7
)

// Extended container constants
const (
	ExtendedHeaderSize = 8 // 4 bytes self-offset + 4 bytes continuation
	EndOfChain         = 0xFFFFFFFF
)

// ExtendedHeader represents the 8-byte management block for extended containers
type ExtendedHeader struct {
	SelfOffset   uint32 // Absolute 32-bit address for validation
	Continuation uint32 // Absolute 32-bit offset to next segment (or EndOfChain)
}

// IsArray determines whether the payload is an array
func IsArray(payloadSize int) bool {
	return payloadSize > MaxScalarSize
}

// ArrayElementSize returns the element size from the first byte of the payload
func ArrayElementSize(payload []byte) (int, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	switch payload[0] {
	case 1, 2, 4, 8:
		return int(payload[0]), true
	default:
		return 0, false
	}
}

// ArrayElementCount calculates the number of elements in the array
func ArrayElementCount(payloadSize, elementSize int) int {
	if elementSize <= 0 {
		return 0
	}
	return (payloadSize - 1) / elementSize
}

// EncodeExtendedHeader encodes an ExtendedHeader into a byte slice
func EncodeExtendedHeader(selfOffset, continuation uint32) []byte {
	buf := make([]byte, ExtendedHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], selfOffset)
	binary.LittleEndian.PutUint32(buf[4:8], continuation)
	return buf
}

// DecodeExtendedHeader decodes an ExtendedHeader from a byte slice
func DecodeExtendedHeader(data []byte) (ExtendedHeader, bool) {
	if len(data) < ExtendedHeaderSize {
		return ExtendedHeader{}, false
	}
	return ExtendedHeader{
		SelfOffset:   binary.LittleEndian.Uint32(data[0:4]),
		Continuation: binary.LittleEndian.Uint32(data[4:8]),
	}, true
}

func (t Type) String() string {
	switch t {
	case TypeInteger:
		return "Integer"
	case TypeFloating:
		return "Float"
	case TypeBool:
		return "bool"
	case TypeString:
		return "string"
	case TypeExtendedTagContainer:
		return "extended_container"
	case TypeTuple:
		return "tuple"
	case TypeMap:
		return "map"
	default:
		return "invalid"
	}
}

func EncodeHeader(offset int, typeID Type) uint16 {
	return uint16(offset<<3) | (uint16(typeID) & 0x07)
}

func EncodeEnd(offset int) uint16 {
	return uint16(offset << 3)
}

func DecodeHeader(header uint16) (offset int, typeID Type) {
	return int(header >> 3), Type(header & 0x07)
}

func DecodeOffset(header uint16) int {
	return int(header >> 3)
}

func DecodeType(header uint16) Type {
	return Type(header & 0x07)
}
