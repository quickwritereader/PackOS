package typetags

// Type is a 3-bit tag encoded into a uint16 header
type Type uint16

// MaxScalarSize is the maximum size of a scalar value (8 bytes)
const MaxScalarSize = 8

const (
	TypeInvalid              Type = 0
	TypeEnd                  Type = 0
	TypeUnk                  Type = 0 // actually, can be used as arg position is not determined by it
	TypeInteger              Type = 1
	TypeExtendedTagContainer Type = 2
	TypeFloating             Type = 3
	TypeTuple                Type = 4
	TypeNull                 Type = 4
	TypeBool                 Type = 5
	TypeString               Type = 6 // used for both string and []byte small chunks
	TypeByteArray            Type = 6
	TypeSlice                Type = 6
	TypeMap                  Type = 7
)

// Extended container constants
const (
	ExtendedContainerValueSize = 4 // 4 bytes continuation
	EndOfChain                 = 0xFFFFFFFF
)

// ExtendedContainerValue represents the 4-byte management block for extended containers
type ExtendedContainerValue struct {
	Continuation uint32 // Absolute 32-bit offset to next segment (or EndOfChain)
	// SelfOffset   uint32 // Absolute 32-bit address for validation
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

// DecodeHeader splits a header entry into offset and type tag
func DecodeHeader(header uint16) (offset int, typeID Type) {
	return int(header >> 3), Type(header & 0x07)
}

func DecodeOffset(header uint16) int {
	return int(header >> 3)
}

func DecodeType(header uint16) Type {
	return Type(header & 0x07)
}
