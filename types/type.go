package types

// Type is a 3-bit tag encoded into a uint16 header
type Type uint16

const (
	TypeInvalid              Type = 0
	TypeEnd                  Type = 0
	TypeUnk                  Type = 0 // actually, can be used as arg position is not determined by it
	TypeInteger              Type = 1
	TypeExtendedTagContainer Type = 2
	TypeFloating             Type = 3
	TypeTuple                Type = 4
	TypeBool                 Type = 5
	TypeString               Type = 6 // used for both string and []byte small chunks
	TypeByteArray            Type = 6
	TypeSlice                Type = 6
	TypeMap                  Type = 7
)

// String returns the human-readable name of the type
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
