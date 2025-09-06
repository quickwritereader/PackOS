package types

// Type is a 3-bit tag encoded into a uint16 header
type Type uint16

const (
	TypeInvalid   Type = 0
	TypeEnd       Type = 0
	TypeInt16     Type = 1 //exact type could be derived from length
	TypeInt32     Type = 1 //exact type could be derived from length
	TypeInt64     Type = 1 //exact type could be derived from length
	TypeUint16    Type = 1 //exact type could be derived from length
	TypeUint32    Type = 1 //exact type could be derived from length
	TypeUint64    Type = 1 //exact type could be derived from length
	TypeFloat32   Type = 3
	TypeFloat64   Type = 4
	TypeBool      Type = 5 // instead of slice with 1 length it has its own identity
	TypeInt8      Type = 5
	TypeUint8     Type = 5
	TypeString    Type = 6 // used for both string and []byte
	TypeByteArray Type = 6
	TypeSlice     Type = 6
	TypeMap       Type = 7
)

// String returns the human-readable name of the type
func (t Type) String() string {
	switch t {
	case TypeInt16:
		return "int16|int32|int64|uint16|uint32|uint64"
	case TypeFloat32:
		return "float32"
	case TypeFloat64:
		return "float64"
	case TypeBool:
		return "bool|int8|uint8"
	case TypeString:
		return "string"
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
