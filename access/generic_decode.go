package access

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/quickwritereader/PackOS/typetags"
)

// DecodePrimitive interprets a primitive payload directly using type tag and width.
// It returns a Go value (int, float, string, []byte as string, bool, nil).
func DecodePrimitive(typ typetags.Type, buf []byte) (interface{}, error) {
	size := len(buf)

	switch typ {
	case typetags.TypeInteger:
		switch size {
		case 0:
			return nil, nil
		case 1:
			return int8(buf[0]), nil
		case 2:
			return int16(binary.LittleEndian.Uint16(buf)), nil
		case 4:
			return int32(binary.LittleEndian.Uint32(buf)), nil
		case 8:
			return int64(binary.LittleEndian.Uint64(buf)), nil
		default:
			return nil, fmt.Errorf("DecodePrimitive: unsupported integer size %d", size)
		}

	case typetags.TypeFloating:
		switch size {
		case 0:
			return nil, nil
		case 4:
			bits := binary.LittleEndian.Uint32(buf)
			return math.Float32frombits(bits), nil
		case 8:
			bits := binary.LittleEndian.Uint64(buf)
			return math.Float64frombits(bits), nil
		default:
			return nil, fmt.Errorf("DecodePrimitive: unsupported float size %d", size)
		}

	case typetags.TypeString:
		return string(buf), nil

	case typetags.TypeBool:
		if size != 1 {
			return nil, fmt.Errorf("DecodePrimitive: invalid bool size %d", size)
		}
		return buf[0] != 0, nil

	case typetags.TypeNull:
		return nil, nil

	default:
		return nil, fmt.Errorf("DecodePrimitive: unsupported type %v", typ)
	}
}

// DecodeTupleGeneric: decode a []any from the current position in a SeqGetAccess.
// If root is true, the caller already consumed the tuple header.
// If ordered is true, maps inside the tuple are decoded as *typetags.OrderedMapAny.
func DecodeTupleGeneric(seq *SeqGetAccess, root bool, ordered bool) ([]any, error) {
	nested := seq
	if !root {
		pos := seq.CurrentIndex()
		typ, width, err := seq.PeekTypeWidth()
		if err != nil {
			return nil, fmt.Errorf("DecodeTuple: peek failed at pos %d: %w", pos, err)
		}
		if typ != typetags.TypeTuple {
			return nil, fmt.Errorf("DecodeTuple: type mismatch at pos %d — expected %v, got %v", pos, typetags.TypeTuple, typ)
		}
		if width == 0 {
			// nil/empty tuple
			if err := seq.Advance(); err != nil {
				return nil, fmt.Errorf("DecodeTuple: advance failed at pos %d: %w", pos, err)
			}
			return nil, nil
		}
		nested, err = seq.PeekNestedSeq()
		if err != nil {
			return nil, fmt.Errorf("DecodeTuple: nested peek failed at pos %d: %w", pos, err)
		}
	}

	out := make([]any, 0, nested.ArgCount())
	for i := 0; i < nested.ArgCount(); i++ {
		valTyp, _, err := nested.PeekTypeWidth()
		if err != nil {
			return nil, fmt.Errorf("DecodeTuple: nested value peek error at %d: %w", i, err)
		}
		switch valTyp {
		case typetags.TypeMap:
			var v any
			if ordered {
				v, err = DecodeOrderedMapAny(nested)
			} else {
				v, err = DecodeMapAny(nested)
			}
			if err != nil {
				return nil, fmt.Errorf("DecodeTuple: nested map decode error at %d: %w", i, err)
			}
			out = append(out, v)

		case typetags.TypeTuple:
			v, err := DecodeTuple(nested)
			if err != nil {
				return nil, fmt.Errorf("DecodeTuple: nested tuple decode error at %d: %w", i, err)
			}
			out = append(out, v)

		default:
			valPayload, valTyp, err := nested.Next()
			if err != nil {
				return nil, fmt.Errorf("DecodeTuple: nested value next error at %d: %w", i, err)
			}
			v, err := DecodePrimitive(valTyp, valPayload)
			if err != nil {
				return nil, fmt.Errorf("DecodeTuple: primitive decode error at %d: %w", i, err)
			}
			out = append(out, v)
		}
	}

	if !root {
		if err := seq.Advance(); err != nil {
			return nil, fmt.Errorf("DecodeTuple: advance failed: %w", err)
		}
	}
	return out, nil
}

// Convenience wrappers
func DecodeTuple(seq *SeqGetAccess) ([]any, error) {
	return DecodeTupleGeneric(seq, false, false)
}

func DecodeTupleOrdered(seq *SeqGetAccess) ([]any, error) {
	return DecodeTupleGeneric(seq, false, true)
}

// DecodeMapAny: decode a map[string]any from the current position in a SeqGetAccess.
func DecodeMapAny(seq *SeqGetAccess) (map[string]any, error) {
	pos := seq.CurrentIndex()
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return nil, fmt.Errorf("DecodeMapAny: peek failed at pos %d: %w", pos, err)
	}
	if typ != typetags.TypeMap {
		return nil, fmt.Errorf("DecodeMapAny: type mismatch at pos %d — expected %v, got %v", pos, typetags.TypeMap, typ)
	}
	if width == 0 {
		// nil/empty map
		if err := seq.Advance(); err != nil {
			return nil, fmt.Errorf("DecodeMapAny: advance failed at pos %d: %w", pos, err)
		}
		return nil, nil
	}

	nested, err := seq.PeekNestedSeq()
	if err != nil {
		return nil, fmt.Errorf("DecodeMapAny: nested peek failed at pos %d: %w", pos, err)
	}

	out := make(map[string]any, nested.ArgCount()/2)
	for i := 0; i < nested.ArgCount(); i += 2 {
		// key
		keyPayload, keyTyp, err := nested.Next()
		if err != nil {
			return nil, fmt.Errorf("DecodeMapAny: key decode error at %d: %w", i, err)
		}
		if keyTyp != typetags.TypeString {
			return nil, fmt.Errorf("DecodeMapAny: map key not string at %d, got %v", i, keyTyp)
		}
		key := string(keyPayload)
		valTyp, _, err := nested.PeekTypeWidth()

		if err != nil {
			return nil, fmt.Errorf("DecodeMapAny: nested value decode error at %d: %w", i+1, err)

		}
		switch valTyp {
		case typetags.TypeMap:
			v, err := DecodeMapAny(nested) // delegate
			if err != nil {
				return nil, fmt.Errorf("DecodeMapAny: nested value decode error at %d: %w", i+1, err)
			}
			out[key] = v
		case typetags.TypeTuple:
			v, err := DecodeTuple(nested) // delegate
			if err != nil {
				return nil, fmt.Errorf("DecodeMapAny: nested value decode error at %d: %w", i+1, err)
			}
			out[key] = v
		default:
			valPayload, valTyp, err := nested.Next()
			if err != nil {
				return nil, fmt.Errorf("DecodeMapAny: nested value decode error at %d: %w", i+1, err)
			}
			v, err := DecodePrimitive(valTyp, valPayload)
			if err != nil {
				return nil, fmt.Errorf("DecodeMapAny: nested value decode error at %d: %w", i+1, err)
			}
			out[key] = v
		}

	}

	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("DecodeMapAny: advance failed at pos %d: %w", pos, err)
	}
	return out, nil
}

// DecodeOrderedMapAny decodes a map from the sequence into an OrderedMapAny,
// preserving insertion order of keys.
func DecodeOrderedMapAny(seq *SeqGetAccess) (*typetags.OrderedMapAny, error) {
	pos := seq.CurrentIndex()
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return nil, fmt.Errorf("DecodeOrderedMapAny: peek failed at pos %d: %w", pos, err)
	}
	if typ != typetags.TypeMap {
		return nil, fmt.Errorf("DecodeOrderedMapAny: type mismatch at pos %d — expected %v, got %v", pos, typetags.TypeMap, typ)
	}
	if width == 0 {
		// nil/empty map
		if err := seq.Advance(); err != nil {
			return nil, fmt.Errorf("DecodeOrderedMapAny: advance failed at pos %d: %w", pos, err)
		}
		return nil, nil
	}

	nested, err := seq.PeekNestedSeq()
	if err != nil {
		return nil, fmt.Errorf("DecodeOrderedMapAny: nested peek failed at pos %d: %w", pos, err)
	}

	out := typetags.NewOrderedMapAny()
	for i := 0; i < nested.ArgCount(); i += 2 {
		// key
		keyPayload, keyTyp, err := nested.Next()
		if err != nil {
			return nil, fmt.Errorf("DecodeOrderedMapAny: key decode error at %d: %w", i, err)
		}
		if keyTyp != typetags.TypeString {
			return nil, fmt.Errorf("DecodeOrderedMapAny: map key not string at %d, got %v", i, keyTyp)
		}
		key := string(keyPayload)

		valTyp, _, err := nested.PeekTypeWidth()
		if err != nil {
			return nil, fmt.Errorf("DecodeOrderedMapAny: nested value decode error at %d: %w", i+1, err)
		}

		switch valTyp {
		case typetags.TypeMap:
			v, err := DecodeOrderedMapAny(nested) // delegate recursively
			if err != nil {
				return nil, fmt.Errorf("DecodeOrderedMapAny: nested value decode error at %d: %w", i+1, err)
			}
			out.Set(key, v)

		case typetags.TypeTuple:
			v, err := DecodeTuple(nested)
			if err != nil {
				return nil, fmt.Errorf("DecodeOrderedMapAny: nested value decode error at %d: %w", i+1, err)
			}
			out.Set(key, v)

		default:
			valPayload, valTyp, err := nested.Next()
			if err != nil {
				return nil, fmt.Errorf("DecodeOrderedMapAny: nested value decode error at %d: %w", i+1, err)
			}
			v, err := DecodePrimitive(valTyp, valPayload)
			if err != nil {
				return nil, fmt.Errorf("DecodeOrderedMapAny: nested value decode error at %d: %w", i+1, err)
			}
			out.Set(key, v)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("DecodeOrderedMapAny: advance failed at pos %d: %w", pos, err)
	}
	return out, nil
}

// Decode: convenience entry point for decoding a buffer that contains a top-level tuple.
// Decode decodes a buffer into Go values.
// Maps inside tuples are decoded as plain map[string]any.
func Decode(buf []byte) (any, error) {
	seq, err := NewSeqGetAccess(buf)
	if err != nil {
		return nil, fmt.Errorf("Decode: failed to create sequence: %w", err)
	}

	vals, err := DecodeTupleGeneric(seq, true, false) // ordered=false
	if err != nil {
		return nil, fmt.Errorf("Decode: tuple decode failed: %w", err)
	}
	if len(vals) == 1 {
		return vals[0], nil
	}
	return vals, nil
}

// DecodeOrdered decodes a buffer into Go values.
// Maps inside tuples are decoded as *typetags.OrderedMapAny.
func DecodeOrdered(buf []byte) (any, error) {
	seq, err := NewSeqGetAccess(buf)
	if err != nil {
		return nil, fmt.Errorf("DecodeOrdered: failed to create sequence: %w", err)
	}

	vals, err := DecodeTupleGeneric(seq, true, true) // ordered=true
	if err != nil {
		return nil, fmt.Errorf("DecodeOrdered: tuple decode failed: %w", err)
	}
	if len(vals) == 1 {
		return vals[0], nil
	}
	return vals, nil
}
