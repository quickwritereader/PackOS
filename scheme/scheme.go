package scheme

import (
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/types"
)

type Scheme interface {
	Validate(seq *access.SeqGetAccess) error
	Decode(seq *access.SeqGetAccess) (any, error)
}

type SchemeGeneric struct {
	ValidateFunc func(seq *access.SeqGetAccess) error
	DecodeFunc   func(seq *access.SeqGetAccess) (any, error)
}

func (f SchemeGeneric) Validate(seq *access.SeqGetAccess) error {
	return f.ValidateFunc(seq)
}
func (f SchemeGeneric) Decode(seq *access.SeqGetAccess) (any, error) {
	return f.DecodeFunc(seq)
}

type SchemeAny struct{}

func (s SchemeAny) Validate(seq *access.SeqGetAccess) error {

	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: failed to skip value: %w", err)
	}
	return nil
}

func (s SchemeAny) Decode(seq *access.SeqGetAccess) (any, error) {
	v, err := access.DecodeTupleGeneric(seq, false)
	if err != nil {
		return nil, fmt.Errorf("ValidateBuffer: decode any error: %w", err)
	}
	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("ValidateBuffer: failed to skip value: %w", err)
	}
	return v, nil
}

type SchemeBool struct{}

func (SchemeBool) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeBool, 1, false)
}

func (SchemeBool) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeBool, 1, false)
	if err != nil {
		return nil, err
	}
	return payload[0] != 0, nil
}

type SchemeInt8 struct{}

func (SchemeInt8) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 1, false)
}

func (SchemeInt8) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 1, false)
	if err != nil {
		return nil, err
	}
	return int8(payload[0]), nil
}

type SchemeInt16 struct{}

func (SchemeInt16) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 2, false)
}

func (SchemeInt16) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 2, false)
	if err != nil {
		return nil, err
	}
	return int16(binary.LittleEndian.Uint16(payload)), nil
}

type SchemeInt32 struct{}

func (SchemeInt32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 4, false)
}

func (SchemeInt32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 4, false)
	if err != nil {
		return nil, err
	}
	return int32(binary.LittleEndian.Uint32(payload)), nil
}

type SchemeInt64 struct{}

func (SchemeInt64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 8, false)
}

func (SchemeInt64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 8, false)
	if err != nil {
		return nil, err
	}
	return int64(binary.LittleEndian.Uint64(payload)), nil
}

type SchemeFloat32 struct{}

func (SchemeFloat32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 4, false)
}

func (SchemeFloat32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeFloating, 4, false)
	if err != nil {
		return nil, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(payload)), nil
}

type SchemeFloat64 struct{}

func (SchemeFloat64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 8, false)
}

func (SchemeFloat64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeFloating, 8, false)
	if err != nil {
		return nil, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(payload)), nil
}

type SchemeString struct{ Width int }

func (s SchemeString) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeString, s.Width, s.IsNullable())
}

func (s SchemeString) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeString, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}
	return string(payload), nil
}

type SchemeBytes struct{ Width int }

func (s SchemeBytes) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeString, s.Width, s.IsNullable())
}

func (s SchemeBytes) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeByteArray, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NOTE: SchemeMap expects keys in sorted order.
// Validation will fail if map keys are unordered or mismatched.
type SchemeMap struct {
	Width  int
	Schema []Scheme
}

func (s SchemeMap) Validate(seq *access.SeqGetAccess) error {

	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, types.TypeMap, s.Width, s.IsNullable())
	if err != nil {
		return err
	}

	if s.Width != 0 {

		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		subState := sub
		for _, sch := range s.Schema {
			subStateErr := sch.Validate(subState)
			if subStateErr != nil {
				return fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, subStateErr)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return nil
}

func (s SchemeMap) Decode(seq *access.SeqGetAccess) (any, error) {

	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, types.TypeMap, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}

	if len(s.Schema)%2 != 0 {
		return nil, fmt.Errorf("SchemeMap should contain key and value scheme pairs, current count %d", len(s.Schema))
	}

	var out map[string]any = nil

	if s.Width != 0 {
		sub, err := seq.PeekNestedSeq()

		if err != nil {
			return nil, fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}

		out = make(map[string]any, sub.ArgCount()/2)

		for i := 0; i < len(s.Schema); i += 2 {
			key, err := s.Schema[i].Decode(sub)
			if err != nil {
				return nil, fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
			}
			value, err := s.Schema[i+1].Decode(sub)
			if err != nil {
				return nil, fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
			}
			keyStr, ok := key.(string)
			if ok {
				out[keyStr] = value
			}

		}
	}
	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return out, nil
}

type SchemeTypeOnly struct {
	Tag types.Type
}

func (s SchemeTypeOnly) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, s.Tag, -1, false)
}

func (s SchemeTypeOnly) Decode(seq *access.SeqGetAccess) (any, error) {
	switch s.Tag {
	case types.TypeMap:
		return access.DecodeMapAny(seq)
	case types.TypeTuple:
		return access.DecodeTuple(seq)
	default:
		pos := seq.CurrentIndex()
		valPayload, valTyp, err := seq.Next()
		if err != nil {
			return nil, fmt.Errorf("Decode: value decode error at %d: %w", pos, err)
		}
		v, err := access.DecodePrimitive(valTyp, valPayload)

		if err != nil {
			return nil, fmt.Errorf("Decode: value decode error at %d: %w", pos, err)
		}
		return v, nil

	}
}

type Nullable interface {
	IsNullable() bool
}

// Nullable Primitives

type SchemeNullableBool struct{}

func (SchemeNullableBool) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeBool, 1, true)
}

func (SchemeNullableBool) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeBool, 1, true)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		return payload[0] != 0, nil
	}
	return nil, nil
}

type SchemeNullableInt8 struct{}

func (SchemeNullableInt8) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 1, true)
}

func (SchemeNullableInt8) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 1, true)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int8(payload[0]), nil
}

type SchemeNullableInt16 struct{}

func (SchemeNullableInt16) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 2, true)
}

func (SchemeNullableInt16) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 2, true)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int16(binary.LittleEndian.Uint16(payload)), nil
}

type SchemeNullableInt32 struct{}

func (SchemeNullableInt32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 4, true)
}

func (SchemeNullableInt32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 4, true)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int32(binary.LittleEndian.Uint32(payload)), nil
}

type SchemeNullableInt64 struct{}

func (SchemeNullableInt64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 8, true)
}

func (SchemeNullableInt64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 8, true)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int64(binary.LittleEndian.Uint64(payload)), nil
}

type SchemeNullableFloat32 struct{}

func (SchemeNullableFloat32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 4, true)
}

func (SchemeNullableFloat32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeFloating, 4, true)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(payload)), nil
}

type SchemeNullableFloat64 struct{}

func (SchemeNullableFloat64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 8, true)
}

func (SchemeNullableFloat64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(seq, types.TypeFloating, 8, true)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(payload)), nil
}

// All others default to non-nullable
func (SchemeBool) IsNullable() bool       { return false }
func (SchemeInt16) IsNullable() bool      { return false }
func (SchemeInt32) IsNullable() bool      { return false }
func (SchemeInt64) IsNullable() bool      { return false }
func (SchemeFloat32) IsNullable() bool    { return false }
func (SchemeFloat64) IsNullable() bool    { return false }
func (s SchemeString) IsNullable() bool   { return s.Width <= 0 }
func (s SchemeBytes) IsNullable() bool    { return s.Width <= 0 }
func (s SchemeMap) IsNullable() bool      { return s.Width <= 0 }
func (s SchemeTypeOnly) IsNullable() bool { return false }

func (SchemeNullableBool) IsNullable() bool    { return true }
func (SchemeNullableInt16) IsNullable() bool   { return true }
func (SchemeNullableInt32) IsNullable() bool   { return true }
func (SchemeNullableInt64) IsNullable() bool   { return true }
func (SchemeNullableFloat32) IsNullable() bool { return true }
func (SchemeNullableFloat64) IsNullable() bool { return true }

func SType(tag types.Type) Scheme {
	return SchemeTypeOnly{Tag: tag}
}

var (
	SBool        Scheme       = SchemeBool{}
	SInt8        Scheme       = SchemeInt8{}
	SInt16       SchemeInt16  = SchemeInt16{}
	SInt32       SchemeInt32  = SchemeInt32{}
	SInt64       SchemeInt64  = SchemeInt64{}
	SFloat32     Scheme       = SchemeFloat32{}
	SFloat64     Scheme       = SchemeFloat64{}
	SNullBool    Scheme       = SchemeNullableBool{}
	SNullInt8    Scheme       = SchemeNullableInt8{}
	SNullInt16   Scheme       = SchemeNullableInt16{}
	SNullInt32   Scheme       = SchemeNullableInt32{}
	SNullInt64   Scheme       = SchemeNullableInt64{}
	SNullFloat32 Scheme       = SchemeNullableFloat32{}
	SNullFloat64 Scheme       = SchemeNullableFloat64{}
	SString      SchemeString = SchemeString{Width: -1}
	SAny                      = SchemeAny{}
)

func SBytes(width int) Scheme { return SchemeBytes{Width: width} }

func SMap(nested ...Scheme) Scheme {
	return SchemeMap{
		Width:  -1,
		Schema: nested,
	}
}

func SVariableString() Scheme {
	return SchemeString{Width: -1}
}

func SVariableBytes() Scheme {
	return SchemeBytes{Width: -1}
}

func SVariableMap(nested ...Scheme) Scheme {
	return SchemeMap{
		Width:  -1,
		Schema: nested,
	}
}

func ValidateBuffer(buf []byte, chain SchemeChain) error {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return fmt.Errorf("ValidateBuffer: failed to initialize accessor: %w", err)
	}

	for _, scheme := range chain.Schemes {
		err = scheme.Validate(seq)
		if err != nil {
			return err
		}
	}
	return nil
}

func DecodeBuffer(buf []byte, chain SchemeChain) (any, error) {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return nil, fmt.Errorf("ValidateBuffer: failed to initialize accessor: %w", err)
	}
	out := make([]any, 0, len(chain.Schemes))
	for _, scheme := range chain.Schemes {
		val, err := scheme.Decode(seq)
		if err != nil {
			return nil, err
		}
		out = append(out, val)
	}
	return out, nil
}

type SchemeNamedChain struct {
	SchemeChain
	FieldNames []string
}

func DecodeBufferNamed(buf []byte, chain SchemeNamedChain) (any, error) {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return nil, fmt.Errorf("ValidateBuffer: failed to initialize accessor: %w", err)
	}
	if len(chain.FieldNames) != len(chain.Schemes) {
		return nil, fmt.Errorf("Scheme FieldNames count and Schemes count mismatch %d!=%d", len(chain.Schemes), len(chain.FieldNames))
	}
	out := make(map[string]any, len(chain.Schemes))
	i := 0
	for _, scheme := range chain.Schemes {
		val, err := scheme.Decode(seq)
		if err != nil {
			return nil, err
		}
		out[chain.FieldNames[i]] = val
		i++
	}
	return out, nil
}

func precheck(pos int, seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) (int, error) {

	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return 0, fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
	}
	if typ != tag {
		return 0, fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", pos, tag, typ)
	}
	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			return 0, fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", pos, hint, width)
		}
	}
	return width, err
}

// Helper for primitive validation
func validatePrimitive(seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) error {
	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, tag, hint, nullable)
	if err != nil {
		return err
	}
	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return nil
}

func validatePrimitiveAndGetPayload(seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) ([]byte, error) {

	pos := seq.CurrentIndex()
	width, err := precheck(pos, seq, tag, hint, nullable)
	if err != nil {
		return nil, err
	}
	var payload []byte = nil
	if width > 0 {
		payload, err = seq.GetPayload(width)
		if err != nil {
			return nil, fmt.Errorf("ValidateBuffer: getting payload failed at pos %d, %w", pos, err)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return payload, nil
}

type SchemeChain struct {
	Schemes []Scheme
}

func SChain(schemes ...Scheme) SchemeChain {
	return SchemeChain{Schemes: schemes}
}

func SStringExact(expected string) Scheme {
	return SString.Match(expected)
}

func SStringLen(width int) Scheme {
	return SString.WithWidth(width)
}

func (s SchemeString) CheckFunc(msgError string, test func(payloadStr string) bool) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return err
			}
			if !test(string(payload)) {
				return fmt.Errorf("ValidateBuffer: string mismatch at pos %d — %s, got '%s'", pos, msgError, string(payload))
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return nil, err
			}
			if !test(string(payload)) {
				return nil, fmt.Errorf("ValidateBuffer: string mismatch at pos %d — %s, got '%s'", pos, msgError, string(payload))
			}
			return string(payload), nil
		},
	}
}

func (s SchemeString) Match(expected string) Scheme {
	return s.CheckFunc(fmt.Sprintf("expected %s", expected), func(payloadStr string) bool {
		return payloadStr == expected
	})
}

func (s SchemeString) Prefix(prefix string) Scheme {
	return s.CheckFunc(fmt.Sprintf("expected prefix %s", prefix), func(payloadStr string) bool {
		return strings.HasPrefix(payloadStr, prefix)
	})
}

func (s SchemeString) Suffix(suffix string) Scheme {
	return s.CheckFunc(fmt.Sprintf("expected suffix %s", suffix), func(payloadStr string) bool {
		return strings.HasSuffix(payloadStr, suffix)
	})
}

func (s SchemeString) WithWidth(n int) Scheme {
	return SchemeString{Width: n}
}

func (s SchemeString) Pattern(expr string) Scheme {
	re := regexp.MustCompile(expr)
	return s.CheckFunc(fmt.Sprintf("expected  match for %s", expr), func(payloadStr string) bool {
		return re.Match([]byte(payloadStr))
	})
}

func (s SchemeInt16) Range(min, max int16) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 2, false)
			if err != nil {
				return err
			}
			val := int16(binary.LittleEndian.Uint16(payload))
			if val < min || val > max {
				return fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d", pos, min, max, val)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 2, false)
			if err != nil {
				return nil, err
			}
			val := int16(binary.LittleEndian.Uint16(payload))
			if val < min || val > max {
				return nil, fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d", pos, min, max, val)
			}
			return val, nil
		},
	}
}

func (s SchemeInt32) Range(min, max int32) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 4, false)
			if err != nil {
				return err
			}
			val := int32(binary.LittleEndian.Uint32(payload))
			if val < min || val > max {
				return fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d",
					pos, min, max, val)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 4, false)
			if err != nil {
				return nil, err
			}
			val := int32(binary.LittleEndian.Uint32(payload))
			if val < min || val > max {
				return nil, fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d",
					pos, min, max, val)
			}
			return val, nil
		},
	}
}

func (s SchemeInt64) Range(min, max int64) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 8, false)
			if err != nil {
				return err
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				return fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d",
					pos, min, max, val)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 8, false)
			if err != nil {
				return nil, err
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				return nil, fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d",
					pos, min, max, val)
			}
			return val, nil
		},
	}
}

func (s SchemeInt64) DateRange(from, to time.Time) Scheme {
	min := from.Unix()
	max := to.Unix()

	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 8, false)
			if err != nil {
				return err
			}
			if payload == nil {
				// Nullable case: allow nil
				return nil
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				pos := seq.CurrentIndex()
				return fmt.Errorf("ValidateBuffer: timestamp out of range at pos %d — expected %d ≤ x ≤ %d, got %d",
					pos, min, max, val)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			payload, err := validatePrimitiveAndGetPayload(seq, types.TypeInteger, 8, false)
			if err != nil {
				return nil, err
			}
			if payload == nil {
				// Nullable case: return nil
				return nil, nil
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				pos := seq.CurrentIndex()
				return nil, fmt.Errorf("ValidateBuffer: timestamp out of range at pos %d — expected %d ≤ x ≤ %d, got %d",
					pos, min, max, val)
			}
			return val, nil
		},
	}
}

type SchemeMapUnordered struct {
	Fields map[string]Scheme
}

func SMapUnordered(mappedSchemes map[string]Scheme) Scheme {
	return SchemeMapUnordered{Fields: mappedSchemes}
}

func (s SchemeMapUnordered) Validate(seq *access.SeqGetAccess) error {

	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
	}
	if typ != types.TypeMap {
		return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeMap, got %v", pos, typ)
	}

	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		seen := make(map[string]bool)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == types.TypeEnd {
				break
			}
			if err != nil {
				if keyType == types.TypeEnd {
					break
				}
				return fmt.Errorf("ValidateBuffer: failed to read key at pos %d: %w", pos, err)
			}
			if keyType != types.TypeString {
				return fmt.Errorf("ValidateBuffer: expected string key at pos %d, got %v", pos, keyType)
			}
			key := string(keyPayload)
			seen[key] = true

			if validator, ok := s.Fields[key]; ok {
				err = validator.Validate(subseq)
				if err != nil {
					return fmt.Errorf("ValidateBuffer: value validation failed for key '%s': %w", key, err)
				}
			} else {
				if err := subseq.Advance(); err != nil {
					return fmt.Errorf("ValidateBuffer: failed to skip value for unknown key '%s': %w", key, err)
				}
			}
		}

		for key := range s.Fields {
			if !seen[key] {
				return fmt.Errorf("ValidateBuffer: missing expected key '%s' at pos %d", key, pos)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return nil
}

func (s SchemeMapUnordered) Decode(seq *access.SeqGetAccess) (any, error) {

	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return nil, fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
	}
	if typ != types.TypeMap {
		return nil, fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeMap, got %v", pos, typ)
	}
	var out map[string]any = nil
	if len(s.Fields) > 0 {

		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		out = make(map[string]any, subseq.ArgCount()/2)
		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == types.TypeEnd {
				break
			}
			if err != nil {
				if keyType == types.TypeEnd {
					break
				}
				return nil, fmt.Errorf("ValidateBuffer: failed to read key at pos %d: %w", pos, err)
			}
			if keyType != types.TypeString {
				return nil, fmt.Errorf("ValidateBuffer: expected string key at pos %d, got %v", pos, keyType)
			}
			key := string(keyPayload)
			if validator, ok := s.Fields[key]; ok {
				val, err := validator.Decode(subseq)
				if err != nil {
					return nil, fmt.Errorf("ValidateBuffer: value validation failed for key '%s': %w", key, err)
				}
				out[key] = val
			} else {
				if err := subseq.Advance(); err != nil {
					return nil, fmt.Errorf("ValidateBuffer: failed to skip value for unknown key '%s': %w", key, err)
				}
			}
		}

		for key := range s.Fields {
			if _, ok := out["a"]; !ok {
				return nil, fmt.Errorf("ValidateBuffer: missing expected key '%s' at pos %d", key, pos)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return out, nil
}

type TupleScheme struct {
	Schema   []Scheme
	Nullable bool
}

func STuple(schema ...Scheme) TupleScheme {
	return TupleScheme{Schema: schema, Nullable: true}
}

func (s TupleScheme) IsNullable() bool {
	return s.Nullable
}

func (s TupleScheme) Validate(seq *access.SeqGetAccess) error {

	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return err
	}
	w := len(s.Schema)
	if w != 0 {

		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return fmt.Errorf("ValidateBuffer: container item count mistmatch at pos %d: %d!=%d", pos, w, sub.ArgCount())
		}

		subState := sub
		for _, sch := range s.Schema {
			err = sch.Validate(subState)
			if err != nil {
				return fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}

	return nil
}

func (s TupleScheme) Decode(seq *access.SeqGetAccess) (any, error) {

	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return nil, err
	}

	var out []any = nil
	w := len(s.Schema)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return nil, fmt.Errorf("ValidateBuffer: container item count mistmatch at pos %d: %d!=%d", pos, w, sub.ArgCount())
		}

		out = make([]any, 0, sub.ArgCount())
		for _, sch := range s.Schema {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
			}
			out = append(out, v)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}

	return out, nil
}

type TupleSchemeNamed struct {
	Schema     []Scheme
	FieldNames []string
	Nullable   bool
}

func STupleNamed(fieldNames []string, schema ...Scheme) TupleSchemeNamed {
	return TupleSchemeNamed{FieldNames: fieldNames, Schema: schema, Nullable: true}
}

func (s TupleSchemeNamed) IsNullable() bool {
	return s.Nullable
}

func (s TupleSchemeNamed) Validate(seq *access.SeqGetAccess) error {

	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return err
	}
	w := len(s.Schema)
	if w != 0 {

		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return fmt.Errorf("ValidateBuffer: container item count mistmatch at pos %d: %d!=%d", pos, w, sub.ArgCount())
		}

		subState := sub
		for _, sch := range s.Schema {
			err = sch.Validate(subState)
			if err != nil {
				return fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}

	return nil
}

func (s TupleSchemeNamed) Decode(seq *access.SeqGetAccess) (any, error) {

	pos := seq.CurrentIndex()
	_, err := precheck(pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return nil, err
	}

	var out map[string]any = nil
	w := len(s.Schema)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return nil, fmt.Errorf("ValidateBuffer: container item count mistmatch at pos %d: %d!=%d", pos, w, sub.ArgCount())
		}

		out = make(map[string]any, sub.ArgCount())
		i := 0
		for _, sch := range s.Schema {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
			}
			out[s.FieldNames[i]] = v
			i++
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}

	return out, nil
}
