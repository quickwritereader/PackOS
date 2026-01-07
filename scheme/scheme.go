package scheme

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"time"

	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/types"
	"github.com/quickwritereader/PackOS/utils"
)

type Scheme interface {
	Validate(seq *access.SeqGetAccess) error
}

type SchemeFunc func(seq *access.SeqGetAccess) error

func (f SchemeFunc) Validate(seq *access.SeqGetAccess) error {
	return f(seq)
}

type SchemeAny struct{}

func (s SchemeAny) Validate(seq *access.SeqGetAccess) error {

	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: failed to skip value: %w", err)
	}
	return nil
}

type SchemeBool struct{}

func (SchemeBool) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeBool, 1, false)
}

type SchemeInt8 struct{}

func (SchemeInt8) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 2, false)
}

type SchemeInt16 struct{}

func (SchemeInt16) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 2, false)
}

type SchemeInt32 struct{}

func (SchemeInt32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 4, false)
}

type SchemeInt64 struct{}

func (SchemeInt64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 8, false)
}

type SchemeFloat32 struct{}

func (SchemeFloat32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 4, false)
}

type SchemeFloat64 struct{}

func (SchemeFloat64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 8, false)
}

type SchemeString struct{ Width int }

func (s SchemeString) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeString, s.Width, s.IsNullable())
}

type SchemeBytes struct{ Width int }

func (s SchemeBytes) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeString, s.Width, s.IsNullable())
}

// NOTE: SchemeMap expects keys in sorted order.
// Validation will fail if map keys are unordered or mismatched.
type SchemeMap struct {
	Width  int
	Schema []Scheme
}

func (s SchemeMap) Validate(seq *access.SeqGetAccess) error {

	pos := seq.CurrentIndex()
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)

	}
	if typ != types.TypeMap {
		return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", pos, types.TypeMap, typ)
	}
	nullable := s.IsNullable()
	hint := s.Width
	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			return fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", pos, hint, width)
		}
	}
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
	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return nil
}

type SchemeTypeOnly struct {
	Tag types.Type
}

func (s SchemeTypeOnly) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, s.Tag, -1, false)
}

type Nullable interface {
	IsNullable() bool
}

// Nullable Primitives

type SchemeNullableBool struct{}

func (SchemeNullableBool) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeBool, 1, true)
}

type SchemeNullableInt8 struct{}

func (SchemeNullableInt8) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 2, true)
}

type SchemeNullableInt16 struct{}

func (SchemeNullableInt16) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 2, true)
}

type SchemeNullableInt32 struct{}

func (SchemeNullableInt32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 4, true)
}

type SchemeNullableInt64 struct{}

func (SchemeNullableInt64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeInteger, 8, true)
}

type SchemeNullableFloat32 struct{}

func (SchemeNullableFloat32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 4, true)
}

type SchemeNullableFloat64 struct{}

func (SchemeNullableFloat64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(seq, types.TypeFloating, 8, true)
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

func ValidateBuffer(buf []byte, args ...Scheme) error {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return fmt.Errorf("ValidateBuffer: failed to initialize accessor: %w", err)
	}

	for _, scheme := range args {
		err = scheme.Validate(seq)
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper for primitive validation
func validatePrimitive(seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) error {

	pos := seq.CurrentIndex()
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
	}
	if typ != tag {
		return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", pos, tag, typ)
	}
	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			return fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", pos, hint, width)
		}
	}
	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return nil
}

type SchemeChain struct {
	Schemes []Scheme
}

func SChain(schemes ...Scheme) SchemeChain {
	return SchemeChain{Schemes: schemes}
}

// Validate applies each Scheme in sequence, short-circuiting on error
func (sc SchemeChain) Validate(seq *access.SeqGetAccess) error {
	for _, s := range sc.Schemes {
		err := s.Validate(seq)
		if err != nil {
			return err
		}
	}
	return nil
}

func SStringExact(expected string) Scheme {
	return SString.Match(expected)
}

func SStringLen(width int) Scheme {
	return SString.WithWidth(width)
}

func (s SchemeString) Match(expected string) Scheme {
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeString {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeString, got %v", pos, typ)
		}
		if string(payload) != expected {
			return fmt.Errorf("ValidateBuffer: string mismatch at pos %d — expected '%s', got '%s'", pos, expected, string(payload))
		}
		return nil
	})
}

func (s SchemeString) Prefix(prefix string) Scheme {
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeString {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeString, got %v", pos, typ)
		}
		if !utils.HasPrefix(payload, prefix) {
			return fmt.Errorf("ValidateBuffer: prefix mismatch at pos %d — expected prefix '%s', got '%s'", pos, prefix, string(payload))
		}
		return nil
	})
}

func (s SchemeString) Suffix(suffix string) Scheme {
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeString {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeString, got %v", pos, typ)
		}
		if !utils.HasSuffix(payload, suffix) {
			return fmt.Errorf("ValidateBuffer: suffix mismatch at pos %d — expected suffix '%s', got '%s'", pos, suffix, string(payload))
		}
		return nil
	})
}

func (s SchemeString) WithWidth(n int) Scheme {
	return SchemeString{Width: n}
}

func (s SchemeString) Pattern(expr string) Scheme {
	re := regexp.MustCompile(expr)
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeString {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeString, got %v", pos, typ)
		}
		if !re.Match(payload) {
			return fmt.Errorf("ValidateBuffer: pattern mismatch at pos %d — expected match for '%s', got '%s'", pos, expr, string(payload))
		}
		return nil
	})
}

func (s SchemeInt16) Range(min, max int16) Scheme {
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeInteger {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeInteger, got %v", pos, typ)
		}
		if len(payload) < 2 {
			return fmt.Errorf("ValidateBuffer: payload too short for int16 at pos %d", pos)
		}
		val := int16(binary.LittleEndian.Uint16(payload))
		if val < min || val > max {
			return fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d", pos, min, max, val)
		}
		return nil
	})
}

func (s SchemeInt32) Range(min, max int32) Scheme {
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeInteger {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeInteger, got %v", pos, typ)
		}
		if len(payload) < 4 {
			return fmt.Errorf("ValidateBuffer: payload too short for int32 at pos %d", pos)
		}
		val := int32(binary.LittleEndian.Uint32(payload))
		if val < min || val > max {
			return fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d", pos, min, max, val)
		}
		return nil
	})
}

func (s SchemeInt64) Range(min, max int64) Scheme {
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeInteger {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeInteger, got %v", pos, typ)
		}
		if len(payload) < 8 {
			return fmt.Errorf("ValidateBuffer: payload too short for int64 at pos %d", pos)
		}
		val := int64(binary.LittleEndian.Uint64(payload))
		if val < min || val > max {
			return fmt.Errorf("ValidateBuffer: value out of range at pos %d — expected %d ≤ x ≤ %d, got %d", pos, min, max, val)
		}
		return nil
	})
}

func (s SchemeInt64) DateRange(from, to time.Time) Scheme {
	min := from.Unix()
	max := to.Unix()
	return SchemeFunc(func(seq *access.SeqGetAccess) error {
		pos := seq.CurrentIndex()
		payload, typ, err := seq.Next()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
		}
		if typ != types.TypeInteger {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeInteger, got %v", pos, typ)
		}
		if len(payload) < 8 {
			return fmt.Errorf("ValidateBuffer: payload too short for int64 at pos %d", pos)
		}
		val := int64(binary.LittleEndian.Uint64(payload))
		if val < min || val > max {
			return fmt.Errorf("ValidateBuffer: timestamp out of range at pos %d — expected %d ≤ x ≤ %d, got %d", pos, min, max, val)
		}
		return nil
	})
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

	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}
	return nil
}

type TupleScheme struct {
	Schema   []Scheme
	Width    int
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
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
	}

	if typ != types.TypeTuple {
		return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", pos, types.TypeTuple, typ)
	}

	nullable := s.IsNullable()
	hint := s.Width
	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			return fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", pos, hint, width)
		}
	}

	sub, err := seq.PeekNestedSeq()
	if err != nil {
		return fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
	}

	subState := sub
	for _, sch := range s.Schema {
		err = sch.Validate(subState)
		if err != nil {
			return fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, err)
		}
	}

	if err := seq.Advance(); err != nil {
		return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
	}

	return nil
}
