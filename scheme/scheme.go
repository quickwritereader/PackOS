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

type ErrorCode int

const (
	ErrUnknown            ErrorCode = iota
	ErrInvalidFormat                // decoding failed due to invalid format
	ErrUnexpectedEOF                // sequence ended unexpectedly while advancing or reading
	ErrConstraintViolated           // validation rule failed (width, type mismatch, nullable constraint)

	// String‑specific validation codes
	ErrStringMismatch // generic string mismatch
	ErrStringPrefix   // prefix check failed
	ErrStringSuffix   // suffix check failed
	ErrStringPattern  // regex/pattern check failed
	ErrStringMatch    // exact match failed

	// Numeric validation codes
	ErrOutOfRange     // integer value out of allowed range
	ErrDateOutOfRange // timestamp/date value out of allowed range
)

type DecodeError struct {
	Code     ErrorCode
	Name     string
	Position int
	InnerErr error
}

type ValidationError struct {
	Code     ErrorCode
	Name     string
	Position int
	InnerErr error
}

// RangeErrorDetails represents a structured range violation.
type RangeErrorDetails struct {
	Min    int64
	Max    int64
	Actual int64
}

func (r RangeErrorDetails) Error() string {
	return fmt.Sprintf("out of range: expected %d ≤ x ≤ %d, got %d", r.Min, r.Max, r.Actual)
}

type StringErrorDetails struct {
	Actual string
}

func (e StringErrorDetails) Error() string {
	return fmt.Sprintf("got '%s'", e.Actual)
}

func formatError(code ErrorCode, field string, pos int, inner error) string {
	if inner != nil {
		return fmt.Sprintf("%d: error in %s position %d, innerError %s", code, field, pos, inner)
	}
	return fmt.Sprintf("%d: error in %s position %d", code, field, pos)
}

func (v *ValidationError) Error() string {
	return formatError(v.Code, v.Name, v.Position, v.InnerErr)
}

func (d *DecodeError) Error() string {
	return formatError(d.Code, d.Name, d.Position, d.InnerErr)
}

func (v *ValidationError) Unwrap() error {
	return v.InnerErr
}

func (d *DecodeError) Unwrap() error {
	return d.InnerErr
}

func NewDecodeError(code ErrorCode, field string, pos int, inner error) *DecodeError {
	return &DecodeError{Code: code, Name: field, Position: pos, InnerErr: inner}
}
func NewValidationError(code ErrorCode, field string, pos int, inner error) *ValidationError {
	return &ValidationError{Code: code, Name: field, Position: pos, InnerErr: inner}
}

type Scheme interface {
	Validate(seq *access.SeqGetAccess) error
	Decode(seq *access.SeqGetAccess) (any, error)
}

const (
	SchemeAnyName          = "SchemeAny"
	SchemeStringName       = "SchemeString"
	SchemeBytesName        = "SchemeBytes"
	SchemeMapName          = "SchemeMap"
	SchemeTypeOnlyName     = "SchemeTypeOnly"
	SchemeBoolName         = "SchemeBool"
	SchemeInt8Name         = "SchemeInt8"
	SchemeInt16Name        = "SchemeInt16"
	SchemeInt32Name        = "SchemeInt32"
	SchemeInt64Name        = "SchemeInt64"
	SchemeFloat32Name      = "SchemeFloat32"
	SchemeFloat64Name      = "SchemeFloat64"
	SchemeNamedChainName   = "SchemeNamedChain"
	SchemeMapUnorderedName = "SchemeMapUnordered"
	ChainName              = "SchemeChain"

	TupleSchemeName      = "TupleScheme"
	TupleSchemeNamedName = "TupleSchemeNamed"
	SRepeatSchemeName    = "SRepeatScheme"
)

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
		return NewValidationError(ErrUnexpectedEOF, SchemeAnyName, seq.CurrentIndex(), err)
	}
	return nil
}

func (s SchemeAny) Decode(seq *access.SeqGetAccess) (any, error) {
	v, err := access.DecodeTupleGeneric(seq, false)
	if err != nil {
		return nil, NewDecodeError(ErrInvalidFormat, SchemeAnyName, seq.CurrentIndex(), err)
	}
	if err := seq.Advance(); err != nil {
		return nil, NewDecodeError(ErrUnexpectedEOF, SchemeAnyName, seq.CurrentIndex(), err)
	}
	return v, nil
}

type SchemeString struct{ Width int }

func (s SchemeString) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
}

func (s SchemeString) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}
	return string(payload), nil
}

type SchemeBytes struct{ Width int }

func (s SchemeBytes) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeBytesName, seq, types.TypeString, s.Width, s.IsNullable())
}

func (s SchemeBytes) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeBytesName, seq, types.TypeByteArray, s.Width, s.IsNullable())
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
	_, err := precheck(SchemeMapName, pos, seq, types.TypeMap, s.Width, s.IsNullable())
	if err != nil {
		return err
	}

	if s.Width != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return NewValidationError(ErrInvalidFormat, SchemeMapName, pos, err)
		}
		for _, sch := range s.Schema {
			if err := sch.Validate(sub); err != nil {
				return NewValidationError(ErrInvalidFormat, SchemeMapName, pos, err)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return NewValidationError(ErrUnexpectedEOF, SchemeMapName, pos, err)
	}
	return nil
}

func (s SchemeMap) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	_, err := precheck(SchemeMapName, pos, seq, types.TypeMap, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}

	if len(s.Schema)%2 != 0 {
		return nil, NewDecodeError(ErrConstraintViolated, SchemeMapName, pos,
			fmt.Errorf("should contain key/value scheme pairs, count %d", len(s.Schema)))
	}

	var out map[string]any
	if s.Width != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewDecodeError(ErrInvalidFormat, SchemeMapName, pos, err)
		}

		out = make(map[string]any, sub.ArgCount()/2)
		for i := 0; i < len(s.Schema); i += 2 {
			key, err := s.Schema[i].Decode(sub)
			if err != nil {
				return nil, NewDecodeError(ErrInvalidFormat, SchemeMapName, pos, err)
			}
			value, err := s.Schema[i+1].Decode(sub)
			if err != nil {
				return nil, NewDecodeError(ErrInvalidFormat, SchemeMapName, pos, err)
			}
			if keyStr, ok := key.(string); ok {
				out[keyStr] = value
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewDecodeError(ErrUnexpectedEOF, SchemeMapName, pos, err)
	}
	return out, nil
}

type SchemeTypeOnly struct {
	Tag types.Type
}

func (s SchemeTypeOnly) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeTypeOnlyName, seq, s.Tag, -1, false)
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
			return nil, NewDecodeError(ErrInvalidFormat, SchemeTypeOnlyName, pos, err)
		}
		v, err := access.DecodePrimitive(valTyp, valPayload)
		if err != nil {
			return nil, NewDecodeError(ErrInvalidFormat, SchemeTypeOnlyName, pos, err)
		}
		return v, nil
	}
}

type Nullable interface {
	IsNullable() bool
}

func (s SchemeString) IsNullable() bool { return s.Width <= 0 }
func (s SchemeBytes) IsNullable() bool  { return s.Width <= 0 }
func (s SchemeMap) IsNullable() bool    { return s.Width <= 0 }

// Primitives
type SchemeBool struct{ Nullable bool }

func (s SchemeBool) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeBoolName, seq, types.TypeBool, 1, s.Nullable)
}

func (s SchemeBool) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeBoolName, seq, types.TypeBool, 1, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return payload[0] != 0, nil
}

func (s SchemeBool) IsNullable() bool { return s.Nullable }

type SchemeInt8 struct{ Nullable bool }

func (s SchemeInt8) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeInt8Name, seq, types.TypeInteger, 1, s.Nullable)
}
func (s SchemeInt8) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeInt8Name, seq, types.TypeInteger, 1, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int8(payload[0]), nil
}
func (s SchemeInt8) IsNullable() bool { return s.Nullable }

type SchemeInt16 struct{ Nullable bool }

func (s SchemeInt16) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeInt16Name, seq, types.TypeInteger, 2, s.Nullable)
}
func (s SchemeInt16) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeInt16Name, seq, types.TypeInteger, 2, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int16(binary.LittleEndian.Uint16(payload)), nil
}
func (s SchemeInt16) IsNullable() bool { return s.Nullable }

type SchemeInt32 struct{ Nullable bool }

func (s SchemeInt32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeInt32Name, seq, types.TypeInteger, 4, s.Nullable)
}
func (s SchemeInt32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeInt32Name, seq, types.TypeInteger, 4, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int32(binary.LittleEndian.Uint32(payload)), nil
}
func (s SchemeInt32) IsNullable() bool { return s.Nullable }

type SchemeInt64 struct{ Nullable bool }

func (s SchemeInt64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeInt64Name, seq, types.TypeInteger, 8, s.Nullable)
}
func (s SchemeInt64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeInt64Name, seq, types.TypeInteger, 8, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int64(binary.LittleEndian.Uint64(payload)), nil
}
func (s SchemeInt64) IsNullable() bool { return s.Nullable }

type SchemeFloat32 struct{ Nullable bool }

func (s SchemeFloat32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeFloat32Name, seq, types.TypeFloating, 4, s.Nullable)
}
func (s SchemeFloat32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeFloat32Name, seq, types.TypeFloating, 4, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(payload)), nil
}
func (s SchemeFloat32) IsNullable() bool { return s.Nullable }

type SchemeFloat64 struct{ Nullable bool }

func (s SchemeFloat64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeFloat64Name, seq, types.TypeFloating, 8, s.Nullable)
}
func (s SchemeFloat64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeFloat64Name, seq, types.TypeFloating, 8, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(payload)), nil
}
func (s SchemeFloat64) IsNullable() bool { return s.Nullable }

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
	SNullBool    Scheme       = SchemeBool{Nullable: true}
	SNullInt8    Scheme       = SchemeInt8{Nullable: true}
	SNullInt16   Scheme       = SchemeInt16{Nullable: true}
	SNullInt32   Scheme       = SchemeInt32{Nullable: true}
	SNullInt64   Scheme       = SchemeInt64{Nullable: true}
	SNullFloat32 Scheme       = SchemeFloat32{Nullable: true}
	SNullFloat64 Scheme       = SchemeFloat64{Nullable: true}
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
		return NewValidationError(ErrInvalidFormat, ChainName, 0, err)
	}
	for _, scheme := range chain.Schemes {
		if err := scheme.Validate(seq); err != nil {
			return err
		}
	}
	return nil
}

func DecodeBuffer(buf []byte, chain SchemeChain) (any, error) {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return nil, NewDecodeError(ErrInvalidFormat, ChainName, 0, err)
	}
	out := make([]any, 0, len(chain.Schemes))
	for _, scheme := range chain.Schemes {
		val, err := scheme.Decode(seq)
		if err != nil {
			return nil, err
		}
		out = append(out, val)
	}
	if len(out) == 1 {
		return out[0], nil
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
		return nil, NewDecodeError(ErrInvalidFormat, SchemeNamedChainName, 0, err)
	}
	if len(chain.FieldNames) != len(chain.Schemes) {
		return nil, NewDecodeError(ErrConstraintViolated, SchemeNamedChainName, 0,
			fmt.Errorf("FieldNames count %d != Schemes count %d", len(chain.FieldNames), len(chain.Schemes)))
	}
	out := make(map[string]any, len(chain.Schemes))
	for i, scheme := range chain.Schemes {
		val, err := scheme.Decode(seq)
		if err != nil {
			return nil, err
		}
		out[chain.FieldNames[i]] = val
	}
	return out, nil
}

func precheck(errorName string, pos int, seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) (int, error) {
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return 0, NewValidationError(ErrConstraintViolated, errorName, pos, err)
	}

	if typ != tag {
		// Type mismatch
		return 0, NewValidationError(ErrConstraintViolated, errorName, pos,
			fmt.Errorf("type mismatch — expected %v, got %v", tag, typ),
		)
	}

	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			// Width mismatch
			return 0, NewValidationError(ErrConstraintViolated, errorName, pos,
				fmt.Errorf("width mismatch — expected %d, got %d", hint, width),
			)
		}
	}

	return width, nil
}

// Helper for primitive validation
func validatePrimitive(errorName string, seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) error {
	pos := seq.CurrentIndex()

	_, err := precheck(errorName, pos, seq, tag, hint, nullable)
	if err != nil {
		return err
	}

	if err := seq.Advance(); err != nil {
		return NewValidationError(ErrUnexpectedEOF, errorName, pos, err)
	}

	return nil
}

func validatePrimitiveAndGetPayload(errorName string, seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) ([]byte, error) {
	pos := seq.CurrentIndex()

	width, err := precheck(errorName, pos, seq, tag, hint, nullable)
	if err != nil {
		return nil, err
	}

	var payload []byte
	if width > 0 {
		payload, err = seq.GetPayload(width)
		if err != nil {
			return nil, NewDecodeError(ErrInvalidFormat, errorName, pos, err)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewDecodeError(ErrUnexpectedEOF, errorName, pos, err)
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

func (s SchemeString) CheckFunc(code ErrorCode, test func(payloadStr string) bool) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return err
			}
			str := string(payload)
			if !test(str) {
				return NewValidationError(code, SchemeStringName, pos, StringErrorDetails{Actual: str})
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return nil, err
			}
			str := string(payload)
			if !test(str) {
				return nil, NewValidationError(code, SchemeStringName, pos, StringErrorDetails{Actual: str})
			}
			return str, nil
		},
	}
}

func (s SchemeString) Match(expected string) Scheme {
	return s.CheckFunc(
		ErrStringMatch,
		func(payloadStr string) bool { return payloadStr == expected },
	)
}

func (s SchemeString) Prefix(prefix string) Scheme {
	return s.CheckFunc(
		ErrStringPrefix,
		func(payloadStr string) bool { return strings.HasPrefix(payloadStr, prefix) },
	)
}

func (s SchemeString) Suffix(suffix string) Scheme {
	return s.CheckFunc(
		ErrStringSuffix,
		func(payloadStr string) bool { return strings.HasSuffix(payloadStr, suffix) },
	)
}

func (s SchemeString) Pattern(expr string) Scheme {
	re := regexp.MustCompile(expr)
	return s.CheckFunc(
		ErrStringPattern,
		func(payloadStr string) bool { return re.MatchString(payloadStr) },
	)
}

func (s SchemeString) WithWidth(n int) Scheme {
	return SchemeString{Width: n}
}
func (s SchemeInt16) Range(min, max int16) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt16Name, seq, types.TypeInteger, 2, false)
			if err != nil {
				return err
			}
			val := int16(binary.LittleEndian.Uint16(payload))
			if val < min || val > max {
				return NewValidationError(ErrOutOfRange, SchemeInt16Name, pos,
					RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(val)},
				)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt16Name, seq, types.TypeInteger, 2, false)
			if err != nil {
				return nil, err
			}
			val := int16(binary.LittleEndian.Uint16(payload))
			if val < min || val > max {
				return nil, NewValidationError(ErrOutOfRange, SchemeInt16Name, pos,
					RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(val)},
				)
			}
			return val, nil
		},
	}
}

func (s SchemeInt32) Range(min, max int32) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt32Name, seq, types.TypeInteger, 4, false)
			if err != nil {
				return err
			}
			val := int32(binary.LittleEndian.Uint32(payload))
			if val < min || val > max {
				return NewValidationError(ErrOutOfRange, SchemeInt32Name, pos,
					RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(val)},
				)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt32Name, seq, types.TypeInteger, 4, false)
			if err != nil {
				return nil, err
			}
			val := int32(binary.LittleEndian.Uint32(payload))
			if val < min || val > max {
				return nil, NewValidationError(
					ErrOutOfRange,
					SchemeInt32Name,
					pos,
					RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(val)},
				)
			}
			return val, nil
		},
	}
}

func (s SchemeInt64) Range(min, max int64) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt64Name, seq, types.TypeInteger, 8, false)
			if err != nil {
				return err
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				return NewValidationError(ErrOutOfRange, SchemeInt64Name, pos,
					RangeErrorDetails{Min: min, Max: max, Actual: val},
				)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt64Name, seq, types.TypeInteger, 8, false)
			if err != nil {
				return nil, err
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				return nil, NewValidationError(ErrOutOfRange, SchemeInt64Name, pos,
					RangeErrorDetails{Min: min, Max: max, Actual: val},
				)
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
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt64Name, seq, types.TypeInteger, 8, false)
			if err != nil {
				return err
			}
			if payload == nil {
				return nil // allow nullable
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				return NewValidationError(ErrDateOutOfRange, SchemeInt64Name, pos,
					RangeErrorDetails{Min: min, Max: max, Actual: val},
				)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeInt64Name, seq, types.TypeInteger, 8, false)
			if err != nil {
				return nil, err
			}
			if payload == nil {
				return nil, nil // allow nullable
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			if val < min || val > max {
				return nil, NewValidationError(ErrDateOutOfRange, SchemeInt64Name, pos,
					RangeErrorDetails{Min: min, Max: max, Actual: val},
				)
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

// Constant scheme name for unordered maps

func (s SchemeMapUnordered) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return NewValidationError(ErrInvalidFormat, SchemeMapUnorderedName, pos, err)
	}
	if typ != types.TypeMap {
		return NewValidationError(ErrConstraintViolated, SchemeMapUnorderedName, pos,
			fmt.Errorf("expected TypeMap, got %v", typ))
	}

	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return NewValidationError(ErrInvalidFormat, SchemeMapUnorderedName, pos, err)
		}
		seen := make(map[string]bool)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == types.TypeEnd {
				break
			}
			if err != nil {
				return NewValidationError(ErrInvalidFormat, SchemeMapUnorderedName, pos, err)
			}
			if keyType != types.TypeString {
				return NewValidationError(ErrConstraintViolated, SchemeMapUnorderedName, pos,
					fmt.Errorf("got %v", keyType))
			}
			key := string(keyPayload)
			seen[key] = true

			if validator, ok := s.Fields[key]; ok {
				if err := validator.Validate(subseq); err != nil {
					return err // child error already structured
				}
			} else {
				if err := subseq.Advance(); err != nil {
					return NewValidationError(ErrUnexpectedEOF, SchemeMapUnorderedName, pos, err)
				}
			}
		}

		for key := range s.Fields {
			if !seen[key] {
				return NewValidationError(ErrConstraintViolated, SchemeMapUnorderedName, pos,
					fmt.Errorf("missing expected key '%s'", key))
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return NewValidationError(ErrUnexpectedEOF, SchemeMapUnorderedName, pos, err)
	}
	return nil
}
func (s SchemeMapUnordered) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return nil, NewDecodeError(ErrInvalidFormat, SchemeMapUnorderedName, pos, err)
	}
	if typ != types.TypeMap {
		return nil, NewDecodeError(ErrConstraintViolated, SchemeMapUnorderedName, pos,
			fmt.Errorf("expected TypeMap, got %v", typ))
	}

	var out map[string]any
	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewDecodeError(ErrInvalidFormat, SchemeMapUnorderedName, pos, err)
		}
		out = make(map[string]any, subseq.ArgCount()/2)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == types.TypeEnd {
				break
			}
			if err != nil {
				return nil, NewDecodeError(ErrInvalidFormat, SchemeMapUnorderedName, pos, err)
			}
			if keyType != types.TypeString {
				return nil, NewDecodeError(ErrConstraintViolated, SchemeMapUnorderedName, pos,
					fmt.Errorf("got %v", keyType))
			}

			key := string(keyPayload)
			if validator, ok := s.Fields[key]; ok {
				val, err := validator.Decode(subseq)
				if err != nil {
					return nil, err // child error already structured
				}
				out[key] = val
			} else {
				if err := subseq.Advance(); err != nil {
					return nil, NewDecodeError(ErrUnexpectedEOF, SchemeMapUnorderedName, pos, err)
				}
			}
		}

		for key := range s.Fields {
			if _, ok := out[key]; !ok {
				return nil, NewDecodeError(ErrConstraintViolated, SchemeMapUnorderedName, pos,
					fmt.Errorf("missing expected key '%s'", key))
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewDecodeError(ErrUnexpectedEOF, SchemeMapUnorderedName, pos, err)
	}
	return out, nil
}

type TupleScheme struct {
	Schema         []Scheme
	Nullable       bool
	VariableLength bool
	Flatten        bool
}

func STuple(schema ...Scheme) TupleScheme {
	return TupleScheme{Schema: schema, Nullable: true, VariableLength: false, Flatten: false}
}

func STupleVal(schema ...Scheme) TupleScheme {
	return TupleScheme{Schema: schema, Nullable: true, VariableLength: true, Flatten: false}
}

func STupleValFlatten(schema ...Scheme) TupleScheme {
	return TupleScheme{Schema: schema, Nullable: true, VariableLength: true, Flatten: true}
}

func (s TupleScheme) IsNullable() bool {
	return s.Nullable
}

func (s TupleScheme) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemeName, pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return err
	}
	w := len(s.Schema)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return NewValidationError(ErrInvalidFormat, TupleSchemeName, pos, err)
		}
		if w > 0 && sub.ArgCount() != w && !s.VariableLength {
			return NewValidationError(ErrConstraintViolated, TupleSchemeName, pos,
				fmt.Errorf("container item count mismatch: %d!=%d", w, sub.ArgCount()))
		}
		for _, sch := range s.Schema {
			if err := sch.Validate(sub); err != nil {
				return NewValidationError(ErrInvalidFormat, TupleSchemeName, pos, err)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return NewValidationError(ErrUnexpectedEOF, TupleSchemeName, pos, err)
	}
	return nil
}

func (s TupleScheme) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemeName, pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return nil, err
	}
	var out []any
	w := len(s.Schema)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewDecodeError(ErrInvalidFormat, TupleSchemeName, pos, err)
		}
		if w > 0 && sub.ArgCount() != w && !s.VariableLength {
			return nil, NewDecodeError(ErrConstraintViolated, TupleSchemeName, pos,
				fmt.Errorf("container item count mismatch: %d!=%d", w, sub.ArgCount()))
		}
		out = make([]any, 0, sub.ArgCount())
		for _, sch := range s.Schema {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, NewDecodeError(ErrInvalidFormat, TupleSchemeName, pos, err)
			}
			if s.Flatten {
				if _, ok := sch.(SRepeatScheme); ok {
					if arr, ok := v.([]any); ok {
						out = append(out, arr...)
						continue
					}
				}
			}
			out = append(out, v)
		}
	}
	if err := seq.Advance(); err != nil {
		return nil, NewDecodeError(ErrUnexpectedEOF, TupleSchemeName, pos, err)
	}
	return out, nil
}

type TupleSchemeNamed struct {
	Schema         []Scheme
	FieldNames     []string
	Nullable       bool
	Flatten        bool
	VariableLength bool
}

func STupleNamed(fieldNames []string, schema ...Scheme) TupleSchemeNamed {
	if len(fieldNames) != len(schema) {
		panic("STupleNamed: fieldNames and schema length mismatch")
	}

	return TupleSchemeNamed{FieldNames: fieldNames, Schema: schema, Nullable: true}
}

// Strict named tuple: exact field count
func STupleNamedVal(fieldNames []string, schema ...Scheme) TupleSchemeNamed {
	if len(fieldNames) != len(schema) {
		panic("STupleNamedVal: fieldNames and schema length mismatch")
	}
	return TupleSchemeNamed{
		FieldNames:     fieldNames,
		Schema:         schema,
		Nullable:       true,
		Flatten:        false,
		VariableLength: true,
	}
}

// Flexible named tuple: allows repeats/extra fields
func STupleNamedValFlattened(fieldNames []string, schema ...Scheme) TupleSchemeNamed {
	if len(fieldNames) != len(schema) {
		panic("STupleNamedValFlattened: fieldNames and schema length mismatch")
	}
	return TupleSchemeNamed{
		FieldNames:     fieldNames,
		Schema:         schema,
		Nullable:       true,
		Flatten:        true,
		VariableLength: true,
	}
}

func (s TupleSchemeNamed) IsNullable() bool {
	return s.Nullable
}

func (s TupleSchemeNamed) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemeNamedName, pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return err
	}
	w := len(s.Schema)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return NewValidationError(ErrInvalidFormat, TupleSchemeNamedName, pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return NewValidationError(ErrConstraintViolated, TupleSchemeNamedName, pos,
				fmt.Errorf("container item count mismatch: %d!=%d", w, sub.ArgCount()))
		}
		for _, sch := range s.Schema {
			if err := sch.Validate(sub); err != nil {
				return NewValidationError(ErrInvalidFormat, TupleSchemeNamedName, pos, err)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return NewValidationError(ErrUnexpectedEOF, TupleSchemeNamedName, pos, err)
	}
	return nil
}

func (s TupleSchemeNamed) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemeNamedName, pos, seq, types.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return nil, err
	}

	out := make(map[string]any)
	w := len(s.Schema)
	if w > 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewDecodeError(ErrInvalidFormat, TupleSchemeNamedName, pos, err)
		}
		if !s.VariableLength && sub.ArgCount() != w {
			return nil, NewDecodeError(ErrConstraintViolated, TupleSchemeNamedName, pos,
				fmt.Errorf("item count mismatch: %d!=%d", w, sub.ArgCount()))
		}
		for i, sch := range s.Schema {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, NewDecodeError(ErrInvalidFormat, TupleSchemeNamedName, pos,
					fmt.Errorf("nested decode failed for field '%s'", s.FieldNames[i]))
			}
			if s.Flatten {
				if _, ok := sch.(SRepeatScheme); ok {
					if arr, ok := v.([]any); ok {
						for j, elem := range arr {
							out[fmt.Sprintf("%s_%d", s.FieldNames[i], j)] = elem
						}
						continue
					}
				}
			}
			out[s.FieldNames[i]] = v
		}
	}
	if err := seq.Advance(); err != nil {
		return nil, NewDecodeError(ErrUnexpectedEOF, TupleSchemeNamedName, pos, err)
	}
	return out, nil
}

type SRepeatScheme struct {
	Schema []Scheme
	max    int
	min    int
}

func SRepeat(minimum int, maximum int, schema ...Scheme) SRepeatScheme {
	if minimum < 0 {
		minimum = -1
	}
	if maximum < 0 {
		maximum = -1
	}
	return SRepeatScheme{Schema: schema, min: minimum, max: maximum}
}

func (s SRepeatScheme) IsNullable() bool {
	return s.min <= 0
}

func (s SRepeatScheme) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	argCount := seq.ArgCount() - pos

	if s.min != -1 && argCount < s.min {
		return NewValidationError(ErrConstraintViolated, SRepeatSchemeName, pos,
			fmt.Errorf("expected minimum %d elements, but only %d remain", s.min, argCount))
	}

	maxIter := argCount
	if s.max != -1 && s.max < argCount {
		maxIter = s.max
	}

	i := 0
outer:
	for {
		for _, scheme := range s.Schema {
			if err := scheme.Validate(seq); err != nil {
				return NewValidationError(ErrInvalidFormat, SRepeatSchemeName, pos, err)
			}
			if i >= maxIter {
				break outer
			}
			i++
		}
	}
	return nil
}

func (s SRepeatScheme) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	argCount := seq.ArgCount() - pos

	if s.min != -1 && argCount < s.min {
		return nil, NewDecodeError(ErrConstraintViolated, SRepeatSchemeName, pos,
			fmt.Errorf("expected minimum %d elements, but only %d remain", s.min, argCount))
	}

	maxIter := argCount
	if s.max != -1 && s.max < argCount {
		maxIter = s.max
	}

	out := make([]any, 0, maxIter)
	i := 0
outer:
	for {
		for _, scheme := range s.Schema {
			if i >= maxIter {
				break outer
			}
			val, err := scheme.Decode(seq)
			if err != nil {
				return nil, NewDecodeError(ErrInvalidFormat, SRepeatSchemeName, pos, err)
			}
			out = append(out, val)
			i++
		}
	}
	return out, nil
}
