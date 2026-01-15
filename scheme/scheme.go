package scheme

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/types"
	"github.com/quickwritereader/PackOS/utils"
)

type ErrorCode int

const (
	ErrUnknown            ErrorCode = iota
	ErrInvalidFormat                // decoding failed due to invalid format
	ErrUnexpectedEOF                // sequence ended unexpectedly while advancing or reading
	ErrConstraintViolated           // validation rule failed (width, type mismatch, nullable constraint)
	ErrEncode

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

// String implements fmt.Stringer
func (e ErrorCode) String() string {
	switch e {
	case ErrUnknown:
		return "ErrUnknown"
	case ErrInvalidFormat:
		return "ErrInvalidFormat"
	case ErrUnexpectedEOF:
		return "ErrUnexpectedEOF"
	case ErrConstraintViolated:
		return "ErrConstraintViolated"
	case ErrEncode:
		return "ErrEncode"
	case ErrStringMismatch:
		return "ErrStringMismatch"
	case ErrStringPrefix:
		return "ErrStringPrefix"
	case ErrStringSuffix:
		return "ErrStringSuffix"
	case ErrStringPattern:
		return "ErrStringPattern"
	case ErrStringMatch:
		return "ErrStringMatch"
	case ErrOutOfRange:
		return "ErrOutOfRange"
	case ErrDateOutOfRange:
		return "ErrDateOutOfRange"
	default:
		return fmt.Sprintf("ErrorCode(%d)", int(e))
	}
}

var ErrTypeMisMatch error = errors.New("Type Mismatch")
var ErrUnsupportedType error = errors.New("Unsuported Type")

type SchemeError struct {
	Code     ErrorCode
	Name     string
	Field    string
	Position int
	InnerErr error
}

type SizeExact struct {
	Exact  int
	Actual int
}

func (r SizeExact) Error() string {
	return fmt.Sprintf("%d != %d", r.Actual, r.Exact)
}

// RangeErrorDetails represents a structured range violation.
type RangeErrorDetails struct {
	Min    int64
	Max    int64
	Actual int64
}

func (r RangeErrorDetails) Error() string {
	return fmt.Sprintf("%d != [%d , %d]", r.Actual, r.Min, r.Max)
}

type StringErrorDetails struct {
	Expected string
	Actual   string
}

func (e StringErrorDetails) Error() string {
	return fmt.Sprintf("'%s'!='%s'", e.Actual, e.Expected)
}

type MissingKeyErrorDetails struct {
	Key string
}

func (e MissingKeyErrorDetails) Error() string {
	return fmt.Sprintf("Missing key '%s'", e.Key)
}

func formatError(code ErrorCode, name string, field string, pos int, inner error) string {
	if inner != nil {
		return fmt.Sprintf("%s %s:%s#%d { %s }", name, code, field, pos, inner)
	}
	return fmt.Sprintf("%s %s:%s#%d", name, code, field, pos)
}

func (v *SchemeError) Error() string {
	return formatError(v.Code, v.Name, v.Field, v.Position, v.InnerErr)
}

func (v *SchemeError) Unwrap() error {
	return v.InnerErr
}

func NewSchemeError(code ErrorCode, name, field string, pos int, inner error) *SchemeError {
	return &SchemeError{Code: code, Name: name, Field: field, Position: pos, InnerErr: inner}
}

type Scheme interface {
	Validate(seq *access.SeqGetAccess) error
	Decode(seq *access.SeqGetAccess) (any, error)
	Encode(put *access.PutAccess, val any) error
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
	EncodeFunc   func(put *access.PutAccess, val any) error
}

func (f SchemeGeneric) Validate(seq *access.SeqGetAccess) error {
	return f.ValidateFunc(seq)
}
func (f SchemeGeneric) Decode(seq *access.SeqGetAccess) (any, error) {
	return f.DecodeFunc(seq)
}

func (f SchemeGeneric) Encode(put *access.PutAccess, val any) error {
	return f.EncodeFunc(put, val)
}

type SchemeAny struct{}

func (s SchemeAny) Validate(seq *access.SeqGetAccess) error {
	if err := seq.Advance(); err != nil {
		return NewSchemeError(ErrUnexpectedEOF, SchemeAnyName, "", seq.CurrentIndex(), err)
	}
	return nil
}

func (s SchemeAny) Decode(seq *access.SeqGetAccess) (any, error) {
	v, err := access.DecodeTupleGeneric(seq, true)
	if err != nil {
		return nil, NewSchemeError(ErrInvalidFormat, SchemeAnyName, "", seq.CurrentIndex(), err)
	}
	if err := seq.Advance(); err != nil {
		return nil, NewSchemeError(ErrUnexpectedEOF, SchemeAnyName, "", seq.CurrentIndex(), err)
	}
	return v, nil
}

func (s SchemeAny) Encode(put *access.PutAccess, val any) error {
	err := put.AddAny(val, true)
	if err != nil {
		return NewSchemeError(ErrEncode, SchemeAnyName, "", -1, err)
	}
	return nil
}

type SchemeString struct {
	Width            int
	DefaultDecodeVal string
}

func (s SchemeString) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
}

func (s SchemeString) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 && len(s.DefaultDecodeVal) > 0 {
		return s.DefaultDecodeVal, nil
	}
	return string(payload), nil
}

func (s SchemeString) Encode(put *access.PutAccess, val any) error {
	if s.IsNullable() && val == nil {
		put.AddString("")
	}
	if value, ok := val.(string); ok {
		put.AddString(value)
	} else {
		return NewSchemeError(ErrEncode, SchemeStringName, "", -1, ErrTypeMisMatch)
	}
	return nil
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

func (s SchemeBytes) Encode(put *access.PutAccess, val any) error {
	if s.IsNullable() && val == nil {
		put.AddBytes(nil)
	}
	if value, ok := val.([]byte); ok {
		put.AddBytes(value)
	} else {
		return NewSchemeError(ErrEncode, SchemeBytesName, "", -1, ErrTypeMisMatch)
	}
	return nil
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
			return NewSchemeError(ErrInvalidFormat, SchemeMapName, "", pos, err)
		}
		for _, sch := range s.Schema {
			if err := sch.Validate(sub); err != nil {
				return NewSchemeError(ErrInvalidFormat, SchemeMapName, "", pos, err)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return NewSchemeError(ErrUnexpectedEOF, SchemeMapName, "", pos, err)
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
		return nil, NewSchemeError(ErrConstraintViolated, SchemeMapName, "", pos, SizeExact{Actual: len(s.Schema), Exact: len(s.Schema) + 1})
	}

	var out map[string]any
	if s.Width != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewSchemeError(ErrInvalidFormat, SchemeMapName, "", pos, err)
		}

		out = make(map[string]any, sub.ArgCount()/2)
		for i := 0; i < len(s.Schema); i += 2 {
			key, err := s.Schema[i].Decode(sub)
			if err != nil {
				return nil, NewSchemeError(ErrInvalidFormat, SchemeMapName, "", pos, err)
			}
			value, err := s.Schema[i+1].Decode(sub)
			if err != nil {
				keyStr := key.(string)
				return nil, NewSchemeError(ErrInvalidFormat, SchemeMapName, keyStr, pos, err)
			}
			if keyStr, ok := key.(string); ok {
				out[keyStr] = value
			} else {
				return nil, NewSchemeError(ErrInvalidFormat, SchemeMapName, "", pos-1, ErrUnsupportedType)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemeError(ErrUnexpectedEOF, SchemeMapName, "", pos, err)
	}
	return out, nil
}

func (s SchemeMap) Encode(put *access.PutAccess, val any) error {
	if s.IsNullable() && val == nil && len(s.Schema) < 1 {
		put.AddMapAny(nil, true)
	}

	if len(s.Schema)%2 != 0 {
		return NewSchemeError(ErrConstraintViolated, SchemeMapName, "", -1, SizeExact{Actual: len(s.Schema), Exact: len(s.Schema) + 1})
	}

	if mapKV, ok := val.(map[string]any); ok {
		keys := utils.SortKeys(mapKV)
		if len(keys) != len(s.Schema)/2 {
			return NewSchemeError(ErrInvalidFormat, SchemeMapName, "", -1, SizeExact{Actual: len(keys), Exact: len(s.Schema) / 2})
		}
		nested := put.BeginMap()
		defer put.EndNested(nested)
		j := 0
		for i := 0; i < len(s.Schema); i += 2 {
			k := keys[j]
			err := s.Schema[i].Encode(nested, k)
			if err != nil {
				return NewSchemeError(ErrInvalidFormat, SchemeMapName, k, -1, err)
			}
			err = s.Schema[i+1].Encode(nested, mapKV[k])
			if err != nil {
				return NewSchemeError(ErrInvalidFormat, SchemeMapName, k, -1, err)
			}
			j++
		}

	} else {
		return NewSchemeError(ErrEncode, SchemeMapName, "", -1, ErrTypeMisMatch)
	}
	return nil
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
			return nil, NewSchemeError(ErrInvalidFormat, SchemeTypeOnlyName, "", pos, err)
		}
		v, err := access.DecodePrimitive(valTyp, valPayload)
		if err != nil {
			return nil, NewSchemeError(ErrInvalidFormat, SchemeTypeOnlyName, "", pos, err)
		}
		return v, nil
	}
}

func (s SchemeTypeOnly) Encode(put *access.PutAccess, val any) error {
	// Handle nulls
	if val == nil {
		switch s.Tag {
		case types.TypeInteger:
			put.AddNullableInt64(nil)
		case types.TypeFloating:
			put.AddNullableFloat64(nil)
		case types.TypeString:
			put.AddString("")
		case types.TypeBool:
			put.AddNullableBool(nil)
		case types.TypeMap:
			put.AddMap(nil)
		case types.TypeTuple:
			put.AddAnyTuple(nil, true)
		default:
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrUnsupportedType)
		}
		return nil
	}

	// Type assertions based on typ
	switch s.Tag {
	case types.TypeInteger:
		switch v := val.(type) {
		case int8:
			put.AddInt8(v)
		case int16:
			put.AddInt16(v)
		case int32:
			put.AddInt32(v)
		case int64:
			put.AddInt64(v)
		default:
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case types.TypeFloating:
		switch v := val.(type) {
		case float32:
			put.AddFloat32(v)
		case float64:
			put.AddFloat64(v)
		default:
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case types.TypeString:
		if v, ok := val.(string); ok {
			put.AddString(v)
		} else {
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case types.TypeBool:
		if v, ok := val.(bool); ok {
			put.AddBool(v)
		} else {
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case types.TypeMap:
		if v, ok := val.(map[string]any); ok {
			put.AddMapAny(v, true)
		} else {
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case types.TypeTuple:
		if v, ok := val.([]any); ok {
			put.AddAnyTuple(v, true)
		} else {
			return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	default:
		return NewSchemeError(ErrEncode, SchemeTypeOnlyName, "", -1, ErrUnsupportedType)
	}

	return nil
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

func (s SchemeBool) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableBool(nil)
		return nil
	}
	if value, ok := val.(bool); ok {
		put.AddBool(value)
	} else {
		return NewSchemeError(ErrEncode, SchemeBoolName, "", -1, ErrTypeMisMatch)
	}

	return nil
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
func (s SchemeInt8) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt8(nil)
		return nil
	}
	switch v := val.(type) {
	case int8:
		put.AddInt8(v)
	case int:
		put.AddInt8(int8(v))
	case float64:
		put.AddInt8(int8(v))
	default:
		return NewSchemeError(ErrEncode, SchemeInt8Name, "", -1, ErrTypeMisMatch)
	}
	return nil
}

func (s SchemeInt16) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt16(nil)
		return nil
	}
	switch v := val.(type) {
	case int16:
		put.AddInt16(v)
	case int:
		put.AddInt16(int16(v))
	case float64:
		put.AddInt16(int16(v))
	default:
		return NewSchemeError(ErrEncode, SchemeInt16Name, "", -1, ErrTypeMisMatch)
	}
	return nil
}

func (s SchemeInt32) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt32(nil)
		return nil
	}
	switch v := val.(type) {
	case int32:
		put.AddInt32(v)
	case int:
		put.AddInt32(int32(v))
	case float64:
		put.AddInt32(int32(v))
	default:
		return NewSchemeError(ErrEncode, SchemeInt32Name, "", -1, ErrTypeMisMatch)
	}
	return nil
}

func (s SchemeInt64) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt64(nil)
		return nil
	}
	switch v := val.(type) {
	case int64:
		put.AddInt64(v)
	case int:
		put.AddInt64(int64(v))
	case float64:
		put.AddInt64(int64(v))
	default:
		return NewSchemeError(ErrEncode, SchemeInt64Name, "", -1, ErrTypeMisMatch)
	}
	return nil
}

func (s SchemeFloat32) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableFloat32(nil)
		return nil
	}
	switch v := val.(type) {
	case float32:
		put.AddFloat32(v)
	case float64:
		put.AddFloat32(float32(v))
	default:
		return NewSchemeError(ErrEncode, SchemeFloat32Name, "", -1, ErrTypeMisMatch)
	}
	return nil
}

func (s SchemeFloat64) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableFloat64(nil)
		return nil
	}
	if value, ok := val.(float64); ok {
		put.AddFloat64(value)
	} else {
		return NewSchemeError(ErrEncode, SchemeFloat64Name, "", -1, ErrTypeMisMatch)
	}
	return nil
}

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
		return NewSchemeError(ErrInvalidFormat, ChainName, "", -1, err)
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
		return nil, NewSchemeError(ErrInvalidFormat, ChainName, "", -1, err)
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

func EncodeValue(val any, chain SchemeChain) ([]byte, error) {

	c := len(chain.Schemes)
	if c > 1 {
		put := access.NewPutAccessFromPool()
		defer access.ReleasePutAccess(put)
		valArr, ok := val.([]any)
		if !ok {
			return nil, NewSchemeError(ErrEncode, ChainName, "", -1, ErrTypeMisMatch)
		}
		i := 0
		for _, scheme := range chain.Schemes {
			err := scheme.Encode(put, valArr[i])
			if err != nil {
				return nil, NewSchemeError(ErrEncode, ChainName, "", -1, err)
			}
			i++
		}
		return put.Pack(), nil
	} else if c == 1 {
		put := access.NewPutAccessFromPool()
		defer access.ReleasePutAccess(put)
		err := chain.Schemes[0].Encode(put, val)
		if err != nil {
			return nil, NewSchemeError(ErrEncode, ChainName, "", -1, err)
		}
		return put.Pack(), nil
	}
	return nil, nil
}

type SchemeNamedChain struct {
	SchemeChain
	FieldNames []string
}

func DecodeBufferNamed(buf []byte, chain SchemeNamedChain) (any, error) {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return nil, NewSchemeError(ErrInvalidFormat, SchemeNamedChainName, "", -1, err)
	}
	if len(chain.FieldNames) != len(chain.Schemes) {
		return nil, NewSchemeError(ErrConstraintViolated, SchemeNamedChainName, "", -1,
			SizeExact{Actual: len(chain.FieldNames), Exact: len(chain.Schemes)})
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

func EncodeValueNamed(val any, chain SchemeNamedChain) ([]byte, error) {

	put := access.NewPutAccessFromPool()
	defer access.ReleasePutAccess(put)
	mapKV, ok := val.(map[string]any)
	if !ok {
		return nil, NewSchemeError(ErrEncode, SchemeNamedChainName, "", -1, ErrTypeMisMatch)
	}
	for i, fn := range chain.FieldNames {
		val, ok := mapKV[fn]
		if ok {
			err := chain.Schemes[i].Encode(put, val)
			if err != nil {
				return nil, NewSchemeError(ErrEncode, SchemeNamedChainName, fn, -1, err)
			}
		} else {
			return nil, NewSchemeError(ErrEncode, SchemeNamedChainName, fn, -1, MissingKeyErrorDetails{Key: fn})
		}

	}
	return put.Pack(), nil
}

func precheck(errorName string, pos int, seq *access.SeqGetAccess, tag types.Type, hint int, nullable bool) (int, error) {
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return 0, NewSchemeError(ErrConstraintViolated, errorName, "", pos, err)
	}

	if typ != tag {
		// Type mismatch
		return 0, NewSchemeError(ErrConstraintViolated, errorName, "", pos, ErrTypeMisMatch)
	}

	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			// Width mismatch
			return 0, NewSchemeError(ErrConstraintViolated, errorName, "", pos, SizeExact{hint, width})
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
		return NewSchemeError(ErrUnexpectedEOF, errorName, "", pos, err)
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
			return nil, NewSchemeError(ErrInvalidFormat, errorName, "", pos, err)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemeError(ErrUnexpectedEOF, errorName, "", pos, err)
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

func (s SchemeString) CheckFunc(code ErrorCode, expected string, test func(payloadStr string) bool) Scheme {
	return SchemeGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return err
			}
			var str string
			if len(payload) == 0 && len(s.DefaultDecodeVal) > 0 {

				str = s.DefaultDecodeVal
			} else {
				str = string(payload)
			}
			if !test(str) {
				return NewSchemeError(code, SchemeStringName, "", pos, StringErrorDetails{Actual: str, Expected: expected})
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemeStringName, seq, types.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return nil, err
			}
			var str string
			if len(payload) == 0 && len(s.DefaultDecodeVal) > 0 {

				str = s.DefaultDecodeVal
			} else {
				str = string(payload)
			}
			if !test(str) {
				return nil, NewSchemeError(code, SchemeStringName, "", pos, StringErrorDetails{Actual: str, Expected: expected})
			}
			return str, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {

			if value, ok := val.(string); ok {
				if test(value) {
					put.AddString(value)
				} else {
					return NewSchemeError(ErrEncode, SchemeStringName, "", -1, StringErrorDetails{Actual: value, Expected: expected})
				}

			} else {
				return NewSchemeError(ErrEncode, SchemeStringName, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
	}
}

func (s SchemeString) DefaultDecodeValue(decodeDefault string) SchemeString {
	s.DefaultDecodeVal = decodeDefault
	return s
}

func (s SchemeString) Match(expected string) Scheme {
	return s.CheckFunc(
		ErrStringMatch,
		expected,
		func(payloadStr string) bool { return payloadStr == expected },
	)
}

func (s SchemeString) Prefix(prefix string) Scheme {
	return s.CheckFunc(
		ErrStringPrefix,
		prefix+"*",
		func(payloadStr string) bool { return strings.HasPrefix(payloadStr, prefix) },
	)
}

func (s SchemeString) Suffix(suffix string) Scheme {
	return s.CheckFunc(
		ErrStringSuffix,
		"*"+suffix,
		func(payloadStr string) bool { return strings.HasSuffix(payloadStr, suffix) },
	)
}

func (s SchemeString) Pattern(expr string) Scheme {
	re := regexp.MustCompile(expr)
	return s.CheckFunc(
		ErrStringPattern,
		expr,
		func(payloadStr string) bool { return re.MatchString(payloadStr) },
	)
}

func (s SchemeString) WithWidth(n int) SchemeString {
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
				return NewSchemeError(ErrOutOfRange, SchemeInt16Name, "", pos,
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
				return nil, NewSchemeError(ErrOutOfRange, SchemeInt16Name, "", pos,
					RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(val)},
				)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := val.(int16); ok {
				if value < min || value > max {
					return NewSchemeError(ErrEncode, SchemeInt16Name, "", -1, RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(value)})
				} else {
					put.AddInt16(value)
				}

			} else {
				return NewSchemeError(ErrEncode, SchemeInt16Name, "", -1, ErrTypeMisMatch)
			}
			return nil
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
				return NewSchemeError(ErrOutOfRange, SchemeInt32Name, "", pos,
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
				return nil, NewSchemeError(
					ErrOutOfRange,
					SchemeInt32Name,
					"", pos,
					RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(val)},
				)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := val.(int32); ok {
				if value < min || value > max {
					return NewSchemeError(ErrEncode, SchemeInt32Name, "", -1, RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(value)})
				} else {
					put.AddInt32(value)
				}

			} else {
				return NewSchemeError(ErrEncode, SchemeInt32Name, "", -1, ErrTypeMisMatch)
			}
			return nil
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
				return NewSchemeError(ErrOutOfRange, SchemeInt64Name, "", pos,
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
				return nil, NewSchemeError(ErrOutOfRange, SchemeInt64Name, "", pos,
					RangeErrorDetails{Min: min, Max: max, Actual: val},
				)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := val.(int64); ok {
				if value < min || value > max {
					return NewSchemeError(ErrEncode, SchemeInt64Name, "", -1, RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(value)})
				} else {
					put.AddInt64(value)
				}

			} else {
				return NewSchemeError(ErrEncode, SchemeInt64Name, "", -1, ErrTypeMisMatch)
			}
			return nil
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
				return NewSchemeError(ErrDateOutOfRange, SchemeInt64Name, "", pos,
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
				return nil, NewSchemeError(ErrDateOutOfRange, SchemeInt64Name, "", pos,
					RangeErrorDetails{Min: min, Max: max, Actual: val},
				)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := val.(int64); ok {
				if value < min || value > max {
					return NewSchemeError(ErrEncode, SchemeInt64Name, "", -1, RangeErrorDetails{Min: int64(min), Max: int64(max), Actual: int64(value)})
				} else {
					put.AddInt64(value)
				}
			} else {
				return NewSchemeError(ErrEncode, SchemeInt64Name, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
	}
}

type SchemeMapUnordered struct {
	Fields      map[string]Scheme
	OptionalMap bool
}

func SMapUnordered(mappedSchemes map[string]Scheme) Scheme {
	return SchemeMapUnordered{Fields: mappedSchemes, OptionalMap: false}
}

func SMapUnorderedOptional(mappedSchemes map[string]Scheme) Scheme {
	return SchemeMapUnordered{Fields: mappedSchemes, OptionalMap: true}
}

// Constant scheme name for unordered maps

func (s SchemeMapUnordered) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", pos, err)
	}
	if typ != types.TypeMap {
		return NewSchemeError(ErrConstraintViolated, SchemeMapUnorderedName, "", pos, ErrUnsupportedType)
	}

	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", pos, err)
		}
		seen := make(map[string]bool)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == types.TypeEnd {
				break
			}
			if err != nil {
				return NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", pos, err)
			}
			if keyType != types.TypeString {
				return NewSchemeError(ErrConstraintViolated, SchemeMapUnorderedName, "", pos, ErrUnsupportedType)
			}
			key := string(keyPayload)
			seen[key] = true

			if validator, ok := s.Fields[key]; ok {
				if err := validator.Validate(subseq); err != nil {
					return NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, key, pos, err)
				}
			} else {
				if err := subseq.Advance(); err != nil {
					return NewSchemeError(ErrUnexpectedEOF, SchemeMapUnorderedName, "", pos, err)
				}
			}
		}
		if !s.OptionalMap {
			for key := range s.Fields {
				if !seen[key] {
					return NewSchemeError(ErrConstraintViolated, SchemeMapUnorderedName, "", pos, MissingKeyErrorDetails{Key: key})
				}
			}
		}

	}

	if err := seq.Advance(); err != nil {
		return NewSchemeError(ErrUnexpectedEOF, SchemeMapUnorderedName, "", pos, err)
	}
	return nil
}

func (s SchemeMapUnordered) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return nil, NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", pos, err)
	}
	if typ != types.TypeMap {
		return nil, NewSchemeError(ErrConstraintViolated, SchemeMapUnorderedName, "", pos, ErrUnsupportedType)
	}

	var out map[string]any
	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", pos, err)
		}
		out = make(map[string]any, subseq.ArgCount()/2)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == types.TypeEnd {
				break
			}
			if err != nil {
				return nil, NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", pos, err)
			}
			if keyType != types.TypeString {
				return nil, NewSchemeError(ErrConstraintViolated, SchemeMapUnorderedName, "", pos, ErrUnsupportedType)
			}

			key := string(keyPayload)
			if validator, ok := s.Fields[key]; ok {
				val, err := validator.Decode(subseq)
				if err != nil {
					return nil, NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, key, pos, err)
				}
				out[key] = val
			} else {
				if err := subseq.Advance(); err != nil {
					return nil, NewSchemeError(ErrUnexpectedEOF, SchemeMapUnorderedName, "", pos, err)
				}
			}
		}
		if !s.OptionalMap {
			for key := range s.Fields {
				if _, ok := out[key]; !ok {
					return nil, NewSchemeError(ErrConstraintViolated, SchemeMapUnorderedName, "", pos, MissingKeyErrorDetails{Key: key})
				}
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemeError(ErrUnexpectedEOF, SchemeMapUnorderedName, "", pos, err)
	}
	return out, nil
}

func (s SchemeMapUnordered) Encode(put *access.PutAccess, val any) error {

	if mapKV, ok := val.(map[string]any); ok {

		nested := put.BeginMap()
		defer put.EndNested(nested)
		ss := SString
		for key, sch := range s.Fields {
			if val, exist := mapKV[key]; exist {
				ss.Encode(nested, key)
				err := sch.Encode(nested, val)
				if err != nil {
					return NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, key, -1, err)
				}
			} else {
				return NewSchemeError(ErrInvalidFormat, SchemeMapUnorderedName, "", -1, MissingKeyErrorDetails{Key: key})
			}

		}

	} else {
		return NewSchemeError(ErrEncode, SchemeMapUnorderedName, "", -1, ErrTypeMisMatch)
	}
	return nil
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
			return NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", pos, err)
		}
		if w > 0 && sub.ArgCount() != w && !s.VariableLength {
			return NewSchemeError(ErrConstraintViolated, TupleSchemeName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		for _, sch := range s.Schema {
			if err := sch.Validate(sub); err != nil {
				return NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", pos, err)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return NewSchemeError(ErrUnexpectedEOF, TupleSchemeName, "", pos, err)
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
			return nil, NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", pos, err)
		}
		if w > 0 && sub.ArgCount() != w && !s.VariableLength {
			return nil, NewSchemeError(ErrConstraintViolated, TupleSchemeName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		out = make([]any, 0, sub.ArgCount())
		for _, sch := range s.Schema {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", pos, err)
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
		return nil, NewSchemeError(ErrUnexpectedEOF, TupleSchemeName, "", pos, err)
	}
	return out, nil
}

func (s TupleScheme) Encode(put *access.PutAccess, val any) error {

	if valArr, ok := val.([]any); ok {

		nested := put.BeginTuple()
		defer put.EndNested(nested)
		j := 0
		lastI := len(s.Schema) - 1
		for k, sch := range s.Schema {

			if schRet, ok := sch.(SRepeatScheme); ok {
				var err error
				if s.Flatten {
					if lastI != k {
						if schRet.max < 1 {
							return NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", -1, fmt.Errorf("max should be provided if repeat is not in the end. max: %d", schRet.max))
						}
						err = schRet.Encode(nested, valArr[j:j+schRet.max])
						j = j + schRet.max
					} else {
						err = schRet.Encode(nested, valArr[j:])
					}

				} else {
					err = schRet.Encode(nested, valArr[j])
				}

				if err != nil {
					return NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", -1, err)
				}

			} else {
				err := sch.Encode(nested, valArr[j])
				if err != nil {
					return NewSchemeError(ErrInvalidFormat, TupleSchemeName, "", -1, err)
				}
				j++
			}

		}

	} else {
		return NewSchemeError(ErrEncode, TupleSchemeName, "", -1, ErrTypeMisMatch)
	}
	return nil
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
			return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, "", pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return NewSchemeError(ErrConstraintViolated, TupleSchemeNamedName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		for _, sch := range s.Schema {
			if err := sch.Validate(sub); err != nil {
				return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, "", pos, err)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return NewSchemeError(ErrUnexpectedEOF, TupleSchemeNamedName, "", pos, err)
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
			return nil, NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, "", pos, err)
		}
		if !s.VariableLength && sub.ArgCount() != w {
			return nil, NewSchemeError(ErrConstraintViolated, TupleSchemeNamedName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		for i, sch := range s.Schema {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, s.FieldNames[i], pos, err)
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
		return nil, NewSchemeError(ErrUnexpectedEOF, TupleSchemeNamedName, "", pos, err)
	}
	return out, nil
}

func (s TupleSchemeNamed) Encode(put *access.PutAccess, val any) error {

	if mapKV, ok := val.(map[string]any); ok {

		nested := put.BeginTuple()
		defer put.EndNested(nested)
		for i, key := range s.FieldNames {
			if sch, ok := s.Schema[i].(SRepeatScheme); ok {

				minx := sch.min
				max := sch.max
				j := 0
				schi := 0
				for ; j < minx; j++ {
					keyx := fmt.Sprintf("%s_%d", key, j)
					if val, exist := mapKV[keyx]; exist {
						err := sch.Schema[schi].Encode(nested, val)
						if err != nil {
							return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, "", -1, err)
						}
					} else {
						return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, "", -1, MissingKeyErrorDetails{Key: keyx})
					}
					schi++
					if schi >= len(sch.Schema) {
						schi = 0
					}
				}
				if max == -1 || max > minx {

					for {
						keyx := fmt.Sprintf("%s_%d", key, j)
						if val, exist := mapKV[keyx]; exist {
							err := sch.Schema[schi].Encode(nested, val)
							if err != nil {
								return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, keyx, -1, err)
							}
							schi++
							if schi >= len(sch.Schema) {
								schi = 0
							}
						} else {
							break
						}

						j++
					}

				}
			} else {
				if val, exist := mapKV[key]; exist {
					err := s.Schema[i].Encode(nested, val)
					if err != nil {
						return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, key, -1, err)
					}
				} else {
					return NewSchemeError(ErrInvalidFormat, TupleSchemeNamedName, "", -1, MissingKeyErrorDetails{Key: key})
				}
			}

		}

	} else {
		return NewSchemeError(ErrEncode, TupleSchemeNamedName, "", -1, ErrTypeMisMatch)
	}
	return nil
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
	return SRepeatScheme{Schema: schema, min: minimum * len(schema), max: maximum * len(schema)}
}

func (s SRepeatScheme) IsNullable() bool {
	return s.min <= 0
}

func (s SRepeatScheme) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	argCount := seq.ArgCount() - pos

	if s.min != -1 && argCount < s.min {
		return NewSchemeError(ErrConstraintViolated, SRepeatSchemeName, "", pos,
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
				return NewSchemeError(ErrInvalidFormat, SRepeatSchemeName, "", pos, err)
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
		return nil, NewSchemeError(ErrConstraintViolated, SRepeatSchemeName, "", pos,
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
				return nil, NewSchemeError(ErrInvalidFormat, SRepeatSchemeName, "", pos, err)
			}
			out = append(out, val)
			i++
		}
	}
	return out, nil
}

func (s SRepeatScheme) Encode(put *access.PutAccess, val any) error {

	valArr, ok := val.([]any)
	if !ok {
		return NewSchemeError(ErrEncode, SRepeatSchemeName, "", -1, ErrTypeMisMatch)
	}
	argCount := len(valArr)
	if s.min != -1 && argCount < s.min {
		return NewSchemeError(ErrConstraintViolated, SRepeatSchemeName, "", -1,
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
			if i >= maxIter {
				break outer
			}
			err := scheme.Encode(put, valArr[i])
			if err != nil {
				return NewSchemeError(ErrEncode, SRepeatSchemeName, "", i, err)
			}
			i++
		}
	}
	return nil
}
