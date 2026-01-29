package schema

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/typetags"
	"golang.org/x/exp/constraints"
	"golang.org/x/text/language"
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
	ErrStringEmail    // email format validation failed
	ErrStringURL      // URL/URI format validation failed
	ErrStringLang     // language tag format validation failed
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
	case ErrStringEmail:
		return "ErrStringEmail"
	case ErrStringURL:
		return "ErrStringURL"
	case ErrStringLang:
		return "ErrStringLang"
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

type SchemaError struct {
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

// RangeErrorDetails represents a structured range violation for any ordered type.
type RangeErrorDetails[T constraints.Ordered] struct {
	Min    *T
	Max    *T
	Actual T
}

func (r RangeErrorDetails[T]) Error() string {
	switch {
	case r.Min != nil && r.Max != nil:
		return fmt.Sprintf("%v not in [%v , %v]", r.Actual, *r.Min, *r.Max)
	case r.Min != nil:
		return fmt.Sprintf("%v < %v", r.Actual, *r.Min)
	case r.Max != nil:
		return fmt.Sprintf("%v > %v", r.Actual, *r.Max)
	default:
		return fmt.Sprintf("%v", r.Actual)
	}
}

// CheckRange validates val against optional min/max bounds.
// Returns a RangeErrorDetails if out of range, otherwise nil.
func CheckRange[T constraints.Ordered](val T, min *T, max *T) error {
	if (min != nil && val < *min) || (max != nil && val > *max) {
		return RangeErrorDetails[T]{Min: min, Max: max, Actual: val}
	}
	return nil
}

// For int64
func CheckIntRange(val int64, min *int64, max *int64) error {
	return CheckRange[int64](val, min, max)
}

// For float64
func CheckFloatRange(val float64, min *float64, max *float64) error {
	return CheckRange[float64](val, min, max)
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

func (v *SchemaError) Error() string {
	return formatError(v.Code, v.Name, v.Field, v.Position, v.InnerErr)
}

func (v *SchemaError) Unwrap() error {
	return v.InnerErr
}

func NewSchemaError(code ErrorCode, name, field string, pos int, inner error) *SchemaError {
	return &SchemaError{Code: code, Name: name, Field: field, Position: pos, InnerErr: inner}
}

type Schema interface {
	Validate(seq *access.SeqGetAccess) error
	Decode(seq *access.SeqGetAccess) (any, error)
	Encode(put *access.PutAccess, val any) error
	IsNullable() bool
}

const (
	SchemaAnyName                    = "SchemaAny"
	SchemaStringName                 = "SchemaString"
	SchemaBytesName                  = "SchemaBytes"
	SchemaMapName                    = "SchemaMap"
	SchemaTypeOnlyName               = "SchemaTypeOnly"
	SchemaBoolName                   = "SchemaBool"
	SchemaInt8Name                   = "SchemaInt8"
	SchemaInt16Name                  = "SchemaInt16"
	SchemaInt32Name                  = "SchemaInt32"
	SchemaInt64Name                  = "SchemaInt64"
	SchemaFloat32Name                = "SchemaFloat32"
	SchemaFloat64Name                = "SchemaFloat64"
	SchemaNamedChainName             = "SchemaNamedChain"
	SchemaMapUnorderedName           = "SchemaMapUnordered"
	SchemaMultiCheckNamesSchemaNamed = "SchemaMultiCheckNamesSchemaNamed"
	SchemaDateName                   = "SchemaDate"
	SchemaEnumNamedListName          = "SchemaEnumNamedList"
	SchemaNumberName                 = "SchemaNumber"
	ChainName                        = "SchemaChain"

	TupleSchemaName      = "TupleSchema"
	TupleSchemaNamedName = "TupleSchemaNamed"
	SRepeatSchemaName    = "SRepeatSchema"
	SchemaMapRepeatName  = "SchemaMapRepeat"
)

type SchemaGeneric struct {
	ValidateFunc  func(seq *access.SeqGetAccess) error
	DecodeFunc    func(seq *access.SeqGetAccess) (any, error)
	EncodeFunc    func(put *access.PutAccess, val any) error
	NullableCheck func() bool
}

func (f SchemaGeneric) Validate(seq *access.SeqGetAccess) error {
	return f.ValidateFunc(seq)
}
func (f SchemaGeneric) Decode(seq *access.SeqGetAccess) (any, error) {
	return f.DecodeFunc(seq)
}

func (f SchemaGeneric) Encode(put *access.PutAccess, val any) error {
	return f.EncodeFunc(put, val)
}

func (f SchemaGeneric) IsNullable() bool {
	return f.NullableCheck()
}

type SchemaAny struct {
	DecodeAsOrderedMap bool
}

func SchemaAnyOrdered() SchemaAny {
	return SchemaAny{DecodeAsOrderedMap: true}
}

func (s SchemaAny) Validate(seq *access.SeqGetAccess) error {
	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, SchemaAnyName, "", seq.CurrentIndex(), err)
	}
	return nil
}

func (s SchemaAny) Decode(seq *access.SeqGetAccess) (any, error) {
	v, err := access.DecodeTupleGeneric(seq, true, s.DecodeAsOrderedMap)
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaAnyName, "", seq.CurrentIndex(), err)
	}
	if err := seq.Advance(); err != nil {
		return nil, NewSchemaError(ErrUnexpectedEOF, SchemaAnyName, "", seq.CurrentIndex(), err)
	}
	return v, nil
}

func (s SchemaAny) Encode(put *access.PutAccess, val any) error {
	err := put.AddAny(val, true)
	if err != nil {
		return NewSchemaError(ErrEncode, SchemaAnyName, "", -1, err)
	}
	return nil
}
func (s SchemaAny) IsNullable() bool {
	return true
}

type SchemaString struct {
	Width            int
	DefaultDecodeVal string
}

func (s SchemaString) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaStringName, seq, typetags.TypeString, s.Width, s.IsNullable())
}

func (s SchemaString) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaStringName, seq, typetags.TypeString, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 && len(s.DefaultDecodeVal) > 0 {
		return s.DefaultDecodeVal, nil
	}
	return string(payload), nil
}

func (s SchemaString) Encode(put *access.PutAccess, val any) error {
	if s.IsNullable() && val == nil {
		put.AddString("")
	}
	if value, ok := val.(string); ok {
		put.AddString(value)
	} else {
		return NewSchemaError(ErrEncode, SchemaStringName, "", -1, ErrTypeMisMatch)
	}
	return nil
}

type SchemaBytes struct{ Width int }

func (s SchemaBytes) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaBytesName, seq, typetags.TypeString, s.Width, s.IsNullable())
}

func (s SchemaBytes) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaBytesName, seq, typetags.TypeByteArray, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s SchemaBytes) Encode(put *access.PutAccess, val any) error {
	if s.IsNullable() && val == nil {
		put.AddBytes(nil)
	}
	if value, ok := val.([]byte); ok {
		put.AddBytes(value)
	} else {
		return NewSchemaError(ErrEncode, SchemaBytesName, "", -1, ErrTypeMisMatch)
	}
	return nil
}

// NOTE: SchemaMap expects keys in sorted order.
// Validation will fail if map keys are unordered or mismatched.
type SchemaMap struct {
	Width   int
	Schemas []Schema
}

// Validate checks that the sequence matches the SchemaMap definition.
func (s SchemaMap) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	_, err := precheck(SchemaMapName, pos, seq, typetags.TypeMap, s.Width, s.IsNullable())
	if err != nil {
		return err
	}

	if s.Width != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return NewSchemaError(ErrInvalidFormat, SchemaMapName, "", pos, err)
		}
		for _, sch := range s.Schemas {
			if err := sch.Validate(sub); err != nil {
				return NewSchemaError(ErrInvalidFormat, SchemaMapName, "", pos, err)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, SchemaMapName, "", pos, err)
	}
	return nil
}

// Decode reads a SchemaMap from the sequence into an OrderedMapAny.
func (s SchemaMap) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	_, err := precheck(SchemaMapName, pos, seq, typetags.TypeMap, s.Width, s.IsNullable())
	if err != nil {
		return nil, err
	}

	if len(s.Schemas)%2 != 0 {
		return nil, NewSchemaError(
			ErrConstraintViolated,
			SchemaMapName,
			"",
			pos,
			SizeExact{Actual: len(s.Schemas), Exact: len(s.Schemas) + 1},
		)
	}

	var out *typetags.OrderedMapAny
	if s.Width != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaMapName, "", pos, err)
		}

		out = typetags.NewOrderedMapAny()
		for i := 0; i < len(s.Schemas); i += 2 {
			key, err := s.Schemas[i].Decode(sub)
			if err != nil {
				return nil, NewSchemaError(ErrInvalidFormat, SchemaMapName, "", pos, err)
			}
			value, err := s.Schemas[i+1].Decode(sub)
			if err != nil {
				keyStr := key.(string)
				return nil, NewSchemaError(ErrInvalidFormat, SchemaMapName, keyStr, pos, err)
			}
			if keyStr, ok := key.(string); ok {
				out.Set(keyStr, value)
			} else {
				return nil, NewSchemaError(ErrInvalidFormat, SchemaMapName, "", pos-1, ErrUnsupportedType)
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemaError(ErrUnexpectedEOF, SchemaMapName, "", pos, err)
	}
	return out, nil
}

// Encode writes an OrderedMapAny into the sequence according to the SchemaMap.
func (s SchemaMap) Encode(put *access.PutAccess, val any) error {
	if s.IsNullable() && val == nil && len(s.Schemas) < 1 {
		put.AddMapAny(nil, true)
	}

	if len(s.Schemas)%2 != 0 {
		return NewSchemaError(
			ErrConstraintViolated,
			SchemaMapName,
			"",
			-1,
			SizeExact{Actual: len(s.Schemas), Exact: len(s.Schemas) + 1},
		)
	}

	if om, ok := val.(*typetags.OrderedMapAny); ok {
		if om.Len() != len(s.Schemas)/2 {
			return NewSchemaError(ErrInvalidFormat, SchemaMapName,
				"", -1, SizeExact{Actual: om.Len(), Exact: len(s.Schemas) / 2},
			)
		}
		nested := put.BeginMap()
		defer put.EndNested(nested)

		i := 0

		for k, v := range om.ItemsIter() {
			if err := s.Schemas[i].Encode(nested, k); err != nil {
				return NewSchemaError(ErrInvalidFormat, SchemaMapName, k, -1, err)
			}
			if err := s.Schemas[i+1].Encode(nested, v); err != nil {
				return NewSchemaError(ErrInvalidFormat, SchemaMapName, k, -1, err)
			}
			i += 2
		}

	} else {
		return NewSchemaError(ErrEncode, SchemaMapName, "", -1, ErrTypeMisMatch)
	}
	return nil
}

type SchemaTypeOnly struct {
	Tag             typetags.Type
	DecodeOrdereMap bool
}

func (s SchemaTypeOnly) IsNullable() bool {
	return true
}

func (s SchemaTypeOnly) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaTypeOnlyName, seq, s.Tag, -1, false)
}

func (s SchemaTypeOnly) Decode(seq *access.SeqGetAccess) (any, error) {
	switch s.Tag {
	case typetags.TypeMap:
		if s.DecodeOrdereMap {
			return access.DecodeMapAny(seq)
		}
		return access.DecodeMapAny(seq)
	case typetags.TypeTuple:
		return access.DecodeTuple(seq)
	default:
		pos := seq.CurrentIndex()
		valPayload, valTyp, err := seq.Next()
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaTypeOnlyName, "", pos, err)
		}
		v, err := access.DecodePrimitive(valTyp, valPayload)
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaTypeOnlyName, "", pos, err)
		}
		return v, nil
	}
}

func (s SchemaTypeOnly) Encode(put *access.PutAccess, val any) error {
	// Handle nulls
	if val == nil {
		switch s.Tag {
		case typetags.TypeInteger:
			put.AddNullableInt64(nil)
		case typetags.TypeFloating:
			put.AddNullableFloat64(nil)
		case typetags.TypeString:
			put.AddString("")
		case typetags.TypeBool:
			put.AddNullableBool(nil)
		case typetags.TypeMap:
			put.AddMap(nil)
		case typetags.TypeTuple:
			put.AddAnyTuple(nil, true)
		default:
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrUnsupportedType)
		}
		return nil
	}

	// Type assertions based on typ
	switch s.Tag {
	case typetags.TypeInteger:
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
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case typetags.TypeFloating:
		switch v := val.(type) {
		case float32:
			put.AddFloat32(v)
		case float64:
			put.AddFloat64(v)
		default:
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case typetags.TypeString:
		if v, ok := val.(string); ok {
			put.AddString(v)
		} else {
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case typetags.TypeBool:
		if v, ok := val.(bool); ok {
			put.AddBool(v)
		} else {
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case typetags.TypeMap:
		switch v := val.(type) {
		case map[string]any:
			put.AddMapAny(v, true)

		case *typetags.OrderedMapAny:
			if err := put.AddMapAnyOrdered(v, true); err != nil {
				return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, err)
			}

		default:
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	case typetags.TypeTuple:
		if v, ok := val.([]any); ok {
			put.AddAnyTuple(v, true)
		} else {
			return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrTypeMisMatch)
		}

	default:
		return NewSchemaError(ErrEncode, SchemaTypeOnlyName, "", -1, ErrUnsupportedType)
	}

	return nil
}

type Nullable interface {
	IsNullable() bool
}

func (s SchemaString) IsNullable() bool { return s.Width < 0 }
func (s SchemaBytes) IsNullable() bool  { return s.Width < 0 }
func (s SchemaMap) IsNullable() bool    { return s.Width <= 0 }

// Primitives
type SchemaBool struct{ Nullable bool }

func (s SchemaBool) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaBoolName, seq, typetags.TypeBool, 1, s.Nullable)
}

func (s SchemaBool) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaBoolName, seq, typetags.TypeBool, 1, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return payload[0] != 0, nil
}

func (s SchemaBool) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableBool(nil)
		return nil
	}
	if value, ok := val.(bool); ok {
		put.AddBool(value)
	} else {
		return NewSchemaError(ErrEncode, SchemaBoolName, "", -1, ErrTypeMisMatch)
	}

	return nil
}

func (s SchemaBool) IsNullable() bool { return s.Nullable }

type SchemaInt8 struct{ Nullable bool }

func (s SchemaInt8) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaInt8Name, seq, typetags.TypeInteger, 1, s.Nullable)
}
func (s SchemaInt8) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaInt8Name, seq, typetags.TypeInteger, 1, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int8(payload[0]), nil
}
func (s SchemaInt8) IsNullable() bool { return s.Nullable }

type SchemaInt16 struct{ Nullable bool }

func (s SchemaInt16) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaInt16Name, seq, typetags.TypeInteger, 2, s.Nullable)
}
func (s SchemaInt16) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaInt16Name, seq, typetags.TypeInteger, 2, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int16(binary.LittleEndian.Uint16(payload)), nil
}
func (s SchemaInt16) IsNullable() bool { return s.Nullable }

type SchemaInt32 struct{ Nullable bool }

func (s SchemaInt32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaInt32Name, seq, typetags.TypeInteger, 4, s.Nullable)
}
func (s SchemaInt32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaInt32Name, seq, typetags.TypeInteger, 4, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int32(binary.LittleEndian.Uint32(payload)), nil
}
func (s SchemaInt32) IsNullable() bool { return s.Nullable }

type SchemaInt64 struct{ Nullable bool }

func (s SchemaInt64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaInt64Name, seq, typetags.TypeInteger, 8, s.Nullable)
}
func (s SchemaInt64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaInt64Name, seq, typetags.TypeInteger, 8, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return int64(binary.LittleEndian.Uint64(payload)), nil
}
func (s SchemaInt64) IsNullable() bool { return s.Nullable }

type SchemaFloat32 struct{ Nullable bool }

func (s SchemaFloat32) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaFloat32Name, seq, typetags.TypeFloating, 4, s.Nullable)
}
func (s SchemaFloat32) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaFloat32Name, seq, typetags.TypeFloating, 4, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(payload)), nil
}
func (s SchemaFloat32) IsNullable() bool { return s.Nullable }

type SchemaFloat64 struct{ Nullable bool }

func (s SchemaFloat64) Validate(seq *access.SeqGetAccess) error {
	return validatePrimitive(SchemaFloat64Name, seq, typetags.TypeFloating, 8, s.Nullable)
}
func (s SchemaFloat64) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaFloat64Name, seq, typetags.TypeFloating, 8, s.Nullable)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(payload)), nil
}
func (s SchemaFloat64) IsNullable() bool { return s.Nullable }

// convertToNumber converts val (any) into target type T (int8, int16, int32, int64, float32, float64).
// It also supports string input by parsing as float64 first.
func convertToNumber[T constraints.Integer | constraints.Float](val any) (T, bool) {
	var zero T
	switch v := val.(type) {
	case int:
		return T(v), true
	case int8:
		return T(v), true
	case int16:
		return T(v), true
	case int32:
		return T(v), true
	case int64:
		return T(v), true
	case float32:
		return T(v), true
	case float64:
		return T(v), true
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return zero, false
		}
		return T(parsed), true
	default:
		return zero, false
	}
}

func (s SchemaInt8) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt8(nil)
		return nil
	}
	if v, ok := convertToNumber[int8](val); ok {
		put.AddInt8(v)
		return nil
	}
	return NewSchemaError(ErrEncode, SchemaInt8Name, "", -1, ErrTypeMisMatch)
}

func (s SchemaInt16) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt16(nil)
		return nil
	}
	if v, ok := convertToNumber[int16](val); ok {
		put.AddInt16(v)
		return nil
	}
	return NewSchemaError(ErrEncode, SchemaInt16Name, "", -1, ErrTypeMisMatch)
}

func (s SchemaInt32) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt32(nil)
		return nil
	}
	if v, ok := convertToNumber[int32](val); ok {
		put.AddInt32(v)
		return nil
	}
	return NewSchemaError(ErrEncode, SchemaInt32Name, "", -1, ErrTypeMisMatch)
}

func (s SchemaInt64) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableInt64(nil)
		return nil
	}
	if v, ok := convertToNumber[int64](val); ok {
		put.AddInt64(v)
		return nil
	}
	return NewSchemaError(ErrEncode, SchemaInt64Name, "", -1, ErrTypeMisMatch)
}

func (s SchemaFloat32) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableFloat32(nil)
		return nil
	}
	if v, ok := convertToNumber[float32](val); ok {
		put.AddFloat32(v)
		return nil
	}
	return NewSchemaError(ErrEncode, SchemaFloat32Name, "", -1, ErrTypeMisMatch)
}

func (s SchemaFloat64) Encode(put *access.PutAccess, val any) error {
	if s.Nullable && val == nil {
		put.AddNullableFloat64(nil)
		return nil
	}
	if v, ok := convertToNumber[float64](val); ok {
		put.AddFloat64(v)
		return nil
	}
	return NewSchemaError(ErrEncode, SchemaFloat64Name, "", -1, ErrTypeMisMatch)
}

func SType(tag typetags.Type) Schema {
	return SchemaTypeOnly{Tag: tag}
}

var (
	SBool        Schema       = SchemaBool{}
	SInt8        Schema       = SchemaInt8{}
	SInt16       SchemaInt16  = SchemaInt16{}
	SInt32       SchemaInt32  = SchemaInt32{}
	SInt64       SchemaInt64  = SchemaInt64{}
	SFloat32     Schema       = SchemaFloat32{}
	SFloat64     Schema       = SchemaFloat64{}
	SNullBool    Schema       = SchemaBool{Nullable: true}
	SNullInt8    Schema       = SchemaInt8{Nullable: true}
	SNullInt16   Schema       = SchemaInt16{Nullable: true}
	SNullInt32   Schema       = SchemaInt32{Nullable: true}
	SNullInt64   Schema       = SchemaInt64{Nullable: true}
	SNullFloat32 Schema       = SchemaFloat32{Nullable: true}
	SNullFloat64 Schema       = SchemaFloat64{Nullable: true}
	SString      SchemaString = SchemaString{Width: 0}
	SAny                      = SchemaAny{}
)

func SBytes(width int) Schema { return SchemaBytes{Width: width} }

func SMap(nested ...Schema) Schema {
	return SchemaMap{
		Width:   -1,
		Schemas: nested,
	}
}

func SVariableString() Schema {
	return SchemaString{Width: -1}
}

func SVariableBytes() Schema {
	return SchemaBytes{Width: -1}
}

func SVariableMap(nested ...Schema) Schema {
	return SchemaMap{
		Width:   -1,
		Schemas: nested,
	}
}

func ValidateBuffer(buf []byte, chain SchemaChain) error {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return NewSchemaError(ErrInvalidFormat, ChainName, "", -1, err)
	}
	for _, schema := range chain.Schemas {
		if err := schema.Validate(seq); err != nil {
			return err
		}
	}
	return nil
}

func DecodeBuffer(buf []byte, chain SchemaChain) (any, error) {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, ChainName, "", -1, err)
	}
	out := make([]any, 0, len(chain.Schemas))
	for _, schema := range chain.Schemas {
		val, err := schema.Decode(seq)
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

func EncodeValue(val any, chain SchemaChain) ([]byte, error) {

	c := len(chain.Schemas)
	if c > 1 {
		put := access.NewPutAccessFromPool()
		defer access.ReleasePutAccess(put)
		valArr, ok := val.([]any)
		if !ok {
			return nil, NewSchemaError(ErrEncode, ChainName, "", -1, ErrTypeMisMatch)
		}
		i := 0
		for _, schema := range chain.Schemas {
			err := schema.Encode(put, valArr[i])
			if err != nil {
				return nil, NewSchemaError(ErrEncode, ChainName, "", -1, err)
			}
			i++
		}
		return put.Pack(), nil
	} else if c == 1 {
		put := access.NewPutAccessFromPool()
		defer access.ReleasePutAccess(put)
		err := chain.Schemas[0].Encode(put, val)
		if err != nil {
			return nil, NewSchemaError(ErrEncode, ChainName, "", -1, err)
		}
		return put.Pack(), nil
	}
	return nil, nil
}

type SchemaNamedChain struct {
	SchemaChain
	FieldNames []string
}

func DecodeBufferNamed(buf []byte, chain SchemaNamedChain) (any, error) {
	seq, err := access.NewSeqGetAccess(buf)
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaNamedChainName, "", -1, err)
	}
	if len(chain.FieldNames) != len(chain.Schemas) {
		return nil, NewSchemaError(ErrConstraintViolated, SchemaNamedChainName, "", -1,
			SizeExact{Actual: len(chain.FieldNames), Exact: len(chain.Schemas)})
	}
	out := make(map[string]any, len(chain.Schemas))
	for i, schema := range chain.Schemas {
		val, err := schema.Decode(seq)
		if err != nil {
			return nil, err
		}
		out[chain.FieldNames[i]] = val
	}
	return out, nil
}

func EncodeValueNamed(val any, chain SchemaNamedChain) ([]byte, error) {

	put := access.NewPutAccessFromPool()
	defer access.ReleasePutAccess(put)
	mapKV, ok := val.(map[string]any)
	if !ok {
		return nil, NewSchemaError(ErrEncode, SchemaNamedChainName, "", -1, ErrTypeMisMatch)
	}
	for i, fn := range chain.FieldNames {
		val, ok := mapKV[fn]
		if ok {
			err := chain.Schemas[i].Encode(put, val)
			if err != nil {
				return nil, NewSchemaError(ErrEncode, SchemaNamedChainName, fn, -1, err)
			}
		} else {
			if chain.Schemas[i].IsNullable() {
				//just add null tag and skip
				chain.Schemas[i].Encode(put, nil)
				continue
			}
			return nil, NewSchemaError(ErrEncode, SchemaNamedChainName, fn, -1, MissingKeyErrorDetails{Key: fn})
		}

	}
	return put.Pack(), nil
}

func precheck(errorName string, pos int, seq *access.SeqGetAccess, tag typetags.Type, hint int, nullable bool) (int, error) {
	typ, width, err := seq.PeekTypeWidth()
	if err != nil {
		return 0, NewSchemaError(ErrConstraintViolated, errorName, "", pos, err)
	}

	if typ != tag {
		// Type mismatch
		return 0, NewSchemaError(ErrConstraintViolated, errorName, "", pos, ErrTypeMisMatch)
	}

	if !nullable && hint != 0 && width != hint {
		return 0, NewSchemaError(ErrConstraintViolated, errorName, "", pos, SizeExact{hint, width})
	}

	return width, nil
}

// Helper for primitive validation
func validatePrimitive(errorName string, seq *access.SeqGetAccess, tag typetags.Type, hint int, nullable bool) error {
	pos := seq.CurrentIndex()

	_, err := precheck(errorName, pos, seq, tag, hint, nullable)
	if err != nil {
		return err
	}

	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, errorName, "", pos, err)
	}

	return nil
}

func validatePrimitiveAndGetPayload(errorName string, seq *access.SeqGetAccess, tag typetags.Type, hint int, nullable bool) ([]byte, error) {
	pos := seq.CurrentIndex()

	width, err := precheck(errorName, pos, seq, tag, hint, nullable)
	if err != nil {
		return nil, err
	}

	var payload []byte
	if width > 0 {
		payload, err = seq.GetPayload(width)
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, errorName, "", pos, err)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemaError(ErrUnexpectedEOF, errorName, "", pos, err)
	}

	return payload, nil
}

type SchemaChain struct {
	Schemas []Schema
}

func SChain(schemas ...Schema) SchemaChain {
	return SchemaChain{Schemas: schemas}
}

func SStringExact(expected string) Schema {
	return SString.Match(expected)
}

func SStringLen(width int) Schema {
	return SString.WithWidth(width)
}

func (s SchemaString) CheckFunc(code ErrorCode, expected string, test func(payloadStr string) bool) Schema {
	return SchemaGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaStringName, seq, typetags.TypeString, s.Width, s.IsNullable())
			if err != nil {
				return err
			}
			var str string
			if len(payload) == 0 && len(s.DefaultDecodeVal) > 0 {

				str = s.DefaultDecodeVal
			} else {
				str = string(payload)
			}
			if s.IsNullable() && str == "" {
				return nil
			}
			if !test(str) {
				return NewSchemaError(code, SchemaStringName, "", pos, StringErrorDetails{Actual: str, Expected: expected})
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaStringName, seq, typetags.TypeString, s.Width, s.IsNullable())
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
				return nil, NewSchemaError(code, SchemaStringName, "", pos, StringErrorDetails{Actual: str, Expected: expected})
			}
			return str, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {

			if value, ok := val.(string); ok {
				if test(value) {
					put.AddString(value)
				} else {
					return NewSchemaError(ErrEncode, SchemaStringName, "", -1, StringErrorDetails{Actual: value, Expected: expected})
				}

			} else {
				return NewSchemaError(ErrEncode, SchemaStringName, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
		NullableCheck: func() bool {
			return s.IsNullable()
		},
	}
}

func (s SchemaString) DefaultDecodeValue(decodeDefault string) SchemaString {
	s.DefaultDecodeVal = decodeDefault
	return s
}

func (s SchemaString) Match(expected string) Schema {
	return s.CheckFunc(
		ErrStringMatch,
		expected,
		func(payloadStr string) bool { return payloadStr == expected },
	)
}

func (s SchemaString) Prefix(prefix string) Schema {
	return s.CheckFunc(
		ErrStringPrefix,
		prefix+"*",
		func(payloadStr string) bool { return strings.HasPrefix(payloadStr, prefix) },
	)
}

func (s SchemaString) Suffix(suffix string) Schema {
	return s.CheckFunc(
		ErrStringSuffix,
		"*"+suffix,
		func(payloadStr string) bool { return strings.HasSuffix(payloadStr, suffix) },
	)
}

func (s SchemaString) Pattern(expr string) Schema {
	re := regexp.MustCompile(expr)
	return s.CheckFunc(
		ErrStringPattern,
		expr,
		func(payloadStr string) bool { return re.MatchString(payloadStr) },
	)
}

func (s SchemaString) WithWidth(n int) SchemaString {
	return SchemaString{Width: n}
}
func (s SchemaInt16) RangeValues(min, max int64) Schema {
	return s.Range(&min, &max)
}
func (s SchemaInt16) Range(min, max *int64) Schema {
	return SchemaGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt16Name, seq, typetags.TypeInteger, 2, false)
			if err != nil {
				return err
			}
			val := int16(binary.LittleEndian.Uint16(payload))
			err = CheckIntRange(int64(val), min, max)
			if err != nil {
				return NewSchemaError(ErrOutOfRange, SchemaInt16Name, "", pos, err)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt16Name, seq, typetags.TypeInteger, 2, false)
			if err != nil {
				return nil, err
			}
			val := int16(binary.LittleEndian.Uint16(payload))
			err = CheckIntRange(int64(val), min, max)
			if err != nil {
				return nil, NewSchemaError(ErrOutOfRange, SchemaInt16Name, "", pos, err)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := convertToNumber[int16](val); ok {
				err := CheckIntRange(int64(value), min, max)
				if err != nil {
					return NewSchemaError(ErrOutOfRange, SchemaInt16Name, "", -1, err)
				}
				put.AddInt16(value)

			} else {
				return NewSchemaError(ErrEncode, SchemaInt16Name, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
	}
}
func (s SchemaInt32) RangeValues(min, max int64) Schema {
	return s.Range(&min, &max)
}
func (s SchemaInt32) Range(min, max *int64) Schema {
	return SchemaGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt32Name, seq, typetags.TypeInteger, 4, false)
			if err != nil {
				return err
			}
			val := int32(binary.LittleEndian.Uint32(payload))
			err = CheckIntRange(int64(val), min, max)
			if err != nil {
				return NewSchemaError(ErrOutOfRange, SchemaInt32Name, "", pos, err)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt32Name, seq, typetags.TypeInteger, 4, false)
			if err != nil {
				return nil, err
			}
			val := int32(binary.LittleEndian.Uint32(payload))
			err = CheckIntRange(int64(val), min, max)
			if err != nil {
				return nil, NewSchemaError(ErrOutOfRange, SchemaInt32Name, "", pos, err)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := convertToNumber[int32](val); ok {
				err := CheckIntRange(int64(value), min, max)
				if err != nil {
					return NewSchemaError(ErrOutOfRange, SchemaInt32Name, "", -1, err)
				}
				put.AddInt32(value)

			} else {
				return NewSchemaError(ErrEncode, SchemaInt32Name, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
	}
}
func (s SchemaInt64) RangeValues(min, max int64) Schema {
	return s.Range(&min, &max)
}
func (s SchemaInt64) Range(min, max *int64) Schema {
	return SchemaGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt64Name, seq, typetags.TypeInteger, 8, false)
			if err != nil {
				return err
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			err = CheckIntRange(val, min, max)
			if err != nil {
				return NewSchemaError(ErrOutOfRange, SchemaInt64Name, "", pos, err)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt64Name, seq, typetags.TypeInteger, 8, false)
			if err != nil {
				return nil, err
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			err = CheckIntRange(val, min, max)
			if err != nil {
				return nil, NewSchemaError(ErrOutOfRange, SchemaInt64Name, "", pos, err)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := convertToNumber[int64](val); ok {
				err := CheckIntRange(value, min, max)
				if err != nil {
					return NewSchemaError(ErrOutOfRange, SchemaInt64Name, "", -1, err)
				}
				put.AddInt64(value)

			} else {
				return NewSchemaError(ErrEncode, SchemaInt64Name, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
	}
}

func PtrToInt64[T constraints.Integer](val T) *int64 {
	var v int64 = int64(val)
	return &v
}
func (s SchemaInt64) DateRangeValues(from, to time.Time) Schema {
	return s.DateRange(&from, &to)
}
func (s SchemaInt64) DateRange(from, to *time.Time) Schema {
	var min, max *int64 = nil, nil
	if from != nil {
		min = PtrToInt64(from.Unix())
	}
	if to != nil {
		max = PtrToInt64(to.Unix())
	}

	return SchemaGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt64Name, seq, typetags.TypeInteger, 8, false)
			if err != nil {
				return err
			}
			if payload == nil {
				return nil // allow nullable
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			err = CheckIntRange(val, min, max)
			if err != nil {
				return NewSchemaError(ErrOutOfRange, SchemaInt64Name, "", pos, err)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaInt64Name, seq, typetags.TypeInteger, 8, false)
			if err != nil {
				return nil, err
			}
			if payload == nil {
				return nil, nil // allow nullable
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			err = CheckIntRange(val, min, max)
			if err != nil {
				return nil, NewSchemaError(ErrOutOfRange, SchemaInt64Name, "", pos, err)
			}
			return val, nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if value, ok := convertToNumber[int64](val); ok {
				err := CheckIntRange(value, min, max)
				if err != nil {
					return NewSchemaError(ErrOutOfRange, SchemaInt64Name, "", -1, err)
				}
				put.AddInt64(value)
			} else {
				return NewSchemaError(ErrEncode, SchemaInt64Name, "", -1, ErrTypeMisMatch)
			}
			return nil
		},
	}
}

type SchemaMapUnordered struct {
	Fields      map[string]Schema
	OptionalMap bool
}

func SMapUnordered(mappedSchemas map[string]Schema) Schema {
	return SchemaMapUnordered{Fields: mappedSchemas, OptionalMap: false}
}

func SMapUnorderedOptional(mappedSchemas map[string]Schema) Schema {
	return SchemaMapUnordered{Fields: mappedSchemas, OptionalMap: true}
}

func (_ SchemaMapUnordered) IsNullable() bool {
	return true
}

// Constant schema name for unordered maps

func (s SchemaMapUnordered) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", pos, err)
	}
	if typ != typetags.TypeMap {
		return NewSchemaError(ErrConstraintViolated, SchemaMapUnorderedName, "", pos, ErrUnsupportedType)
	}

	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", pos, err)
		}
		seen := make(map[string]bool)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == typetags.TypeEnd {
				break
			}
			if err != nil {
				return NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", pos, err)
			}
			if keyType != typetags.TypeString {
				return NewSchemaError(ErrConstraintViolated, SchemaMapUnorderedName, "", pos, ErrUnsupportedType)
			}
			key := string(keyPayload)
			seen[key] = true

			if validator, ok := s.Fields[key]; ok {
				if err := validator.Validate(subseq); err != nil {
					return NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, key, pos, err)
				}
			} else {
				if err := subseq.Advance(); err != nil {
					return NewSchemaError(ErrUnexpectedEOF, SchemaMapUnorderedName, "", pos, err)
				}
			}
		}
		if !s.OptionalMap {
			for key := range s.Fields {
				if !seen[key] {
					return NewSchemaError(ErrConstraintViolated, SchemaMapUnorderedName, "", pos, MissingKeyErrorDetails{Key: key})
				}
			}
		}

	}

	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, SchemaMapUnorderedName, "", pos, err)
	}
	return nil
}

func (s SchemaMapUnordered) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	typ, _, err := seq.PeekTypeWidth()
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", pos, err)
	}
	if typ != typetags.TypeMap {
		return nil, NewSchemaError(ErrConstraintViolated, SchemaMapUnorderedName, "", pos, ErrUnsupportedType)
	}

	var out map[string]any
	if len(s.Fields) > 0 {
		subseq, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", pos, err)
		}
		out = make(map[string]any, subseq.ArgCount()/2)

		for {
			keyPayload, keyType, err := subseq.Next()
			if keyType == typetags.TypeEnd {
				break
			}
			if err != nil {
				return nil, NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", pos, err)
			}
			if keyType != typetags.TypeString {
				return nil, NewSchemaError(ErrConstraintViolated, SchemaMapUnorderedName, "", pos, ErrUnsupportedType)
			}

			key := string(keyPayload)
			if validator, ok := s.Fields[key]; ok {
				val, err := validator.Decode(subseq)
				if err != nil {
					return nil, NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, key, pos, err)
				}
				out[key] = val
			} else {
				if err := subseq.Advance(); err != nil {
					return nil, NewSchemaError(ErrUnexpectedEOF, SchemaMapUnorderedName, "", pos, err)
				}
			}
		}
		if !s.OptionalMap {
			for key := range s.Fields {
				if _, ok := out[key]; !ok {
					return nil, NewSchemaError(ErrConstraintViolated, SchemaMapUnorderedName, "", pos, MissingKeyErrorDetails{Key: key})
				}
			}
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemaError(ErrUnexpectedEOF, SchemaMapUnorderedName, "", pos, err)
	}
	return out, nil
}

func (s SchemaMapUnordered) Encode(put *access.PutAccess, val any) error {

	if mapKV, ok := val.(map[string]any); ok {

		nested := put.BeginMap()
		defer put.EndNested(nested)
		ss := SString
		for key, sch := range s.Fields {
			if val, exist := mapKV[key]; exist {
				ss.Encode(nested, key)
				err := sch.Encode(nested, val)
				if err != nil {
					return NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, key, -1, err)
				}
			} else {
				return NewSchemaError(ErrInvalidFormat, SchemaMapUnorderedName, "", -1, MissingKeyErrorDetails{Key: key})
			}

		}

	} else {
		return NewSchemaError(ErrEncode, SchemaMapUnorderedName, "", -1, ErrTypeMisMatch)
	}
	return nil
}

type TupleSchema struct {
	Schemas        []Schema
	Nullable       bool
	VariableLength bool
	Flatten        bool
}

func STuple(Schema ...Schema) TupleSchema {
	return TupleSchema{Schemas: Schema, Nullable: true, VariableLength: false, Flatten: false}
}

func STupleVal(Schema ...Schema) TupleSchema {
	return TupleSchema{Schemas: Schema, Nullable: true, VariableLength: true, Flatten: false}
}

func STupleValFlatten(Schema ...Schema) TupleSchema {
	return TupleSchema{Schemas: Schema, Nullable: true, VariableLength: true, Flatten: true}
}

func (s TupleSchema) IsNullable() bool {
	return s.Nullable
}

func (s TupleSchema) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemaName, pos, seq, typetags.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return err
	}
	w := len(s.Schemas)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", pos, err)
		}
		if w > 0 && sub.ArgCount() != w && !s.VariableLength {
			return NewSchemaError(ErrConstraintViolated, TupleSchemaName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		for _, sch := range s.Schemas {
			if err := sch.Validate(sub); err != nil {
				return NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", pos, err)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, TupleSchemaName, "", pos, err)
	}
	return nil
}

func (s TupleSchema) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemaName, pos, seq, typetags.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return nil, err
	}
	var out []any
	w := len(s.Schemas)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", pos, err)
		}
		if w > 0 && sub.ArgCount() != w && !s.VariableLength {
			return nil, NewSchemaError(ErrConstraintViolated, TupleSchemaName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		out = make([]any, 0, sub.ArgCount())
		for _, sch := range s.Schemas {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", pos, err)
			}
			if s.Flatten {
				if _, ok := sch.(SRepeatSchema); ok {
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
		return nil, NewSchemaError(ErrUnexpectedEOF, TupleSchemaName, "", pos, err)
	}
	return out, nil
}

func (s TupleSchema) Encode(put *access.PutAccess, val any) error {

	if valArr, ok := val.([]any); ok {

		nested := put.BeginTuple()
		defer put.EndNested(nested)
		j := 0
		lastI := len(s.Schemas) - 1
		for k, sch := range s.Schemas {

			if schRet, ok := sch.(SRepeatSchema); ok {
				var err error
				if s.Flatten {
					if lastI != k {
						if schRet.max < 1 {
							return NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", -1, fmt.Errorf("max should be provided if repeat is not in the end. max: %d", schRet.max))
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
					return NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", -1, err)
				}

			} else {
				err := sch.Encode(nested, valArr[j])
				if err != nil {
					return NewSchemaError(ErrInvalidFormat, TupleSchemaName, "", -1, err)
				}
				j++
			}

		}

	} else {
		return NewSchemaError(ErrEncode, TupleSchemaName, "", -1, ErrTypeMisMatch)
	}
	return nil
}

type TupleSchemaNamed struct {
	Schemas        []Schema
	FieldNames     []string
	Nullable       bool
	Flatten        bool
	VariableLength bool
}

func STupleNamed(fieldNames []string, Schema ...Schema) TupleSchemaNamed {

	return TupleSchemaNamed{FieldNames: fieldNames, Schemas: Schema, Nullable: true}
}

// Strict named tuple: exact field count
func STupleNamedVal(fieldNames []string, Schema ...Schema) TupleSchemaNamed {
	return TupleSchemaNamed{
		FieldNames:     fieldNames,
		Schemas:        Schema,
		Nullable:       true,
		Flatten:        false,
		VariableLength: true,
	}
}

// Flexible named tuple: allows repeats/extra fields
func STupleNamedValFlattened(fieldNames []string, Schema ...Schema) TupleSchemaNamed {
	return TupleSchemaNamed{
		FieldNames:     fieldNames,
		Schemas:        Schema,
		Nullable:       true,
		Flatten:        true,
		VariableLength: true,
	}
}

func (s TupleSchemaNamed) IsNullable() bool {
	return s.Nullable
}

func (s TupleSchemaNamed) Validate(seq *access.SeqGetAccess) error {
	if len(s.FieldNames) != len(s.Schemas) {
		return NewSchemaError(ErrConstraintViolated, TupleSchemaNamedName, "", 0, SizeExact{Actual: len(s.FieldNames), Exact: len(s.Schemas)})
	}
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemaNamedName, pos, seq, typetags.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return err
	}
	w := len(s.Schemas)
	if w != 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, "", pos, err)
		}
		if w > 0 && sub.ArgCount() != w {
			return NewSchemaError(ErrConstraintViolated, TupleSchemaNamedName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		for _, sch := range s.Schemas {
			if err := sch.Validate(sub); err != nil {
				return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, "", pos, err)
			}
		}
	}
	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, TupleSchemaNamedName, "", pos, err)
	}
	return nil
}

func (s TupleSchemaNamed) Decode(seq *access.SeqGetAccess) (any, error) {
	if len(s.FieldNames) != len(s.Schemas) {
		return nil, NewSchemaError(ErrConstraintViolated, TupleSchemaNamedName, "", 0, SizeExact{Actual: len(s.FieldNames), Exact: len(s.Schemas)})
	}
	pos := seq.CurrentIndex()
	_, err := precheck(TupleSchemaNamedName, pos, seq, typetags.TypeTuple, -1, s.IsNullable())
	if err != nil {
		return nil, err
	}

	out := make(map[string]any)
	w := len(s.Schemas)
	if w > 0 {
		sub, err := seq.PeekNestedSeq()
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, "", pos, err)
		}
		if !s.VariableLength && sub.ArgCount() != w {
			return nil, NewSchemaError(ErrConstraintViolated, TupleSchemaNamedName, "", pos, SizeExact{Actual: w, Exact: sub.ArgCount()})
		}
		for i, sch := range s.Schemas {
			v, err := sch.Decode(sub)
			if err != nil {
				return nil, NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, s.FieldNames[i], pos, err)
			}
			if s.Flatten {
				if _, ok := sch.(SRepeatSchema); ok {
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
		return nil, NewSchemaError(ErrUnexpectedEOF, TupleSchemaNamedName, "", pos, err)
	}
	return out, nil
}

func (s TupleSchemaNamed) Encode(put *access.PutAccess, val any) error {
	if len(s.FieldNames) != len(s.Schemas) {
		return NewSchemaError(ErrConstraintViolated, TupleSchemaNamedName, "", 0, SizeExact{Actual: len(s.FieldNames), Exact: len(s.Schemas)})
	}
	if mapKV, ok := val.(map[string]any); ok {

		nested := put.BeginTuple()
		defer put.EndNested(nested)
		for i, key := range s.FieldNames {
			if sch, ok := s.Schemas[i].(SRepeatSchema); ok && s.Flatten {

				minx := sch.min
				max := sch.max
				j := 0
				schi := 0
				for ; j < minx; j++ {
					keyx := fmt.Sprintf("%s_%d", key, j)
					if val, exist := mapKV[keyx]; exist {
						err := sch.Schemas[schi].Encode(nested, val)
						if err != nil {
							return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, "", -1, err)
						}
					} else {
						return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, "", -1, MissingKeyErrorDetails{Key: keyx})
					}
					schi++
					if schi >= len(sch.Schemas) {
						schi = 0
					}
				}
				if max == -1 || max > minx {

					for {
						keyx := fmt.Sprintf("%s_%d", key, j)
						if val, exist := mapKV[keyx]; exist {
							err := sch.Schemas[schi].Encode(nested, val)
							if err != nil {
								return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, keyx, -1, err)
							}
							schi++
							if schi >= len(sch.Schemas) {
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
					err := s.Schemas[i].Encode(nested, val)
					if err != nil {
						return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, key, -1, err)
					}
				} else if s.Schemas[i].IsNullable() {
					//just add null tag and skip
					s.Schemas[i].Encode(put, nil)
				} else {
					return NewSchemaError(ErrInvalidFormat, TupleSchemaNamedName, "", -1, MissingKeyErrorDetails{Key: key})
				}
			}

		}

	} else {
		return NewSchemaError(ErrEncode, TupleSchemaNamedName, "", -1, ErrTypeMisMatch)
	}
	return nil
}

type SRepeatSchema struct {
	Schemas []Schema
	max     int
	min     int
}

func SRepeat(minimum int64, maximum int64, schemas ...Schema) SRepeatSchema {
	return SRepeatRange(&minimum, &maximum, schemas...)
}

func SRepeatRange(minimum *int64, maximum *int64, schemas ...Schema) SRepeatSchema {
	mmin := -1
	mmax := -1
	if minimum != nil && *minimum >= 0 {
		mmin = int(*minimum) * len(schemas)
	}
	if maximum != nil && *maximum >= 0 {
		mmax = int(*maximum) * len(schemas)
	}
	return SRepeatSchema{Schemas: schemas, min: mmin, max: mmax}
}

func (s SRepeatSchema) IsNullable() bool {
	return s.min <= 0
}

func (s SRepeatSchema) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	argCount := seq.ArgCount() - pos

	if s.min != -1 && argCount < s.min {
		return NewSchemaError(ErrConstraintViolated, SRepeatSchemaName, "", pos, RangeErrorDetails[int64]{
			Min:    PtrToInt64(s.min),
			Max:    PtrToInt64(s.max),
			Actual: int64(argCount),
		})

	}

	maxIter := argCount
	if s.max != -1 && s.max < argCount {
		maxIter = s.max
	}

	i := 0
outer:
	for {
		for _, schema := range s.Schemas {
			if err := schema.Validate(seq); err != nil {
				return NewSchemaError(ErrInvalidFormat, SRepeatSchemaName, "", pos, err)
			}
			if i >= maxIter {
				break outer
			}
			i++
		}
	}
	return nil
}

func (s SRepeatSchema) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	argCount := seq.ArgCount() - pos

	if s.min != -1 && argCount < s.min {
		return nil, NewSchemaError(ErrConstraintViolated, SRepeatSchemaName, "", pos,
			RangeErrorDetails[int64]{
				Min:    PtrToInt64(s.min),
				Max:    PtrToInt64(s.max),
				Actual: int64(argCount),
			})
	}

	maxIter := argCount
	if s.max != -1 && s.max < argCount {
		maxIter = s.max
	}

	out := make([]any, 0, maxIter)
	i := 0
outer:
	for {
		for _, schema := range s.Schemas {
			if i >= maxIter {
				break outer
			}
			val, err := schema.Decode(seq)
			if err != nil {
				return nil, NewSchemaError(ErrInvalidFormat, SRepeatSchemaName, "", pos, err)
			}
			out = append(out, val)
			i++
		}
	}
	return out, nil
}

func (s SRepeatSchema) Encode(put *access.PutAccess, val any) error {

	valArr, ok := val.([]any)
	if !ok {
		return NewSchemaError(ErrEncode, SRepeatSchemaName, "", -1, ErrTypeMisMatch)
	}
	argCount := len(valArr)
	if s.min != -1 && argCount < s.min {
		return NewSchemaError(ErrConstraintViolated, SRepeatSchemaName, "", -1, RangeErrorDetails[int64]{
			Min:    PtrToInt64(s.min),
			Max:    PtrToInt64(s.max),
			Actual: int64(argCount),
		})
	}
	maxIter := argCount
	if s.max != -1 && s.max < argCount {
		maxIter = s.max
	}
	i := 0
outer:
	for {
		for _, schema := range s.Schemas {
			if i >= maxIter {
				break outer
			}
			err := schema.Encode(put, valArr[i])
			if err != nil {
				return NewSchemaError(ErrEncode, SRepeatSchemaName, "", i, err)
			}
			i++
		}
	}
	return nil
}

// SchemaMultiCheckNamesSchema is a convenience schema: every field is a SchemaBool.
type SchemaMultiCheckNamesSchema struct {
	FieldNames []string
	Nullable   bool
}

func SMultiCheckNames(fieldNames []string) SchemaMultiCheckNamesSchema {
	return SchemaMultiCheckNamesSchema{
		FieldNames: fieldNames,
		Nullable:   true,
	}
}

func (s SchemaMultiCheckNamesSchema) IsNullable() bool {
	return s.Nullable
}

func (s SchemaMultiCheckNamesSchema) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	byteCount := (len(s.FieldNames) + 7) / 8

	// Direct primitive validation: expect a bytes value of exact width
	err := validatePrimitive(SchemaBytesName, seq, typetags.TypeString, byteCount, s.IsNullable())
	if err != nil {
		return NewSchemaError(ErrInvalidFormat, SchemaMultiCheckNamesSchemaNamed, "", pos, err)
	}

	return nil
}

func (s SchemaMultiCheckNamesSchema) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	byteCount := (len(s.FieldNames) + 7) / 8

	payload, err := validatePrimitiveAndGetPayload(SchemaMultiCheckNamesSchemaNamed, seq, typetags.TypeByteArray, byteCount, s.IsNullable())
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaMultiCheckNamesSchemaNamed, "", pos, err)
	}
	if payload == nil {
		if s.Nullable {
			return nil, nil // allow nullable
		} else {

			return nil, NewSchemaError(ErrInvalidFormat, SchemaMultiCheckNamesSchemaNamed, "", pos, nil)
		}
	}

	selected := make([]string, 0)
	for i, name := range s.FieldNames {
		byteIndex := i / 8
		bitIndex := uint(i % 8)
		if payload[byteIndex]&(1<<bitIndex) != 0 {
			selected = append(selected, name)
		}
	}

	return selected, nil
}

func (s SchemaMultiCheckNamesSchema) Encode(put *access.PutAccess, val any) error {

	set := make(map[string]struct{}, len(s.FieldNames))
	switch v := val.(type) {
	case string:
		set[v] = struct{}{}
	case []string:
		for _, name := range v {
			set[name] = struct{}{}
		}
	case []interface{}:
		for _, elem := range v {
			str, ok := elem.(string)
			if !ok {
				return NewSchemaError(ErrEncode, SchemaMultiCheckNamesSchemaNamed, "", -1, ErrTypeMisMatch)
			}
			set[str] = struct{}{}
		}
	default:
		return NewSchemaError(ErrEncode, SchemaMultiCheckNamesSchemaNamed, "", -1, ErrTypeMisMatch)
	}

	byteCount := (len(s.FieldNames) + 7) / 8
	buf := make([]byte, byteCount)

	for i, key := range s.FieldNames {
		if _, ok := set[key]; ok {
			byteIndex := i / 8
			bitIndex := uint(i % 8)
			buf[byteIndex] |= 1 << bitIndex
		}
	}
	put.AddBytes(buf)
	return nil
}

func (s SchemaString) Optional() SchemaString {
	s.Width = -1
	return s
}

func SEmail(optional bool) Schema {
	s := SString
	if optional {
		s = s.Optional()
	}
	return s.CheckFunc(
		ErrStringEmail,
		"email",
		func(payloadStr string) bool {
			// Use net/mail parser for RFC-compliant syntax check
			_, err := mail.ParseAddress(payloadStr)
			return err == nil
		},
	)
}

// SURI adds URI validation + normalization (prepend https:// if missing)
func SURI(optional bool) Schema {
	s := SString
	if optional {
		s.Optional()
	}
	return s.CheckFunc(
		ErrStringURL,
		"URI",
		func(payloadStr string) bool {
			// prepend https:// if missing
			if !strings.HasPrefix(payloadStr, "http://") && !strings.HasPrefix(payloadStr, "https://") {
				payloadStr = "https://" + payloadStr
			}
			parsed, err := url.ParseRequestURI(payloadStr)
			return err == nil && parsed.Host != ""
		},
	)
}

// SLang validates language codes using golang.org/x/text/language
func SLang(optional bool) Schema {
	s := SString
	if optional {
		s.Optional()
	}
	return s.CheckFunc(
		ErrStringLang, // define your own error type similar to ErrStringURL
		"Language Code",
		func(payloadStr string) bool {
			payloadStr = strings.TrimSpace(payloadStr)
			if len(payloadStr) != 2 {
				return false
			}

			// Try parsing with x/text/language
			tag, err := language.Parse(payloadStr)
			if err != nil {
				return false
			}

			_, conf := tag.Base()
			return conf != language.No

		},
	)
}

// SDate constrains an int64 payload to a date range (Unix seconds)
// and decodes into time.Time.

func SDate(nullable bool, from, to time.Time) Schema {
	return SDateRange(nullable, &from, &to)
}
func SDateRange(nullable bool, from, to *time.Time) Schema {
	var min, max *int64 = nil, nil
	if from != nil {
		min = PtrToInt64(from.Unix())
	}
	if to != nil {
		max = PtrToInt64(to.Unix())
	}

	return SchemaGeneric{
		ValidateFunc: func(seq *access.SeqGetAccess) error {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaDateName, seq, typetags.TypeInteger, 8, nullable)
			if err != nil {
				return err
			}
			if payload == nil {
				return nil // allow nullable
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			err = CheckIntRange(val, min, max)
			if err != nil {
				return NewSchemaError(ErrDateOutOfRange, SchemaDateName, "", pos, err)
			}
			return nil
		},
		DecodeFunc: func(seq *access.SeqGetAccess) (any, error) {
			pos := seq.CurrentIndex()
			payload, err := validatePrimitiveAndGetPayload(SchemaDateName, seq, typetags.TypeInteger, 8, nullable)
			if err != nil {
				return nil, err
			}
			if payload == nil {
				return nil, nil // allow nullable
			}
			val := int64(binary.LittleEndian.Uint64(payload))
			err = CheckIntRange(val, min, max)
			if err != nil {
				return nil, NewSchemaError(ErrDateOutOfRange, SchemaDateName, "", pos, err)
			}
			// decode as time.Time
			return time.Unix(val, 0).UTC(), nil
		},
		EncodeFunc: func(put *access.PutAccess, val any) error {
			if nullable && val == nil {
				put.AddNullableInt64(nil)
				return nil
			}
			var ret int64
			switch v := val.(type) {
			case int64:
				ret = v
			case time.Time:
				ret = v.Unix()

			default:
				return NewSchemaError(ErrEncode, SchemaDateName, "", -1, ErrTypeMisMatch)
			}
			err := CheckIntRange(ret, min, max)
			if err != nil {
				return NewSchemaError(ErrDateOutOfRange, SchemaDateName, "", -1, err)
			}
			put.AddInt64(ret)
			return nil
		},
	}
}

// SchemaEnumNamedList constrains an index to a list of names, encoded in 2 bytes.
// Perfect for radio groups or select dropdowns.
type SchemaEnumNamedList struct {
	FieldNames []string
	Nullable   bool
}

func SEnum(fieldNames []string, nullable bool) Schema {
	return SchemaEnumNamedList{FieldNames: fieldNames, Nullable: nullable}
}

func (s SchemaEnumNamedList) IsNullable() bool { return s.Nullable }

func (s SchemaEnumNamedList) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	payload, err := validatePrimitiveAndGetPayload(SchemaEnumNamedListName, seq, typetags.TypeInteger, 2, s.IsNullable())
	if err != nil {
		return err
	}
	if payload == nil {
		return nil
	}
	idx := int(binary.LittleEndian.Uint16(payload))
	if idx < 0 || idx >= len(s.FieldNames) {
		return NewSchemaError(ErrConstraintViolated, SchemaEnumNamedListName, "", pos,
			SizeExact{Actual: idx, Exact: len(s.FieldNames)})
	}
	return nil
}

func (s SchemaEnumNamedList) Decode(seq *access.SeqGetAccess) (any, error) {
	payload, err := validatePrimitiveAndGetPayload(SchemaEnumNamedListName, seq, typetags.TypeInteger, 2, s.IsNullable())
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	idx := int(binary.LittleEndian.Uint16(payload))
	if idx < 0 || idx >= len(s.FieldNames) {
		return nil, NewSchemaError(ErrConstraintViolated, SchemaEnumNamedListName, "", -1,
			SizeExact{Actual: idx, Exact: len(s.FieldNames)})
	}
	return s.FieldNames[idx], nil // return the name string
}

func (s SchemaEnumNamedList) Encode(put *access.PutAccess, val any) error {
	if val == nil && s.Nullable {
		put.AddNullableInt16(nil)
		return nil
	}

	var idx int
	switch v := val.(type) {
	case int:
		idx = v
	case string:
		idx = -1
		for i, name := range s.FieldNames {
			if name == v {
				idx = i
				break
			}
		}
		if idx == -1 {
			return NewSchemaError(ErrEncode, SchemaEnumNamedListName, "", -1, ErrTypeMisMatch)
		}
	default:
		return NewSchemaError(ErrEncode, SchemaEnumNamedListName, "", -1, ErrTypeMisMatch)
	}

	if idx < 0 || idx >= len(s.FieldNames) {
		return NewSchemaError(ErrEncode, SchemaEnumNamedListName, "", -1,
			SizeExact{Actual: idx, Exact: len(s.FieldNames)})
	}

	put.AddInt16(int16(idx))
	return nil
}

func SColor(nullable bool) Schema {
	s := SString
	if nullable {
		s = s.Optional()
	}
	return s.Pattern(`^#(?:[0-9a-fA-F]{3}){1,2}$`)
}

type SchemaMapRepeat struct {
	Key   Schema
	Value Schema
	min   int
	max   int
}

func SMapRepeat(key Schema, value Schema) SchemaMapRepeat {
	return SchemaMapRepeat{Key: key, Value: value, min: -1, max: -1}
}

func SMapRepeatRange(key Schema, value Schema, minimum, maximum *int64) SchemaMapRepeat {
	mmin := -1
	mmax := -1
	if minimum != nil && *minimum >= 0 {
		mmin = int(*minimum)
	}
	if maximum != nil && *maximum >= 0 {
		mmax = int(*maximum)
	}
	return SchemaMapRepeat{Key: key, Value: value, min: mmin, max: mmax}
}

func (s SchemaMapRepeat) IsNullable() bool {
	return s.min <= 0
}

func (s SchemaMapRepeat) Validate(seq *access.SeqGetAccess) error {
	pos := seq.CurrentIndex()
	_, err := precheck(SchemaMapRepeatName, pos, seq, typetags.TypeMap, -1, s.IsNullable())
	if err != nil {
		return err
	}
	subseq, err := seq.PeekNestedSeq()
	if err != nil {
		return NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos, err)
	}
	pairCount := subseq.ArgCount() / 2
	maxIter := pairCount
	if s.max != -1 && s.max < pairCount {
		maxIter = s.max
	}
	if s.min != -1 && pairCount < s.min {
		return NewSchemaError(ErrConstraintViolated, SchemaMapRepeatName, "", pos,
			RangeErrorDetails[int64]{
				Min:    PtrToInt64(s.min),
				Max:    PtrToInt64(s.max),
				Actual: int64(pairCount),
			})
	}

	for i := 0; i < maxIter; i++ {

		if err := s.Key.Validate(subseq); err != nil {
			return NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos, err)
		}
		if err := s.Value.Validate(subseq); err != nil {
			return NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos, err)
		}
	}

	if err := seq.Advance(); err != nil {
		return NewSchemaError(ErrUnexpectedEOF, SchemaMapRepeatName, "", pos, err)
	}
	return nil
}

func (s SchemaMapRepeat) Decode(seq *access.SeqGetAccess) (any, error) {
	pos := seq.CurrentIndex()
	_, err := precheck(SchemaMapRepeatName, pos, seq, typetags.TypeMap, 0, s.IsNullable())
	if err != nil {
		return nil, err
	}
	subseq, err := seq.PeekNestedSeq()
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos, err)
	}
	pairCount := subseq.ArgCount() / 2
	maxIter := pairCount
	if s.max != -1 && s.max < pairCount {
		maxIter = s.max
	}
	if s.min != -1 && pairCount < s.min {
		return nil, NewSchemaError(ErrConstraintViolated, SchemaMapRepeatName, "", pos,
			RangeErrorDetails[int64]{
				Min:    PtrToInt64(s.min),
				Max:    PtrToInt64(s.max),
				Actual: int64(pairCount),
			})
	}
	out := make(map[string]any)
	for i := 0; i < maxIter; i++ {
		k, err := s.Key.Decode(subseq)
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos, err)
		}
		v, err := s.Value.Decode(subseq)
		if err != nil {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos, err)
		}
		if keyStr, ok := k.(string); ok {
			out[keyStr] = v
		} else {
			return nil, NewSchemaError(ErrInvalidFormat, SchemaMapRepeatName, "", pos-1, ErrUnsupportedType)
		}
	}

	if err := seq.Advance(); err != nil {
		return nil, NewSchemaError(ErrUnexpectedEOF, SchemaMapRepeatName, "", pos, err)
	}
	return out, nil
}

func (s SchemaMapRepeat) Encode(put *access.PutAccess, val any) error {
	mapKV, ok := val.(map[string]any)
	if !ok {
		return NewSchemaError(ErrEncode, SchemaMapRepeatName, "", -1, ErrTypeMisMatch)
	}

	nested := put.BeginMap()
	defer put.EndNested(nested)

	count := 0
	for key, v := range mapKV {
		// Encode key
		if err := s.Key.Encode(nested, key); err != nil {
			return NewSchemaError(ErrEncode, SchemaMapRepeatName, key, -1, err)
		}
		// Encode value
		if err := s.Value.Encode(nested, v); err != nil {
			return NewSchemaError(ErrEncode, SchemaMapRepeatName, key, -1, err)
		}
		count++
	}

	if s.min != -1 && count < s.min {
		return NewSchemaError(ErrConstraintViolated, SchemaMapRepeatName, "", -1,
			RangeErrorDetails[int64]{
				Min:    PtrToInt64(s.min),
				Max:    PtrToInt64(s.max),
				Actual: int64(count),
			})
	}
	if s.max != -1 && count > s.max {
		return NewSchemaError(ErrConstraintViolated, SchemaMapRepeatName, "", -1,
			RangeErrorDetails[int64]{
				Min:    PtrToInt64(s.min),
				Max:    PtrToInt64(s.max),
				Actual: int64(count),
			})
	}

	return nil
}

type SchemaNumber struct {
	DecodeAsString bool
	Min            *float64
	Max            *float64
}

func (s SchemaNumber) IsNullable() bool {
	return true
}

func (s SchemaNumber) DecodeValidate(seq *access.SeqGetAccess, decodeAlways bool) (any, error) {
	pos := seq.CurrentIndex()
	valPayload, valTyp, err := seq.Next()
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaNumberName, "", pos, err)
	}
	if valTyp != typetags.TypeInteger && valTyp != typetags.TypeFloating {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaNumberName, "", pos, ErrUnsupportedType)
	}

	// If no range constraints and not decoding, skip decodePrimitive entirely
	if s.Min == nil && s.Max == nil && !decodeAlways {
		return nil, nil
	}

	v, err := access.DecodePrimitive(valTyp, valPayload)
	if err != nil {
		return nil, NewSchemaError(ErrInvalidFormat, SchemaNumberName, "", pos, err)
	}

	// Handle nil value
	if v == nil {
		if s.DecodeAsString {
			return "", nil
		}
		return nil, nil
	}

	// Normalize to float64
	f, ok := convertToNumber[float64](v)
	if !ok {
		return v, nil
	}

	// Range check if constraints exist
	if s.Min != nil || s.Max != nil {
		if err := CheckFloatRange(f, s.Min, s.Max); err != nil {
			return nil, NewSchemaError(ErrOutOfRange, SchemaNumberName, "", pos, err)
		}
	}

	if s.DecodeAsString {
		return fmt.Sprintf("%v", f), nil
	}
	return f, nil
}

func (s SchemaNumber) Validate(seq *access.SeqGetAccess) error {
	_, err := s.DecodeValidate(seq, false)
	return err
}

func (s SchemaNumber) Decode(seq *access.SeqGetAccess) (any, error) {
	return s.DecodeValidate(seq, true)
}

func (s SchemaNumber) Encode(put *access.PutAccess, val any) error {
	if val == nil {
		put.AddNullableInt64(nil)
		return nil
	}

	// Handle empty string first
	if str, ok := val.(string); ok && str == "" {
		put.AddNullableInt64(nil)
		return nil
	}

	f, ok := convertToNumber[float64](val)
	if !ok {
		return NewSchemaError(ErrEncode, SchemaNumberName, "", -1, ErrUnsupportedType)
	}

	if err := CheckFloatRange(f, s.Min, s.Max); err != nil {
		return NewSchemaError(ErrOutOfRange, SchemaNumberName, "", -1, err)
	}

	put.AddNumeric(f)
	return nil
}
