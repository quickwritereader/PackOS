package scheme

import (
	"fmt"

	"github.com/BranchAndLink/packos/access"
	"github.com/BranchAndLink/packos/types"
)

type ValidationState struct {
	Seq *access.SeqGetAccess
	Err error
}

type Scheme interface {
	Validate(state ValidationState) ValidationState
}

type SchemeFunc func(state ValidationState) ValidationState

func (f SchemeFunc) Validate(state ValidationState) ValidationState {
	return f(state)
}

type SchemeBool struct{}

func (SchemeBool) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeBool, 1, false)
}

type SchemeInt8 struct{}

func (SchemeInt8) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 2, false)
}

type SchemeInt16 struct{}

func (SchemeInt16) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 2, false)
}

type SchemeInt32 struct{}

func (SchemeInt32) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 4, false)
}

type SchemeInt64 struct{}

func (SchemeInt64) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 8, false)
}

type SchemeFloat32 struct{}

func (SchemeFloat32) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeFloating, 4, false)
}

type SchemeFloat64 struct{}

func (SchemeFloat64) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeFloating, 8, false)
}

type SchemeString struct{ Width int }

func (s SchemeString) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeString, s.Width, s.IsNullable())
}

type SchemeBytes struct{ Width int }

func (s SchemeBytes) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeString, s.Width, s.IsNullable())
}

type SchemeMap struct {
	Width  int
	Schema []Scheme
}

func (s SchemeMap) Validate(state ValidationState) ValidationState {
	if state.Err != nil {
		return state
	}
	pos := state.Seq.CurrentIndex()
	typ, width, err := state.Seq.PeekTypeWidth()
	if err != nil {
		state.Err = fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
		return state
	}
	if typ != types.TypeMap {
		state.Err = fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", pos, types.TypeMap, typ)
		return state
	}
	nullable := s.IsNullable()
	hint := s.Width
	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			state.Err = fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", pos, hint, width)
			return state
		}
	}
	sub, err := state.Seq.PeekNestedSeq()
	if err != nil {
		state.Err = fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", pos, err)
		return state
	}
	subState := ValidationState{Seq: sub}
	for _, sch := range s.Schema {
		subState = sch.Validate(subState)
		if subState.Err != nil {
			state.Err = fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", pos, subState.Err)
			return state
		}
	}
	if err := state.Seq.Advance(); err != nil {
		state.Err = fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
		return state
	}
	return state
}

type SchemeTypeOnly struct {
	Tag types.Type
}

func (s SchemeTypeOnly) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, s.Tag, -1, false)
}

type Nullable interface {
	IsNullable() bool
}

// Nullable Primitives

type SchemeNullableBool struct{}

func (SchemeNullableBool) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeBool, 1, true)
}

type SchemeNullableInt8 struct{}

func (SchemeNullableInt8) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 2, true)
}

type SchemeNullableInt16 struct{}

func (SchemeNullableInt16) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 2, true)
}

type SchemeNullableInt32 struct{}

func (SchemeNullableInt32) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 4, true)
}

type SchemeNullableInt64 struct{}

func (SchemeNullableInt64) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeInteger, 8, true)
}

type SchemeNullableFloat32 struct{}

func (SchemeNullableFloat32) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeFloating, 4, true)
}

type SchemeNullableFloat64 struct{}

func (SchemeNullableFloat64) Validate(state ValidationState) ValidationState {
	return validatePrimitive(state, types.TypeFloating, 8, true)
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

func SBool() Scheme        { return SchemeBool{} }
func SInt8() Scheme        { return SchemeInt8{} }
func SInt16() Scheme       { return SchemeInt16{} }
func SInt32() Scheme       { return SchemeInt32{} }
func SInt64() Scheme       { return SchemeInt64{} }
func SFloat32() Scheme     { return SchemeFloat32{} }
func SFloat64() Scheme     { return SchemeFloat64{} }
func SNullBool() Scheme    { return SchemeNullableBool{} }
func SNullInt8() Scheme    { return SchemeNullableInt8{} }
func SNullInt16() Scheme   { return SchemeNullableInt16{} }
func SNullInt32() Scheme   { return SchemeNullableInt32{} }
func SNullInt64() Scheme   { return SchemeNullableInt64{} }
func SNullFloat32() Scheme { return SchemeNullableFloat32{} }
func SNullFloat64() Scheme { return SchemeNullableFloat64{} }

func SString(width int) Scheme { return SchemeString{Width: width} }
func SBytes(width int) Scheme  { return SchemeBytes{Width: width} }

func SMap(nested ...Scheme) Scheme {
	return SchemeMap{
		Width:  -1,
		Schema: nested,
	}
}

func SMapSorted(nested ...Scheme) Scheme {
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
	state := ValidationState{Seq: seq}
	for _, scheme := range args {
		state = scheme.Validate(state)
		if state.Err != nil {
			return state.Err
		}
	}
	return nil
}

// Helper for primitive validation
func validatePrimitive(state ValidationState, tag types.Type, hint int, nullable bool) ValidationState {
	if state.Err != nil {
		return state
	}
	pos := state.Seq.CurrentIndex()
	typ, width, err := state.Seq.PeekTypeWidth()
	if err != nil {
		state.Err = fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", pos, err)
		return state
	}
	if typ != tag {
		state.Err = fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", pos, tag, typ)
		return state
	}
	if hint >= 0 && width != hint {
		if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
			state.Err = fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", pos, hint, width)
			return state
		}
	}
	if err := state.Seq.Advance(); err != nil {
		state.Err = fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", pos, err)
		return state
	}
	return state
}

type SchemeChain struct {
	Schemes []Scheme
}

func SChain(schemes ...Scheme) SchemeChain {
	return SchemeChain{Schemes: schemes}
}

// Validate applies each Scheme in sequence, short-circuiting on error
func (sc SchemeChain) Validate(state ValidationState) ValidationState {
	for _, s := range sc.Schemes {
		state = s.Validate(state)
		if state.Err != nil {
			return state
		}
	}
	return state
}

func SStringExact(expected string) Scheme {
	return SchemeFunc(func(state ValidationState) ValidationState {
		if state.Err != nil {
			return state
		}
		pos := state.Seq.CurrentIndex()

		payload, typ, err := state.Seq.Next()
		if err != nil {
			state.Err = fmt.Errorf("ValidateBuffer: next failed at pos %d: %w", pos, err)
			return state
		}
		if typ != types.TypeString {
			state.Err = fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected TypeString, got %v", pos, typ)
			return state
		}
		if string(payload) != expected {
			state.Err = fmt.Errorf("ValidateBuffer: string mismatch at pos %d — expected '%s', got '%s'", pos, expected, string(payload))
			return state
		}
		return state
	})
}
