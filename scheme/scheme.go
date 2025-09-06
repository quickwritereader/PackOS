package scheme

import (
	"fmt"

	"github.com/BranchAndLink/paosp/access"
	"github.com/BranchAndLink/paosp/types"
)

type Scheme interface {
	TypeTag() types.Type // declared type tag
	WidthHint() int      // expected payload width (0 for nil)
	HasNested() bool     // true if this field contains a nested layout

}
type SchemeInt16 struct{}

func (SchemeInt16) TypeTag() types.Type { return types.TypeInt16 }
func (SchemeInt16) WidthHint() int      { return 2 }
func (SchemeInt16) HasNested() bool     { return false }

type SchemeInt32 struct{}

func (SchemeInt32) TypeTag() types.Type { return types.TypeInt32 }
func (SchemeInt32) WidthHint() int      { return 4 }
func (SchemeInt32) HasNested() bool     { return false }

type SchemeInt64 struct{}

func (SchemeInt64) TypeTag() types.Type { return types.TypeInt64 }
func (SchemeInt64) WidthHint() int      { return 8 }
func (SchemeInt64) HasNested() bool     { return false }

type SchemeFloat32 struct{}

func (SchemeFloat32) TypeTag() types.Type { return types.TypeFloat32 }
func (SchemeFloat32) WidthHint() int      { return 4 }
func (SchemeFloat32) HasNested() bool     { return false }

type SchemeFloat64 struct{}

func (SchemeFloat64) TypeTag() types.Type { return types.TypeFloat64 }
func (SchemeFloat64) WidthHint() int      { return 8 }
func (SchemeFloat64) HasNested() bool     { return false }

type SchemeString struct{ Width int }

func (s SchemeString) TypeTag() types.Type { return types.TypeString }
func (s SchemeString) WidthHint() int      { return s.Width }
func (s SchemeString) HasNested() bool     { return false }

type SchemeBytes struct{ Width int }

func (s SchemeBytes) TypeTag() types.Type { return types.TypeString }
func (s SchemeBytes) WidthHint() int      { return s.Width }
func (s SchemeBytes) HasNested() bool     { return false }

type SchemeMap struct {
	Width  int
	Schema []Scheme
}

func (s SchemeMap) TypeTag() types.Type { return types.TypeMap }
func (s SchemeMap) WidthHint() int      { return s.Width }
func (s SchemeMap) HasNested() bool     { return len(s.Schema) > 0 }
func (s SchemeMap) Nested() []Scheme    { return s.Schema }

type SchemeTypeOnly struct {
	Tag types.Type
}

func (s SchemeTypeOnly) TypeTag() types.Type { return s.Tag }
func (s SchemeTypeOnly) WidthHint() int      { return -1 } // sentinel
func (s SchemeTypeOnly) HasNested() bool     { return false }

type Nullable interface {
	IsNullable() bool
}

// Nullable Primitives
type SchemeNullableInt16 struct{}

func (SchemeNullableInt16) TypeTag() types.Type { return types.TypeInt16 }
func (SchemeNullableInt16) WidthHint() int      { return 2 }
func (SchemeNullableInt16) HasNested() bool     { return false }
func (SchemeNullableInt16) IsNullable() bool    { return true }

type SchemeNullableInt32 struct{}

func (SchemeNullableInt32) TypeTag() types.Type { return types.TypeInt32 }
func (SchemeNullableInt32) WidthHint() int      { return 4 }
func (SchemeNullableInt32) HasNested() bool     { return false }
func (SchemeNullableInt32) IsNullable() bool    { return true }

type SchemeNullableInt64 struct{}

func (SchemeNullableInt64) TypeTag() types.Type { return types.TypeInt64 }
func (SchemeNullableInt64) WidthHint() int      { return 8 }
func (SchemeNullableInt64) HasNested() bool     { return false }
func (SchemeNullableInt64) IsNullable() bool    { return true }

type SchemeNullableFloat32 struct{}

func (SchemeNullableFloat32) TypeTag() types.Type { return types.TypeFloat32 }
func (SchemeNullableFloat32) WidthHint() int      { return 4 }
func (SchemeNullableFloat32) HasNested() bool     { return false }
func (SchemeNullableFloat32) IsNullable() bool    { return true }

type SchemeNullableFloat64 struct{}

func (SchemeNullableFloat64) TypeTag() types.Type { return types.TypeFloat64 }
func (SchemeNullableFloat64) WidthHint() int      { return 8 }
func (SchemeNullableFloat64) HasNested() bool     { return false }
func (SchemeNullableFloat64) IsNullable() bool    { return true }

// All others default to non-nullable
func (SchemeInt16) IsNullable() bool      { return false }
func (SchemeInt32) IsNullable() bool      { return false }
func (SchemeInt64) IsNullable() bool      { return false }
func (SchemeFloat32) IsNullable() bool    { return false }
func (SchemeFloat64) IsNullable() bool    { return false }
func (s SchemeString) IsNullable() bool   { return s.Width <= 0 }
func (s SchemeBytes) IsNullable() bool    { return s.Width <= 0 }
func (s SchemeMap) IsNullable() bool      { return s.Width <= 0 }
func (s SchemeTypeOnly) IsNullable() bool { return false }

func SType(tag types.Type) Scheme {
	return SchemeTypeOnly{Tag: tag}
}

func SInt16() Scheme   { return SchemeInt16{} }
func SInt32() Scheme   { return SchemeInt32{} }
func SInt64() Scheme   { return SchemeInt64{} }
func SFloat32() Scheme { return SchemeFloat32{} }
func SFloat64() Scheme { return SchemeFloat64{} }

func SNullInt16() Scheme   { return SchemeNullableInt16{} }
func SNullInt32() Scheme   { return SchemeNullableInt32{} }
func SNullInt64() Scheme   { return SchemeNullableInt64{} }
func SNullFloat32() Scheme { return SchemeNullableFloat32{} }
func SNullFloat64() Scheme { return SchemeNullableFloat64{} }

func SString(width int) Scheme { return SchemeString{Width: width} }
func SBytes(width int) Scheme  { return SchemeBytes{Width: width} }

func SMap(width int, nested ...Scheme) Scheme {
	return SchemeMap{
		Width:  width,
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
		return fmt.Errorf("ValidateBuffer: failed to initialize accessor")
	}

	for i := 0; i < len(args); i++ {
		scheme := args[i]

		typ, width, err := seq.PeekTypeWidth()
		if err != nil {
			return fmt.Errorf("ValidateBuffer: peek failed at pos %d: %w", i, err)
		}
		if typ != scheme.TypeTag() {
			return fmt.Errorf("ValidateBuffer: type mismatch at pos %d — expected %v, got %v", i, scheme.TypeTag(), typ)
		}

		nullable := false
		if n, ok := scheme.(interface{ IsNullable() bool }); ok {
			nullable = n.IsNullable()
		}

		hint := scheme.WidthHint()
		if hint >= 0 && width != hint {
			if !(nullable && (hint == 0 || hint == -1 || width == 0)) {
				return fmt.Errorf("ValidateBuffer: width mismatch at pos %d — expected %d, got %d", i, hint, width)
			}
			// width mismatch is allowed due to nullability or width omission
		}

		if scheme.HasNested() {
			nested, ok := scheme.(interface{ Nested() []Scheme })
			if !ok {
				return fmt.Errorf("ValidateBuffer: invalid nested scheme at pos %d", i)
			}
			sub, err := seq.PeekNestedSeq()
			if err != nil {
				return fmt.Errorf("ValidateBuffer: nested peek failed at pos %d: %w", i, err)
			}
			if err := ValidateBuffer(sub.UnderlineBuffer(), nested.Nested()...); err != nil {
				return fmt.Errorf("ValidateBuffer: nested validation failed at pos %d: %w", i, err)
			}
		}

		if err := seq.Advance(); err != nil {
			return fmt.Errorf("ValidateBuffer: advance failed at pos %d: %w", i, err)
		}
	}

	return nil
}
