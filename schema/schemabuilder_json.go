package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type SchemaJSON struct {
	Type           string       `json:"type"`
	FieldNames     []string     `json:"fieldNames,omitempty"`
	Schema         []SchemaJSON `json:"schema,omitempty"`
	Nullable       bool         `json:"nullable,omitempty"`
	VariableLength bool         `json:"variableLength,omitempty"`
	Flatten        bool         `json:"flatten,omitempty"`

	// Constraint helpers
	Width         int    `json:"width,omitempty"`
	Min           *int64 `json:"min,omitempty"`
	Max           *int64 `json:"max,omitempty"`
	Exact         string `json:"exact,omitempty"`
	Prefix        string `json:"prefix,omitempty"`
	Suffix        string `json:"suffix,omitempty"`
	Pattern       string `json:"pattern,omitempty"`
	DateFrom      string `json:"dateFrom,omitempty"`
	DateTo        string `json:"dateTo,omitempty"`
	DecodeDefault string `json:"decodeDefault,omitempty"`

	// Extra metadata for UI or other purposes
	Extra map[string]any `json:"extra,omitempty"`
}

// Registry of custom schema builders.
// Key: type name (case-sensitive), Value: builder function.
var customSchemaBuilders = map[string]func(*SchemaJSON) (Schema, error){}

var schemaJSONKeys = map[string]bool{
	"type":           true,
	"fieldNames":     true,
	"schema":         true,
	"nullable":       true,
	"variableLength": true,
	"flatten":        true,
	"width":          true,
	"min":            true,
	"max":            true,
	"exact":          true,
	"prefix":         true,
	"suffix":         true,
	"pattern":        true,
	"dateFrom":       true,
	"dateTo":         true,
	"decodeDefault":  true,
	"extra":          true,
}

var (
	ErrEmptyTypeName         = errors.New("cannot register empty type name")
	ErrTypeNameConflict      = errors.New("cannot register type name that conflicts with schema field")
	ErrTypeAlreadyRegistered = errors.New("schema type already registered")
)

// RegisterSchemaType registers a custom Schema builder for a given type name.
//
// Usage:
//
//	err := schema.RegisterSchemaType("MyCustomType", func(js *schema.SchemaJSON) (schema.Schema, error) {
//	    // Build your own Schema based on js
//	    return SString.WithWidth(js.Width), nil
//	})
//
// Notes:
//   - Type names are case-sensitive ("MyCustomType" ≠ "mycustomtype").
//   - Returns an error if the type name is empty, already registered, or conflicts with schema field keys.
//   - Use UnregisterSchemaType to remove a custom type.
//
// This allows users to extend BuildSchema with their own typetags without
// modifying the core switch.
func RegisterSchemaType(typeName string, builder func(*SchemaJSON) (Schema, error)) error {
	if typeName == "" {
		return ErrEmptyTypeName
	}
	if schemaJSONKeys[typeName] {
		return fmt.Errorf("%w: %s", ErrTypeNameConflict, typeName)
	}
	if _, exists := customSchemaBuilders[typeName]; exists {
		return fmt.Errorf("%w: %s", ErrTypeAlreadyRegistered, typeName)
	}
	customSchemaBuilders[typeName] = builder
	return nil
}

// UnregisterSchemaType removes a previously registered custom Schema builder.
//
// Usage:
//
//	schema.UnregisterSchemaType("MyCustomType")
//
// If the type name is not found, the function does nothing.
func UnregisterSchemaType(typeName string) {
	delete(customSchemaBuilders, typeName)
}

// convert *int64 bounds to *uint64 safely
func toUint64Bounds(min, max *int64) (umin, umax *uint64) {
	if min != nil && *min >= 0 {
		v := uint64(*min)
		umin = &v
	}
	if max != nil && *max >= 0 {
		v := uint64(*max)
		umax = &v
	}
	return
}

// Register all built-in schema types at init
func init() {
	RegisterSchemaType("bool", func(js *SchemaJSON) (Schema, error) {
		if js.Nullable {
			return SNullBool, nil
		}
		return SBool, nil
	})

	RegisterSchemaType("int8", func(js *SchemaJSON) (Schema, error) {
		if js.Nullable {
			return SNullInt8, nil
		}
		return SInt8, nil
	})

	RegisterSchemaType("int16", func(js *SchemaJSON) (Schema, error) {
		s := SInt16
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			return s.Range(js.Min, js.Max), nil
		}
		return s, nil
	})

	RegisterSchemaType("int32", func(js *SchemaJSON) (Schema, error) {
		s := SInt32
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			return s.Range(js.Min, js.Max), nil
		}
		return s, nil
	})

	RegisterSchemaType("int64", func(js *SchemaJSON) (Schema, error) {
		s := SInt64
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			return s.Range(js.Min, js.Max), nil
		}
		return s, nil
	})

	RegisterSchemaType("uint8", func(js *SchemaJSON) (Schema, error) {
		s := SUint8
		if js.Nullable {
			s.Nullable = true
		}
		return s, nil
	})

	RegisterSchemaType("uint16", func(js *SchemaJSON) (Schema, error) {
		s := SUint16
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			umin, umax := toUint64Bounds(js.Min, js.Max)
			return s.Range(umin, umax), nil
		}
		return s, nil
	})

	RegisterSchemaType("uint32", func(js *SchemaJSON) (Schema, error) {
		s := SUint32
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			umin, umax := toUint64Bounds(js.Min, js.Max)
			return s.Range(umin, umax), nil
		}
		return s, nil
	})

	RegisterSchemaType("uint64", func(js *SchemaJSON) (Schema, error) {
		s := SUint64
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			umin, umax := toUint64Bounds(js.Min, js.Max)
			return s.Range(umin, umax), nil
		}
		return s, nil
	})

	RegisterSchemaType("date", func(js *SchemaJSON) (Schema, error) {
		if js.DateFrom != "" && js.DateTo != "" {
			from, err1 := time.Parse(time.RFC3339, js.DateFrom)
			to, err2 := time.Parse(time.RFC3339, js.DateTo)
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("invalid date range: %v %v", err1, err2)
			}
			return SDateRange(js.Nullable, &from, &to), nil
		}
		return SDateRange(js.Nullable, nil, nil), nil
	})

	RegisterSchemaType("float32", func(js *SchemaJSON) (Schema, error) {
		if js.Nullable {
			return SNullFloat32, nil
		}
		return SFloat32, nil
	})

	RegisterSchemaType("float64", func(js *SchemaJSON) (Schema, error) {
		if js.Nullable {
			return SNullFloat64, nil
		}
		return SFloat64, nil
	})

	RegisterSchemaType("string", func(js *SchemaJSON) (Schema, error) {
		s := SString
		if js.Nullable {
			s = s.Optional()
		} else if js.Width > 0 {
			s = s.WithWidth(js.Width)
		}
		if js.DecodeDefault != "" {
			s = s.DefaultDecodeValue(js.DecodeDefault)
		}
		if js.Exact != "" {
			return s.Match(js.Exact), nil
		}
		if js.Prefix != "" {
			return s.Prefix(js.Prefix), nil
		}
		if js.Suffix != "" {
			return s.Suffix(js.Suffix), nil
		}
		if js.Pattern != "" {
			return s.Pattern(js.Pattern), nil
		}
		return s, nil
	})

	RegisterSchemaType("email", func(js *SchemaJSON) (Schema, error) {
		return SEmail(js.Nullable), nil
	})
	RegisterSchemaType("uri", func(js *SchemaJSON) (Schema, error) {
		return SURI(js.Nullable), nil
	})
	RegisterSchemaType("lang", func(js *SchemaJSON) (Schema, error) {
		return SLang(js.Nullable), nil
	})

	RegisterSchemaType("bytes", func(js *SchemaJSON) (Schema, error) {
		if js.Width > 0 {
			return SBytes(js.Width), nil
		}
		return SVariableBytes(), nil
	})

	RegisterSchemaType("number", func(js *SchemaJSON) (Schema, error) {
		var xmin, xmax *float64
		if js.Min != nil {
			v := float64(*js.Min)
			xmin = &v
		}
		if js.Max != nil {
			v := float64(*js.Max)
			xmax = &v
		}
		return SchemaNumber{false, xmin, xmax}, nil
	})

	RegisterSchemaType("numberString", func(js *SchemaJSON) (Schema, error) {
		var xmin, xmax *float64
		if js.Min != nil {
			v := float64(*js.Min)
			xmin = &v
		}
		if js.Max != nil {
			v := float64(*js.Max)
			xmax = &v
		}
		return SchemaNumber{true, xmin, xmax}, nil
	})

	RegisterSchemaType("any", func(js *SchemaJSON) (Schema, error) {
		return SchemaAny{}, nil
	})

	RegisterSchemaType("tuple", func(js *SchemaJSON) (Schema, error) {
		schemas, err := buildSchemas(js.Schema)
		if err != nil {
			return nil, err
		}
		if len(js.FieldNames) > 0 {
			if js.VariableLength && js.Flatten {
				return STupleNamedValFlattened(js.FieldNames, schemas...), nil
			} else if js.VariableLength {
				return STupleNamedVal(js.FieldNames, schemas...), nil
			}
			return STupleNamed(js.FieldNames, schemas...), nil
		}
		if js.VariableLength && js.Flatten {
			return STupleValFlatten(schemas...), nil
		} else if js.VariableLength {
			return STupleVal(schemas...), nil
		}
		return STuple(schemas...), nil
	})

	RegisterSchemaType("repeat", func(js *SchemaJSON) (Schema, error) {
		schemas, err := buildSchemas(js.Schema)
		if err != nil {
			return nil, err
		}
		return SRepeatRange(js.Min, js.Max, schemas...), nil
	})

	RegisterSchemaType("map", func(js *SchemaJSON) (Schema, error) {
		schemas, err := buildSchemas(js.Schema)
		if err != nil {
			return nil, err
		}
		return SMap(schemas...), nil
	})

	RegisterSchemaType("mapUnordered", func(js *SchemaJSON) (Schema, error) {
		mapped := make(map[string]Schema)
		for i := range js.Schema {
			s, err := BuildSchema(&js.Schema[i])
			if err != nil {
				return nil, err
			}
			mapped[js.FieldNames[i]] = s
		}
		if js.Nullable {
			return SMapUnorderedOptional(mapped), nil
		}
		return SMapUnordered(mapped), nil
	})

	RegisterSchemaType("mapRepeat", func(js *SchemaJSON) (Schema, error) {
		if len(js.Schema) == 2 {
			s0, err0 := BuildSchema(&js.Schema[0])
			if err0 != nil {
				return nil, err0
			}
			s1, err1 := BuildSchema(&js.Schema[1])
			if err1 != nil {
				return nil, err1
			}
			return SMapRepeatRange(s0, s1, js.Min, js.Max), nil
		}
		return nil, fmt.Errorf("mapRepeat requires 2 schemas, got %d", len(js.Schema))
	})

	RegisterSchemaType("multicheck", func(js *SchemaJSON) (Schema, error) {
		return SMultiCheckNames(js.FieldNames), nil
	})

	RegisterSchemaType("enum", func(js *SchemaJSON) (Schema, error) {
		return SEnum(js.FieldNames, js.Nullable), nil
	})

	RegisterSchemaType("color", func(js *SchemaJSON) (Schema, error) {
		return SColor(js.Nullable), nil
	})

	RegisterSchemaType("checkbox", func(js *SchemaJSON) (Schema, error) {
		return SCheckboxBool{SchemaBool: SchemaBool{Nullable: js.Nullable}}, nil
	})
}

// BuildSchema constructs a Schema instance from a SchemaJSON definition.
// Returns an error if the type is not recognized or js is nil.
//
// It inspects the `Type` field of the provided SchemaJSON and returns the
// corresponding Schema. Built-in registered include:
//
//   - "bool"       → SBool / SNullBool
//   - "int8"       → SInt8 / SNullInt8
//   - "int16"      → SInt16 with optional Range
//   - "int32"      → SInt32 with optional Range
//   - "int64"      → SInt64 with optional Range
//   - "uint8"      → SUint8 / SNullUint8
//   - "uint16"     → SUint16 with optional Range
//   - "uint32"     → SUint32 with optional Range
//   - "uint64"     → SUint64 with optional Range
//   - "date"       → SDate with optional DateFrom/DateTo
//   - "float32"    → SFloat32 / SNullFloat32
//   - "float64"    → SFloat64 / SNullFloat64
//   - "string"     → SString with optional width, exact, prefix, suffix, pattern
//   - "email"      → SEmail
//   - "uri"        → SURI
//   - "lang"       → SLang
//   - "bytes"      → SBytes / SVariableBytes
//   - "any"        → SAny
//   - "tuple"      → STuple / STupleNamed / STupleVal (with flatten/variableLength)
//   - "repeat"     → SRepeat
//   - "map"        → SMap
//   - "mapUnordered" → SMapUnordered / SMapUnorderedOptional
//   - "mapRepeat"  → SMapRepeatRange
//   - "multicheck" → SMultiCheckNames
//   - "enum"       → SEnum
//   - "color"      → SColor
//   - "checkbox"      → SCheckBoxBool
//
// If the type is not recognized, BuildSchema checks the custom registry
// (see RegisterSchemaType) before panicking.
//
// Usage:
//
//	js := SchemaJSON{Type: "string", Width: 20, Prefix: "ID_"}
//	s := BuildSchema(js)
//	s now validates strings up to 20 chars starting with "ID_"
//
// Custom type example:
//
//	schema.RegisterSchemaType("MyCustomType", func(js schema.SchemaJSON) schema.Schema {
//	    return SString.Pattern("[A-Z]{3}[0-9]{2}")
//	})
//	custom := BuildSchema(SchemaJSON{Type: "MyCustomType"})
//
// Notes:
//   - Type names are case-sensitive.
//   - Nullable fields are respected where applicable.
//   - Min/Max apply to numeric typetags.
//   - DateFrom/DateTo must be RFC3339 strings.
//   - For "mapUnordered", FieldNames and Schema must align in length.
//   - For "mapRepeat", Schema must contain exactly two entries.
func BuildSchema(js *SchemaJSON) (Schema, error) {
	if js == nil {
		return nil, fmt.Errorf("nil schema")
	}
	builder, ok := customSchemaBuilders[js.Type]
	if !ok {
		return nil, fmt.Errorf("unknown schema type: %s", js.Type)
	}
	return builder(js)
}

// MustBuildSchema is a convenience wrapper around BuildSchema.
// It panics if BuildSchema returns an error.
func MustBuildSchema(js *SchemaJSON) Schema {
	s, err := BuildSchema(js)
	if err != nil {
		panic(err)
	}
	return s
}

// buildSchemas converts a slice of SchemaJSON definitions into a slice of Schema instances.
// Returns an error if any schema fails to build.
func buildSchemas(list []SchemaJSON) ([]Schema, error) {
	out := make([]Schema, len(list))
	for i := range list {
		s, err := BuildSchema(&list[i])
		if err != nil {
			return nil, fmt.Errorf("failed to build schema at index %d: %w", i, err)
		}
		out[i] = s
	}
	return out, nil
}

func (s *SchemaJSON) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// 1. Support old flat style: contains an explicit "type" key at the root
	if typeVal, ok := raw["type"]; ok {
		var typeStr string
		if err := json.Unmarshal(typeVal, &typeStr); err != nil {
			return fmt.Errorf("invalid type field: %w", err)
		}
		type auxiliary SchemaJSON
		var aux auxiliary
		if err := json.Unmarshal(data, &aux); err != nil {
			return err
		}
		*s = SchemaJSON(aux)
		s.Type = typeStr
		return nil
	}

	// 2. Support new tag-as-key style: exactly 1 key which is the type name
	if len(raw) == 1 {
		for typeName, payload := range raw {
			s.Type = typeName
			if string(payload) == "null" || string(payload) == "" {
				return nil
			}
			type auxiliary SchemaJSON
			var aux auxiliary
			if err := json.Unmarshal(payload, &aux); err != nil {
				return fmt.Errorf("failed to parse options for type %q: %w", typeName, err)
			}
			*s = SchemaJSON(aux)
			s.Type = typeName
			return nil
		}
	}

	return fmt.Errorf("invalid schema object format")
}

func (s SchemaJSON) MarshalJSON() ([]byte, error) {
	if s.Type == "" {
		return []byte("{}"), nil
	}

	type auxiliary SchemaJSON
	aux := auxiliary(s)

	bytes, err := json.Marshal(aux)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	if err := json.Unmarshal(bytes, &m); err != nil {
		return nil, err
	}

	// Always marshal exclusively to the new tag-as-key format
	delete(m, "type")
	wrapped := map[string]any{
		s.Type: m,
	}

	return json.Marshal(wrapped)
}

func (s *SchemaJSON) UnmarshalYAML(unmarshal func(any) error) error {
	var raw map[string]yaml.Node
	if err := unmarshal(&raw); err != nil {
		return err
	}

	// 1. Support old flat style: contains an explicit "type" key at the root
	if typeNode, ok := raw["type"]; ok {
		var typeStr string
		if err := typeNode.Decode(&typeStr); err != nil {
			return fmt.Errorf("invalid type field: %w", err)
		}
		type auxiliary SchemaJSON
		var aux auxiliary
		if err := unmarshal(&aux); err != nil {
			return err
		}
		*s = SchemaJSON(aux)
		s.Type = typeStr
		return nil
	}

	// 2. Support new tag-as-key style: exactly 1 key which is the type name
	if len(raw) == 1 {
		for typeName, payload := range raw {
			s.Type = typeName
			if payload.Kind == yaml.ScalarNode && (payload.Value == "null" || payload.Value == "") {
				return nil
			}
			type auxiliary SchemaJSON
			var aux auxiliary
			if err := payload.Decode(&aux); err != nil {
				return fmt.Errorf("failed to parse options for type %q: %w", typeName, err)
			}
			*s = SchemaJSON(aux)
			s.Type = typeName
			return nil
		}
	}

	return fmt.Errorf("invalid schema object format")
}

func (s SchemaJSON) MarshalYAML() (any, error) {
	if s.Type == "" {
		return map[string]any{}, nil
	}

	type auxiliary SchemaJSON
	aux := auxiliary(s)

	bytes, err := yaml.Marshal(aux)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	if err := yaml.Unmarshal(bytes, &m); err != nil {
		return nil, err
	}

	// Always marshal exclusively to the new tag-as-key format
	delete(m, "type")
	wrapped := map[string]any{
		s.Type: m,
	}

	return wrapped, nil
}
