package schema

import (
	"fmt"
	"time"
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
var customSchemaBuilders = map[string]func(*SchemaJSON) Schema{}

// RegisterSchemaType registers a custom Schema builder for a given type name.
//
// Usage:
//
//	schema.RegisterSchemaType("MyCustomType", func(js schema.SchemaJSON) schema.Schema {
//	    // Build your own Schema based on js
//	    return SString.WithWidth(js.Width) // or any custom logic
//	})
//
// Notes:
//   - Type names are case-sensitive ("MyCustomType" ≠ "mycustomtype").
//   - Panics if the type name is already registered (built-in or custom).
//   - Use UnregisterSchemaType to remove a custom type.
//
// This allows users to extend BuildSchema with their own typetags without
// modifying the core switch.
func RegisterSchemaType(typeName string, builder func(*SchemaJSON) Schema) {
	if typeName == "" {
		panic("cannot register empty type name")
	}
	if _, exists := customSchemaBuilders[typeName]; exists {
		panic("schema type already registered: " + typeName)
	}
	customSchemaBuilders[typeName] = builder
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

// BuildSchema constructs a Schema instance from a SchemaJSON definition.
//
// It inspects the `Type` field of the provided SchemaJSON and returns the
// corresponding Schema. Built-in typetags include:
//
//   - "bool"       → SBool / SNullBool
//   - "int8"       → SInt8 / SNullInt8
//   - "int16"      → SInt16 with optional Range
//   - "int32"      → SInt32 with optional Range
//   - "int64"      → SInt64 with optional Range
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
func BuildSchema(js *SchemaJSON) Schema {
	if js == nil {
		panic("nil schema")

	}
	switch js.Type {
	case "bool":
		if js.Nullable {
			return SNullBool
		}
		return SBool
	case "int8":
		if js.Nullable {
			return SNullInt8
		}
		return SInt8
	case "int16":
		s := SInt16
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			return s.Range(js.Min, js.Max)
		}
		return s
	case "int32":
		s := SInt32
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			return s.Range(js.Min, js.Max)
		}
		return s
	case "int64":
		s := SInt64
		if js.Nullable {
			s.Nullable = true
		}
		if js.Min != nil || js.Max != nil {
			return s.Range(js.Min, js.Max)
		}
		return s
	case "date":
		if js.DateFrom != "" && js.DateTo != "" {
			from, _ := time.Parse(time.RFC3339, js.DateFrom)
			to, _ := time.Parse(time.RFC3339, js.DateTo)
			return SDateRange(js.Nullable, &from, &to)
		}
		return SDateRange(js.Nullable, nil, nil)
	case "float32":
		if js.Nullable {
			return SNullFloat32
		}
		return SFloat32
	case "float64":
		if js.Nullable {
			return SNullFloat64
		}
		return SFloat64
	case "string":
		s := SString

		if js.Nullable {
			// Make it optional s.Optional() == s.WithWidth(-1)
			s = s.Optional()
		} else if js.Width > 0 {
			s = s.WithWidth(js.Width)
		}
		if js.DecodeDefault != "" {
			s = s.DefaultDecodeValue(js.DecodeDefault)
		}
		if js.Exact != "" {
			return s.Match(js.Exact)
		}
		if js.Prefix != "" {
			return s.Prefix(js.Prefix)
		}
		if js.Suffix != "" {
			return s.Suffix(js.Suffix)
		}
		if js.Pattern != "" {
			return s.Pattern(js.Pattern)
		}
		return s
	case "email":
		return SEmail(js.Nullable)
	case "uri":
		return SURI(js.Nullable)
	case "lang":
		return SLang(js.Nullable)
	case "bytes":
		if js.Width > 0 {
			return SBytes(js.Width)
		}
		return SVariableBytes()
	case "number":
		var xmin, xmax *float64
		if js.Min != nil {
			xret := float64(*js.Min)
			xmin = &xret
		}
		if js.Max != nil {
			xret := float64(*js.Max)
			xmax = &xret
		}
		return SchemaNumber{false, xmin, xmax}
	case "numberString":
		var xmin, xmax *float64
		if js.Min != nil {
			xret := float64(*js.Min)
			xmin = &xret
		}
		if js.Max != nil {
			xret := float64(*js.Max)
			xmax = &xret
		}
		return SchemaNumber{true, xmin, xmax}
	case "any":
		return SchemaAny{}
	case "tuple":
		if len(js.FieldNames) > 0 {

			if js.VariableLength && js.Flatten {
				return STupleNamedValFlattened(js.FieldNames, buildSchemas(js.Schema)...)
			} else if js.VariableLength {
				return STupleNamedVal(js.FieldNames, buildSchemas(js.Schema)...)
			}
			return STupleNamed(js.FieldNames, buildSchemas(js.Schema)...)

		}
		if js.VariableLength && js.Flatten {
			return STupleValFlatten(buildSchemas(js.Schema)...)
		} else if js.VariableLength {
			return STupleVal(buildSchemas(js.Schema)...)
		}
		return STuple(buildSchemas(js.Schema)...)
	case "repeat":
		return SRepeatRange(js.Min, js.Max, buildSchemas(js.Schema)...)

	case "map":
		return SMap(buildSchemas(js.Schema)...)
	case "mapUnordered":
		mapped := make(map[string]Schema)
		for i := range js.Schema {
			mapped[js.FieldNames[i]] = BuildSchema(&js.Schema[i])
		}
		if js.Nullable {
			return SMapUnorderedOptional(mapped)
		}
		return SMapUnordered(mapped)
	case "mapRepeat":
		if len(js.Schema) == 2 {
			return SMapRepeatRange(BuildSchema(&js.Schema[0]), BuildSchema(&js.Schema[1]), js.Min, js.Max)
		} else {
			panic(fmt.Sprintf("should be 2 schemas %v", len(js.FieldNames)))
		}
	case "multicheck":
		if len(js.FieldNames) > 0 {
			return SMultiCheckNames(js.FieldNames)
		}
		return SMultiCheckNames([]string{})
	case "enum":
		if len(js.FieldNames) > 0 {
			return SEnum(js.FieldNames, js.Nullable)
		}
		return SEnum([]string{}, js.Nullable)
	case "color":
		return SColor(js.Nullable)
	default:
		// Check custom registry before panicking
		if builder, ok := customSchemaBuilders[js.Type]; ok {
			return builder(js)
		}
		panic("unknown schema type: " + js.Type)
	}
}

// buildSchemas is an internal helper that converts a slice of SchemaJSON
// definitions into a slice of Schema instances by delegating to BuildSchema.
// It preserves the order of the input list and is primarily used by composite
// typetags (tuple, map, repeat, etc.) when constructing nested schemas.
func buildSchemas(list []SchemaJSON) []Schema {
	out := make([]Schema, len(list))
	for i := range list {
		out[i] = BuildSchema(&list[i])
	}
	return out
}
