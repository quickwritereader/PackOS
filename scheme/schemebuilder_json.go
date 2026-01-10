package scheme

import (
	"fmt"
	"time"
)

type SchemeJSON struct {
	Type           string       `json:"type"`
	FieldNames     []string     `json:"fieldNames,omitempty"`
	Schema         []SchemeJSON `json:"schema,omitempty"`
	Min            int          `json:"min,omitempty"`
	Max            int          `json:"max,omitempty"`
	Width          int          `json:"width,omitempty"`
	Nullable       bool         `json:"nullable,omitempty"`
	VariableLength bool         `json:"variableLength,omitempty"`
	Flatten        bool         `json:"flatten,omitempty"`
	OptionalMap    bool         `json:"optionalMap,omitempty"`

	// Constraint helpers
	Exact         string `json:"exact,omitempty"`
	Prefix        string `json:"prefix,omitempty"`
	Suffix        string `json:"suffix,omitempty"`
	Pattern       string `json:"pattern,omitempty"`
	RangeMin      int64  `json:"rangeMin,omitempty"`
	RangeMax      int64  `json:"rangeMax,omitempty"`
	DateFrom      string `json:"dateFrom,omitempty"`
	DateTo        string `json:"dateTo,omitempty"`
	DecodeDefault string `json:"decodeDefault,omitempty"`

	// Extra metadata for UI or other purposes
	Extra map[string]any `json:"extra,omitempty"`
}

// Registry of custom scheme builders.
// Key: type name (case-sensitive), Value: builder function.
var customSchemeBuilders = map[string]func(SchemeJSON) Scheme{}

// RegisterSchemeType registers a custom Scheme builder for a given type name.
//
// Usage:
//
//	scheme.RegisterSchemeType("MyCustomType", func(js scheme.SchemeJSON) scheme.Scheme {
//	    // Build your own Scheme based on js
//	    return SString.WithWidth(js.Width) // or any custom logic
//	})
//
// Notes:
//   - Type names are case-sensitive ("MyCustomType" ≠ "mycustomtype").
//   - Panics if the type name is already registered (built-in or custom).
//   - Use UnregisterSchemeType to remove a custom type.
//
// This allows users to extend BuildScheme with their own types without
// modifying the core switch.
func RegisterSchemeType(typeName string, builder func(SchemeJSON) Scheme) {
	if typeName == "" {
		panic("cannot register empty type name")
	}
	if _, exists := customSchemeBuilders[typeName]; exists {
		panic("scheme type already registered: " + typeName)
	}
	customSchemeBuilders[typeName] = builder
}

// UnregisterSchemeType removes a previously registered custom Scheme builder.
//
// Usage:
//
//	scheme.UnregisterSchemeType("MyCustomType")
//
// If the type name is not found, the function does nothing.
func UnregisterSchemeType(typeName string) {
	delete(customSchemeBuilders, typeName)
}

// BuildScheme constructs a Scheme instance from a SchemeJSON definition.
//
// It inspects the `Type` field of the provided SchemeJSON and returns the
// corresponding Scheme. Built-in types include:
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
// If the type is not recognized, BuildScheme checks the custom registry
// (see RegisterSchemeType) before panicking.
//
// Usage:
//
//	js := SchemeJSON{Type: "string", Width: 20, Prefix: "ID_"}
//	s := BuildScheme(js)
//	s now validates strings up to 20 chars starting with "ID_"
//
// Custom type example:
//
//	scheme.RegisterSchemeType("MyCustomType", func(js scheme.SchemeJSON) scheme.Scheme {
//	    return SString.Pattern("[A-Z]{3}[0-9]{2}")
//	})
//	custom := BuildScheme(SchemeJSON{Type: "MyCustomType"})
//
// Notes:
//   - Type names are case-sensitive.
//   - Nullable fields are respected where applicable.
//   - RangeMin/RangeMax apply to numeric types.
//   - DateFrom/DateTo must be RFC3339 strings.
//   - For "mapUnordered", FieldNames and Schema must align in length.
//   - For "mapRepeat", Schema must contain exactly two entries.
func BuildScheme(js SchemeJSON) Scheme {
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
		if js.RangeMin != 0 || js.RangeMax != 0 {
			return s.Range(int16(js.RangeMin), int16(js.RangeMax))
		}
		return s
	case "int32":
		s := SInt32
		if js.Nullable {
			s.Nullable = true
		}
		if js.RangeMin != 0 || js.RangeMax != 0 {
			return s.Range(int32(js.RangeMin), int32(js.RangeMax))
		}
		return s
	case "int64":
		s := SInt64
		if js.Nullable {
			s.Nullable = true
		}
		if js.RangeMin != 0 || js.RangeMax != 0 {
			return s.Range(js.RangeMin, js.RangeMax)
		}
		return s
	case "date":
		if js.DateFrom != "" && js.DateTo != "" {
			from, _ := time.Parse(time.RFC3339, js.DateFrom)
			to, _ := time.Parse(time.RFC3339, js.DateTo)
			return SDate(js.Nullable, from, to)
		}
		return SDate(js.Nullable, time.Unix(0, 0), time.Unix(1<<63-1, 0))
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
	case "any":
		return SAny

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
		return SRepeat(js.Min, js.Max, buildSchemas(js.Schema)...)

	case "map":
		return SMap(buildSchemas(js.Schema)...)
	case "mapUnordered":
		mapped := make(map[string]Scheme)
		for i, sub := range js.Schema {
			mapped[js.FieldNames[i]] = BuildScheme(sub)
		}
		if js.OptionalMap {
			return SMapUnorderedOptional(mapped)
		}
		return SMapUnordered(mapped)
	case "mapRepeat":
		if len(js.Schema) == 2 {
			return SMapRepeatRange(BuildScheme(js.Schema[0]), BuildScheme(js.Schema[1]), js.Min, js.Max)
		} else {
			panic(fmt.Sprintf("should be 2 schemes %v", len(js.FieldNames)))
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
		if builder, ok := customSchemeBuilders[js.Type]; ok {
			return builder(js)
		}
		panic("unknown scheme type: " + js.Type)
	}
}

// buildSchemas is an internal helper that converts a slice of SchemeJSON
// definitions into a slice of Scheme instances by delegating to BuildScheme.
// It preserves the order of the input list and is primarily used by composite
// types (tuple, map, repeat, etc.) when constructing nested schemas.
func buildSchemas(list []SchemeJSON) []Scheme {
	out := make([]Scheme, len(list))
	for i, sub := range list {
		out[i] = BuildScheme(sub)
	}
	return out
}
