package scheme

import "time"

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
		if js.DateFrom != "" && js.DateTo != "" {
			from, _ := time.Parse(time.RFC3339, js.DateFrom)
			to, _ := time.Parse(time.RFC3339, js.DateTo)
			return s.DateRange(from, to)
		}
		if js.RangeMin != 0 || js.RangeMax != 0 {
			return s.Range(js.RangeMin, js.RangeMax)
		}
		return s
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
		if js.Width > 0 {
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
	case "multicheck":
		if len(js.FieldNames) > 0 {

			return SMultiCheckNames(js.FieldNames)
		}
		return SMultiCheckNames([]string{})
	default:
		panic("unknown scheme type: " + js.Type)
	}
}

func buildSchemas(list []SchemeJSON) []Scheme {
	out := make([]Scheme, len(list))
	for i, sub := range list {
		out[i] = BuildScheme(sub)
	}
	return out
}
