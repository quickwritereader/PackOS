package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSchema_WithRepeatTuples(t *testing.T) {
	// Define schema in JSON form
	schemaJSON := SchemaJSON{
		Type: "repeat",
		Min:  PtrToInt64(1),
		Max:  nil,
		Schema: []SchemaJSON{
			{
				Type: "tuple",
				Schema: []SchemaJSON{
					{Type: "int32"},
					{Type: "bool"},
					{Type: "string"},
				},
			},
			{
				Type: "tuple",
				Schema: []SchemaJSON{
					{Type: "int16"},
					{Type: "bool"},
					{Type: "string"},
				},
			},
		},
	}

	// Build schema from JSON
	built := BuildSchema(&schemaJSON)

	// Manually constructed schema
	expected := SRepeat(1, -1,
		STuple(
			SInt32,
			SBool,
			SString,
		),
		STuple(
			SInt16,
			SBool,
			SString,
		),
	)

	// Compare structurally
	assert.EqualValues(t, expected, built,
		"Built schema from JSON should equal manually constructed schema")
}

func TestBuildSchema_NamedTuple(t *testing.T) {
	schemaJSON := SchemaJSON{
		Type:       "tuple",
		FieldNames: []string{"id", "name", "active"},
		Schema: []SchemaJSON{
			{Type: "int32"},
			{Type: "string"},
			{Type: "bool", Nullable: true},
		},
		VariableLength: true,
	}

	built := BuildSchema(&schemaJSON)

	expected := STupleNamedVal(
		[]string{"id", "name", "active"},
		SInt32,
		SString,
		SNullBool,
	)

	assert.EqualValues(t, expected, built,
		"Built schema from JSON should equal manually constructed named tuple")
}
