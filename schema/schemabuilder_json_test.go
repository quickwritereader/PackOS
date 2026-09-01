package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
	built := MustBuildSchema(&schemaJSON)

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

	built := MustBuildSchema(&schemaJSON)

	expected := STupleNamedVal(
		[]string{"id", "name", "active"},
		SInt32,
		SString,
		SNullBool,
	)

	assert.EqualValues(t, expected, built,
		"Built schema from JSON should equal manually constructed named tuple")
}

func TestSmallSchemaUnmarshal(t *testing.T) {
	schemaJSONStr := `{"string": {"pattern": "^(https?://.*|/)$", "nullable": true}}`

	var sj SchemaJSON
	err := json.Unmarshal([]byte(schemaJSONStr), &sj)
	require.NoError(t, err, "failed to unmarshal small schema")

	assert.Equal(t, "string", sj.Type)
	assert.Equal(t, "^(https?://.*|/)$", sj.Pattern)
	assert.True(t, sj.Nullable)
}

func TestSchemaJSONRoundTripNewStyle(t *testing.T) {
	oldStyleJSON := `{
		"type": "tuple",
		"variableLength": true,
		"fieldNames": ["name"],
		"schema": [
			{ "type": "string", "pattern": "^[A-Z]" }
		]
	}`

	newStyleJSON := `{
		"tuple": {
			"variableLength": true,
			"fieldNames": ["name"],
			"schema": [
				{ "string": { "pattern": "^[A-Z]" } }
			]
		}
	}`

	var schemaOld SchemaJSON
	require.NoError(t, json.Unmarshal([]byte(oldStyleJSON), &schemaOld))

	var schemaNew SchemaJSON
	require.NoError(t, json.Unmarshal([]byte(newStyleJSON), &schemaNew))

	marshaledOld, err := json.Marshal(schemaOld)
	require.NoError(t, err)

	marshaledNew, err := json.Marshal(schemaNew)
	require.NoError(t, err)

	assert.JSONEq(t, newStyleJSON, string(marshaledOld))
	assert.JSONEq(t, newStyleJSON, string(marshaledNew))
}

func TestRegisterSchemaTypeErrors(t *testing.T) {
	// Test empty type name
	err := RegisterSchemaType("", func(js *SchemaJSON) (Schema, error) { return nil, nil })
	assert.ErrorIs(t, err, ErrEmptyTypeName)

	// Test schema field name conflict
	err = RegisterSchemaType("max", func(js *SchemaJSON) (Schema, error) { return nil, nil })
	assert.ErrorIs(t, err, ErrTypeNameConflict)

	// Test duplicate registration
	typeName := "testCustomType"
	dummyBuilder := func(js *SchemaJSON) (Schema, error) { return nil, nil }

	err = RegisterSchemaType(typeName, dummyBuilder)
	require.NoError(t, err)

	err = RegisterSchemaType(typeName, dummyBuilder)
	assert.ErrorIs(t, err, ErrTypeAlreadyRegistered)
}
func TestSchemaYAML(t *testing.T) {
	original := SchemaJSON{
		Type:       "string",
		FieldNames: []string{},
		Schema:     []SchemaJSON{},
		Extra:      map[string]any{},
	}

	data, err := yaml.Marshal(original)
	require.NoError(t, err)

	var decoded SchemaJSON
	err = yaml.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}
