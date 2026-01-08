package scheme

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildScheme_WithRepeatTuples(t *testing.T) {
	// Define scheme in JSON form
	schemeJSON := SchemeJSON{
		Type: "repeat",
		Min:  1,
		Max:  -1,
		Schema: []SchemeJSON{
			{
				Type: "tuple",
				Schema: []SchemeJSON{
					{Type: "int32"},
					{Type: "bool"},
					{Type: "string"},
				},
			},
			{
				Type: "tuple",
				Schema: []SchemeJSON{
					{Type: "int16"},
					{Type: "bool"},
					{Type: "string"},
				},
			},
		},
	}

	// Build scheme from JSON
	built := BuildScheme(schemeJSON)

	// Manually constructed scheme
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
		"Built scheme from JSON should equal manually constructed scheme")
}

func TestBuildScheme_NamedTuple(t *testing.T) {
	schemeJSON := SchemeJSON{
		Type:       "tupleNamedVal",
		FieldNames: []string{"id", "name", "active"},
		Schema: []SchemeJSON{
			{Type: "int32"},
			{Type: "string"},
			{Type: "bool", Nullable: true},
		},
	}

	built := BuildScheme(schemeJSON)

	expected := STupleNamedVal(
		[]string{"id", "name", "active"},
		SInt32,
		SString,
		SNullBool,
	)

	assert.EqualValues(t, expected, built,
		"Built scheme from JSON should equal manually constructed named tuple")
}
