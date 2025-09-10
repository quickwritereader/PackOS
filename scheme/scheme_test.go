package scheme

import (
	"testing"

	pack "github.com/BranchAndLink/packos/packable"
	"github.com/stretchr/testify/assert"
)

func TestValidatePackedStructure(t *testing.T) {
	actual := pack.Pack(
		pack.PackInt16(12345),
		pack.PackFloat32(3.14),
		pack.PackInt64(9876543210),
		pack.PackBool(true),
		pack.PackMapSorted{
			"meta": pack.PackMapSorted{
				"user": pack.PackByteArray([]byte("alice")),
				"role": pack.PackByteArray([]byte("admin")),
			},
			"name": pack.PackString("gopher"),
		},
	)

	chain := SChain(
		SInt16(),
		SFloat32(),
		SInt64(),
		SBool(),
		SMapSorted(
			SStringExact("meta"), // key
			SMapSorted(
				SStringExact("role"),
				SBytes(len("admin")), // key → value
				SStringExact("user"),
				SBytes(len("alice")),
			),
			SStringExact("name"),   // key
			SString(len("gopher")), // value
		),
	)

	err := ValidateBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed for packed structure")
}

func TestValidatePackedStructure_Failure(t *testing.T) {
	actual := pack.Pack(
		pack.PackInt16(12345),
		pack.PackFloat32(3.14),
		pack.PackInt64(9876543210),
		pack.PackBool(true),
		pack.PackMapSorted{
			"meta": pack.PackMapSorted{
				"user": pack.PackByteArray([]byte("alice")),
				"role": pack.PackByteArray([]byte("admin")),
			},
			"name": pack.PackString("gopher"),
		},
	)

	// Intentionally break schema: expect wrong length for "admin"
	chain := SChain(
		SInt16(),
		SFloat32(),
		SInt64(),
		SBool(),
		SMapSorted(
			SStringExact("meta"), // key
			SMapSorted(
				SStringExact("role"),
				SBytes(len("admin")+1), // key → value
				SStringExact("user"),
				SBytes(len("alice")),
			),
			SStringExact("name"),   // key
			SString(len("gopher")), // value
		),
	)

	err := ValidateBuffer(actual, chain)
	assert.Error(t, err, "Validation should fail due to incorrect byte length")
	t.Log("error was: ", err)
}
