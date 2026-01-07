package scheme

import (
	"fmt"
	"testing"

	pack "github.com/quickwritereader/PackOS/packable"
	"github.com/quickwritereader/PackOS/types"
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
		SInt16.Range(0, 20000),
		SFloat32,
		SInt64,
		SBool,
		SMap(
			SString.Match("meta"), // key
			SMap(
				SString.Match("role"),
				SBytes(len("admin")), // key → value
				SString.Match("user"),
				SBytes(len("alice")),
			),
			SString.Match("name"),     // key
			SStringLen(len("gopher")), // value
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
		SInt16,
		SFloat32,
		SInt64,
		SBool,
		SMap(
			SString.Match("meta"), // key
			SMap(
				SString.Match("role"),
				SBytes(len("admin")+1), // key → value
				SString.Match("user"),
				SBytes(len("alice")),
			),
			SString.Match("name"),            // key
			SString.WithWidth(len("gopher")), // value
		),
	)

	err := ValidateBuffer(actual, chain)
	assert.Error(t, err, "Validation should fail due to incorrect byte length")
	t.Log("error was: ", err)
}

func TestValidateUnorderedMap_Failure(t *testing.T) {
	actual := pack.Pack(
		pack.PackInt16(12345),
		pack.PackFloat32(3.14),
		pack.PackInt64(9876543210),
		pack.PackBool(true),
		pack.PackMapSorted{ // unordered content, still packed as sorted
			"meta": pack.PackMapSorted{
				"user": pack.PackByteArray([]byte("alice")), // correct
				"role": pack.PackString("adminX"),           // invalid pattern
				"age":  pack.PackInt32(17),                  // out of range
			},
			"name": pack.PackString("gopher"),
		},
	)

	chain := SChain(
		SInt16,
		SFloat32,
		SInt64,
		SBool,
		SMap(
			SString.Match("meta"),
			SMapUnordered(map[string]Scheme{
				"user": SBytes(len("alice")),
				"role": SString.Pattern(`^(admin|guest)$`),
				"age":  SInt32.Range(18, 99),
			}),
			SString.Match("name"),
			SString.WithWidth(len("gopher")),
		),
	)

	err := ValidateBuffer(actual, chain)
	assert.Error(t, err, "Validation should fail due to pattern mismatch and range violation")
	t.Log("error was:", err)
}

func TestValidateUnorderedMap_ShuffledDeclaration_Success(t *testing.T) {
	actual := pack.Pack(
		pack.PackInt16(12345),
		pack.PackFloat32(3.14),
		pack.PackInt64(9876543210),
		pack.PackBool(true),
		pack.PackMapSorted{
			"meta": pack.PackMapSorted{
				"user": pack.PackByteArray([]byte("alice")), // valid
				"role": pack.PackString("admin"),            // valid
				"age":  pack.PackInt32(30),                  // valid
			},
			"name": pack.PackString("gopher"), // valid
		},
	)

	chain := SChain(
		SInt16,
		SFloat32,
		SInt64,
		SBool,
		SMap(
			SString.Match("meta"),
			SMapUnordered(map[string]Scheme{
				"age":  SInt32.Range(18, 99),               // declared first
				"user": SBytes(len("alice")),               // declared second
				"role": SString.Pattern(`^(admin|guest)$`), // declared third
			}),
			SString.Match("name"),
			SString.WithWidth(len("gopher")),
		),
	)

	err := ValidateBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed regardless of key declaration order")
}

func TestValidateChain_DateEmailPrefixSuffix_Success(t *testing.T) {
	actual := pack.Pack(
		pack.PackString("2025-09-10"),        // date
		pack.PackInt32(42),                   // range
		pack.PackString("alice@example.com"), // email
		pack.PackString("prefix-hello"),      // prefix
		pack.PackString("world-suffix"),      // suffix
	)

	chain := SChain(
		SString.Pattern(`^\d{4}-\d{2}-\d{2}$`),                              // date pattern YYYY-MM-DD
		SInt32.Range(1, 100),                                                // int range
		SString.Pattern(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`), // email
		SString.Prefix("prefix-"),                                           // prefix match
		SString.Suffix("-suffix"),                                           // suffix match
	)

	err := ValidateBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed with correct date, range, email, prefix, and suffix")
}

func TestValidatePackedTuples(t *testing.T) {
	actual := pack.Pack(
		pack.PackTuple(
			pack.PackInt32(2025),
			pack.PackBool(false),
			pack.PackString("az"),
		),
		pack.PackTuple(
			pack.PackInt16(7),
			pack.PackBool(true),
			pack.PackString("go"),
		),
	)

	chain := SChain(
		STuple(
			SInt32,
			SBool,
			SStringLen(len("az")),
		),
		STuple(
			SInt16,
			SBool,
			SStringLen(len("go")),
		),
	)

	err := ValidateBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed for two packed tuples")
}

func TestDecodePackedStructure(t *testing.T) {
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
		SInt16.Range(0, 20000),
		SFloat32,
		SInt64,
		SBool,
		SMap(
			SString.Match("meta"), // key
			SMap(
				SString.Match("role"),
				SBytes(len("admin")), // key → value
				SString.Match("user"),
				SBytes(len("alice")),
			),
			SString.Match("name"),     // key
			SStringLen(len("gopher")), // value
		),
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed for packed structure")
	// Expected values
	expectedInt16 := int16(12345)
	expectedFloat32 := float32(3.14)
	expectedInt64 := int64(9876543210)
	expectedBool := true
	expectedMeta := map[string]any{
		"role": []byte("admin"),
		"user": []byte("alice"),
	}
	expectedName := "gopher"

	// ret should be []any
	resultSlice, ok := ret.([]any)
	assert.True(t, ok, "Decoded result should be a slice")

	// Check primitive values
	assert.Equal(t, expectedInt16, resultSlice[0])
	assert.Equal(t, expectedFloat32, resultSlice[1])
	assert.Equal(t, expectedInt64, resultSlice[2])
	assert.Equal(t, expectedBool, resultSlice[3])

	// Check map values without assuming order
	resultMap, ok := resultSlice[4].(map[string]any)
	assert.True(t, ok, "Last element should be a map")

	// Compare "meta" submap
	metaMap, ok := resultMap["meta"].(map[string]any)
	assert.True(t, ok, "meta should be a map")
	assert.EqualValues(t, expectedMeta, metaMap)

	// Compare "name"
	assert.Equal(t, expectedName, resultMap["name"])

}

func TestDecodePackedTuples(t *testing.T) {
	actual := pack.Pack(
		pack.PackTuple(
			pack.PackInt32(2025),
			pack.PackBool(false),
			pack.PackString("az"),
		),
		pack.PackTuple(
			pack.PackInt16(7),
			pack.PackBool(true),
			pack.PackString("go"),
		),
	)

	chain := SChain(
		STuple(
			SInt32,
			SBool,
			SStringLen(len("az")),
		),
		STuple(
			SInt16,
			SBool,
			SStringLen(len("go")),
		),
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed for two packed tuples")

	// Expected structure: slice of two tuples
	expected := []any{
		[]any{int32(2025), false, "az"},
		[]any{int16(7), true, "go"},
	}

	// Print for debugging
	fmt.Println("Decoded:", ret)

	// Assert equality
	assert.Equal(t, expected, ret, "Decoded tuples should match expected values")
}

func TestDecodeChain_DateEmailPrefixSuffix_Success(t *testing.T) {
	actual := pack.Pack(
		pack.PackString("2025-09-10"),        // date
		pack.PackInt32(42),                   // range
		pack.PackString("alice@example.com"), // email
		pack.PackString("prefix-hello"),      // prefix
		pack.PackString("world-suffix"),      // suffix
	)

	chain := SChain(
		SString.Pattern(`^\d{4}-\d{2}-\d{2}$`),                              // date pattern YYYY-MM-DD
		SInt32.Range(1, 100),                                                // int range
		SString.Pattern(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`), // email
		SString.Prefix("prefix-"),                                           // prefix match
		SString.Suffix("-suffix"),                                           // suffix match
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed with correct date, range, email, prefix, and suffix")

	// Expected decoded values
	expected := []any{
		"2025-09-10",
		int32(42),
		"alice@example.com",
		"prefix-hello",
		"world-suffix",
	}

	// Print for debugging
	fmt.Println("Decoded:", ret)

	// Assert equality
	assert.Equal(t, expected, ret, "Decoded values should match expected sequence")
}

func TestDecodePackedTuplesNamed(t *testing.T) {
	actual := pack.Pack(
		pack.PackTuple(
			pack.PackInt32(2025),
			pack.PackBool(false),
			pack.PackString("az"),
		),
		pack.PackTuple(
			pack.PackInt16(7),
			pack.PackBool(true),
			pack.PackString("go"),
		),
	)

	chain := SchemeNamedChain{
		SchemeChain: SChain(
			STupleNamed(
				[]string{"year", "flag", "code"},
				SInt32,
				SBool,
				SStringLen(len("az")),
			),
			STupleNamed(
				[]string{"num", "flag", "lang"},
				SInt16,
				SBool,
				SStringLen(len("go")),
			),
		),
		FieldNames: []string{"firstTuple", "secondTuple"},
	}

	ret, err := DecodeBufferNamed(actual, chain)
	assert.NoError(t, err, "Decoding should succeed for two named packed tuples")

	expected := map[string]any{
		"firstTuple": map[string]any{
			"year": int32(2025),
			"flag": false,
			"code": "az",
		},
		"secondTuple": map[string]any{
			"num":  int16(7),
			"flag": true,
			"lang": "go",
		},
	}

	// Print for debugging
	fmt.Println("Decoded:", ret)

	// Use EqualValues to ignore ordering differences in maps
	assert.EqualValues(t, expected, ret, "Decoded named tuples should match expected values")
}

func TestDecodeSTypeTuple(t *testing.T) {
	actual := pack.Pack(
		pack.PackTuple(
			pack.PackInt32(2025),
			pack.PackBool(true),
			pack.PackString("hello"),
		),
	)

	// Chain only checks that the next element is a tuple
	chain := SChain(
		SType(types.TypeTuple),
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed when expecting a tuple type")

	// Since SType only validates type, Decode returns the raw payload
	// In this case, it will decode the tuple into []any
	expected := []any{
		[]any{int32(2025), true, "hello"},
	}

	fmt.Println("Decoded:", ret)

	assert.Equal(t, expected, ret, "Decoded tuple should match expected values")
}
