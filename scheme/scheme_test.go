package scheme

import (
	"fmt"
	"testing"

	"github.com/quickwritereader/PackOS/access"
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
	expectedMeta := types.NewOrderedMapAny(
		types.OPAny("role", []byte("admin")),
		types.OPAny("user", []byte("alice")),
	)

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
	resultMap, ok := resultSlice[4].(*types.OrderedMapAny)
	assert.True(t, ok, "Last element should be an OrderedMapAny")

	// Compare "meta" submap
	metaMap, ok := resultMap.Get("meta")
	assert.True(t, ok, "meta should exist")
	omMeta, ok := metaMap.(*types.OrderedMapAny)
	assert.True(t, ok, "meta should be an OrderedMapAny")
	assert.True(t, omMeta.Equal(expectedMeta), "meta OrderedMapAny does not match expected")

	// Compare "name"
	valName, ok := resultMap.Get("name")
	assert.True(t, ok, "name should exist")
	assert.Equal(t, expectedName, valName)

}

func TestDecodePackedMapUnOrderedOptional(t *testing.T) {
	actual := pack.Pack(
		pack.PackMapSorted{
			"meta": pack.PackMapSorted{
				"role": pack.PackString("admin"),
			},
			"name": pack.PackString("gopher"),
		},
	)

	chain := SChain(
		SMap(
			SString.Match("meta"), // key
			SMapUnorderedOptional(map[string]Scheme{
				"user": SBytes(len("alice")),
				"role": SString.Pattern(`^(admin|guest)$`),
			}),
			SString.Match("name"),     // key
			SStringLen(len("gopher")), // value
		),
	)

	err2 := ValidateBuffer(actual, chain)
	assert.NoError(t, err2, "Validation should succeed for packed structure")
	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed for packed structure")

	expectedMeta := map[string]any{
		"role": "admin",
	}
	expectedName := "gopher"

	// Top-level result should be an OrderedMapAny
	resultMap, ok := ret.(*types.OrderedMapAny)
	assert.True(t, ok, "element should be an OrderedMapAny")

	// Compare "meta" submap
	metaMap := types.GetAs[map[string]any](resultMap, "meta")
	assert.EqualValues(t, expectedMeta, metaMap)

	// Compare "name"
	assert.Equal(t, expectedName, types.GetAs[string](resultMap, "name"))

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

func TestDecodeChain_Default_Success(t *testing.T) {
	actual := pack.Pack(
		pack.PackString("2025-09-10"), // date
		pack.PackInt32(42),            // range
		pack.PackString(""),           // email
		pack.PackString(""),           // prefix
		pack.PackString(""),           // suffix
	)

	chain := SChain(
		SString.Pattern(`^\d{4}-\d{2}-\d{2}$`), // date pattern YYYY-MM-DD
		SInt32.Range(1, 100),                   // int range
		SString.DefaultDecodeValue("alice@example.com").Pattern(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`), // email
		SString.DefaultDecodeValue("prefix-hello").Prefix("prefix-"),                                                // prefix match
		SString.DefaultDecodeValue("world-suffix").Suffix("-suffix"),                                                // suffix match
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
	expected := []any{int32(2025), true, "hello"}

	fmt.Println("Decoded:", ret)

	assert.Equal(t, expected, ret, "Decoded tuple should match expected values")
}

func TestDecodePackedTuples_ExtraTuplesIgnored(t *testing.T) {
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
		// Extra tuples beyond what the chain expects
		pack.PackTuple(
			pack.PackInt32(111),
			pack.PackBool(true),
			pack.PackString("xx"),
		),
		pack.PackTuple(
			pack.PackInt32(222),
			pack.PackBool(false),
			pack.PackString("yy"),
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
		// Only two tuples defined in the chain
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed even with extra tuples in buffer")

	// Expected: only the first two tuples decoded
	expected := []any{
		[]any{int32(2025), false, "az"},
		[]any{int16(7), true, "go"},
	}

	fmt.Println("Decoded:", ret)

	assert.Equal(t, expected, ret, "Decoder should only consume tuples defined in the chain")
}

func TestDecodePackedTuples_WithRepeat(t *testing.T) {
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
		// Extra tuples
		pack.PackTuple(
			pack.PackInt32(111),
			pack.PackBool(true),
			pack.PackString("xx"),
		),
		pack.PackTuple(
			pack.PackInt16(222),
			pack.PackBool(false),
			pack.PackString("yy"),
		),
	)

	// Instead of two fixed tuples, allow repetition
	chain := SChain(
		SRepeat(2, 2, // 2 times
			STuple(
				SInt32,
				SBool,
				SString, // no fixed length needed here
			),
			STuple(
				SInt16,
				SBool,
				SString, // no fixed length needed here
			),
		),
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed with repeat scheme")

	// Expected: all tuples decoded
	expected := []any{
		[]any{int32(2025), false, "az"},
		[]any{int16(7), true, "go"},
		[]any{int32(111), true, "xx"},
		[]any{int16(222), false, "yy"},
	}

	fmt.Println("Decoded:", ret)

	assert.Equal(t, expected, ret, "Decoder should consume all tuples with repeat scheme")

}

func TestDecodeTupleWithRepeatField(t *testing.T) {
	actual := pack.Pack(pack.PackTuple(
		pack.PackInt32(42),
		pack.PackString("alpha"),
		// repeated booleans
		pack.PackBool(true),
		pack.PackBool(false),
		pack.PackBool(true),
	))

	chain := SChain(
		STupleVal(
			SInt32,                // ID
			SString,               // Name
			SRepeat(1, -1, SBool), // repeated field inside tuple
		),
	)

	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed with repeat inside tuple")

	// Expected: tuple with repeated field as slice
	expected := []any{
		int32(42),
		"alpha",
		[]any{true, false, true}, // repeated field
	}

	fmt.Println("Decoded:", ret)

	assert.Equal(t, expected, ret, "Decoder should decode repeated field inside tuple correctly")

	generic, err := access.Decode(actual)
	fmt.Println("access.Decode:", generic)
	assert.NotEqual(t, generic, ret, "Decoder should not decode repeated field inside tuple correctly as generic decoder")

	// now do it with flattened version
	chain2 := SChain(
		STupleValFlatten(
			SInt32,                // ID
			SString,               // Name
			SRepeat(1, -1, SBool), // repeated field inside tuple
		),
	)

	ret2, err := DecodeBuffer(actual, chain2)
	fmt.Println("Decoded Flattened:", ret2)
	assert.NoError(t, err, "Decoding should succeed with repeat inside tuple")
	assert.Equal(t, generic, ret2, "Decoder should decode repeated field inside tuple correctly as generic decoder")

}

func TestDecodeNamedTupleValAndFlattened(t *testing.T) {
	// Build packed tuple: (42, "alpha", true, false, true)
	actual := pack.Pack(pack.PackTuple(
		pack.PackInt32(42),
		pack.PackString("alpha"),
		pack.PackBool(true),
		pack.PackBool(false),
		pack.PackBool(true),
	))

	// Strict schema: repeated field stays grouped
	chainStrict := SChain(
		STupleNamedVal(
			[]string{"id", "name", "flags"},
			SInt32,
			SString,
			SRepeat(1, -1, SBool),
		),
	)

	// Flattened schema: repeated field expands inline
	chainFlat := SChain(
		STupleNamedValFlattened(
			[]string{"id", "name", "flag"},
			SInt32,
			SString,
			SRepeat(1, -1, SBool),
		),
	)

	// Decode with strict schema
	decodedStrict, err := DecodeBuffer(actual, chainStrict)
	assert.NoError(t, err, "Strict named tuple decode should succeed")

	expectedStrict := map[string]any{
		"id":    int32(42),
		"name":  "alpha",
		"flags": []any{true, false, true},
	}
	fmt.Println("DecodedStrict:", decodedStrict)
	assert.EqualValues(t, expectedStrict, decodedStrict,
		"Strict schema should group repeated field into a slice")

	// Decode with flattened schema
	decodedFlat, err := DecodeBuffer(actual, chainFlat)
	assert.NoError(t, err, "Flattened named tuple decode should succeed")

	expectedFlat := map[string]any{
		"id":     int32(42),
		"name":   "alpha",
		"flag_0": true,
		"flag_1": false,
		"flag_2": true,
	}
	fmt.Println("DecodedFlat:", decodedFlat)
	assert.EqualValues(t, expectedFlat, decodedFlat,
		"Flattened schema should expand repeated field inline")
}

func TestEncodePackedTuples(t *testing.T) {
	expected := pack.Pack(
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

	val := []any{
		[]any{int32(2025), false, "az"},
		[]any{int16(7), true, "go"},
	}

	actual, err := EncodeValue(val, chain)
	if err != nil {
		fmt.Println(err)
	}
	assert.Equal(t, expected, actual)
}

func TestEncodeFlattenedTuple(t *testing.T) {
	// Expected packed buffer: (42, "alpha", true, false, true)
	expected := pack.Pack(pack.PackTuple(
		pack.PackInt32(42),
		pack.PackString("alpha"),
		pack.PackBool(true),
		pack.PackBool(false),
		pack.PackBool(true),
	))

	// Flattened schema: repeated field expands inline
	chainFlat := SChain(
		STupleNamedValFlattened(
			[]string{"id", "name", "flag"},
			SInt32,
			SString,
			SRepeat(1, -1, SBool),
		),
	)

	// Value to encode: flattened map with flag_0, flag_1, flag_2
	val := map[string]any{
		"id":     int32(42),
		"name":   "alpha",
		"flag_0": true,
		"flag_1": false,
		"flag_2": true,
	}

	actual, err := EncodeValue(val, chainFlat)
	assert.NoError(t, err, "Flattened named tuple encode should succeed")

	fmt.Println("EncodedFlat:", actual)
	assert.Equal(t, expected, actual)
}

func TestEncodePackedTuples_WithRepeat(t *testing.T) {
	// Expected packed buffer: four tuples, two repetitions of the pair
	expected := pack.Pack(
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
		pack.PackTuple(
			pack.PackInt32(111),
			pack.PackBool(true),
			pack.PackString("xx"),
		),
		pack.PackTuple(
			pack.PackInt16(222),
			pack.PackBool(false),
			pack.PackString("yy"),
		),
	)

	// Schema: repeat the pair of tuples twice
	chain := SChain(
		SRepeat(2, 2,
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
		),
	)

	// Values to encode: slice of tuples
	val := []any{
		[]any{int32(2025), false, "az"},
		[]any{int16(7), true, "go"},
		[]any{int32(111), true, "xx"},
		[]any{int16(222), false, "yy"},
	}

	actual, err := EncodeValue(val, chain)
	assert.NoError(t, err, "Encoding should succeed with repeat scheme")

	fmt.Println("Encoded:", actual)

	assert.Equal(t, expected, actual)
}

func TestEncodeTupleWithRepeatField_Flattened(t *testing.T) {
	// Expected packed buffer: (42, "alpha", true, false, true)
	expected := pack.Pack(pack.PackTuple(
		pack.PackInt32(42),
		pack.PackString("alpha"),
		pack.PackBool(true),
		pack.PackBool(false),
		pack.PackBool(true),
	))

	chain1 := SChain(
		STupleVal(
			SInt32,                // ID
			SString,               // Name
			SRepeat(1, -1, SBool), // repeated field inside tuple
		),
	)

	// Flattened schema: repeated field expands inline
	chain2 := SChain(
		STupleValFlatten(
			SInt32,                // ID
			SString,               // Name
			SRepeat(1, -1, SBool), // repeated field inside tuple
		),
	)

	// Value to encode: repeated field provided as slice, expanded inline by flatten
	val := []any{
		int32(42),
		"alpha",
		[]any{true, false, true},
	}

	actual, err := EncodeValue(val, chain1)
	assert.NoError(t, err, "Encoding should succeed with flattened repeat inside tuple")

	fmt.Println("Encoded Flattened:", actual)

	assert.Equal(t, expected, actual)

	val2 := []any{
		int32(42),
		"alpha", true, false, true,
	}

	actual2, err := EncodeValue(val2, chain2)

	assert.Equal(t, expected, actual2)

}

func TestEncodeTupleWithRepeatField_Flattened_NoMaxBeforeEnd(t *testing.T) {
	// Schema: repeat comes before the last field, but max is not set
	chain := SChain(
		STupleValFlatten(
			SInt32,                // ID
			SRepeat(1, -1, SBool), // repeated field BEFORE the last element, no max
			SString,               // Name
		),
	)

	// Value to encode: ID, repeated flags, then name
	val := []any{
		int32(42),
		true, false, true,
		"alpha",
	}

	actual, err := EncodeValue(val, chain)

	// Expect an error because repeat is not last and max < 1
	assert.Error(t, err, "Encoding should fail when repeat is not last and max is missing")
	assert.Nil(t, actual, "No buffer should be produced")
}

func TestEncodeTupleWithRepeatField_Flattened_WithMaxBeforeEnd(t *testing.T) {
	// Expected packed buffer: (42, true, false, "alpha")
	expected := pack.Pack(pack.PackTuple(
		pack.PackInt32(42),
		pack.PackBool(true),
		pack.PackBool(false),
		pack.PackString("alpha"),
	))

	// Schema: repeat comes before the last field, with max=2
	chain := SChain(
		STupleValFlatten(
			SInt32,               // ID
			SRepeat(2, 2, SBool), // exactly 2 booleans before the last element
			SString,              // Name
		),
	)

	// Value to encode: ID, two flags, then name
	val := []any{
		int32(42),
		true, false,
		"alpha",
	}

	actual, err := EncodeValue(val, chain)
	assert.NoError(t, err, "Encoding should succeed when repeat is not last but max is provided")

	fmt.Println("Encoded Flattened WithMax:", actual)

	assert.Equal(t, expected, actual,
		"Encoder should consume exactly max repeated values before the last field")
}

func TestEncodePackedStructure(t *testing.T) {
	// Expected packed buffer
	expected := pack.Pack(
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

	// Schema definition
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

	// Value to encode
	val := []any{
		int16(12345),
		float32(3.14),
		int64(9876543210),
		true,
		types.NewOrderedMapAny(
			types.OPAny("meta", types.NewOrderedMapAny(
				types.OPAny("role", []byte("admin")),
				types.OPAny("user", []byte("alice")),
			)),
			types.OPAny("name", "gopher"),
		),
	}

	actual, err := EncodeValue(val, chain)
	assert.NoError(t, err, "Encoding packed structure should succeed")

	fmt.Println("Encoded:", actual)

	assert.Equal(t, expected, actual,
		"Encoder should produce packed structure matching expected buffer")
}

func TestEncodePackedStructure_WithInvalidValues(t *testing.T) {

	// Schema with constraints
	chain := SChain(
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

	// Value to encode
	val := types.NewOrderedMapAny(
		types.OPAny("meta", map[string]any{
			"user": []byte("alice"),
			"role": "adminX",  // invalid
			"age":  int32(17), // out of range
		}),
		types.OPAny("name", "gopher"),
	)

	actual, err := EncodeValue(val, chain)

	// We expect an error because constraints are violated
	assert.Error(t, err, "Encoding should fail due to invalid pattern and out-of-range value")
	assert.Nil(t, actual, "No buffer should be produced when constraints fail")

}

func TestEncodePackedStructure_WithValidValues(t *testing.T) {

	// Schema with constraints
	chain := SChain(
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

	// Value to encode
	val := types.NewOrderedMapAny(
		types.OPAny("meta", map[string]any{
			"user": []byte("alice"),
			"role": "admin",
			"age":  int32(27),
		}),
		types.OPAny("name", "gopher"),
	)

	actual, err := EncodeValue(val, chain)
	assert.NoError(t, err, "Encoding should succeed with valid values")

	fmt.Println("Encoded Valid:", actual)

	retVal, err := DecodeBuffer(actual, chain)

	fmt.Println(retVal, val)

	assert.EqualValues(t, val, retVal)

}

func TestEncodePackedTuplesNamed(t *testing.T) {
	// Expected packed buffer: two tuples
	expected := pack.Pack(
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

	// Schema with named tuples
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

	// Value to encode: map with named fields
	val := map[string]any{
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

	actual, err := EncodeValueNamed(val, chain)
	assert.NoError(t, err, "Encoding should succeed for two named packed tuples")

	fmt.Println("Encoded Named:", actual)

	assert.Equal(t, expected, actual,
		"Encoder should produce packed tuples matching expected buffer")
}

func TestSchemeMultiCheckNamesScheme(t *testing.T) {
	// Suppose we have three checkboxes: "read", "write", "execute"
	fieldNames := []string{"read", "write", "execute"}
	chain := SChain(
		SMultiCheckNames(fieldNames),
	)

	// Pack a tuple of bools: read=true, write=false, execute=true
	actual := pack.Pack(
		pack.PackTuple(
			pack.PackBool(true),
			pack.PackBool(false),
			pack.PackBool(true),
		),
	)

	// Validate
	err := ValidateBuffer(actual, chain)
	assert.NoError(t, err, "Validation should succeed for packed structure")

	// Decode
	ret, err := DecodeBuffer(actual, chain)
	assert.NoError(t, err, "Decoding should succeed for packed structure")

	// Expected slice of selected names
	expected := []string{"read", "execute"}

	// Assert type and values
	selected, ok := ret.([]string)
	assert.True(t, ok, "decoded value should be []string")
	assert.ElementsMatch(t, expected, selected, "selected names should match expected")
}

func TestSchemeMultiCheckNamesScheme_Encode(t *testing.T) {
	fieldNames := []string{"read", "write", "execute"}
	chain := SChain(
		SMultiCheckNames(fieldNames),
	)

	// Encode []string{"write"} → should produce tuple [false,true,false]
	val := []string{"write"}
	encoded, err := EncodeValue(val, chain)
	assert.NoError(t, err, "Encoding should succeed")

	// Decode back
	decoded, err := DecodeBuffer(encoded, chain)
	assert.NoError(t, err, "Decoding should succeed")

	expected := []string{"write"}
	selected, ok := decoded.([]string)
	assert.True(t, ok, "decoded value should be []string")
	assert.ElementsMatch(t, expected, selected, "round-trip should preserve selected names")
}
