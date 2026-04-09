package packable

import (
	"testing"

	"github.com/quickwritereader/PackOS/access"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPackExtended_LargeData(t *testing.T) {
	// Create large dataset
	largeString := make([]byte, 1000)
	for i := range largeString {
		largeString[i] = byte('A' + i%26)
	}

	// Pack with 4KB segments
	result, err := PackExtended(4096,
		PackInt32(12345),
		PackString(string(largeString)),
		PackInt32(67890),
	)

	require.NoError(t, err)
	assert.Greater(t, len(result), 1000)
}

func TestPackExtended_SmallPivot(t *testing.T) {
	// Use very small pivot to force many segments
	// Create enough data to exceed 128 bytes
	largeStr := make([]byte, 30) // 30 bytes each
	for i := range largeStr {
		largeStr[i] = 'X'
	}

	result, err := PackExtended(128,
		PackString(string(largeStr)),
		PackString(string(largeStr)),
		PackString(string(largeStr)),
		PackString(string(largeStr)),
		PackString(string(largeStr)),
	)

	require.NoError(t, err)
	assert.NotEmpty(t, result)
	// Should be at least 150 bytes (5 * 30), so > 128
	assert.Greater(t, len(result), 128)
}

func TestPackExtended_EmptyArgs(t *testing.T) {
	result, err := PackExtended(4096)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestPackExtended_MapStr(t *testing.T) {
	testMap := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	result, err := PackExtendedWithMapStr(4096, testMap)
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestPackExtended_BytesMap(t *testing.T) {
	testMap := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	result, err := PackExtendedWithBytesMap(4096, testMap)
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestPackExtended_AnyMap(t *testing.T) {
	testMap := map[string]any{
		"int32":   int32(42),
		"int64":   int64(123456789),
		"string":  "test",
		"bool":    true,
		"float64": 3.14,
	}

	result, err := PackExtendedWithAnyMap(4096, testMap, true)
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestPackExtended_PackableMap(t *testing.T) {
	testMap := map[string]access.Packable{
		"key1": PackString("value1"),
		"key2": PackString("value2"),
		"key3": PackString("value3"),
	}

	result, err := PackExtendedWithPackableMap(4096, testMap)
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestPackExtended_MixedTypes(t *testing.T) {
	result, err := PackExtended(2048,
		PackInt32(100),
		PackString("test"),
		PackBool(true),
		PackFloat64(3.14159),
		PackByteArray([]byte{1, 2, 3, 4, 5}),
	)

	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestPackExtended_NestedSegments(t *testing.T) {
	// Create data that will span multiple segments
	largeStr := make([]byte, 3000)
	for i := range largeStr {
		largeStr[i] = 'X'
	}

	// Create nested packable
	packableMap := PackMapStr{
		"nested1": "value1",
		"nested2": "value2",
	}

	result, err := PackExtended(2048,
		PackString(string(largeStr)),
		packableMap,
		PackString(string(largeStr)),
	)

	require.NoError(t, err)
	assert.Greater(t, len(result), 6000)
}

func BenchmarkPackExtended_SmallData(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PackExtended(4096,
			PackInt32(100),
			PackString("test"),
			PackBool(true),
		)
	}
}

func BenchmarkPackExtended_LargeData(b *testing.B) {
	largeStr := make([]byte, 5000)
	for i := range largeStr {
		largeStr[i] = 'A'
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PackExtended(4096,
			PackInt32(100),
			PackString(string(largeStr)),
		)
	}
}

func BenchmarkPackExtended_MapData(b *testing.B) {
	testMap := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PackExtendedWithMapStr(4096, testMap)
	}
}
