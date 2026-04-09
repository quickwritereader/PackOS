package access

import (
	"encoding/binary"
	"testing"

	"github.com/quickwritereader/PackOS/typetags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendedPutAccess_SingleSegment(t *testing.T) {
	ec := NewExtendedContainer(4096)

	// Add small amount of data
	for i := range 10 {
		require.NoError(t, ec.AddInt16(int16(i)))
	}

	result, err := ec.Pack()
	require.NoError(t, err)

	// Should not create extended container
	assert.Less(t, len(result), 4096)
}

func TestExtendedPutAccess_MultipleSegments(t *testing.T) {
	ec := NewExtendedContainer(1024) // Small pivot to force segmentation

	// Add large string to exceed threshold
	largeString := make([]byte, 800)
	for i := range largeString {
		largeString[i] = 'A'
	}

	for range 10 {
		require.NoError(t, ec.AddString(string(largeString)))
	}

	result, err := ec.Pack()
	require.NoError(t, err)

	// Should create extended container
	assert.Greater(t, len(result), 1024)

	// Verify structure - should have extended container header
	if len(result) >= 2 {
		h := result[0:2]
		_, typ := typetags.DecodeHeader(binary.LittleEndian.Uint16(h))
		assert.Equal(t, typetags.TypeExtendedTagContainer, typ)
	}
}

func TestExtendedPutAccess_EmptySegments(t *testing.T) {
	ec := NewExtendedContainer(4096)

	result, err := ec.Pack()
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestExtendedPutAccess_MixedTypes(t *testing.T) {
	ec := NewExtendedContainer(2048)

	// Add mixed data types
	for i := range 100 {
		require.NoError(t, ec.AddInt16(int16(i)))
		require.NoError(t, ec.AddString("test"))
	}

	result, err := ec.Pack()
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestExtendedPutAccess_ExactThreshold(t *testing.T) {
	ec := NewExtendedContainer(100)

	// Add data exactly at threshold
	for range 10 {
		require.NoError(t, ec.AddString("1234567890")) // 10 bytes each
	}

	result, err := ec.Pack()
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}
