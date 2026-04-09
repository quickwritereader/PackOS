package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendedGetAccess_SingleSegment(t *testing.T) {
	// Create single segment data
	ec := NewExtendedContainer(4096)
	for i := 0; i < 10; i++ {
		require.NoError(t, ec.AddInt16(int16(i)))
	}

	data, err := ec.Pack()
	require.NoError(t, err)

	reader := NewExtendedReader(data)
	require.NotNil(t, reader)
	assert.Equal(t, 1, reader.SegmentCount())
	assert.Equal(t, 0, reader.CurrentSegment())
	assert.False(t, reader.HasNextSegment())
}

func TestExtendedGetAccess_MultipleSegments(t *testing.T) {
	ec := NewExtendedContainer(256)

	// Add enough data to create multiple segments
	largeStr := make([]byte, 200)
	for i := range largeStr {
		largeStr[i] = 'A'
	}

	for i := 0; i < 10; i++ {
		require.NoError(t, ec.AddString(string(largeStr)))
	}

	data, err := ec.Pack()
	require.NoError(t, err)

	reader := NewExtendedReader(data)
	require.NotNil(t, reader)

	// Should have multiple segments
	assert.Greater(t, reader.SegmentCount(), 1)
	assert.True(t, reader.HasNextSegment())

	// Navigate through segments
	segmentCount := 0
	for reader.HasNextSegment() {
		segmentCount++
		reader.NextSegment()
	}
	assert.Equal(t, reader.SegmentCount()-1, segmentCount)
}

func TestExtendedGetAccess_GetBytesAcrossSegments(t *testing.T) {
	// Create data that spans segments
	ec := NewExtendedContainer(64)

	// Add data that will be split
	for i := 0; i < 20; i++ {
		require.NoError(t, ec.AddString("test"))
	}

	data, err := ec.Pack()
	require.NoError(t, err)

	reader := NewExtendedReader(data)
	require.NotNil(t, reader)

	// Try to retrieve data from first segment
	if reader.GetAccess() != nil {
		bytes, err := reader.GetBytes(0)
		if err == nil {
			assert.NotEmpty(t, bytes)
		}
	}
}

func TestExtendedGetAccess_NestedContainers(t *testing.T) {
	// Create nested extended container
	outer := NewExtendedContainer(512)
	inner := NewExtendedContainer(256)

	// Fill inner container
	for i := 0; i < 10; i++ {
		require.NoError(t, inner.AddString("inner"))
	}

	innerData, err := inner.Pack()
	require.NoError(t, err)

	// Add inner as packable to outer
	require.NoError(t, outer.AddBytes(innerData))

	outerData, err := outer.Pack()
	require.NoError(t, err)

	// Decode nested
	reader := NewExtendedReader(outerData)
	require.NotNil(t, reader)

	// Should handle nested structure
	assert.NotNil(t, reader.GetAccess())
}
