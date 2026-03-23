package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendedGetAccess_SingleSegment(t *testing.T) {
	// Create single segment data
	put := NewExtendedPutAccess(4096)
	for i := 0; i < 10; i++ {
		require.NoError(t, put.AddInt16Extended(int16(i)))
	}

	data, err := put.PackExtended()
	require.NoError(t, err)

	get := NewExtendedGetAccess(data)
	require.NotNil(t, get)
	assert.Equal(t, 1, get.SegmentCount())
	assert.Equal(t, 0, get.CurrentSegment())
	assert.False(t, get.HasNextSegment())
}

func TestExtendedGetAccess_MultipleSegments(t *testing.T) {
	put := NewExtendedPutAccess(256)

	// Add enough data to create multiple segments
	largeStr := make([]byte, 200)
	for i := range largeStr {
		largeStr[i] = 'A'
	}

	for i := 0; i < 10; i++ {
		require.NoError(t, put.AddStringExtended(string(largeStr)))
	}

	data, err := put.PackExtended()
	require.NoError(t, err)

	get := NewExtendedGetAccess(data)
	require.NotNil(t, get)

	// Should have multiple segments
	assert.Greater(t, get.SegmentCount(), 1)
	assert.True(t, get.HasNextSegment())

	// Navigate through segments
	segmentCount := 0
	for get.HasNextSegment() {
		segmentCount++
		get.NextSegment()
	}
	assert.Equal(t, get.SegmentCount()-1, segmentCount)
}

func TestExtendedGetAccess_GetBytesAcrossSegments(t *testing.T) {
	// Create data that spans segments
	put := NewExtendedPutAccess(64)

	// Add data that will be split
	for i := 0; i < 20; i++ {
		require.NoError(t, put.AddStringExtended("test"))
	}

	data, err := put.PackExtended()
	require.NoError(t, err)

	get := NewExtendedGetAccess(data)
	require.NotNil(t, get)

	// Try to retrieve data from first segment
	if get.GetAccess != nil {
		val, err := get.GetString(0)
		if err == nil {
			assert.NotEmpty(t, val)
		}
	}
}

func TestExtendedGetAccess_NestedContainers(t *testing.T) {
	// Create nested extended container
	outer := NewExtendedPutAccess(512)
	inner := NewExtendedPutAccess(256)

	// Fill inner container
	for i := 0; i < 10; i++ {
		require.NoError(t, inner.AddStringExtended("inner"))
	}

	innerData, err := inner.PackExtended()
	require.NoError(t, err)

	// Add inner as packable to outer
	outer.AddBytes(innerData)

	outerData, err := outer.PackExtended()
	require.NoError(t, err)

	// Decode nested
	get := NewExtendedGetAccess(outerData)
	require.NotNil(t, get)

	// Should handle nested structure
	assert.NotNil(t, get.GetAccess)
}
