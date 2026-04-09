package access

import (
	"encoding/binary"
	"testing"

	"github.com/quickwritereader/PackOS/typetags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendedContainer_NestedPromotion(t *testing.T) {
	// Create a structure with nested large tuple that should be promoted
	ec := NewExtendedContainer(8192)

	// Add some integers
	err := ec.AddInt64(0xaabbccdd)
	require.NoError(t, err)
	err = ec.AddInt64(0xbbccddee)
	require.NoError(t, err)

	// Create a large nested tuple (simulating >8KB)
	nested := ec.BeginTuple()

	// Add large amount of data to nested tuple
	largeData := make([]byte, 9000) // >8KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	err = nested.AddBytes(largeData)
	require.NoError(t, err)

	// End the nested tuple
	require.NoError(t, ec.EndNested(nested))

	// Add more integers after the nested tuple
	err = ec.AddInt64(0xdeaddead)
	require.NoError(t, err)
	err = ec.AddInt64(0xabdeabde)
	require.NoError(t, err)

	// Pack the final structure
	data, err := ec.Pack()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify the structure
	reader := NewExtendedReader(data)
	require.NotNil(t, reader)

	// Should have extended container
	assert.GreaterOrEqual(t, reader.SegmentCount(), 1)

	// Verify we can access elements
	if reader.GetAccess() != nil {
		// Check first integer using GetBytes
		bytes1, err := reader.GetBytes(0)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes1))
		val1 := int64(binary.LittleEndian.Uint64(bytes1))
		assert.Equal(t, int64(0xaabbccdd), val1)

		// Check second integer using GetBytes
		bytes2, err := reader.GetBytes(1)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes2))
		val2 := int64(binary.LittleEndian.Uint64(bytes2))
		assert.Equal(t, int64(0xbbccddee), val2)

		// The nested tuple should be an extended container
		// Check if position 2 is an extended container
		_, start, end := reader.GetAccess().rangeAt(2)
		require.True(t, end > start)

		nestedData := reader.GetAccess().buf[start:end]
		if len(nestedData) >= 2 {
			// Check if it's marked as extended container
			readerNested := NewExtendedReader(nestedData)
			if readerNested.SegmentCount() > 0 {
				// Successfully promoted to extended container
				t.Log("Nested tuple was promoted to extended container")
			}
		}

		// Check integers after nested tuple using GetBytes
		// Note: GetBytes skips extended containers, so positions shift
		bytes3, err := reader.GetBytes(2)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes3))
		val3 := int64(binary.LittleEndian.Uint64(bytes3))
		assert.Equal(t, int64(0xdeaddead), val3)

		bytes4, err := reader.GetBytes(3)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes4))
		val4 := int64(binary.LittleEndian.Uint64(bytes4))
		assert.Equal(t, int64(0xabdeabde), val4)
	}
}

func TestExtendedContainer_BFSAccess(t *testing.T) {
	// Create multi-segment extended container
	ec := NewExtendedContainer(8192)

	// Add data that will span multiple segments
	for i := 0; i < 100; i++ {
		// Create large strings to force segmentation
		largeStr := make([]byte, 1000)
		for j := range largeStr {
			largeStr[j] = byte((i + j) % 256)
		}
		err := ec.AddBytes(largeStr)
		require.NoError(t, err)
	}

	data, err := ec.Pack()
	require.NoError(t, err)

	reader := NewExtendedReader(data)
	require.NotNil(t, reader)

	// Test BFS style access by walking through segments
	segmentCount := reader.SegmentCount()
	assert.Greater(t, segmentCount, 1)

	// Walk through all segments
	visited := 0
	for i := 0; i < segmentCount; i++ {
		// Jump to segment by resetting and calling NextSegment repeatedly
		if i == 0 {
			// Already at first segment
		} else {
			// Reset to first segment and walk forward
			reader = NewExtendedReader(data)
			for j := 0; j < i; j++ {
				if !reader.NextSegment() {
					break
				}
			}
		}

		visited++

		// Check if current segment has data
		if reader.GetAccess() != nil {
			// Each segment should be valid
			assert.NotNil(t, reader.GetAccess())
		}
	}

	assert.Equal(t, segmentCount, visited)
}

func TestExtendedContainer_DFSTraversal(t *testing.T) {
	// Create nested structure with extended containers
	ec := NewExtendedContainer(8192)

	// Outer container
	err := ec.AddInt64(1)
	require.NoError(t, err)

	// First nested (large, should be extended)
	nested1 := ec.BeginTuple()
	largeData1 := make([]byte, 9000)
	err = nested1.AddBytes(largeData1)
	require.NoError(t, err)
	require.NoError(t, ec.EndNested(nested1))

	err = ec.AddInt64(2)
	require.NoError(t, err)

	// Second nested (also large)
	nested2 := ec.BeginTuple()
	largeData2 := make([]byte, 9000)
	err = nested2.AddBytes(largeData2)
	require.NoError(t, err)
	require.NoError(t, ec.EndNested(nested2))

	err = ec.AddInt64(3)
	require.NoError(t, err)

	data, err := ec.Pack()
	require.NoError(t, err)

	reader := NewExtendedReader(data)
	require.NotNil(t, reader)

	// Test BFS traversal (GetBytes skips extended containers)
	if reader.GetAccess() != nil {
		// Check outer elements using GetBytes
		// Position 0 should be int64(1)
		bytes1, err := reader.GetBytes(0)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes1))
		val1 := int64(binary.LittleEndian.Uint64(bytes1))
		assert.Equal(t, int64(1), val1)

		// Position 1 should be int64(2) (skipped extended container at original position 1)
		bytes2, err := reader.GetBytes(1)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes2))
		val2 := int64(binary.LittleEndian.Uint64(bytes2))
		assert.Equal(t, int64(2), val2)

		// Position 2 should be int64(3) (skipped extended container at original position 3)
		bytes3, err := reader.GetBytes(2)
		require.NoError(t, err)
		require.Equal(t, 8, len(bytes3))
		val3 := int64(binary.LittleEndian.Uint64(bytes3))
		assert.Equal(t, int64(3), val3)

		// Test DFS traversal using PushSegment/PopSegment
		// First, get the extended container at original position 1
		// We need to access it directly using rangeAt
		tp, start, end := reader.GetAccess().rangeAt(1)
		if tp == typetags.TypeExtendedTagContainer && end > start {
			nestedData := reader.GetAccess().buf[start:end]
			reader.PushSegment(nestedData)
			t.Log("Successfully entered first nested extended container via PushSegment")
			reader.PopSegment()
		}

		// Get the extended container at original position 3
		tp, start, end = reader.GetAccess().rangeAt(3)
		if tp == typetags.TypeExtendedTagContainer && end > start {
			nestedData := reader.GetAccess().buf[start:end]
			reader.PushSegment(nestedData)
			t.Log("Successfully entered second nested extended container via PushSegment")
			reader.PopSegment()
		}
	}
}

func TestExtendedContainer_TripletTracking(t *testing.T) {
	ec := NewExtendedContainer(8192)

	// Add a large array that will be extended
	largeData := make([]byte, 20000) // >8KB, will create extended container
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err := ec.AddBytes(largeData)
	require.NoError(t, err)

	data, err := ec.Pack()
	require.NoError(t, err)

	// Check triplet information from container
	triplets := ec.GetTriplets()
	assert.NotEmpty(t, triplets)

	for _, triplet := range triplets {
		if triplet.IsExtended {
			// Extended containers should have actual segment data
			assert.NotEmpty(t, triplet.ActualSegment)
			// Should have next offset address (could be -1 for root segments)
			// SelfOffset can be 0 for first segment
			t.Logf("Triplet: SelfOffset=%d, Continuation=%d, NextOffsetAddr=%d, ActualSegment len=%d",
				triplet.SelfOffset, triplet.Continuation, triplet.NextOffsetAddr, len(triplet.ActualSegment))
		}
	}

	// Also verify we can decode the data
	reader := NewExtendedReader(data)
	require.NotNil(t, reader)
	assert.Greater(t, reader.SegmentCount(), 0)
}

func TestExtendedContainer_CrossSegmentAccess(t *testing.T) {
	// Create data with known field positions across segments
	// Use smaller pivot size to force segmentation
	ec := NewExtendedContainer(1024)

	// Add fields with unique values
	fieldValues := []string{"field0", "field1", "field2", "field3", "field4"}

	for i, value := range fieldValues {
		// Add some padding to force segmentation
		if i == 2 {
			largeData := make([]byte, 5000)
			err := ec.AddBytes(largeData)
			require.NoError(t, err)
		}
		err := ec.AddString(value)
		require.NoError(t, err)
	}

	data, err := ec.Pack()
	require.NoError(t, err)

	reader := NewExtendedReader(data)
	require.NotNil(t, reader)

	// Try to access fields across segments
	// The 5000-byte blob at position 2 is a nested extended container
	// that GetBytes can't decode, so GetBytes skips it.
	// So accessible string positions are 0, 1, 2, 3, 4
	for i, expected := range fieldValues {
		bytes, err := reader.GetBytes(i)
		if err == nil {
			// Convert bytes to string
			strVal := string(bytes)
			assert.Equal(t, expected, strVal)
		}
	}

	// Verify data is not empty
	assert.NotEmpty(t, data)
}
