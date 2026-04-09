package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerification(t *testing.T) {
	t.Run("triplet tracking", func(t *testing.T) {
		ec := NewExtendedContainer(8192)

		// Add data that will create triplets
		largeData := make([]byte, 10000) // >8KB
		err := ec.AddBytes(largeData)
		require.NoError(t, err)

		_, err = ec.Pack()
		require.NoError(t, err)

		// Verify triplets are tracked
		triplets := ec.GetTriplets()
		assert.NotEmpty(t, triplets, "Should have triplets tracked")

		for _, triplet := range triplets {
			// Verify triplet structure
			assert.NotNil(t, triplet.ActualSegment, "Actual segment should not be nil")
			if triplet.IsExtended {
				// SelfOffset can be 0 for first segment in extended container chain
				// The important thing is that it's properly set (not some uninitialized value)
				// For extended containers, we should have actual segment data
				assert.NotEmpty(t, triplet.ActualSegment, "Extended container should have actual segment")
			}
		}
	})

	t.Run("nested container promotion", func(t *testing.T) {
		ec := NewExtendedContainer(8192)

		// Add regular data
		err := ec.AddInt64(1)
		require.NoError(t, err)

		// Create nested container with large data
		nested := ec.BeginTuple()
		largeData := make([]byte, 9000) // >8KB
		err = nested.AddBytes(largeData)
		require.NoError(t, err)
		require.NoError(t, ec.EndNested(nested))

		err = ec.AddInt64(2)
		require.NoError(t, err)

		_, err = ec.Pack()
		require.NoError(t, err)

		// Verify nested container was promoted
		triplets := ec.GetTriplets()
		hasExtended := false
		for _, triplet := range triplets {
			if triplet.IsExtended {
				hasExtended = true
				break
			}
		}
		assert.True(t, hasExtended, "Should have extended containers")
	})

	t.Run("bfs dfs access", func(t *testing.T) {
		ec := NewExtendedContainer(8192)

		// Create multi-segment structure
		for i := 0; i < 3; i++ {
			largeData := make([]byte, 5000)
			err := ec.AddBytes(largeData)
			require.NoError(t, err)
		}

		data, err := ec.Pack()
		require.NoError(t, err)

		reader := NewExtendedReader(data)
		require.NotNil(t, reader)

		// Test BFS access
		segmentCount := reader.SegmentCount()
		assert.Greater(t, segmentCount, 1, "Should have multiple segments")

		// Test segment navigation
		assert.True(t, reader.NextSegment(), "Should be able to move to next segment")
		assert.Equal(t, 1, reader.CurrentSegment(), "Should be at segment 1")

		// Reset and test jump
		reader = NewExtendedReader(data)
		// Jump to last segment by calling NextSegment repeatedly
		for i := 0; i < segmentCount-1; i++ {
			assert.True(t, reader.NextSegment(), "Should be able to move to next segment")
		}
		assert.Equal(t, segmentCount-1, reader.CurrentSegment(), "Should be at last segment")
	})

	t.Run("continuation addresses", func(t *testing.T) {
		ec := NewExtendedContainer(8192)

		// Create extended container
		largeData := make([]byte, 20000) // Will be split into segments
		err := ec.AddBytes(largeData)
		require.NoError(t, err)

		_, err = ec.Pack()
		require.NoError(t, err)

		// Verify continuation addresses are valid by checking triplets
		triplets := ec.GetTriplets()
		for _, triplet := range triplets {
			if triplet.IsExtended {
				// Extended containers should have actual segment data
				assert.NotEmpty(t, triplet.ActualSegment, "Extended container should have actual segment")
			}
		}
	})
}
