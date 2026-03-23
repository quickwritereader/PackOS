package typetags

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendedHeader_EncodeDecode(t *testing.T) {
	tests := []struct {
		name         string
		selfOffset   uint32
		continuation uint32
	}{
		{"valid headers", 100, 200},
		{"end of chain", 0, EndOfChain},
		{"large offsets", 0xFFFFFFFF, 0xFFFFFFFE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeExtendedHeader(tt.selfOffset, tt.continuation)
			assert.Len(t, encoded, ExtendedHeaderSize)

			decoded, ok := DecodeExtendedHeader(encoded)
			require.True(t, ok)
			assert.Equal(t, tt.selfOffset, decoded.SelfOffset)
			assert.Equal(t, tt.continuation, decoded.Continuation)
		})
	}
}

func TestExtendedHeader_InvalidDecode(t *testing.T) {
	shortBuf := make([]byte, 4)
	_, ok := DecodeExtendedHeader(shortBuf)
	assert.False(t, ok)
}
