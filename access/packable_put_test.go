package access

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPackable_TestWithSortedMaps(t *testing.T) {
	actual := PackArgs(
		PackInt16(12345),
		PackSortedMapPackable{
			"meta": PackSortedMapPackable{
				"user": PackBytes([]byte("alice")),
				"role": PackBytes([]byte("admin")),
			},
			"name": PackString("gopher"),
		},
	)

	fmt.Printf("% X  \n(%d)\n", actual, len(actual))

	expected := []byte{
		// Outer Header Block
		0x31, 0x00, // offset = 6, TypeInt16
		0x17, 0x00, // delta = 2, TypeMap
		0xB0, 0x01, // delta = 54, TypeEnd

		// Outer Payload
		0x39, 0x30, // int16(12345)

		// inner1 Header Block
		0x56, 0x00, // "meta"
		0x27, 0x00, // map
		0x06, 0x01, // "name"
		0x26, 0x01, // "gopher"
		0x50, 0x01, // TypeEnd

		// inner1 Payload
		'm', 'e', 't', 'a',

		// inner1.1 Header Block
		0x56, 0x00, // "role"
		0x26, 0x00, // "admin"
		0x4E, 0x00, // "user"
		0x6E, 0x00, // "alice"
		0x90, 0x00, // TypeEnd

		// inner1.1 Payload
		'r', 'o', 'l', 'e',
		'a', 'd', 'm', 'i', 'n',
		'u', 's', 'e', 'r',
		'a', 'l', 'i', 'c', 'e',

		// Remaining inner1 Payload
		'n', 'a', 'm', 'e',
		'g', 'o', 'p', 'h', 'e', 'r',
	}

	require.Equal(t, len(expected), len(actual), "Length mismatch: expected %d, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch: expected %02X, got %02X", i, expected[i], actual[i])
	}
}
