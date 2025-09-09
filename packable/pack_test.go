package packable

import (
	"fmt"
	"testing"

	"github.com/BranchAndLink/packos/access"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPackable_ExplicitByteMatch(t *testing.T) {

	actual := Pack(PackInt16(42), PackBool(true), PackString("go"),
		PackByteArray([]byte{0xAA, 0xBB}))

	expected := []byte{
		// Headers (5 × 2 bytes)
		0x51, 0x00, // header[0]: absolute offset=10, TypeInt16
		0x15, 0x00, // header[1]: delta=2, TypeBool
		0x1E, 0x00, // header[2]: delta=3, TypeString ("go")
		0x2E, 0x00, // header[3]: delta=5, TypeString (bytes)
		0x38, 0x00, // header[4]: delta=7, TypeEnd

		// Payload (7 bytes)
		0x2A, 0x00, // int16(42)
		0x01,       // bool(true)
		0x67, 0x6F, // "go"
		0xAA, 0xBB, // bytes
	}

	fmt.Printf("% X  \n(%d)\n", actual, len(actual))
	fmt.Printf("% X  \n(%d)\n", expected, len(expected))
	require.Equal(t, len(expected), len(actual), "Length mismatch")

	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch", i)
	}
}

func TestPackable_TestWithSortedMaps(t *testing.T) {
	actual := Pack(
		PackInt16(12345),
		PackMapSorted{
			"meta": PackMapSorted{
				"user": PackByteArray([]byte("alice")),
				"role": PackByteArray([]byte("admin")),
			},
			"name": PackString("gopher"),
		},
	)

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

	fmt.Printf("% X  \n(%d)\n", actual, len(actual))
	fmt.Printf("% X  \n(%d)\n", expected, len(expected))
	require.Equal(t, len(expected), len(actual), "Length mismatch: expected %d, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch: expected %02X, got %02X", i, expected[i], actual[i])
	}
}

func TestPackable_TestPutAccessWithPack(t *testing.T) {
	mapx := PackMapSorted{
		"meta": PackMapSorted{
			"user": PackByteArray([]byte("alice")),
			"role": PackByteArray([]byte("admin")),
		},
		"name": PackString("gopher"),
	}
	p := access.NewPutAccess()
	p.AddInt16(12345)
	p.AddPackable(mapx)
	p.AddFloat32(4.45)
	actual := p.Pack()

	expected := Pack(PackInt16(12345), mapx, PackFloat32(4.45))
	require.Equal(t, len(expected), len(actual), "Length mismatch: expected %d, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch: expected %02X, got %02X", i, expected[i], actual[i])
	}
}
