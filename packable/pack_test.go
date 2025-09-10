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
		0x51, 0x00, // header[0]: absolute offset = 10, TypeInt16
		0x15, 0x00, // header[1]: delta = 2,  TypeBool   → offset = 10 + 2 = 12
		0x1E, 0x00, // header[2]: delta = 3,  TypeString → offset = 10 + 3 = 13
		0x2E, 0x00, // header[3]: delta = 5,  TypeString → offset = 10 + 5 = 15
		0x38, 0x00, // header[4]: delta = 7,  TypeEnd    → offset = 10 + 7 = 17

		// Payload (7 bytes)
		0x2A, 0x00, // int16(42)        @ offset 10
		0x01,       // bool(true)       @ offset 12
		0x67, 0x6F, // "go"             @ offset 13
		0xAA, 0xBB, // bytes            @ offset 15
		// @ offset 17
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

func TestPackable_TwoTuplesByteMatch(t *testing.T) {
	actual := Pack(
		PackTuple(
			PackInt32(2025),
			PackBool(false),
			PackString("az"),
		),
		PackTuple(
			PackInt16(7),
			PackBool(true),
			PackString("go"),
		),
	)

	expected := []byte{
		// Outer headers (3 × 2 bytes)
		0x34, 0x00, // header[0]: absolute offset = 6,  type=4 → Tuple        @ offset 0 → payload @ offset 6
		0x7C, 0x00, // header[1]: delta = 15,           type=4 → Tuple        @ offset 2 → payload @ offset 21 (6 + 15)
		0xE0, 0x00, // header[2]: delta = 28,           type=0 → End          @ offset 4 → marks end @ offset 34 (6 + 28)

		// Tuple 1 headers (4 × 2 bytes)                                      @ offset 6
		0x41, 0x00, // header[0]: absolute offset = 8,  type=1 → Int32        @ offset 6  → inner_offset 8
		0x25, 0x00, // header[1]: delta = 4,            type=5 → Bool         @ offset 8  → inner_offset 12 (8 + 4)
		0x2E, 0x00, // header[2]: delta = 5,            type=6 → String       @ offset 10 → inner_offset 13 (8 + 5)
		0x38, 0x00, // header[3]: delta = 7,            type=0 → End          @ offset 12 → inner_offset 15 (8 + 7)

		// Tuple 1 payload (7 bytes)
		0xE9, 0x07, 0x00, 0x00, // int32(2025)                                @ offset 14 → inner_offset 8
		0x00,       // bool(false)                                            @ offset 18 → inner_offset 12
		0x61, 0x7A, // "az"                                                   @ offset 19 → inner_offset 13

		// Tuple 2 headers (4 × 2 bytes)                                      @ offset 21
		0x41, 0x00, // header[0]: absolute offset = 8,   type=1 → Int16       @ offset 21 → inner_offset 8
		0x15, 0x00, // header[1]: delta = 2,             type=5 → Bool        @ offset 23 → inner_offset 10 (8 + 2)
		0x1E, 0x00, // header[2]: delta = 3,             type=6 → String      @ offset 25 → inner_offset 11 (8 + 3)
		0x28, 0x00, // header[3]: delta = 5,             type=0 → End         @ offset 27 → inner_offset 13 (8 + 5)

		// Tuple 2 payload (5 bytes)
		0x07, 0x00, // int16(7)                                               @ offset 29 → inner_offset 8
		0x01,       // bool(true)                                             @ offset 31 → inner_offset 10
		0x67, 0x6F, // "go"                                                   @ offset 32 → inner_offset 11
		//                                               final byte           @ offset 34 → inner_offset 13
	}

	fmt.Printf("% X  \n(%d)\n", actual, len(actual))
	fmt.Printf("% X  \n(%d)\n", expected, len(expected))
	require.Equal(t, len(expected), len(actual), "Length mismatch")

	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch", i)
	}
}
