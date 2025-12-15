package access

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutAccess_ExplicitByteMatch(t *testing.T) {
	put := NewPutAccess()

	put.AddInt16(42)                 // 2 bytes
	put.AddBool(true)                // 1 byte
	put.AddString("go")              // 2 bytes (no prefix)
	put.AddBytes([]byte{0xAA, 0xBB}) // 2 bytes (no prefix)

	actual := put.Pack()

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

	require.Equal(t, len(expected), len(actual), "Length mismatch")

	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch", i)
	}
}

func TestPutAccess_MapSortedKey(t *testing.T) {
	put := NewPutAccess()

	put.AddMapSortedKey(map[string][]byte{
		"user": []byte("alice"),
		"role": []byte("admin"),
	})

	actual := put.Pack()
	expected := []byte{
		// Outer Header Block (base = 0)
		0x27, 0x00, // header[0]: absolute offset = 4, TypeMap
		0xE0, 0x00, // header[1]: delta = 28, TypeEnd → payload ends at 4 + 28 = 32

		// Inner1 Header Block (base = 32)
		0x56, 0x00, // inner1 absolute = 10, TypeString ("role")
		0x26, 0x00, // inner1 delta = 4,  TypeString ("admin")
		0x4E, 0x00, // inner1 delta = 9,  TypeString ("user")
		0x6E, 0x00, // inner1 delta = 13, TypeString ("alice")
		0x90, 0x00, // inner1 delta = 18, TypeEnd

		// Payload Block
		'r', 'o', 'l', 'e', // offset 14
		'a', 'd', 'm', 'i', 'n', // offset 18
		'u', 's', 'e', 'r', // offset 23
		'a', 'l', 'i', 'c', 'e', // offset 27
	}

	require.Equal(t, len(expected), len(actual), "Length mismatch: expected %d, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch: expected %02X, got %02X", i, expected[i], actual[i])
	}
}

func TestPutAccess_IntThenMapWithInnerMapAndString(t *testing.T) {
	put := NewPutAccess()

	put.AddInt16(12345) // 2 bytes

	put.AddMapAnySortedKey(map[string]any{
		"meta": map[string][]byte{
			"user": []byte("alice"),
			"role": []byte("admin"),
		},
		"name": "gopher",
	}, false)

	expected := []byte{
		// Outer Header Block (base = 0)
		0x31, 0x00, // absolute offset = 6, TypeInt16
		0x17, 0x00, // delta = 2, TypeMap → offset = 8
		0xB0, 0x01, // delta = 54, TypeEnd → offset = 60

		// Outer Payload
		0x39, 0x30, // int16(12345)

		// inner1 Header Block (base = 8)
		0x56, 0x00, // inner1 absolute = 10, TypeString ("meta") → offset = 18
		0x27, 0x00, // inner1 delta = 4, TypeMap → offset = 12
		0x06, 0x01, // inner1 delta = 66, TypeString ("name") → offset = 74
		0x26, 0x01, // inner1 delta = 74, TypeString ("gopher") → offset = 82
		0x50, 0x01, // inner1 delta = 80, TypeEnd → offset = 88

		// inner1 Payload
		'm', 'e', 't', 'a',

		// inner1.1 Header Block (base = 12)
		0x56, 0x00, // inner1.1 absolute = 10, TypeString ("role") → offset = 22
		0x26, 0x00, // inner1.1 delta = 4,  TypeString ("admin") → offset = 16
		0x4E, 0x00, // inner1.1 delta = 9,  TypeString ("user") → offset = 21
		0x6E, 0x00, // inner1.1 delta = 13, TypeString ("alice") → offset = 25
		0x90, 0x00, // inner1.1 delta = 18, TypeEnd → offset = 30

		// inner1.1 Payload
		'r', 'o', 'l', 'e', // offset 22
		'a', 'd', 'm', 'i', 'n', // offset 16
		'u', 's', 'e', 'r', // offset 21
		'a', 'l', 'i', 'c', 'e', // offset 25

		// Remaining inner1 Payload
		'n', 'a', 'm', 'e', // offset 74
		'g', 'o', 'p', 'h', 'e', 'r', // offset 82
	}

	actual := put.Pack()
	fmt.Printf("% X  \n(%d)\n", actual, len(actual))

	require.Equal(t, len(expected), len(actual), "Length mismatch: expected %d, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch: expected %02X, got %02X", i, expected[i], actual[i])
	}

}

func TestPutAccess_NullableFloat32ExplicitBuffer(t *testing.T) {
	var (
		vInt32   int32   = 123456
		vFloat32 float32 = 3.14159
		vBool    bool    = true
	)

	put := NewPutAccess()
	put.AddNullableInt32(nil)
	put.AddNullableInt32(&vInt32)
	put.AddNullableFloat32(nil)
	put.AddNullableFloat32(&vFloat32)
	put.AddNullableBool(nil)
	put.AddNullableBool(&vBool)
	actual := put.Pack()

	expected := []byte{
		0x71, 0x00, // Int32 nil  	    [0] = 0x0071 → absolute=14, type=1
		0x01, 0x00, // Int32 value  	[1] = 0x0001 → delta=0,  type=1
		0x23, 0x00, // Float32 nil  	[2] = 0x0023 → delta=4,  type=3
		0x23, 0x00, // Float32 value  	[3] = 0x0023 → delta=4,  type=3
		0x45, 0x00, // Bool nil  	    [4] = 0x0045 → delta=8,  type=5
		0x45, 0x00, // Bool value    	[5] = 0x0045 → delta=8,  type=5
		0x48, 0x00, // End  	        [6] = 0x0048 → delta=9,  type=0 (End)

		0x40, 0xE2, 0x01, 0x00, // int32(123456)
		0xD0, 0x0F, 0x49, 0x40, // float32(3.14159)
		0x01, // bool(true)
	}

	fmt.Printf("% X  \n(%d)\n", actual, len(actual))

	if len(actual) != len(expected) {
		t.Fatalf("Length mismatch: expected %d, got %d", len(expected), len(actual))
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Errorf("Byte %d mismatch: expected %02X, got %02X", i, expected[i], actual[i])
		}
	}
}
