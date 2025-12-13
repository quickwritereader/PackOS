package access

import (
	"testing"

	"github.com/quickwritereader/packos/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeqGetAccess_UnpacksNestedMap(t *testing.T) {
	input := []byte{
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

	seq, _ := NewSeqGetAccess(input)
	require.NotNil(t, seq, "accessor should be initialized")

	// Field 0: Int16
	payload, typ, err := seq.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeInteger, typ)
	assert.Equal(t, []byte{0x39, 0x30}, payload)

	// Field 1: Map
	typ, w, err := seq.PeekTypeWidth()
	require.NoError(t, err)
	assert.Equal(t, w, 52)
	assert.Equal(t, types.TypeMap, typ)

	nested, err := seq.PeekNestedSeq()
	require.NoError(t, err)

	// Field 0 in outer map: "meta" → nested map
	payload, typ, err = nested.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, typ)
	assert.Equal(t, "meta", string(payload))

	meta, err := nested.PeekNestedSeq()
	require.NoError(t, err)

	// Validate "role"
	rolePayload, roleType, err := meta.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, roleType)
	assert.Equal(t, "role", string(rolePayload))

	// Validate "admin"
	adminPayload, adminType, err := meta.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, adminType)
	assert.Equal(t, "admin", string(adminPayload))

	// Field 1 in outer map: "name"
	err = nested.Advance()
	require.NoError(t, err)

	namePayload, nameType, err := nested.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, nameType)
	assert.Equal(t, "name", string(namePayload))

	namePayload, nameType, err = nested.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, nameType)
	assert.Equal(t, "gopher", string(namePayload))
}

func TestSeqGetAccess_ExplicitByteMatch(t *testing.T) {
	input := []byte{
		// Headers (base = 0)
		0x51, 0x00, // offset = 10, TypeInt16
		0x15, 0x00, // delta = 2, TypeBool
		0x1E, 0x00, // delta = 3, TypeString ("go")
		0x2E, 0x00, // delta = 5, TypeString (bytes)
		0x38, 0x00, // delta = 7, TypeEnd

		// Payload
		0x2A, 0x00, // int16(42)
		0x01,       // bool(true)
		0x67, 0x6F, // "go"
		0xAA, 0xBB, // bytes
	}

	seq, _ := NewSeqGetAccess(input)
	require.NotNil(t, seq, "accessor should be initialized")

	// Field 0: Int16
	payload, typ, err := seq.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeInteger, typ)
	assert.Equal(t, []byte{0x2A, 0x00}, payload)

	// Field 1: Bool
	payload, typ, err = seq.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeBool, typ)
	assert.Equal(t, []byte{0x01}, payload)

	// Field 2: String ("go")
	payload, typ, err = seq.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, typ)
	assert.Equal(t, "go", string(payload))

	// Field 3: Bytes
	payload, typ, err = seq.Next()
	require.NoError(t, err)
	assert.Equal(t, types.TypeString, typ) // treated as raw string
	assert.Equal(t, []byte{0xAA, 0xBB}, payload)

	// Field 4: End
	_, typ, err = seq.Next()
	assert.Equal(t, types.TypeEnd, typ)
	require.Error(t, err)

}
