package access

import (
	"testing"

	"github.com/quickwritereader/PackOS/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAccess_ExplicitByteMatch(t *testing.T) {
	buf := []byte{
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

	get := NewGetAccess(buf)

	v0, err := get.GetInt16(0)
	require.NoError(t, err)
	assert.Equal(t, int16(42), v0)

	v1, err := get.GetBool(1)
	require.NoError(t, err)
	assert.Equal(t, true, v1)

	v2, err := get.GetString(2)
	require.NoError(t, err)
	assert.Equal(t, "go", v2)

	v3, err := get.GetBytes(3)
	require.NoError(t, err)
	assert.Equal(t, []byte{0xAA, 0xBB}, v3)
}

func TestGetAccess_Map2(t *testing.T) {
	buf := []byte{
		0x27, 0x00, 0xE0, 0x00,
		0x56, 0x00, 0x26, 0x00, 0x4E, 0x00, 0x6E, 0x00, 0x90, 0x00,
		'r', 'o', 'l', 'e',
		'a', 'd', 'm', 'i', 'n',
		'u', 's', 'e', 'r',
		'a', 'l', 'i', 'c', 'e',
	}
	get := NewGetAccess(buf)

	m, err := get.GetMapStr(0)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"role": "admin",
		"user": "alice",
	}, m)
}

func TestGetAccess_MapOrderedAny(t *testing.T) {
	buf := []byte{
		0x27, 0x00, 0xE0, 0x00,
		0x56, 0x00, 0x26, 0x00, 0x4E, 0x00, 0x6E, 0x00, 0x90, 0x00,
		'r', 'o', 'l', 'e',
		'a', 'd', 'm', 'i', 'n',
		'u', 's', 'e', 'r',
		'a', 'l', 'i', 'c', 'e',
	}
	get := NewGetAccess(buf)

	om, err := get.GetMapOrderedAny(0)
	require.NoError(t, err)
	require.NotNil(t, om)

	// Build expected ordered map
	expected := types.NewOrderedMapAny(
		types.OPAny("role", "admin"),
		types.OPAny("user", "alice"),
	)

	// Use Equal method to compare
	assert.True(t, om.Equal(expected), "decoded OrderedMapAny does not match expected")

	// Also check insertion order explicitly
	keys := []string{}
	for k := range om.KeysIter() {
		keys = append(keys, k)
	}

	assert.Equal(t, []string{"role", "user"}, keys)
}

func TestGetAccess_IntThenMapWithInnerMapAndString(t *testing.T) {
	buf := []byte{
		0x31, 0x00, 0x17, 0x00, 0xB0, 0x01,
		0x39, 0x30,
		0x56, 0x00, 0x27, 0x00, 0x06, 0x01, 0x26, 0x01, 0x50, 0x01,
		'm', 'e', 't', 'a',
		0x56, 0x00, 0x26, 0x00, 0x4E, 0x00, 0x6E, 0x00, 0x90, 0x00,
		'r', 'o', 'l', 'e',
		'a', 'd', 'm', 'i', 'n',
		'u', 's', 'e', 'r',
		'a', 'l', 'i', 'c', 'e',
		'n', 'a', 'm', 'e',
		'g', 'o', 'p', 'h', 'e', 'r',
	}
	get := NewGetAccess(buf)

	v0, err := get.GetInt16(0)
	require.NoError(t, err)
	assert.Equal(t, int16(12345), v0)

	m, err := get.GetMapAny(1)
	require.NoError(t, err)

	meta := m["meta"].(map[string]any)
	assert.Equal(t, "admin", meta["role"].(string))
	assert.Equal(t, "alice", meta["user"].(string))

	assert.Equal(t, "gopher", m["name"].(string))
}
