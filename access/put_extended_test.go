package access

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/quickwritereader/PackOS/typetags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Layout agreed in the ADR 001 discussion: with a 16-byte pivot a 16-byte
// string leaves a placeholder in the root segment and its data moves into a
// linked segment, while the values around it stay where PutAccess puts them.
func TestExtendedPutAccess_ExplicitByteMatch(t *testing.T) {
	put := NewExtendedPutAccess(16)

	put.AddInt16(42)                       // 2 bytes
	put.AddBool(true)                      // 1 byte
	put.AddString(strings.Repeat("A", 16)) // reaches the pivot: placeholder + one data segment
	put.AddBytes([]byte{0xAA, 0xBB})       // 2 bytes

	actual, err := put.PackExtended()
	require.NoError(t, err)

	expected := []byte{
		// Root segment headers (5 × 2 bytes)
		0x51, 0x00, // header[0]: absolute offset=10, TypeInt16
		0x15, 0x00, // header[1]: delta=2, TypeBool
		0x1A, 0x00, // header[2]: delta=3, TypeExtendedTagContainer (placeholder)
		0x3E, 0x00, // header[3]: delta=7, TypeString (bytes)
		0x48, 0x00, // header[4]: delta=9, TypeEnd

		// Root segment payload (9 bytes)
		0x2A, 0x00, // int16(42)
		0x01,                   // bool(true)
		0x13, 0x00, 0x00, 0x00, // NextSegmentOffset: the data segment starts at absolute offset 19
		0xAA, 0xBB, // bytes

		// Data segment: a container with one TypeExtendedTagContainer element (payload 24 bytes)
		0x22, 0x00, // header[0]: absolute offset=4, TypeExtendedTagContainer
		0xC0, 0x00, // header[1]: delta=24, TypeEnd
		0x00, 0x00, 0x00, 0x00, // NextSegmentOffset: EndOfChain, last segment
		0x26, 0x00, // inner header[0]: absolute offset=4, TypeString
		0x80, 0x00, // inner header[1]: delta=16, TypeEnd
		'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
		'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
	}

	require.Equal(t, len(expected), len(actual), "Length mismatch")
	for i := range expected {
		assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch", i)
	}
}

func TestExtendedPutAccess_ValuesBelowPivotMatchPutAccess(t *testing.T) {
	type adder interface {
		AddInt16(int16)
		AddString(string)
		AddBytes([]byte)
		AddIntegerArray([]int64)
		AddNullableString(*string)
	}
	small := "fifteen bytes.."
	fill := func(a adder) {
		a.AddInt16(7)
		a.AddString(small)
		a.AddBytes(bytes.Repeat([]byte{0x01}, 15))
		a.AddIntegerArray([]int64{1, 2, 3, 4, 5, 6})
		a.AddNullableString(nil)
		a.AddNullableString(&small)
	}

	plain := NewPutAccess()
	fill(plain)
	ext := NewExtendedPutAccess(16)
	fill(ext)

	actual, err := ext.PackExtended()
	require.NoError(t, err)

	assert.Equal(t, plain.Pack(), actual)
	assert.Equal(t, 0, ext.SegmentCount())
}

func TestExtendedPutAccess_PivotDefaultsAndBounds(t *testing.T) {
	assert.Equal(t, DefaultPivotSize, NewExtendedPutAccess(0).PivotSize())
	assert.Equal(t, DefaultPivotSize, NewExtendedPutAccess(-1).PivotSize())
	assert.Equal(t, MaxChunkSize, NewExtendedPutAccess(MaxSegmentSize*2).PivotSize())

	inline := NewExtendedPutAccess(16)
	inline.AddString(strings.Repeat("x", 15))
	assert.Equal(t, 0, inline.SegmentCount(), "a value below the pivot stays inline")

	chained := NewExtendedPutAccess(16)
	chained.AddString(strings.Repeat("x", 16))
	assert.Equal(t, 1, chained.SegmentCount(), "a value reaching the pivot is chained")
}

func TestExtendedPutAccess_LargeStringIsChunkedAcrossLinkedSegments(t *testing.T) {
	text := strings.Repeat("abcdefgh", 14*1024/8) // 14 KiB: needs two data segments
	put := NewExtendedPutAccess(0)
	put.AddInt32(1)
	put.AddString(text)
	put.AddInt32(2)

	buf, err := put.PackExtended()
	require.NoError(t, err)
	require.Equal(t, 2, put.SegmentCount())

	root := NewGetAccess(buf)
	require.NotNil(t, root)
	require.Equal(t, 3, root.FieldCount())

	before, err := root.GetInt32(0)
	require.NoError(t, err)
	assert.Equal(t, int32(1), before)
	after, err := root.GetInt32(2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), after, "values after the placeholder stay addressable")

	tp, placeholder := root.GetTypeAndValue(1)
	assert.Equal(t, typetags.TypeExtendedTagContainer, tp)
	require.Len(t, placeholder, typetags.ExtendedContainerValueSize, "the placeholder carries only the link")

	chunks := followChain(t, buf, binary.LittleEndian.Uint32(placeholder))
	require.Len(t, chunks, 2)
	assert.Equal(t, MaxChunkSize, len(chunks[0].data))
	assert.Equal(t, len(text)-MaxChunkSize, len(chunks[1].data))
	var joined strings.Builder
	for _, c := range chunks {
		assert.Equal(t, typetags.TypeString, c.tag)
		joined.Write(c.data)
	}
	assert.Equal(t, text, joined.String())
}

func TestExtendedPutAccess_EachChainedValueGetsItsOwnChain(t *testing.T) {
	first := bytes.Repeat([]byte{0xF1}, 20)
	second := strings.Repeat("s", 40)
	put := NewExtendedPutAccess(16)
	put.AddBytes(first)
	put.AddString(second)

	buf, err := put.PackExtended()
	require.NoError(t, err)
	require.Equal(t, 2, put.SegmentCount())

	root := NewGetAccess(buf)
	require.NotNil(t, root)
	require.Equal(t, 2, root.FieldCount())

	_, link := root.GetTypeAndValue(0)
	chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 1)
	assert.Equal(t, first, chunks[0].data)

	_, link = root.GetTypeAndValue(1)
	chunks = followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 1)
	assert.Equal(t, second, string(chunks[0].data))
}

func TestExtendedPutAccess_IntegerArrayChunksCarryTheirElementSize(t *testing.T) {
	values := make([]int64, 3000) // 4-byte elements: 12001-byte payload, 2045 elements per chunk
	for i := range values {
		values[i] = int64(i) * 1000
	}
	put := NewExtendedPutAccess(0)
	put.AddIntegerArray(values)

	buf, err := put.PackExtended()
	require.NoError(t, err)

	_, link := NewGetAccess(buf).GetTypeAndValue(0)
	chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 2)

	var decoded []int64
	for _, c := range chunks {
		assert.Equal(t, typetags.TypeInteger, c.tag)
		assert.Equal(t, byte(4), c.data[0], "every chunk starts with the element size")
		assert.Equal(t, 0, (len(c.data)-1)%4, "chunks end on an element boundary")
		v, err := DecodePrimitive(c.tag, c.data)
		require.NoError(t, err)
		decoded = append(decoded, v.([]int64)...)
	}
	assert.Equal(t, values, decoded)
}

func TestExtendedPutAccess_FloatArrayChunksCarryTheirElementSize(t *testing.T) {
	values := make([]float64, 2000) // 16001-byte payload, 1022 elements per chunk
	for i := range values {
		values[i] = float64(i) + 0.5
	}
	put := NewExtendedPutAccess(0)
	put.AddFloatArray(values)

	buf, err := put.PackExtended()
	require.NoError(t, err)

	_, link := NewGetAccess(buf).GetTypeAndValue(0)
	chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 2)

	var decoded []float64
	for _, c := range chunks {
		assert.Equal(t, typetags.TypeFloating, c.tag)
		assert.Equal(t, byte(8), c.data[0], "every chunk starts with the element size")
		v, err := DecodePrimitive(c.tag, c.data)
		require.NoError(t, err)
		decoded = append(decoded, v.([]float64)...)
	}
	assert.Equal(t, values, decoded)
}

// chainChunk is one data segment of a chain, as read back with GetAccess.
type chainChunk struct {
	tag  typetags.Type
	data []byte
}

// followChain reads the data segments of a chain starting at an absolute
// offset, using the existing random-access reader on each stand-alone segment.
func followChain(t *testing.T, buf []byte, offset uint32) []chainChunk {
	t.Helper()

	var chunks []chainChunk
	for offset != typetags.EndOfChain {
		seg := NewGetAccess(buf[offset:])
		require.NotNil(t, seg)
		require.Equal(t, 1, seg.FieldCount())
		tp, val := seg.GetTypeAndValue(0)
		require.Equal(t, typetags.TypeExtendedTagContainer, tp)
		require.Less(t, len(val), MaxSegmentSize, "segment payload must stay addressable by a 13-bit delta")

		inner := NewGetAccess(val[typetags.ExtendedContainerValueSize:])
		require.NotNil(t, inner)
		require.Equal(t, 1, inner.FieldCount())
		innerType, data := inner.GetTypeAndValue(0)
		chunks = append(chunks, chainChunk{tag: innerType, data: data})

		offset = binary.LittleEndian.Uint32(val)
	}
	return chunks
}
