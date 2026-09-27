package access

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
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
	assertBytes(t, expected, actual)
}

// A container that reaches the pivot leaves a placeholder among its siblings
// and its elements move, as a sub-container, into a linked segment.
func TestExtendedPutAccess_ChainedTupleExplicitByteMatch(t *testing.T) {
	put := NewExtendedPutAccess(16)

	put.AddInt16(42)
	tuple := put.BeginTuple()
	tuple.AddString("hello")
	tuple.AddString("world") // the tuple packs to 16 bytes: it reaches the pivot
	put.EndNested(tuple)
	put.AddBytes([]byte{0xAA, 0xBB})

	actual, err := put.PackExtended()
	require.NoError(t, err)

	expected := []byte{
		// Root segment headers (4 × 2 bytes)
		0x41, 0x00, // header[0]: absolute offset=8, TypeInt16
		0x12, 0x00, // header[1]: delta=2, TypeExtendedTagContainer (placeholder)
		0x36, 0x00, // header[2]: delta=6, TypeString (bytes)
		0x40, 0x00, // header[3]: delta=8, TypeEnd

		// Root segment payload (8 bytes)
		0x2A, 0x00, // int16(42)
		0x10, 0x00, 0x00, 0x00, // NextSegmentOffset: the tuple's segment starts at absolute offset 16
		0xAA, 0xBB, // bytes

		// Data segment: a container with one TypeExtendedTagContainer element (payload 24 bytes)
		0x22, 0x00, // header[0]: absolute offset=4, TypeExtendedTagContainer
		0xC0, 0x00, // header[1]: delta=24, TypeEnd
		0x00, 0x00, 0x00, 0x00, // NextSegmentOffset: EndOfChain, last segment
		0x24, 0x00, // inner header[0]: absolute offset=4, TypeTuple
		0x80, 0x00, // inner header[1]: delta=16, TypeEnd
		// The chunk: the tuple, laid out as PutAccess packs it
		0x36, 0x00, // header[0]: absolute offset=6, TypeString
		0x2E, 0x00, // header[1]: delta=5, TypeString
		0x50, 0x00, // header[2]: delta=10, TypeEnd
		'h', 'e', 'l', 'l', 'o',
		'w', 'o', 'r', 'l', 'd',
	}
	assertBytes(t, expected, actual)
}

// A chained value inside a chained container: the tuple's chunk carries the
// string's placeholder, whose NextSegmentOffset is patched with the absolute
// offset of the string's segment, laid out after the tuple's own segment.
func TestExtendedPutAccess_NestedChainIsPatchedInsideTheParentSegment(t *testing.T) {
	put := NewExtendedPutAccess(16)

	tuple := put.BeginTuple()
	tuple.AddInt32(1)
	tuple.AddString(strings.Repeat("A", 16)) // reaches the pivot: chained inside the tuple
	tuple.AddInt32(2)
	put.EndNested(tuple) // packs to 20 bytes with the placeholder: chained as well

	actual, err := put.PackExtended()
	require.NoError(t, err)
	require.Equal(t, 2, put.SegmentCount())

	expected := []byte{
		// Root segment: the tuple's placeholder only
		0x22, 0x00, // header[0]: absolute offset=4, TypeExtendedTagContainer
		0x20, 0x00, // header[1]: delta=4, TypeEnd
		0x08, 0x00, 0x00, 0x00, // NextSegmentOffset: the tuple's segment starts at absolute offset 8

		// Tuple segment at offset 8 (payload 28 bytes)
		0x22, 0x00, // header[0]: absolute offset=4, TypeExtendedTagContainer
		0xE0, 0x00, // header[1]: delta=28, TypeEnd
		0x00, 0x00, 0x00, 0x00, // NextSegmentOffset: EndOfChain
		0x24, 0x00, // inner header[0]: absolute offset=4, TypeTuple
		0xA0, 0x00, // inner header[1]: delta=20, TypeEnd
		// The chunk: int32, the string's placeholder, int32
		0x41, 0x00, // header[0]: absolute offset=8, TypeInt32
		0x22, 0x00, // header[1]: delta=4, TypeExtendedTagContainer (placeholder)
		0x41, 0x00, // header[2]: delta=8, TypeInt32
		0x60, 0x00, // header[3]: delta=12, TypeEnd
		0x01, 0x00, 0x00, 0x00, // int32(1)
		0x28, 0x00, 0x00, 0x00, // NextSegmentOffset: the string's segment starts at absolute offset 40
		0x02, 0x00, 0x00, 0x00, // int32(2)

		// String segment at offset 40 (payload 24 bytes)
		0x22, 0x00, // header[0]: absolute offset=4, TypeExtendedTagContainer
		0xC0, 0x00, // header[1]: delta=24, TypeEnd
		0x00, 0x00, 0x00, 0x00, // NextSegmentOffset: EndOfChain
		0x26, 0x00, // inner header[0]: absolute offset=4, TypeString
		0x80, 0x00, // inner header[1]: delta=16, TypeEnd
		'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
		'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
	}
	assertBytes(t, expected, actual)
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

// Containers below the pivot, whichever method builds them, are laid out
// exactly as PutAccess lays them out, empty and nil ones included.
func TestExtendedPutAccess_ContainersBelowPivotMatchPutAccess(t *testing.T) {
	inner := NewPutAccess()
	inner.AddString("p")
	packed := &PackableTagValue{head: typetags.TypeTuple, value: inner.Pack()}
	ordered := typetags.NewOrderedMapAny(typetags.OPAny("k", "v"), typetags.OPAny("n", int16(2)))

	plain := NewPutAccess()
	pt := plain.BeginTuple()
	pt.AddString("x")
	pt.AddInt8(1)
	plain.EndNested(pt)
	pe := plain.BeginTuple()
	plain.EndNested(pe)
	pm := plain.BeginMap()
	pm.AddString("k")
	pm.AddBytes([]byte{1})
	plain.EndNested(pm)
	plain.AddMapStr(nil)
	plain.AddStringArray(nil)
	plain.AddMap(nil)
	plain.AddMapSortedKeyStr(map[string]string{"a": "1", "b": "2"})
	plain.AddStringArray([]string{"s", "t"})
	require.NoError(t, plain.AddAnyTuple([]any{int16(1), "s", []any{"n"}}, false))
	require.NoError(t, plain.AddMapAnyOrdered(ordered, false))
	plain.AddPackable(packed)
	require.NoError(t, plain.AddAny(map[string][]byte{"z": {9}}, false))

	ext := NewExtendedPutAccess(32)
	et := ext.BeginTuple()
	et.AddString("x")
	et.AddInt8(1)
	ext.EndNested(et)
	ee := ext.BeginTuple()
	ext.EndNested(ee)
	em := ext.BeginMap()
	em.AddString("k")
	em.AddBytes([]byte{1})
	ext.EndNested(em)
	ext.AddMapStr(nil)
	ext.AddStringArray(nil)
	ext.AddMap(nil)
	ext.AddMapSortedKeyStr(map[string]string{"a": "1", "b": "2"})
	ext.AddStringArray([]string{"s", "t"})
	require.NoError(t, ext.AddAnyTuple([]any{int16(1), "s", []any{"n"}}, false))
	require.NoError(t, ext.AddMapAnyOrdered(ordered, false))
	ext.AddPackable(packed)
	require.NoError(t, ext.AddAny(map[string][]byte{"z": {9}}, false))

	actual, err := ext.PackExtended()
	require.NoError(t, err)

	assert.Equal(t, plain.Pack(), actual)
	assert.Equal(t, 0, ext.SegmentCount())
}

func TestExtendedPutAccess_PivotDefaultsAndBounds(t *testing.T) {
	assert.Equal(t, DefaultPivotSize, NewExtendedPutAccess(0).PivotSize())
	assert.Equal(t, DefaultPivotSize, NewExtendedPutAccess(-1).PivotSize())
	assert.Equal(t, MaxPivotSize, NewExtendedPutAccess(MaxSegmentSize*2).PivotSize())

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

// A payload of at most MaxScalarSize bytes decodes as a scalar, so the last
// chunk of an array must never be left that short.
func TestExtendedPutAccess_ArrayChunksNeverShrinkToScalarSize(t *testing.T) {
	values := make([]int64, 2046) // 4-byte elements: 8185-byte payload, one element past a full chunk
	for i := range values {
		values[i] = int64(i) * 100000
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
		assert.Greater(t, len(c.data), typetags.MaxScalarSize, "a chunk this short would decode as a scalar")
		v, err := DecodePrimitive(c.tag, c.data)
		require.NoError(t, err)
		decoded = append(decoded, v.([]int64)...)
	}
	assert.Equal(t, values, decoded)
}

// ADR 001 exists because a 13-bit delta cannot address more than 8 KB: a value
// past that limit is split at MaxChunkSize, and its segments are linked with
// full 32-bit offsets that reach beyond what the root could address.
func TestExtendedPutAccess_ValuesBeyondThe13BitLimitSpanSegments(t *testing.T) {
	cases := []struct {
		name     string
		size     int
		segments int
	}{
		{"largest single chunk", MaxChunkSize, 1},
		{"one byte past a chunk", MaxChunkSize + 1, 2},
		{"one byte past the 13-bit limit", MaxSegmentSize + 1, 2},
		{"three chunks", 2*MaxChunkSize + 1, 3},
		{"64 KiB", 64 * 1024, 9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			for i := range data {
				data[i] = byte(i) // position-dependent, so a misplaced chunk is caught
			}
			put := NewExtendedPutAccess(0)
			put.AddBytes(data)

			buf, err := put.PackExtended()
			require.NoError(t, err)
			assert.Equal(t, tc.segments, put.SegmentCount())

			root := NewGetAccess(buf)
			require.NotNil(t, root)
			tp, link := root.GetTypeAndValue(0)
			require.Equal(t, typetags.TypeExtendedTagContainer, tp)

			chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
			require.Len(t, chunks, tc.segments)
			var joined []byte
			for _, c := range chunks {
				assert.LessOrEqual(t, len(c.data), MaxChunkSize)
				joined = append(joined, c.data...)
			}
			assert.Equal(t, data, joined)
			if tc.segments > 1 {
				assert.Greater(t, int(chunks[len(chunks)-1].offset), MaxSegmentSize-1,
					"the last segment lies beyond the 13-bit range and is reached through 32-bit links")
			}
		})
	}
}

// A container larger than a chunk is split at element boundaries into
// sub-containers that each stay addressable, and the elements keep their order.
func TestExtendedPutAccess_LargeTupleIsSplitAtElementBoundaries(t *testing.T) {
	put := NewExtendedPutAccess(0)
	tuple := put.BeginTuple()
	for i := 0; i < 3000; i++ {
		tuple.AddInt32(int32(i)) // 18002 bytes packed: 1363 elements of 6 bytes fill a chunk
	}
	put.EndNested(tuple)

	buf, err := put.PackExtended()
	require.NoError(t, err)

	tp, link := NewGetAccess(buf).GetTypeAndValue(0)
	require.Equal(t, typetags.TypeExtendedTagContainer, tp)
	chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 3)

	var values []int32
	for _, c := range chunks {
		assert.Equal(t, typetags.TypeTuple, c.tag)
		assert.LessOrEqual(t, len(c.data), MaxChunkSize)
		part := NewGetAccess(c.data)
		require.NotNil(t, part)
		for i := 0; i < part.FieldCount(); i++ {
			v, err := part.GetInt32(i)
			require.NoError(t, err)
			values = append(values, v)
		}
	}
	require.Len(t, values, 3000)
	for i, v := range values {
		require.Equal(t, int32(i), v)
	}
}

// The elements of a map are chunked in key/value pairs: a key never ends a
// segment with its value in the next one.
func TestExtendedPutAccess_MapPairsAreNeverSplitAcrossSegments(t *testing.T) {
	m := make(map[string]string, 100)
	for i := 0; i < 100; i++ {
		m[fmt.Sprintf("key%03d", i)] = strings.Repeat(string(rune('a'+i%26)), 100)
	}
	put := NewExtendedPutAccess(0)
	put.AddMapSortedKeyStr(m) // 11002 bytes packed: 74 pairs of 110 bytes fill a chunk

	buf, err := put.PackExtended()
	require.NoError(t, err)

	tp, link := NewGetAccess(buf).GetTypeAndValue(0)
	require.Equal(t, typetags.TypeExtendedTagContainer, tp)
	chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 2)

	got := make(map[string]string, len(m))
	var keys []string
	for _, c := range chunks {
		assert.Equal(t, typetags.TypeMap, c.tag)
		part := NewGetAccess(c.data)
		require.NotNil(t, part)
		require.Equal(t, 0, part.FieldCount()%2, "a key and its value share a segment")
		for i := 0; i < part.FieldCount(); i += 2 {
			k, err := part.GetString(i)
			require.NoError(t, err)
			v, err := part.GetString(i + 1)
			require.NoError(t, err)
			got[k] = v
			keys = append(keys, k)
		}
	}
	assert.Equal(t, m, got)
	assert.True(t, sort.StringsAreSorted(keys), "elements keep their order across segments")
}

// Map members are chained from MapMemberPivot so that a key and its value can
// always share a chunk, even when the pivot itself is higher.
func TestExtendedPutAccess_MapMembersAreChainedAtHalfAChunk(t *testing.T) {
	put := NewExtendedPutAccess(MaxPivotSize)
	member := strings.Repeat("m", MapMemberPivot)

	tuple := put.BeginTuple()
	tuple.AddString(member)
	put.EndNested(tuple)
	assert.Equal(t, 0, put.SegmentCount(), "in a tuple the value stays inline below the pivot")

	m := put.BeginMap()
	m.AddString("k")
	m.AddString(member)
	put.EndNested(m)
	assert.Equal(t, 1, put.SegmentCount(), "in a map the same value is chained")

	pair := put.BeginMap()
	pair.AddString(member[1:])
	pair.AddString(member[1:])
	put.EndNested(pair)
	assert.Equal(t, 2, put.SegmentCount(), "a key and a value just below the map pivot fill one segment together")

	buf, err := put.PackExtended()
	require.NoError(t, err)
	root := NewGetAccess(buf)
	require.NotNil(t, root)

	inlineMap, tp, err := root.GetNestedGetAccess(1)
	require.NoError(t, err)
	require.Equal(t, typetags.TypeMap, tp)
	tp, link := inlineMap.GetTypeAndValue(1)
	require.Equal(t, typetags.TypeExtendedTagContainer, tp)
	chunks := followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 1)
	assert.Equal(t, member, string(chunks[0].data))

	tp, link = root.GetTypeAndValue(2)
	require.Equal(t, typetags.TypeExtendedTagContainer, tp)
	chunks = followChain(t, buf, binary.LittleEndian.Uint32(link))
	require.Len(t, chunks, 1)
	assert.Equal(t, MaxChunkSize, len(chunks[0].data), "the pair fills the chunk exactly")
	part := NewGetAccess(chunks[0].data)
	require.NotNil(t, part)
	require.Equal(t, 2, part.FieldCount())
	k, err := part.GetString(0)
	require.NoError(t, err)
	v, err := part.GetString(1)
	require.NoError(t, err)
	assert.Equal(t, member[1:], k)
	assert.Equal(t, member[1:], v)
}

// Whatever builds a container, its oversized members are chained at every
// depth, and reading the chains back reassembles the original values.
func TestExtendedPutAccess_BulkBuildersChainNestedValuesAtEveryDepth(t *testing.T) {
	big := strings.Repeat("b", 200)
	ints := make([]int64, 100)
	floats := make([]float64, 20)
	for i := range ints {
		ints[i] = int64(i)
	}
	for i := range floats {
		floats[i] = float64(i) / 2
	}
	inner := NewPutAccess()
	inner.AddString(big)
	innerTuple := inner.BeginTuple()
	innerTuple.AddString(big)
	innerTuple.AddInt16(3)
	inner.EndNested(innerTuple)
	packed := &PackableTagValue{head: typetags.TypeTuple, value: inner.Pack()}

	put := NewExtendedPutAccess(64)
	put.AddStringArray([]string{"a", big, "c"})
	put.AddMapSortedKeyStr(map[string]string{"k1": big, "k2": "small"})
	require.NoError(t, put.AddAnyTuple([]any{int32(7), big, []any{big, int8(1)}, map[string]any{"x": big}}, false))
	require.NoError(t, put.AddMapAnyOrdered(typetags.NewOrderedMapAny(
		typetags.OPAny("first", big), typetags.OPAny("second", []any{big})), false))
	put.AddPackable(packed)
	arrays := put.BeginTuple()
	arrays.AddIntegerArray(ints)
	arrays.AddFloatArray(floats)
	put.EndNested(arrays)
	require.NoError(t, put.AddAny(map[string][]byte{"z": []byte(big)}, false))
	require.Equal(t, 12, put.SegmentCount(), "every oversized value got a chain, containers stayed inline once their members left")

	buf, err := put.PackExtended()
	require.NoError(t, err)
	root := NewGetAccess(buf)
	require.NotNil(t, root)
	require.Equal(t, 7, root.FieldCount())

	assert.Equal(t, []any{"a", big, "c"}, resolve(t, buf, root, 0))
	assert.Equal(t, []pair{{"k1", big}, {"k2", "small"}}, resolve(t, buf, root, 1))
	assert.Equal(t, []any{int32(7), big, []any{big, int8(1)}, []pair{{"x", big}}}, resolve(t, buf, root, 2))
	assert.Equal(t, []pair{{"first", big}, {"second", []any{big}}}, resolve(t, buf, root, 3))
	assert.Equal(t, []any{big, []any{big, int16(3)}}, resolve(t, buf, root, 4))
	assert.Equal(t, []any{ints, floats}, resolve(t, buf, root, 5))
	assert.Equal(t, []pair{{"z", big}}, resolve(t, buf, root, 6))
}

func TestExtendedPutAccess_PackExtendedRejectsARootBeyondThe13BitRange(t *testing.T) {
	put := NewExtendedPutAccess(0)
	for i := 0; i < 3000; i++ {
		put.AddInt32(int32(i)) // 12000-byte root payload: nothing chains the root yet
	}

	_, err := put.PackExtended()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "13-bit")
}

// chainChunk is one data segment of a chain, as read back with GetAccess.
type chainChunk struct {
	offset uint32 // absolute offset of the segment in the buffer
	tag    typetags.Type
	data   []byte
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
		chunks = append(chunks, chainChunk{offset: offset, tag: innerType, data: data})

		offset = binary.LittleEndian.Uint32(val)
	}
	return chunks
}

// pair is a map entry as resolve reads it back, in encoded order.
type pair struct {
	key   string
	value any
}

// resolve reads field pos of g back the way a decoder will: chained strings
// are joined, chained arrays decoded chunk by chunk, chained tuples and maps
// reassembled from their sub-containers, and nested containers resolved in
// turn.
func resolve(t *testing.T, buf []byte, g *GetAccess, pos int) any {
	t.Helper()

	tp, val := g.GetTypeAndValue(pos)
	switch tp {
	case typetags.TypeExtendedTagContainer:
		require.Len(t, val, typetags.ExtendedContainerValueSize)
		chunks := followChain(t, buf, binary.LittleEndian.Uint32(val))
		require.NotEmpty(t, chunks)
		switch chunks[0].tag {
		case typetags.TypeString:
			var joined []byte
			for _, c := range chunks {
				joined = append(joined, c.data...)
			}
			return string(joined)
		case typetags.TypeInteger:
			var all []int64
			for _, c := range chunks {
				v, err := DecodePrimitive(c.tag, c.data)
				require.NoError(t, err)
				all = append(all, v.([]int64)...)
			}
			return all
		case typetags.TypeFloating:
			var all []float64
			for _, c := range chunks {
				v, err := DecodePrimitive(c.tag, c.data)
				require.NoError(t, err)
				all = append(all, v.([]float64)...)
			}
			return all
		case typetags.TypeTuple, typetags.TypeMap:
			var fields []any
			for _, c := range chunks {
				require.Equal(t, chunks[0].tag, c.tag)
				part := NewGetAccess(c.data)
				require.NotNil(t, part)
				for i := 0; i < part.FieldCount(); i++ {
					fields = append(fields, resolve(t, buf, part, i))
				}
			}
			return assemble(t, chunks[0].tag, fields)
		}
		require.Failf(t, "unexpected chain", "chunk tag %v", chunks[0].tag)
	case typetags.TypeTuple, typetags.TypeMap:
		if len(val) == 0 {
			return nil
		}
		inner := NewGetAccess(val)
		require.NotNil(t, inner)
		var fields []any
		for i := 0; i < inner.FieldCount(); i++ {
			fields = append(fields, resolve(t, buf, inner, i))
		}
		return assemble(t, tp, fields)
	case typetags.TypeString:
		return string(val)
	case typetags.TypeInteger, typetags.TypeFloating:
		v, err := DecodePrimitive(tp, val)
		require.NoError(t, err)
		return v
	case typetags.TypeBool:
		return val[0] != 0
	}
	require.Failf(t, "unexpected field", "type %v at position %d", tp, pos)
	return nil
}

// assemble turns resolved fields into a tuple's []any or a map's []pair.
func assemble(t *testing.T, tag typetags.Type, fields []any) any {
	t.Helper()

	if tag == typetags.TypeTuple {
		return fields
	}
	require.Equal(t, 0, len(fields)%2, "a map holds key/value pairs")
	pairs := make([]pair, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		pairs = append(pairs, pair{key: fields[i].(string), value: fields[i+1]})
	}
	return pairs
}

// assertBytes compares two buffers byte by byte, naming the first mismatch.
func assertBytes(t *testing.T, expected, actual []byte) {
	t.Helper()

	require.Equal(t, len(expected), len(actual), "Length mismatch")
	for i := range expected {
		if !assert.Equalf(t, expected[i], actual[i], "Byte %d mismatch", i) {
			return
		}
	}
}
