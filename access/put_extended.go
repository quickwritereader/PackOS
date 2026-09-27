package access

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/quickwritereader/PackOS/typetags"
	"github.com/quickwritereader/PackOS/utils"
)

const (
	// DefaultPivotSize is the oversize limit used when NewExtendedPutAccess is
	// given a non-positive pivot. It keeps root-level arguments addressable
	// with 13-bit deltas while leaving room for several of them (ADR 001).
	DefaultPivotSize = 4096

	// MaxSegmentSize is the addressing limit of a 13-bit delta.
	MaxSegmentSize = 8192

	// MaxChunkSize is the largest chunk stored in one data segment: a slice of
	// a string, a run of array elements, or a sub-container with a run of a
	// container's elements. Ten bytes are reserved for the segment's own
	// framing so that every delta in it stays below the 13-bit limit.
	MaxChunkSize = MaxSegmentSize - 10

	// MaxPivotSize is the largest pivot: a value just below it must still fit
	// in one chunk together with its header and the chunk's TypeEnd header.
	MaxPivotSize = MaxChunkSize - 2*HeaderTagSize

	// MapMemberPivot is the pivot applied to the keys and values of a map,
	// lowered so that a key and its value always share a chunk: two members
	// just below it fit in one chunk with their headers and the TypeEnd
	// header.
	MapMemberPivot = (MaxChunkSize-3*HeaderTagSize)/2 + 1

	// dataSegmentFrameSize is the size of a data segment without its chunk:
	// the outer header pair, NextSegmentOffset and the inner header pair.
	dataSegmentFrameSize = 2*HeaderTagSize + typetags.ExtendedContainerValueSize + 2*HeaderTagSize

	// nextSegmentOffsetPos is where NextSegmentOffset sits inside a data
	// segment: right after the outer header pair.
	nextSegmentOffsetPos = 2 * HeaderTagSize

	// rootSegment marks a link whose NextSegmentOffset lives in the writer's
	// own payload rather than in a data segment.
	rootSegment = -1
)

// ExtendedPutAccess is a PutAccess that moves oversized values out of the
// segment being written into a chain of linked segments (ADR 001).
//
// A value whose payload reaches the pivot is written as a placeholder: a
// TypeExtendedTagContainer element holding only the absolute offset of the
// first data segment of its chain. Each data segment is a stand-alone
// container with a single TypeExtendedTagContainer element whose payload is
// the NextSegmentOffset followed by an inner container with one chunk of the
// value. The last segment stores typetags.EndOfChain.
//
// Strings and byte slices are chunked in slices, integer and float arrays in
// whole elements with the element size repeated in every chunk (ADR 002), and
// tuples and maps at element boundaries: each chunk of a container is a
// sub-container with a run of its elements, with keys and values of a map
// kept together. Containers are written through nested writers (BeginTuple,
// BeginMap and EndNested, which the bulk methods use as well), so their
// members follow the same rules at every depth: an oversized member leaves
// its placeholder inside the chunk that holds its siblings, and PackExtended
// patches that placeholder like any other. Every NextSegmentOffset is tracked
// as a triplet (segment holding it, offset inside that segment, segment it
// points at), which is what makes the segment order free to change: today
// PackExtended lays the segments out after the root in depth-first order, a
// container's chunks first and its members' chains after them.
//
// Values below the pivot are written exactly as PutAccess writes them. The
// root segment itself is not split yet: PackExtended fails when it exceeds
// the 13-bit range. Use PackExtended to finish; the promoted Pack, PackAppend
// and PackBuff write the root segment alone, with its links unresolved.
type ExtendedPutAccess struct {
	*PutAccess
	pivot    int           // oversize limit of the root writer
	limit    int           // this writer's pivot: the root's, lowered to MapMemberPivot inside a map
	tag      typetags.Type // TypeTuple or TypeMap for a writer opened with BeginTuple or BeginMap
	segments [][]byte      // data segments in chain order
	links    []segmentLink // NextSegmentOffset fields to patch once the layout is known
}

// segmentLink is the triplet of the ADR 001 discussion: the segment holding a
// NextSegmentOffset field (rootSegment for the writer's own payload), the
// offset of that field inside the segment, and the segment it must point at.
type segmentLink struct {
	from int
	at   int
	to   int
}

// element is one element of a nested writer: its tag, its payload and, for a
// placeholder, the index of the segment it points at in the writer's own list.
type element struct {
	tag     typetags.Type
	payload []byte
	link    int
}

// NewExtendedPutAccess creates an ExtendedPutAccess. A non-positive pivot
// selects DefaultPivotSize; a pivot above MaxPivotSize is lowered to it, as a
// larger inline value could not be addressed by a 13-bit delta anyway.
func NewExtendedPutAccess(pivot int) *ExtendedPutAccess {
	if pivot <= 0 {
		pivot = DefaultPivotSize
	}
	if pivot > MaxPivotSize {
		pivot = MaxPivotSize
	}
	return &ExtendedPutAccess{PutAccess: NewPutAccess(), pivot: pivot, limit: pivot}
}

// PivotSize returns the oversize limit: values whose payload reaches it are chained.
func (p *ExtendedPutAccess) PivotSize() int {
	return p.pivot
}

// SegmentCount returns the number of data segments written so far.
func (p *ExtendedPutAccess) SegmentCount() int {
	return len(p.segments)
}

// AddBytes packs a byte slice, chaining it when it reaches the pivot.
func (p *ExtendedPutAccess) AddBytes(b []byte) {
	if len(b) < p.limit {
		p.PutAccess.AddBytes(b)
		return
	}
	p.addByteChain(typetags.TypeByteArray, b)
}

// AddString packs a string, chaining it when it reaches the pivot.
func (p *ExtendedPutAccess) AddString(s string) {
	if len(s) < p.limit {
		p.PutAccess.AddString(s)
		return
	}
	p.addByteChain(typetags.TypeString, unsafe.Slice(unsafe.StringData(s), len(s)))
}

// AddNullableString packs a string pointer, chaining the string when it reaches the pivot.
func (p *ExtendedPutAccess) AddNullableString(s *string) {
	if s == nil {
		p.AddNull()
		return
	}
	p.AddString(*s)
}

// AddIntegerArray packs an integer array, chaining it when its payload reaches
// the pivot. Every chunk starts with its own element size so that ADR 002's
// implicit count applies to each segment on its own.
func (p *ExtendedPutAccess) AddIntegerArray(values []int64) {
	if len(values) == 0 {
		p.PutAccess.AddIntegerArray(values)
		return
	}
	elementSize := DetermineIntegerSize(values)
	if 1+len(values)*elementSize < p.limit {
		p.PutAccess.AddIntegerArray(values)
		return
	}
	payload := make([]byte, 1+len(values)*elementSize)
	payload[0] = byte(elementSize)
	EncodeIntegers(payload[1:], values, elementSize)
	p.addArrayChain(typetags.TypeInteger, payload)
}

// AddFloatArray packs a float array, chaining it when its payload reaches the
// pivot. Every chunk starts with its own element size, always 8.
func (p *ExtendedPutAccess) AddFloatArray(values []float64) {
	const elementSize = 8
	if len(values) == 0 {
		p.PutAccess.AddFloatArray(values)
		return
	}
	if 1+len(values)*elementSize < p.limit {
		p.PutAccess.AddFloatArray(values)
		return
	}
	payload := make([]byte, 1+len(values)*elementSize)
	payload[0] = elementSize
	EncodeFloatingArray(payload[1:], values)
	p.addArrayChain(typetags.TypeFloating, payload)
}

// AppendTagAndValue appends an already encoded value under its tag. A value
// that reaches the pivot is chained by the rules of its tag: strings and byte
// slices in slices, ADR 002 arrays in whole elements, tuples and maps at
// element boundaries with their own oversized members chained in turn.
func (p *ExtendedPutAccess) AppendTagAndValue(tag typetags.Type, val []byte) {
	if len(val) < p.limit {
		p.PutAccess.AppendTagAndValue(tag, val)
		return
	}
	switch tag {
	case typetags.TypeString:
		p.addByteChain(tag, val)
	case typetags.TypeInteger, typetags.TypeFloating:
		p.addArrayChain(tag, val)
	case typetags.TypeTuple, typetags.TypeMap:
		p.addPackedContainer(tag, val)
	default:
		p.PutAccess.AppendTagAndValue(tag, val)
	}
}

// AddPackable packs a Packable, chaining its value by the rules of its header
// type when the value reaches the pivot.
func (p *ExtendedPutAccess) AddPackable(v Packable) {
	size := v.ValueSize()
	if size < p.limit {
		v.PackInto(p.PutAccess)
		return
	}
	buf := make([]byte, size)
	p.AppendTagAndValue(v.HeaderType(), buf[:v.Write(buf, 0)])
}

// BeginTuple opens a nested tuple. Unlike PutAccess.BeginTuple it writes
// nothing yet: EndNested decides whether the tuple is stored inline or chained.
func (p *ExtendedPutAccess) BeginTuple() *ExtendedPutAccess {
	return p.beginNested(typetags.TypeTuple)
}

// BeginMap opens a nested map. Its keys and values are chained from
// MapMemberPivot, or from the pivot when that is lower, so that a key and its
// value always share a segment.
func (p *ExtendedPutAccess) BeginMap() *ExtendedPutAccess {
	return p.beginNested(typetags.TypeMap)
}

// EndNested closes a container opened with BeginTuple or BeginMap and writes
// it as the next element. A container packed below the pivot is stored inline,
// exactly as PutAccess stores it, and the chains its members started are
// adopted. Otherwise a placeholder takes its place and the container is
// chained: its elements, in order, are split at element boundaries (at pair
// boundaries for a map) into sub-containers of at most MaxChunkSize bytes, one
// per data segment. The members' own segments follow the container's.
func (p *ExtendedPutAccess) EndNested(nested *ExtendedPutAccess) {
	if nested.PackSize() < p.limit {
		p.inlineNested(nested)
	} else {
		p.chainNested(nested)
	}
	nested.discard()
}

// AddMap packs a map of byte slices through a nested writer.
func (p *ExtendedPutAccess) AddMap(m map[string][]byte) {
	if len(m) == 0 {
		p.PutAccess.AddMap(m)
		return
	}
	nested := p.BeginMap()
	for k, v := range m {
		nested.AddString(k)
		nested.AddBytes(v)
	}
	p.EndNested(nested)
}

// AddMapSortedKey packs a map of byte slices in key order through a nested writer.
func (p *ExtendedPutAccess) AddMapSortedKey(m map[string][]byte) {
	if len(m) == 0 {
		p.PutAccess.AddMapSortedKey(m)
		return
	}
	nested := p.BeginMap()
	for _, k := range utils.SortKeys(m) {
		nested.AddString(k)
		nested.AddBytes(m[k])
	}
	p.EndNested(nested)
}

// AddMapStr packs a map of strings through a nested writer.
func (p *ExtendedPutAccess) AddMapStr(m map[string]string) {
	if len(m) == 0 {
		p.PutAccess.AddMapStr(m)
		return
	}
	nested := p.BeginMap()
	for k, v := range m {
		nested.AddString(k)
		nested.AddString(v)
	}
	p.EndNested(nested)
}

// AddMapSortedKeyStr packs a map of strings in key order through a nested writer.
func (p *ExtendedPutAccess) AddMapSortedKeyStr(m map[string]string) {
	if len(m) == 0 {
		p.PutAccess.AddMapSortedKeyStr(m)
		return
	}
	nested := p.BeginMap()
	for _, k := range utils.SortKeys(m) {
		nested.AddString(k)
		nested.AddString(m[k])
	}
	p.EndNested(nested)
}

// AddStringArray packs a []string as a tuple through a nested writer.
func (p *ExtendedPutAccess) AddStringArray(arr []string) {
	if len(arr) == 0 {
		p.PutAccess.AddStringArray(arr)
		return
	}
	nested := p.BeginTuple()
	for _, s := range arr {
		nested.AddString(s)
	}
	p.EndNested(nested)
}

// AddAnyTuple packs a []any as a tuple through a nested writer.
func (p *ExtendedPutAccess) AddAnyTuple(m []any, useNumeric bool) error {
	return p.addAnyTuple(m, useNumeric, false)
}

// AddAnyTupleSortedMap packs a []any as a tuple through a nested writer, using
// the sorted map variants for the maps it contains.
func (p *ExtendedPutAccess) AddAnyTupleSortedMap(m []any, useNumeric bool) error {
	return p.addAnyTuple(m, useNumeric, true)
}

// AddMapAny packs a map[string]any through a nested writer.
func (p *ExtendedPutAccess) AddMapAny(m map[string]any, useNumeric bool) error {
	if len(m) == 0 {
		return p.PutAccess.AddMapAny(m, useNumeric)
	}
	nested := p.BeginMap()
	for k, v := range m {
		nested.AddString(k)
		if err := nested.addAny(v, useNumeric, false); err != nil {
			nested.discard()
			return fmt.Errorf("AddMapAny: key %q: %w", k, err)
		}
	}
	p.EndNested(nested)
	return nil
}

// AddMapAnySortedKey packs a map[string]any in key order through a nested writer.
func (p *ExtendedPutAccess) AddMapAnySortedKey(m map[string]any, useNumeric bool) error {
	if len(m) == 0 {
		return p.PutAccess.AddMapAnySortedKey(m, useNumeric)
	}
	nested := p.BeginMap()
	for _, k := range utils.SortKeys(m) {
		nested.AddString(k)
		if err := nested.addAny(m[k], useNumeric, true); err != nil {
			nested.discard()
			return fmt.Errorf("AddMapAnySortedKey: key %q: %w", k, err)
		}
	}
	p.EndNested(nested)
	return nil
}

// AddMapAnyOrdered packs an OrderedMap in insertion order through a nested writer.
func (p *ExtendedPutAccess) AddMapAnyOrdered(om *typetags.OrderedMap[any], useNumeric bool) error {
	if om == nil || om.Len() == 0 {
		return p.PutAccess.AddMapAnyOrdered(om, useNumeric)
	}
	nested := p.BeginMap()
	for k, v := range om.ItemsIter() {
		nested.AddString(k)
		if err := nested.addAny(v, useNumeric, false); err != nil {
			nested.discard()
			return fmt.Errorf("AddMapAnyOrdered: key %q: %w", k, err)
		}
	}
	p.EndNested(nested)
	return nil
}

// AddAny packs a generic value, chaining it and its members by the same rules
// as the typed methods.
func (p *ExtendedPutAccess) AddAny(m any, useNumeric bool) error {
	return p.addAny(m, useNumeric, false)
}

// PackExtended finalizes the buffer: the root segment followed by every data
// segment, with all NextSegmentOffset fields resolved to absolute offsets. It
// fails when the root segment exceeds the 13-bit range, as splitting the root
// is not implemented yet, or when the buffer exceeds the 32-bit offset range.
func (p *ExtendedPutAccess) PackExtended() ([]byte, error) {
	headers := len(p.offsets) + HeaderTagSize // PackAppend adds the TypeEnd header
	if headers >= MaxSegmentSize || len(p.buf) >= MaxSegmentSize {
		return nil, fmt.Errorf("PackExtended: root segment (%d header bytes, %d payload bytes) exceeds the 13-bit range; splitting the root segment is not implemented yet", headers, len(p.buf))
	}
	total := headers + len(p.buf)
	starts := make([]int, len(p.segments))
	for i, seg := range p.segments {
		starts[i] = total
		total += len(seg)
	}
	if uint64(total) > math.MaxUint32 {
		return nil, fmt.Errorf("PackExtended: %d bytes exceed the 32-bit offset range", total)
	}

	for _, l := range p.links {
		field := p.buf
		if l.from != rootSegment {
			field = p.segments[l.from]
		}
		binary.LittleEndian.PutUint32(field[l.at:], uint32(starts[l.to]))
	}

	out := p.PackAppend(make([]byte, 0, total))
	for _, seg := range p.segments {
		out = append(out, seg...)
	}
	return out, nil
}

// beginNested opens a nested writer for a container of the given tag, sharing
// the pivot and lowering the limit for the members of a map.
func (p *ExtendedPutAccess) beginNested(tag typetags.Type) *ExtendedPutAccess {
	limit := p.pivot
	if tag == typetags.TypeMap && limit > MapMemberPivot {
		limit = MapMemberPivot
	}
	return &ExtendedPutAccess{PutAccess: NewPutAccessFromPool(), pivot: p.pivot, limit: limit, tag: tag}
}

// discard returns the pooled buffers of a closed nested writer.
func (p *ExtendedPutAccess) discard() {
	ReleasePutAccess(p.PutAccess)
	p.PutAccess = nil
}

// inlineNested writes a nested container as PutAccess.EndNested does and
// adopts the chains its members started, re-basing their placeholders to
// where the nested payload landed.
func (p *ExtendedPutAccess) inlineNested(nested *ExtendedPutAccess) {
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets, typetags.EncodeHeader(p.position, nested.tag))
	payloadAt := len(p.buf) + len(nested.offsets) + HeaderTagSize // after the nested headers and their TypeEnd
	p.buf = nested.PackAppend(p.buf)
	p.position = len(p.buf)
	p.adoptSegments(nested, payloadAt)
}

// chainNested writes a placeholder for a nested container and chains its
// elements in sub-containers. The placeholders among them are re-pointed from
// the chunks that hold them; the members' segments follow the container's.
func (p *ExtendedPutAccess) chainNested(nested *ExtendedPutAccess) {
	elems := nested.elements()
	groups := groupElements(elems, nested.tag)
	sizes := make([]int, len(groups))
	for i, g := range groups {
		sizes[i] = containerSize(elems[g[0]:g[1]])
	}

	first := len(p.segments)
	shift := first + len(groups) // where the members' segments will land
	p.addChain(nested.tag, sizes, func(i int, seg []byte) []byte {
		run := elems[groups[i][0]:groups[i][1]]
		seg, ats := appendContainer(seg, run)
		for _, e := range run {
			if e.link < 0 {
				continue
			}
			p.links = append(p.links, segmentLink{from: first + i, at: ats[0], to: e.link + shift})
			ats = ats[1:]
		}
		return seg
	})

	nested.links = slices.DeleteFunc(nested.links, func(l segmentLink) bool { return l.from == rootSegment })
	p.adoptSegments(nested, 0)
}

// adoptSegments appends the data segments of a closed nested writer and its
// remaining links, shifting segment indices and moving the links into the
// nested payload to payloadAt in p's payload.
func (p *ExtendedPutAccess) adoptSegments(nested *ExtendedPutAccess, payloadAt int) {
	shift := len(p.segments)
	p.segments = append(p.segments, nested.segments...)
	for _, l := range nested.links {
		if l.from == rootSegment {
			l.at += payloadAt
		} else {
			l.from += shift
		}
		l.to += shift
		p.links = append(p.links, l)
	}
}

// elements lists the elements written to p so far, with the placeholders among
// them carrying the segment they link to. A header keeps only the low 13 bits
// of its element's offset, and a container written through this writer may
// have grown past that range; as no element of it is 8 KB or longer, each
// offset is restored from its distance to the previous element's.
func (p *ExtendedPutAccess) elements() []element {
	targets := make(map[int]int)
	for _, l := range p.links {
		if l.from == rootSegment {
			targets[l.at] = l.to
		}
	}

	n := len(p.offsets) / HeaderTagSize
	starts := make([]int, n+1)
	for i := 1; i < n; i++ {
		offset := typetags.DecodeOffset(binary.LittleEndian.Uint16(p.offsets[i*HeaderTagSize:]))
		starts[i] = starts[i-1] + (offset-starts[i-1])&(MaxSegmentSize-1)
	}
	starts[n] = len(p.buf)

	elems := make([]element, n)
	for i := range elems {
		tag := typetags.DecodeType(binary.LittleEndian.Uint16(p.offsets[i*HeaderTagSize:]))
		elems[i] = element{tag: tag, payload: p.buf[starts[i]:starts[i+1]], link: -1}
		if tag == typetags.TypeExtendedTagContainer {
			if to, ok := targets[starts[i]]; ok {
				elems[i].link = to
			}
		}
	}
	return elems
}

// addPackedContainer re-adds the elements of an already packed container
// through a nested writer, so that they are chained like elements added one
// by one. A container that does not parse is appended as it is.
func (p *ExtendedPutAccess) addPackedContainer(tag typetags.Type, val []byte) {
	g := NewGetAccess(val)
	if g == nil {
		p.PutAccess.AppendTagAndValue(tag, val)
		return
	}
	nested := p.beginNested(tag)
	for i := 0; i < g.FieldCount(); i++ {
		t, v := g.GetTypeAndValue(i)
		if v == nil {
			nested.discard()
			p.PutAccess.AppendTagAndValue(tag, val)
			return
		}
		nested.AppendTagAndValue(t, v)
	}
	p.EndNested(nested)
}

// addAnyTuple packs a []any as a tuple through a nested writer.
func (p *ExtendedPutAccess) addAnyTuple(m []any, useNumeric, sorted bool) error {
	if len(m) == 0 {
		return p.PutAccess.AddAnyTuple(m, useNumeric)
	}
	nested := p.BeginTuple()
	for _, elem := range m {
		if err := nested.addAny(elem, useNumeric, sorted); err != nil {
			nested.discard()
			if sorted {
				return fmt.Errorf("AddAnyTupleSortedMap: element %T: %w", elem, err)
			}
			return fmt.Errorf("AddAnyTuple: element %T: %w", elem, err)
		}
	}
	p.EndNested(nested)
	return nil
}

// addAny mirrors packAnyValue, and packAnyValueSortedMap when sorted is set,
// so that generic values go through the chaining rules at every depth.
func (p *ExtendedPutAccess) addAny(v any, useNumeric, sorted bool) error {
	switch val := v.(type) {
	case nil:
		p.AddNull()
	case string:
		p.AddString(val)
	case []byte:
		p.AddBytes(val)
	case map[string]string:
		if sorted {
			p.AddMapSortedKeyStr(val)
		} else {
			p.AddMapStr(val)
		}
	case map[string][]byte:
		if sorted {
			p.AddMapSortedKey(val)
		} else {
			p.AddMap(val)
		}
	case map[string]any:
		return p.AddMapAny(val, useNumeric)
	case *typetags.OrderedMap[any]:
		return p.AddMapAnyOrdered(val, useNumeric)
	case []string:
		p.AddStringArray(val)
	case []any:
		return p.addAnyTuple(val, useNumeric, sorted)
	case uint8:
		p.AddUint8(val)
	case uint16:
		p.AddUint16(val)
	case uint32:
		p.AddUint32(val)
	case uint64:
		p.AddUint64(val)
	case int8:
		p.AddInt8(val)
	case int16:
		p.AddInt16(val)
	case int32:
		p.AddInt32(val)
	case int64:
		p.AddInt64(val)
	case float32:
		p.AddFloat32(val)
	case float64:
		if useNumeric {
			p.AddNumeric(val)
		} else {
			p.AddFloat64(val)
		}
	case bool:
		p.AddBool(val)
	case Packable:
		p.AddPackable(val)
	default:
		return fmt.Errorf("packAnyValue: invalid type %T", val)
	}
	return nil
}

// addByteChain chains a byte value in slices of at most MaxChunkSize bytes.
func (p *ExtendedPutAccess) addByteChain(tag typetags.Type, b []byte) {
	p.addChain(tag, chunkSizes(len(b), MaxChunkSize), func(i int, seg []byte) []byte {
		return append(seg, b[i*MaxChunkSize:min((i+1)*MaxChunkSize, len(b))]...)
	})
}

// addArrayChain chains an ADR 002 array payload, the element size followed by
// the packed elements, so that every chunk starts with the element size and
// holds whole elements: the implicit count then applies to each segment on
// its own. A payload that is not an array is appended as it is.
func (p *ExtendedPutAccess) addArrayChain(tag typetags.Type, payload []byte) {
	elementSize, ok := typetags.ArrayElementSize(payload)
	if !ok || !typetags.IsArray(len(payload)) {
		p.PutAccess.AppendTagAndValue(tag, payload)
		return
	}
	data := payload[1:]
	bounds := arrayChunkBounds(len(data), elementSize)
	sizes := make([]int, len(bounds)-1)
	for i := range sizes {
		sizes[i] = 1 + bounds[i+1] - bounds[i]
	}
	p.addChain(tag, sizes, func(i int, seg []byte) []byte {
		seg = append(seg, byte(elementSize))
		return append(seg, data[bounds[i]:bounds[i+1]]...)
	})
}

// addChain writes the placeholder for a chained value into the writer's own
// payload and appends its data segments, one per chunk, linked in order. fill
// appends chunk i, of sizes[i] bytes, to the segment frame it is given.
func (p *ExtendedPutAccess) addChain(tag typetags.Type, sizes []int, fill func(i int, seg []byte) []byte) {
	first := len(p.segments)
	p.addPlaceholder(first)
	for i, size := range sizes {
		p.segments = append(p.segments, fill(i, newDataSegment(tag, size)))
		if i+1 < len(sizes) {
			p.links = append(p.links, segmentLink{from: first + i, at: nextSegmentOffsetPos, to: first + i + 1})
		}
	}
}

// addPlaceholder writes the element that stands in for a chained value: a
// TypeExtendedTagContainer element carrying only the NextSegmentOffset of the
// value's first data segment. It keeps the value's position among its
// siblings at a cost of six bytes.
func (p *ExtendedPutAccess) addPlaceholder(first int) {
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets,
		typetags.EncodeHeader(p.position, typetags.TypeExtendedTagContainer))
	p.links = append(p.links, segmentLink{from: rootSegment, at: len(p.buf), to: first})
	p.buf = binary.LittleEndian.AppendUint32(p.buf, typetags.EndOfChain)
	p.position = len(p.buf)
}

// chunkSizes splits n bytes into chunks of at most perChunk bytes.
func chunkSizes(n, perChunk int) []int {
	sizes := make([]int, 0, chunkCount(n, perChunk))
	for ; n > perChunk; n -= perChunk {
		sizes = append(sizes, perChunk)
	}
	return append(sizes, n)
}

// chunkCount returns how many chunks of at most perChunk items n items need.
func chunkCount(n, perChunk int) int {
	return (n + perChunk - 1) / perChunk
}

// arrayChunkBounds partitions n bytes of elementSize-byte elements into runs
// of whole elements that fit in a chunk behind its element size byte, and
// returns the run boundaries. The last run is kept above MaxScalarSize bytes,
// shortening the run before it if needed, as a decoder takes a payload of at
// most MaxScalarSize bytes for a scalar.
func arrayChunkBounds(n, elementSize int) []int {
	perChunk := (MaxChunkSize - 1) / elementSize * elementSize
	bounds := []int{0}
	for start := 0; start < n; {
		end := min(start+perChunk, n)
		if rest := n - end; rest > 0 && rest < typetags.MaxScalarSize {
			end -= typetags.MaxScalarSize - rest
		}
		bounds = append(bounds, end)
		start = end
	}
	return bounds
}

// groupElements splits a container's elements into runs whose sub-container
// fits in MaxChunkSize, and returns the runs as index ranges. The elements of
// a map are taken in key/value pairs so that no pair is split. A run that
// does not fit on its own is still emitted alone, which the pivots rule out
// for values written through this writer.
func groupElements(elems []element, tag typetags.Type) [][2]int {
	step := 1
	if tag == typetags.TypeMap {
		step = 2
	}
	var groups [][2]int
	start, size := 0, HeaderTagSize // the TypeEnd header
	for i := 0; i < len(elems); i += step {
		unit := 0
		for _, e := range elems[i:min(i+step, len(elems))] {
			unit += HeaderTagSize + len(e.payload)
		}
		if i > start && size+unit > MaxChunkSize {
			groups = append(groups, [2]int{start, i})
			start, size = i, HeaderTagSize
		}
		size += unit
	}
	return append(groups, [2]int{start, len(elems)})
}

// containerSize returns the packed size of a container holding elems.
func containerSize(elems []element) int {
	size := (len(elems) + 1) * HeaderTagSize
	for _, e := range elems {
		size += len(e.payload)
	}
	return size
}

// appendContainer appends a container holding elems, laid out as PutAccess
// packs one, and returns the offsets in seg of the placeholder payloads.
func appendContainer(seg []byte, elems []element) ([]byte, []int) {
	base := (len(elems) + 1) * HeaderTagSize
	if len(elems) == 0 {
		return binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(base, typetags.TypeEnd)), nil
	}
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(base, elems[0].tag))
	offset := len(elems[0].payload)
	for _, e := range elems[1:] {
		seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(offset, e.tag))
		offset += len(e.payload)
	}
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeEnd(offset))

	var ats []int
	for _, e := range elems {
		if e.link >= 0 {
			ats = append(ats, len(seg))
		}
		seg = append(seg, e.payload...)
	}
	return seg, ats
}

// newDataSegment starts a data segment for a chunk of chunkLen bytes: a
// stand-alone container holding a single TypeExtendedTagContainer element
// whose payload is NextSegmentOffset (EndOfChain until the chain is linked)
// followed by an inner container with one element, the chunk, which the
// caller appends.
func newDataSegment(tag typetags.Type, chunkLen int) []byte {
	payload := typetags.ExtendedContainerValueSize + 2*HeaderTagSize + chunkLen

	seg := make([]byte, 0, dataSegmentFrameSize+chunkLen)
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(2*HeaderTagSize, typetags.TypeExtendedTagContainer))
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeEnd(payload))
	seg = binary.LittleEndian.AppendUint32(seg, typetags.EndOfChain)
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(2*HeaderTagSize, tag))
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeEnd(chunkLen))
	return seg
}
