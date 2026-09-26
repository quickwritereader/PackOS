package access

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/quickwritereader/PackOS/typetags"
)

const (
	// DefaultPivotSize is the oversize limit used when NewExtendedPutAccess is
	// given a non-positive pivot. It keeps root-level arguments addressable
	// with 13-bit deltas while leaving room for several of them (ADR 001).
	DefaultPivotSize = 4096

	// MaxSegmentSize is the addressing limit of a 13-bit delta.
	MaxSegmentSize = 8192

	// MaxChunkSize is the largest slice of a value stored in one data segment.
	// Ten bytes are reserved for the segment's own framing so that every delta
	// in it (at most NextSegmentOffset + inner headers + chunk) stays below the
	// 13-bit limit.
	MaxChunkSize = MaxSegmentSize - 10

	// nextSegmentOffsetPos is where NextSegmentOffset sits inside a data
	// segment: right after the outer header pair.
	nextSegmentOffsetPos = 2 * HeaderTagSize

	// rootSegment marks a link whose NextSegmentOffset lives in the root
	// segment's payload rather than in a data segment.
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
// value. The last segment stores typetags.EndOfChain. PackExtended lays the
// data segments out after the root segment, in chain order.
//
// This is the encoding side of ADR 001 for strings, byte slices and integer
// and float arrays. Values below the pivot are written exactly as PutAccess
// writes them. Nested tuples and maps, and splitting the root segment itself,
// are later steps: they still go through the embedded PutAccess unchanged.
// Use PackExtended to finish; the promoted Pack, PackAppend and PackBuff
// write the root segment alone, with its links unresolved.
type ExtendedPutAccess struct {
	*PutAccess
	pivot    int
	segments [][]byte      // data segments in chain order
	links    []segmentLink // NextSegmentOffset fields to patch once the layout is known
}

// segmentLink records one NextSegmentOffset to patch in PackExtended: the
// segment holding the field (rootSegment for the root), the offset of the
// field inside that segment's payload, and the segment it must point at.
type segmentLink struct {
	from int
	at   int
	to   int
}

// NewExtendedPutAccess creates an ExtendedPutAccess. A non-positive pivot
// selects DefaultPivotSize; a pivot above MaxChunkSize is lowered to it, as a
// larger inline value could not be addressed by a 13-bit delta anyway.
func NewExtendedPutAccess(pivot int) *ExtendedPutAccess {
	if pivot <= 0 {
		pivot = DefaultPivotSize
	}
	if pivot > MaxChunkSize {
		pivot = MaxChunkSize
	}
	return &ExtendedPutAccess{PutAccess: NewPutAccess(), pivot: pivot}
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
	if len(b) < p.pivot {
		p.PutAccess.AddBytes(b)
		return
	}
	p.addByteChain(typetags.TypeByteArray, b)
}

// AddString packs a string, chaining it when it reaches the pivot.
func (p *ExtendedPutAccess) AddString(s string) {
	if len(s) < p.pivot {
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
	elementSize := DetermineIntegerSize(values)
	if len(values) == 0 || 1+len(values)*elementSize < p.pivot {
		p.PutAccess.AddIntegerArray(values)
		return
	}

	perChunk := (MaxChunkSize - 1) / elementSize
	p.addChain(typetags.TypeInteger, chunkCount(len(values), perChunk), func(i int) []byte {
		part := values[i*perChunk : min((i+1)*perChunk, len(values))]
		chunk := make([]byte, 1+len(part)*elementSize)
		chunk[0] = byte(elementSize)
		EncodeIntegers(chunk[1:], part, elementSize)
		return chunk
	})
}

// AddFloatArray packs a float array, chaining it when its payload reaches the
// pivot. Every chunk starts with its own element size, always 8.
func (p *ExtendedPutAccess) AddFloatArray(values []float64) {
	const elementSize = 8
	if len(values) == 0 || 1+len(values)*elementSize < p.pivot {
		p.PutAccess.AddFloatArray(values)
		return
	}

	perChunk := (MaxChunkSize - 1) / elementSize
	p.addChain(typetags.TypeFloating, chunkCount(len(values), perChunk), func(i int) []byte {
		part := values[i*perChunk : min((i+1)*perChunk, len(values))]
		chunk := make([]byte, 1+len(part)*elementSize)
		chunk[0] = elementSize
		EncodeFloatingArray(chunk[1:], part)
		return chunk
	})
}

// PackExtended finalizes the buffer: the root segment followed by every data
// segment in chain order, with all NextSegmentOffset fields resolved to
// absolute offsets. It fails when the buffer exceeds the 32-bit offset range.
func (p *ExtendedPutAccess) PackExtended() ([]byte, error) {
	total := len(p.offsets) + HeaderTagSize + len(p.buf) // PackAppend adds the TypeEnd header
	starts := make([]int, len(p.segments))
	for i, seg := range p.segments {
		starts[i] = total
		total += len(seg)
	}
	if uint64(total) > math.MaxUint32 {
		return nil, fmt.Errorf("PackExtended: %d bytes exceed the 32-bit offset range", total)
	}

	for _, l := range p.links {
		field := p.buf[l.at:]
		if l.from != rootSegment {
			field = p.segments[l.from][l.at:]
		}
		binary.LittleEndian.PutUint32(field, uint32(starts[l.to]))
	}

	out := p.PackAppend(make([]byte, 0, total))
	for _, seg := range p.segments {
		out = append(out, seg...)
	}
	return out, nil
}

// addByteChain chains a byte value in slices of at most MaxChunkSize bytes.
func (p *ExtendedPutAccess) addByteChain(tag typetags.Type, b []byte) {
	p.addChain(tag, chunkCount(len(b), MaxChunkSize), func(i int) []byte {
		return b[i*MaxChunkSize : min((i+1)*MaxChunkSize, len(b))]
	})
}

// addChain writes the placeholder for a chained value into the root segment
// and appends its data segments, one per chunk, linked in order.
func (p *ExtendedPutAccess) addChain(tag typetags.Type, chunks int, chunk func(i int) []byte) {
	first := len(p.segments)

	// The placeholder keeps the value's position among its siblings and
	// carries nothing but the link to the first data segment.
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets,
		typetags.EncodeHeader(p.position, typetags.TypeExtendedTagContainer))
	p.links = append(p.links, segmentLink{from: rootSegment, at: len(p.buf), to: first})
	p.buf = binary.LittleEndian.AppendUint32(p.buf, typetags.EndOfChain)
	p.position = len(p.buf)

	for i := 0; i < chunks; i++ {
		p.segments = append(p.segments, newDataSegment(tag, chunk(i)))
		if i+1 < chunks {
			p.links = append(p.links, segmentLink{from: first + i, at: nextSegmentOffsetPos, to: first + i + 1})
		}
	}
}

// chunkCount returns how many chunks of at most perChunk items n items need.
func chunkCount(n, perChunk int) int {
	return (n + perChunk - 1) / perChunk
}

// newDataSegment frames one chunk as a stand-alone container holding a single
// TypeExtendedTagContainer element: NextSegmentOffset (EndOfChain until the
// chain is linked) followed by an inner container with the chunk.
func newDataSegment(tag typetags.Type, chunk []byte) []byte {
	inner := 2*HeaderTagSize + len(chunk)
	payload := typetags.ExtendedContainerValueSize + inner

	seg := make([]byte, 0, 2*HeaderTagSize+payload)
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(2*HeaderTagSize, typetags.TypeExtendedTagContainer))
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeEnd(payload))
	seg = binary.LittleEndian.AppendUint32(seg, typetags.EndOfChain)
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeHeader(2*HeaderTagSize, tag))
	seg = binary.LittleEndian.AppendUint16(seg, typetags.EncodeEnd(len(chunk)))
	return append(seg, chunk...)
}
