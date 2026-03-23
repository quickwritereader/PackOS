package access

import (
	"encoding/binary"
	"fmt"

	"github.com/quickwritereader/PackOS/typetags"
)

// ExtendedGetAccess extends GetAccess with support for extended containers
type ExtendedGetAccess struct {
	*GetAccess
	segments     [][]byte
	currentSeg   int
	segmentStack []*ExtendedGetAccess
}

// NewExtendedGetAccess creates a new extended get access
func NewExtendedGetAccess(buf []byte) *ExtendedGetAccess {
	e := &ExtendedGetAccess{
		segments:     [][]byte{buf},
		currentSeg:   0,
		segmentStack: make([]*ExtendedGetAccess, 0, 4),
	}

	// Check if this is an extended container
	if len(buf) >= 2 {
		h := binary.LittleEndian.Uint16(buf[0:2])
		_, typ := typetags.DecodeHeader(h)

		if typ == typetags.TypeExtendedTagContainer {
			e.loadExtendedContainer(buf)
		} else {
			e.GetAccess = NewGetAccess(buf)
		}
	}

	return e
}

// loadExtendedContainer loads and validates extended container chain
func (e *ExtendedGetAccess) loadExtendedContainer(buf []byte) {
	if len(buf) < typetags.ExtendedHeaderSize+2 {
		return
	}

	// Get container payload (skip header)
	container := NewGetAccess(buf)
	if container == nil {
		return
	}

	// Extract payload
	_, start, end := container.rangeAt(0)
	if end < start {
		return
	}

	payload := container.buf[start:end]
	offset := 0
	segments := make([][]byte, 0)

	for offset+typetags.ExtendedHeaderSize <= len(payload) {
		// Read extended header
		extHeader, ok := typetags.DecodeExtendedHeader(payload[offset:])
		if !ok {
			break
		}

		offset += typetags.ExtendedHeaderSize

		// Calculate segment end
		var segmentEnd int
		if extHeader.Continuation == typetags.EndOfChain {
			segmentEnd = len(payload)
		} else {
			segmentEnd = offset + int(extHeader.Continuation-extHeader.SelfOffset)
		}

		if segmentEnd > len(payload) {
			break
		}

		// Extract segment
		segment := payload[offset:segmentEnd]
		segments = append(segments, segment)
		offset = segmentEnd
	}

	if len(segments) > 0 {
		e.segments = segments
		e.GetAccess = NewGetAccess(segments[0])
		e.currentSeg = 0
	}
}

// NextSegment moves to the next segment in chain
func (e *ExtendedGetAccess) NextSegment() bool {
	if e.currentSeg+1 >= len(e.segments) {
		return false
	}

	e.currentSeg++
	e.GetAccess = NewGetAccess(e.segments[e.currentSeg])
	return true
}

// HasNextSegment checks if there are more segments
func (e *ExtendedGetAccess) HasNextSegment() bool {
	return e.currentSeg+1 < len(e.segments)
}

// CurrentSegment returns current segment index
func (e *ExtendedGetAccess) CurrentSegment() int {
	return e.currentSeg
}

// SegmentCount returns total number of segments
func (e *ExtendedGetAccess) SegmentCount() int {
	return len(e.segments)
}

// PushSegment saves current context and switches to nested segment
func (e *ExtendedGetAccess) PushSegment(segment []byte) {
	e.segmentStack = append(e.segmentStack, &ExtendedGetAccess{
		GetAccess:    e.GetAccess,
		segments:     e.segments,
		currentSeg:   e.currentSeg,
		segmentStack: e.segmentStack,
	})

	e.segments = [][]byte{segment}
	e.GetAccess = NewGetAccess(segment)
	e.currentSeg = 0
}

// PopSegment restores previous context
func (e *ExtendedGetAccess) PopSegment() {
	if len(e.segmentStack) == 0 {
		return
	}

	last := len(e.segmentStack) - 1
	*e = *e.segmentStack[last]
	e.segmentStack = e.segmentStack[:last]
}

// GetBytesExtended gets bytes across segment boundaries
func (e *ExtendedGetAccess) GetBytesExtended(pos int) ([]byte, error) {
	if e.GetAccess == nil {
		return nil, fmt.Errorf("no active segment")
	}

	result, err := e.GetAccess.GetBytes(pos)
	if err == nil {
		return result, nil
	}

	// Try to find in next segments
	originalSeg := e.currentSeg
	defer func() {
		e.currentSeg = originalSeg
		e.GetAccess = NewGetAccess(e.segments[originalSeg])
	}()

	for e.NextSegment() {
		result, err = e.GetAccess.GetBytes(pos)
		if err == nil {
			return result, nil
		}
	}

	return nil, fmt.Errorf("field %d not found in any segment", pos)
}
