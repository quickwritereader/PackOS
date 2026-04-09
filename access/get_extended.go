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
	if len(buf) < 4 {
		return
	}

	// Parse extended container manually (bypass NewGetAccess for large payloads)
	// Read first header to get type
	h1 := binary.LittleEndian.Uint16(buf[0:2])
	offset1, typ := typetags.DecodeHeader(h1)

	if typ != typetags.TypeExtendedTagContainer {
		return
	}

	// Read TypeEnd marker
	h2 := binary.LittleEndian.Uint16(buf[2:4])
	endOffset := typetags.DecodeOffset(h2)

	// For extended containers, the payload might be larger than 8191 bytes
	// but EncodeEnd() can only encode 13 bits. So we need to handle this specially.
	// If endOffset is 8191 (the maximum 13-bit value), we assume the payload
	// extends to the end of the buffer (extended container case).
	payloadStart := offset1
	var payloadEnd int

	if endOffset == 8191 {
		// Maximum 13-bit value - extended container with large payload
		payloadEnd = len(buf)
	} else if endOffset > 0 && offset1+endOffset <= len(buf) {
		// Use the encoded end offset if it's valid
		payloadEnd = offset1 + endOffset
	} else {
		// Invalid end offset
		return
	}

	if payloadEnd <= payloadStart || payloadEnd > len(buf) {
		return
	}

	payload := buf[payloadStart:payloadEnd]
	offset := 0
	segments := make([][]byte, 0)

	for offset+typetags.ExtendedHeaderSize <= len(payload) {
		// Read extended header
		extHeader, ok := typetags.DecodeExtendedHeader(payload[offset:])
		if !ok {
			break
		}

		// Validate SelfOffset matches current position
		if uint32(offset) != extHeader.SelfOffset {
			// SelfOffset should match where we found this header
			break
		}

		// Calculate segment start and end
		segmentStart := offset + typetags.ExtendedHeaderSize
		var segmentEnd int

		if extHeader.Continuation == typetags.EndOfChain {
			segmentEnd = len(payload)
		} else {
			segmentEnd = int(extHeader.Continuation)
		}

		if segmentEnd > len(payload) || segmentStart >= segmentEnd {
			break
		}

		// Extract segment
		segment := payload[segmentStart:segmentEnd]
		segments = append(segments, segment)

		// Move to next segment
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

	// Save original state
	originalSeg := e.currentSeg
	originalGetAccess := e.GetAccess

	// Track current position as we iterate through segments
	currentPos := 0

	// Start from first segment
	e.currentSeg = 0
	e.GetAccess = NewGetAccess(e.segments[0])

	// Iterate through all segments
	for segIndex := 0; segIndex < len(e.segments); segIndex++ {
		if segIndex > 0 {
			// Move to next segment
			e.currentSeg = segIndex
			e.GetAccess = NewGetAccess(e.segments[segIndex])
		}

		if e.GetAccess == nil {
			continue
		}

		// Try to get fields in current segment
		// Use argCount to know how many fields are in this segment
		if e.GetAccess.argCount > 0 {
			for segmentFieldIndex := 0; segmentFieldIndex < e.GetAccess.argCount; segmentFieldIndex++ {
				// Get the raw bytes for this field using rangeAt
				tp, start, end := e.GetAccess.rangeAt(segmentFieldIndex)
				if end <= start {
					// Empty field, skip it
					continue
				}

				// Skip extended containers (GetBytes can't decode them)
				if tp == typetags.TypeExtendedTagContainer {
					continue
				}

				// Extract the raw bytes
				result := e.GetAccess.buf[start:end]

				// Check if this is the field we're looking for
				if currentPos == pos {
					// Found it! Restore original state before returning
					e.currentSeg = originalSeg
					e.GetAccess = originalGetAccess
					return result, nil
				}

				// Move to next field
				currentPos++
			}
		}
	}

	// Restore original state
	e.currentSeg = originalSeg
	e.GetAccess = originalGetAccess

	return nil, fmt.Errorf("field %d not found in any segment", pos)
}
