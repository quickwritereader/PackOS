package access

import (
	"encoding/binary"
	"fmt"

	"github.com/quickwritereader/PackOS/typetags"
)

// ExtendedReader provides BFS/DFS access to extended containers
type ExtendedReader struct {
	segments     [][]byte          // All segments in the container
	currentSeg   int               // Current segment index
	segmentStack []*ExtendedReader // Stack for DFS traversal
	getAccess    *GetAccess        // Current GetAccess for the segment
}

// NewExtendedReader creates a new reader for extended containers
func NewExtendedReader(data []byte) *ExtendedReader {
	er := &ExtendedReader{
		segments:     make([][]byte, 0),
		currentSeg:   0,
		segmentStack: make([]*ExtendedReader, 0, 4),
		getAccess:    nil,
	}

	if len(data) == 0 {
		return er
	}

	// Check if this is an extended container
	if len(data) >= 2 {
		h := binary.LittleEndian.Uint16(data[0:2])
		_, typ := typetags.DecodeHeader(h)

		if typ == typetags.TypeExtendedTagContainer {
			er.loadExtendedContainer(data)
		} else {
			// Regular container
			er.segments = [][]byte{data}
			er.getAccess = NewGetAccess(data)
		}
	}

	return er
}

// loadExtendedContainer loads and parses an extended container
func (er *ExtendedReader) loadExtendedContainer(buf []byte) {
	if len(buf) < 4 {
		return
	}

	// Parse extended container
	h1 := binary.LittleEndian.Uint16(buf[0:2])
	offset1, typ := typetags.DecodeHeader(h1)

	if typ != typetags.TypeExtendedTagContainer {
		return
	}

	// Read TypeEnd marker
	h2 := binary.LittleEndian.Uint16(buf[2:4])
	endOffset := typetags.DecodeOffset(h2)

	// Handle extended container payload
	payloadStart := offset1
	var payloadEnd int

	if endOffset == 8191 {
		// Maximum 13-bit value - extended container with large payload
		payloadEnd = len(buf)
	} else if endOffset > 0 && offset1+endOffset <= len(buf) {
		// Use the encoded end offset
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

	// Parse extended headers and extract segments
	for offset+typetags.ExtendedHeaderSize <= len(payload) {
		// Read extended header
		extHeader, ok := typetags.DecodeExtendedHeader(payload[offset:])
		if !ok {
			break
		}

		// Validate SelfOffset matches current position
		if uint32(offset) != extHeader.SelfOffset {
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
		er.segments = segments
		er.getAccess = NewGetAccess(segments[0])
		er.currentSeg = 0
	}
}

// NextSegment moves to the next segment in the chain (BFS traversal)
func (er *ExtendedReader) NextSegment() bool {
	if er.currentSeg+1 >= len(er.segments) {
		return false
	}

	er.currentSeg++
	er.getAccess = NewGetAccess(er.segments[er.currentSeg])
	return true
}

// HasNextSegment checks if there are more segments
func (er *ExtendedReader) HasNextSegment() bool {
	return er.currentSeg+1 < len(er.segments)
}

// CurrentSegment returns the current segment index
func (er *ExtendedReader) CurrentSegment() int {
	return er.currentSeg
}

// SegmentCount returns the total number of segments
func (er *ExtendedReader) SegmentCount() int {
	return len(er.segments)
}

// PushSegment saves current context and switches to a nested segment (DFS traversal)
func (er *ExtendedReader) PushSegment(segment []byte) {
	// Save current state
	er.segmentStack = append(er.segmentStack, &ExtendedReader{
		segments:     er.segments,
		currentSeg:   er.currentSeg,
		segmentStack: er.segmentStack,
		getAccess:    er.getAccess,
	})

	// Switch to new segment
	er.segments = [][]byte{segment}
	er.getAccess = NewGetAccess(segment)
	er.currentSeg = 0
}

// PopSegment restores previous context
func (er *ExtendedReader) PopSegment() {
	if len(er.segmentStack) == 0 {
		return
	}

	last := len(er.segmentStack) - 1
	*er = *er.segmentStack[last]
	er.segmentStack = er.segmentStack[:last]
}

// GetBytes gets bytes from the current position (BFS access across segments)
func (er *ExtendedReader) GetBytes(pos int) ([]byte, error) {
	if er.getAccess == nil {
		return nil, fmt.Errorf("no active segment")
	}

	// Save original state
	originalSeg := er.currentSeg
	originalGetAccess := er.getAccess

	// Track current position as we iterate through segments
	currentPos := 0

	// Start from first segment
	er.currentSeg = 0
	er.getAccess = NewGetAccess(er.segments[0])

	// Iterate through all segments (BFS traversal)
	for segIndex := 0; segIndex < len(er.segments); segIndex++ {
		if segIndex > 0 {
			// Move to next segment
			er.currentSeg = segIndex
			er.getAccess = NewGetAccess(er.segments[segIndex])
		}

		if er.getAccess == nil {
			continue
		}

		// Try to get fields in current segment
		if er.getAccess.argCount > 0 {
			for segmentFieldIndex := 0; segmentFieldIndex < er.getAccess.argCount; segmentFieldIndex++ {
				// Get the raw bytes for this field using rangeAt
				tp, start, end := er.getAccess.rangeAt(segmentFieldIndex)
				if end <= start {
					// Empty field, skip it
					continue
				}

				// Skip extended containers (GetBytes can't decode them)
				if tp == typetags.TypeExtendedTagContainer {
					continue
				}

				// Extract the raw bytes
				result := er.getAccess.buf[start:end]

				// Check if this is the field we're looking for
				if currentPos == pos {
					// Found it! Restore original state before returning
					er.currentSeg = originalSeg
					er.getAccess = originalGetAccess
					return result, nil
				}

				// Move to next field
				currentPos++
			}
		}
	}

	// Restore original state
	er.currentSeg = originalSeg
	er.getAccess = originalGetAccess

	return nil, fmt.Errorf("field %d not found in any segment", pos)
}

// GetAccess returns the current GetAccess for direct operations
func (er *ExtendedReader) GetAccess() *GetAccess {
	return er.getAccess
}

// Reset resets to the first segment
func (er *ExtendedReader) Reset() {
	if len(er.segments) > 0 {
		er.currentSeg = 0
		er.getAccess = NewGetAccess(er.segments[0])
	}
}

// IsExtendedContainer checks if the data is an extended container
func IsExtendedContainer(data []byte) bool {
	if len(data) < 2 {
		return false
	}

	h := binary.LittleEndian.Uint16(data[0:2])
	_, typ := typetags.DecodeHeader(h)
	return typ == typetags.TypeExtendedTagContainer
}
