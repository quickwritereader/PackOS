package access

import (
	"encoding/binary"
	"fmt"

	"github.com/quickwritereader/PackOS/typetags"
)

// ExtendedPutAccess extends PutAccess with automatic segment management
type ExtendedPutAccess struct {
	*PutAccess
	segments     [][]byte // Chain of completed segments
	currentSize  int      // Current segment size (headers + payload)
	pivotSize    int      // Size threshold for creating new segment
	extendedMode bool     // Whether we're in extended container mode
	segmentCount int      // Number of segments created

	triplets    []Triplet            // Track [parent segment, nextOffset address, actual segment]
	nestedStack []*ExtendedPutAccess // Stack for nested containers

	// For nested containers: track parent offset address
	parentOffsetAddr int // Address in parent's offsets where our header is
}

// NewExtendedPutAccess creates a new extended put access with custom pivot size
func NewExtendedPutAccess(pivotSize int) *ExtendedPutAccess {
	if pivotSize <= 0 {
		pivotSize = 4096 // 4KB default
	}

	// Ensure pivot size doesn't exceed 8KB for root segment optimization
	if pivotSize > 8192 {
		pivotSize = 8192
	}

	return &ExtendedPutAccess{
		PutAccess:        NewPutAccess(),
		segments:         make([][]byte, 0, 4),
		currentSize:      0,
		pivotSize:        pivotSize,
		extendedMode:     false,
		segmentCount:     0,
		triplets:         make([]Triplet, 0, 8),
		nestedStack:      make([]*ExtendedPutAccess, 0, 4),
		parentOffsetAddr: -1, // -1 means not a nested container
	}
}

// updateCurrentSize updates the current segment size
func (p *ExtendedPutAccess) updateCurrentSize() {
	headerSize := len(p.offsets) + 2 // +2 for TypeEnd
	p.currentSize = headerSize + p.position
}

// checkSegmentThreshold checks if current segment size exceeds threshold
func (p *ExtendedPutAccess) checkSegmentThreshold(additionalSize int) bool {
	// Update current size before checking
	p.updateCurrentSize()
	return p.currentSize+additionalSize > p.pivotSize
}

// finalizeSegment completes the current segment and starts a new one
func (p *ExtendedPutAccess) finalizeSegment() error {
	if p.position == 0 && len(p.offsets) == 0 {
		return nil // Empty segment, nothing to finalize
	}

	// Update final size
	p.updateCurrentSize()

	// Complete current segment
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets,
		typetags.EncodeEnd(p.position))

	// Pack current segment
	segment := p.Pack()
	p.segments = append(p.segments, segment)

	// Create triplet for this segment if we're in extended mode
	if p.extendedMode && len(p.segments) > 1 {
		// For extended containers, track the segment
		triplet := Triplet{
			ParentSegment:  nil, // Root segments don't have parent
			NextOffsetAddr: -1,  // Not applicable for root segments
			ActualSegment:  segment,
			IsExtended:     true,
			SelfOffset:     uint32(len(segment)), // Will be updated in buildExtendedContainer
		}
		p.triplets = append(p.triplets, triplet)
	}

	// Reset for next segment
	p.buf = make([]byte, 0, p.pivotSize)
	p.offsets = make([]byte, 0, 64)
	p.position = 0
	p.currentSize = 0
	p.segmentCount++

	// Switch to extended mode after first segment
	if !p.extendedMode && len(p.segments) == 1 {
		p.extendedMode = true
	}

	return nil
}

// buildExtendedContainer creates the final extended container structure
func (p *ExtendedPutAccess) buildExtendedContainer() ([]byte, error) {
	if len(p.segments) == 0 {
		return nil, fmt.Errorf("no segments to build")
	}

	// Build the container payload with extended headers and segments
	var currentOffset uint32 = 0
	payload := make([]byte, 0)

	for i, segment := range p.segments {
		selfOffset := currentOffset
		var continuation uint32

		if i < len(p.segments)-1 {
			// Calculate continuation offset
			// Next extended header will be at currentOffset + ExtendedHeaderSize + segment length
			continuation = currentOffset + typetags.ExtendedHeaderSize + uint32(len(segment))
		} else {
			continuation = typetags.EndOfChain
		}

		// Add extended header
		payload = append(payload,
			typetags.EncodeExtendedHeader(selfOffset, continuation)...)

		// Add segment payload
		payload = append(payload, segment...)

		// Update triplet SelfOffset if this is an extended container segment
		if p.extendedMode && i < len(p.triplets) {
			p.triplets[i].SelfOffset = selfOffset
			p.triplets[i].Continuation = continuation
		}

		currentOffset += typetags.ExtendedHeaderSize + uint32(len(segment))
	}

	// Calculate payload size
	payloadSize := len(payload)

	// Create headers manually
	// First header: extended container type with offset to payload
	headers := make([]byte, 0, 4) // Reserve space for header + TypeEnd
	headers = binary.LittleEndian.AppendUint16(headers,
		typetags.EncodeHeader(4, typetags.TypeExtendedTagContainer)) // Offset is 4 (size of headers section)

	// Add TypeEnd marker
	// For extended containers, we use the maximum 13-bit value (8191)
	// since the actual payload might be larger. The loader will handle this.
	max13Bit := 8191
	if payloadSize < max13Bit {
		max13Bit = payloadSize
	}
	headers = binary.LittleEndian.AppendUint16(headers,
		typetags.EncodeEnd(max13Bit))

	// Combine headers and payload
	result := make([]byte, 0, len(headers)+payloadSize)
	result = append(result, headers...)
	result = append(result, payload...)

	return result, nil
}

// PackExtended finalizes and returns the packed buffer with extended container support
func (p *ExtendedPutAccess) PackExtended() ([]byte, error) {
	// Finalize current segment if there's data
	if p.position > 0 || len(p.offsets) > 0 {
		if err := p.finalizeSegment(); err != nil {
			return nil, err
		}
	}

	// If no segments were created, return empty buffer
	if len(p.segments) == 0 {
		return []byte{}, nil
	}

	// If only one segment and it's within limits, return as is
	if len(p.segments) == 1 && !p.extendedMode {
		return p.segments[0], nil
	}

	// Build extended container
	return p.buildExtendedContainer()
}

// AddWithExtendedCheck adds data with automatic segment creation
func (p *ExtendedPutAccess) AddWithExtendedCheck(adder func(*PutAccess), dataSize int) error {
	// If data is larger than pivot size, we need to handle it specially
	if dataSize > p.pivotSize {
		// For very large data, we need to create an extended container
		// Create a nested extended container for this large data
		nested := p.BeginNested(typetags.TypeTuple)
		adder(nested.PutAccess)

		// Force the nested container to be extended
		nested.extendedMode = true

		// Finalize and end the nested container
		if err := p.EndNested(nested); err != nil {
			return err
		}
		return nil
	}

	// Regular size data
	if p.checkSegmentThreshold(dataSize) {
		if err := p.finalizeSegment(); err != nil {
			return err
		}
	}
	adder(p.PutAccess)
	p.updateCurrentSize()
	return nil
}

// AddInt16Extended adds int16 with segment check
func (p *ExtendedPutAccess) AddInt16Extended(v int16) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddInt16(v)
	}, 2)
}

// AddInt32Extended adds int32 with segment check
func (p *ExtendedPutAccess) AddInt32Extended(v int32) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddInt32(v)
	}, 4)
}

// AddInt64Extended adds int64 with segment check
func (p *ExtendedPutAccess) AddInt64Extended(v int64) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddInt64(v)
	}, 8)
}

// AddStringExtended adds string with segment check
func (p *ExtendedPutAccess) AddStringExtended(s string) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddString(s)
	}, len(s))
}

// AddBytesExtended adds bytes with segment check
func (p *ExtendedPutAccess) AddBytesExtended(b []byte) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddBytes(b)
	}, len(b))
}

// AddBoolExtended adds bool with segment check
func (p *ExtendedPutAccess) AddBoolExtended(b bool) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddBool(b)
	}, 1)
}

// AddFloat32Extended adds float32 with segment check
func (p *ExtendedPutAccess) AddFloat32Extended(v float32) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddFloat32(v)
	}, 4)
}

// AddFloat64Extended adds float64 with segment check
func (p *ExtendedPutAccess) AddFloat64Extended(v float64) error {
	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddFloat64(v)
	}, 8)
}

// AddMapExtended adds map with segment check
func (p *ExtendedPutAccess) AddMapExtended(m map[string][]byte) error {
	// Estimate map size
	estimatedSize := 0
	for k, v := range m {
		estimatedSize += len(k) + len(v)
	}

	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddMap(m)
	}, estimatedSize)
}

// AddMapStrExtended adds map[string]string with segment check
func (p *ExtendedPutAccess) AddMapStrExtended(m map[string]string) error {
	// Estimate map size
	estimatedSize := 0
	for k, v := range m {
		estimatedSize += len(k) + len(v)
	}

	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddMapStr(m)
	}, estimatedSize)
}

// AddMapAnyExtended adds map[string]any with segment check
func (p *ExtendedPutAccess) AddMapAnyExtended(m map[string]any, useNumeric bool) error {
	// Estimate map size
	estimatedSize := 0
	for k, v := range m {
		estimatedSize += len(k)
		switch val := v.(type) {
		case string:
			estimatedSize += len(val)
		case []byte:
			estimatedSize += len(val)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			estimatedSize += 8
		case float32, float64:
			estimatedSize += 8
		case bool:
			estimatedSize += 1
		default:
			estimatedSize += 64 // fallback for complex types
		}
	}

	return p.AddWithExtendedCheck(func(pa *PutAccess) {
		pa.AddMapAny(m, useNumeric)
	}, estimatedSize)
}

// GetCurrentSize returns the current segment size
func (p *ExtendedPutAccess) GetCurrentSize() int {
	p.updateCurrentSize()
	return p.currentSize
}

// GetSegmentCount returns the number of segments created
func (p *ExtendedPutAccess) GetSegmentCount() int {
	return p.segmentCount
}

// IsExtendedMode returns whether we're in extended mode
func (p *ExtendedPutAccess) IsExtendedMode() bool {
	return p.extendedMode
}

// BeginNested starts a nested container that may become extended
func (p *ExtendedPutAccess) BeginNested(tag typetags.Type) *ExtendedPutAccess {
	// Record where our header will be in parent's offsets
	nextOffsetAddr := len(p.offsets)

	// Write placeholder header (will be patched later if extended)
	p.offsets = binary.LittleEndian.AppendUint16(p.offsets,
		typetags.EncodeHeader(p.position, tag))

	// Create nested access
	nested := &ExtendedPutAccess{
		PutAccess:        NewPutAccess(),
		segments:         make([][]byte, 0, 4),
		currentSize:      0,
		pivotSize:        p.pivotSize,
		extendedMode:     false,
		segmentCount:     0,
		triplets:         p.triplets,               // Share triplets with parent
		nestedStack:      append(p.nestedStack, p), // Push parent to stack
		parentOffsetAddr: nextOffsetAddr,           // Store where our header is in parent
	}

	return nested
}

// EndNested ends a nested container and handles potential extension
func (p *ExtendedPutAccess) EndNested(nested *ExtendedPutAccess) error {
	// Pack the nested container
	nestedData, err := nested.PackExtended()
	if err != nil {
		return err
	}

	// Check if this nested container needs to be extended
	// (either because it's large or contains extended segments)
	needsExtension := len(nestedData) > p.pivotSize || nested.extendedMode

	// Create triplet for tracking
	triplet := Triplet{
		ParentSegment:  p.buf,
		NextOffsetAddr: nested.parentOffsetAddr, // Address of our header in parent
		ActualSegment:  nestedData,
		IsExtended:     needsExtension,
	}

	// Add to shared triplets
	p.triplets = append(p.triplets, triplet)

	if needsExtension {
		// Update parent header to extended container type
		headerIdx := nested.parentOffsetAddr
		if headerIdx >= 0 && headerIdx+2 <= len(p.offsets) {
			currentHeader := binary.LittleEndian.Uint16(p.offsets[headerIdx:])
			offset, _ := typetags.DecodeHeader(currentHeader)
			newHeader := typetags.EncodeHeader(offset, typetags.TypeExtendedTagContainer)
			binary.LittleEndian.PutUint16(p.offsets[headerIdx:], newHeader)
		}

		// Store extended container data
		p.buf = append(p.buf, nestedData...)
		p.position = len(p.buf)
	} else {
		// Store regular nested container
		p.buf = append(p.buf, nestedData...)
		p.position = len(p.buf)
	}

	return nil
}

// BeginTuple starts a tuple that may become extended
func (p *ExtendedPutAccess) BeginTuple() *ExtendedPutAccess {
	return p.BeginNested(typetags.TypeTuple)
}

// BeginMap starts a map that may become extended
func (p *ExtendedPutAccess) BeginMap() *ExtendedPutAccess {
	return p.BeginNested(typetags.TypeMap)
}

// GetTriplets returns all tracked triplets
func (p *ExtendedPutAccess) GetTriplets() []Triplet {
	return p.triplets
}
