package access

import (
	"encoding/binary"
	"fmt"

	"github.com/quickwritereader/PackOS/typetags"
)

// Segment represents a segment in an extended container
type Segment struct {
	Data         []byte // Segment data
	SelfOffset   uint32 // Offset of this segment's header within container
	Continuation uint32 // Offset of next segment header (or EndOfChain)
	IsExtended   bool   // Whether this segment is itself an extended container
}

// Triplet tracks the relationship between nested containers
type Triplet struct {
	ParentSegment  []byte // Reference to parent segment buffer
	NextOffsetAddr int    // Address within parent segment where nextOffset should be written
	ActualSegment  []byte // The actual segment data
	IsExtended     bool   // Whether this segment is an extended container
	SelfOffset     uint32 // Absolute offset for extended header
	Continuation   uint32 // Continuation offset for extended header
}

// ExtendedContainer manages extended containers with automatic segmentation
type ExtendedContainer struct {
	segments         []Segment          // All segments in this container
	triplets         []Triplet          // Tracked triplets for nested containers
	current          *PutAccess         // Current segment being written
	pivotSize        int                // Size threshold for creating new segments
	isExtended       bool               // Whether this container is in extended mode
	parent           *ExtendedContainer // Parent container (nil for root)
	parentOffsetAddr int                // Where our header is in parent's offsets
}

// NewExtendedContainer creates a new extended container
func NewExtendedContainer(pivotSize int) *ExtendedContainer {
	if pivotSize <= 0 {
		pivotSize = 4096 // 4KB default
	}
	if pivotSize > 8192 {
		pivotSize = 8192 // Max 8KB for optimization
	}

	return &ExtendedContainer{
		segments:         make([]Segment, 0, 4),
		triplets:         make([]Triplet, 0, 8),
		current:          NewPutAccess(),
		pivotSize:        pivotSize,
		isExtended:       false,
		parent:           nil,
		parentOffsetAddr: -1,
	}
}

// newNestedContainer creates a nested extended container
func newNestedContainer(parent *ExtendedContainer, offsetAddr int) *ExtendedContainer {
	return &ExtendedContainer{
		segments:         make([]Segment, 0, 4),
		triplets:         parent.triplets, // Share triplets with parent
		current:          NewPutAccess(),
		pivotSize:        parent.pivotSize,
		isExtended:       false,
		parent:           parent,
		parentOffsetAddr: offsetAddr,
	}
}

// currentSize returns the current segment size including headers
func (ec *ExtendedContainer) currentSize() int {
	if ec.current == nil {
		return 0
	}
	headerSize := len(ec.current.offsets) + 2 // +2 for TypeEnd
	return headerSize + ec.current.position
}

// checkThreshold checks if adding data would exceed the pivot size
func (ec *ExtendedContainer) checkThreshold(additional int) bool {
	return ec.currentSize()+additional > ec.pivotSize
}

// finalizeSegment completes the current segment and starts a new one
func (ec *ExtendedContainer) finalizeSegment() error {
	if ec.current.position == 0 && len(ec.current.offsets) == 0 {
		return nil // Empty segment
	}

	// Complete current segment
	ec.current.offsets = binary.LittleEndian.AppendUint16(ec.current.offsets,
		typetags.EncodeEnd(ec.current.position))

	// Pack the segment
	segmentData := ec.current.Pack()

	// Create segment
	segment := Segment{
		Data:       segmentData,
		IsExtended: false, // Will be updated if needed
	}

	ec.segments = append(ec.segments, segment)

	// Reset for next segment
	ec.current = NewPutAccess()

	// Switch to extended mode after first segment
	if !ec.isExtended && len(ec.segments) == 1 {
		ec.isExtended = true
	}

	return nil
}

// Add adds data with automatic segmentation
func (ec *ExtendedContainer) Add(adder func(*PutAccess), dataSize int) error {
	// Handle very large data
	if dataSize > ec.pivotSize {
		// Create nested extended container for large data
		nested := ec.BeginNested(typetags.TypeTuple)
		adder(nested.current)
		nested.isExtended = true // Force extended mode
		return ec.EndNested(nested)
	}

	// Check if we need a new segment
	if ec.checkThreshold(dataSize) {
		if err := ec.finalizeSegment(); err != nil {
			return err
		}
	}

	// Add data to current segment
	adder(ec.current)
	return nil
}

// BeginNested starts a nested container
func (ec *ExtendedContainer) BeginNested(tag typetags.Type) *ExtendedContainer {
	// Record where our header will be in parent's offsets
	offsetAddr := len(ec.current.offsets)

	// Write placeholder header
	ec.current.offsets = binary.LittleEndian.AppendUint16(ec.current.offsets,
		typetags.EncodeHeader(ec.current.position, tag))

	// Create nested container
	return newNestedContainer(ec, offsetAddr)
}

// BeginTuple starts a tuple that may become extended
func (ec *ExtendedContainer) BeginTuple() *ExtendedContainer {
	return ec.BeginNested(typetags.TypeTuple)
}

// BeginMap starts a map that may become extended
func (ec *ExtendedContainer) BeginMap() *ExtendedContainer {
	return ec.BeginNested(typetags.TypeMap)
}

// EndNested ends a nested container
func (ec *ExtendedContainer) EndNested(nested *ExtendedContainer) error {
	// Pack the nested container
	nestedData, err := nested.Pack()
	if err != nil {
		return err
	}

	// Check if nested container needs to be extended
	needsExtension := len(nestedData) > ec.pivotSize || nested.isExtended

	// Create triplet for tracking
	triplet := Triplet{
		ParentSegment:  ec.current.buf,
		NextOffsetAddr: nested.parentOffsetAddr,
		ActualSegment:  nestedData,
		IsExtended:     needsExtension,
	}

	// Add to triplets (shared with parent chain)
	ec.triplets = append(ec.triplets, triplet)

	if needsExtension {
		// Update parent header to extended container type
		if triplet.NextOffsetAddr >= 0 && triplet.NextOffsetAddr+2 <= len(ec.current.offsets) {
			currentHeader := binary.LittleEndian.Uint16(ec.current.offsets[triplet.NextOffsetAddr:])
			offset, _ := typetags.DecodeHeader(currentHeader)
			newHeader := typetags.EncodeHeader(offset, typetags.TypeExtendedTagContainer)
			binary.LittleEndian.PutUint16(ec.current.offsets[triplet.NextOffsetAddr:], newHeader)
		}
	}

	// Store the nested data
	ec.current.buf = append(ec.current.buf, nestedData...)
	ec.current.position = len(ec.current.buf)

	return nil
}

// buildExtendedContainer builds the final extended container structure
func (ec *ExtendedContainer) buildExtendedContainer() ([]byte, error) {
	if len(ec.segments) == 0 {
		return nil, fmt.Errorf("no segments to build")
	}

	// Build payload with extended headers
	var currentOffset uint32 = 0
	payload := make([]byte, 0)

	for i, segment := range ec.segments {
		selfOffset := currentOffset
		var continuation uint32

		if i < len(ec.segments)-1 {
			// Next header will be at currentOffset + ExtendedHeaderSize + segment length
			continuation = currentOffset + typetags.ExtendedHeaderSize + uint32(len(segment.Data))
		} else {
			continuation = typetags.EndOfChain
		}

		// Add extended header
		payload = append(payload,
			typetags.EncodeExtendedHeader(selfOffset, continuation)...)

		// Add segment data
		payload = append(payload, segment.Data...)

		// Update segment metadata
		ec.segments[i].SelfOffset = selfOffset
		ec.segments[i].Continuation = continuation

		currentOffset += typetags.ExtendedHeaderSize + uint32(len(segment.Data))
	}

	// Create container headers
	headers := make([]byte, 0, 4)
	headers = binary.LittleEndian.AppendUint16(headers,
		typetags.EncodeHeader(4, typetags.TypeExtendedTagContainer))

	// Handle large payloads (>8191 bytes)
	payloadSize := len(payload)
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

// Pack finalizes and returns the packed container
func (ec *ExtendedContainer) Pack() ([]byte, error) {
	// Finalize current segment if it has data
	if ec.current.position > 0 || len(ec.current.offsets) > 0 {
		if err := ec.finalizeSegment(); err != nil {
			return nil, err
		}
	}

	// If no segments, return empty
	if len(ec.segments) == 0 {
		return []byte{}, nil
	}

	// If single segment and not extended, return as-is
	if len(ec.segments) == 1 && !ec.isExtended {
		return ec.segments[0].Data, nil
	}

	// Build extended container
	return ec.buildExtendedContainer()
}

// GetTriplets returns all tracked triplets
func (ec *ExtendedContainer) GetTriplets() []Triplet {
	return ec.triplets
}

// SegmentCount returns the number of segments
func (ec *ExtendedContainer) SegmentCount() int {
	return len(ec.segments)
}

// IsExtended returns whether this container is in extended mode
func (ec *ExtendedContainer) IsExtended() bool {
	return ec.isExtended
}

// Convenience methods for common data types
func (ec *ExtendedContainer) AddInt16(v int16) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddInt16(v)
	}, 2)
}

func (ec *ExtendedContainer) AddInt32(v int32) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddInt32(v)
	}, 4)
}

func (ec *ExtendedContainer) AddInt64(v int64) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddInt64(v)
	}, 8)
}

func (ec *ExtendedContainer) AddString(s string) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddString(s)
	}, len(s))
}

func (ec *ExtendedContainer) AddBytes(b []byte) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddBytes(b)
	}, len(b))
}

func (ec *ExtendedContainer) AddBool(b bool) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddBool(b)
	}, 1)
}

func (ec *ExtendedContainer) AddFloat32(v float32) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddFloat32(v)
	}, 4)
}

func (ec *ExtendedContainer) AddFloat64(v float64) error {
	return ec.Add(func(pa *PutAccess) {
		pa.AddFloat64(v)
	}, 8)
}
