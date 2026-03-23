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
		PutAccess:    NewPutAccess(),
		segments:     make([][]byte, 0, 4),
		currentSize:  0,
		pivotSize:    pivotSize,
		extendedMode: false,
		segmentCount: 0,
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

	// Create a new PutAccess for the container
	container := NewPutAccess()

	// Write extended container header
	container.offsets = binary.LittleEndian.AppendUint16(container.offsets,
		typetags.EncodeHeader(0, typetags.TypeExtendedTagContainer))

	// Build the container payload
	var currentOffset uint32 = 0

	for i, segment := range p.segments {
		selfOffset := currentOffset
		var continuation uint32

		if i < len(p.segments)-1 {
			// Calculate continuation offset
			continuation = currentOffset + uint32(len(segment))
		} else {
			continuation = typetags.EndOfChain
		}

		// Add extended header
		container.buf = append(container.buf,
			typetags.EncodeExtendedHeader(selfOffset, continuation)...)

		// Add segment payload
		container.buf = append(container.buf, segment...)
		currentOffset += uint32(len(segment))
	}

	// Complete container
	container.offsets = binary.LittleEndian.AppendUint16(container.offsets,
		typetags.EncodeEnd(int(len(container.buf))))

	return container.Pack(), nil
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
