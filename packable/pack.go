package packable

import (
	"github.com/BranchAndLink/paosp/access"
	"github.com/BranchAndLink/paosp/types"
)

type PackContainer struct {
	args []access.Packable
}

func NewPackContainer(args ...access.Packable) PackContainer {
	return PackContainer{args: args}
}

// ValueSize returns the size of the packed
func (p PackContainer) ValueSize() int {
	value_size := 0
	for _, arg := range p.args {
		value_size += arg.ValueSize()
	}
	return value_size + len(p.args)*access.HeaderTagSize + access.HeaderTagSize
}

// Add packs the container into a byte buffer
func (p PackContainer) Write(buf []byte, pos int) int {
	if len(p.args) < 1 {
		return pos
	}

	// Calculate the size of the headers.
	headerSize := len(p.args)*2 + 2

	// 'posH' is for writing headers, 'pos' is for writing data.
	posH := pos
	pos += headerSize
	delta_start := pos

	// First header will be encoded with absolute Position relative to itself
	posH = access.WriteTypeHeader(buf, posH, headerSize, p.args[0].HeaderType())
	// write
	pos = p.args[0].Write(buf, pos)

	for _, arg := range p.args[1:] {

		// First, add the header for the key.
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, arg.HeaderType())
		// write
		pos = arg.Write(buf, pos)
	}
	_ = access.WriteTypeHeader(buf, posH, pos-delta_start, types.TypeEnd)
	return pos
}

func Pack(args ...access.Packable) []byte {
	pp := NewPackContainer(args...)
	size := pp.ValueSize()
	buffer := make([]byte, size)
	pos := 0
	pp.Write(buffer, pos)
	return buffer
}

func (pack PackContainer) PackInto(p *access.PutAccess) {
	size := pack.ValueSize()
	buffer := bPool.Acquire(size)
	pos := 0
	pos = pack.Write(buffer, pos)
	p.AppendTagValue(types.TypeContainer, buffer[:pos])
	bPool.Release(buffer)
}
