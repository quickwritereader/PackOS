package packable

import (
	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/typetags"
)

type Tuple struct {
	args *[]access.Packable
}

func NewTuple(args ...access.Packable) Tuple {
	return Tuple{args: &args}
}

// ValueSize returns the size of the packed
func (p Tuple) ValueSize() int {
	if len(*p.args) == 0 {
		// return 0
		return 0
	}
	value_size := 0
	for _, arg := range *p.args {
		value_size += arg.ValueSize()
	}
	return value_size + len(*p.args)*access.HeaderTagSize + access.HeaderTagSize
}

// Add packs the container into a byte buffer
func (p Tuple) Write(buf []byte, pos int) int {
	if len(*p.args) < 1 {
		return pos
	}

	// Calculate the size of the headers.
	headerSize := len(*p.args)*2 + 2

	// 'posH' is for writing headers, 'pos' is for writing data.
	posH := pos
	pos += headerSize
	delta_start := pos

	// First header will be encoded with absolute Position relative to itself
	posH = access.WriteTypeHeader(buf, posH, headerSize, (*p.args)[0].HeaderType())
	// write
	pos = (*p.args)[0].Write(buf, pos)

	for _, arg := range (*p.args)[1:] {

		// First, add the header for the key.
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, arg.HeaderType())
		// write
		pos = arg.Write(buf, pos)
	}
	_ = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeEnd)
	return pos
}

func Pack(args ...access.Packable) []byte {
	pp := NewTuple(args...)
	size := pp.ValueSize()
	buffer := make([]byte, size)
	pos := 0
	//headless tuple
	pp.Write(buffer, pos)
	return buffer
}

func (p Tuple) HeaderType() typetags.Type {
	return typetags.TypeTuple
}

func (pack Tuple) PackInto(p *access.PutAccess) {
	//prepare buffer and write everything, then append it into p
	size := pack.ValueSize()
	buffer := BufferPoolInst.Acquire(size)
	pos := 0
	pos = pack.Write(buffer, pos)
	p.AppendTagAndValue(typetags.TypeTuple, buffer[:pos])
	BufferPoolInst.Release(buffer)
}

func PackTuple(args ...access.Packable) Tuple {
	return Tuple{args: &args}
}
