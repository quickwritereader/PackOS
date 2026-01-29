package packable

import (
	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/typetags"
	"github.com/quickwritereader/PackOS/utils"
)

// PackMapSorted packs a map of Packable values after sorting its keys.
type PackMapSorted map[string]access.Packable

// ValueSize returns the size of the packed map's content.
func (p PackMapSorted) ValueSize() int {
	size := 0
	for k, v := range p {
		// Add the size of the key and the size of the packed value.
		size += len(k) + v.ValueSize()
	}
	return size + len(p)*2*access.HeaderTagSize + access.HeaderTagSize
}

// HeaderType returns the type of the header for a map.
func (p PackMapSorted) HeaderType() typetags.Type {
	return typetags.TypeMap
}

// Add packs the map into a byte buffer by sorting keys first for a deterministic result.
func (p PackMapSorted) Write(buf []byte, pos int) int {
	keys := utils.SortKeys(p)
	headerSize := len(p)*2*access.HeaderTagSize + access.HeaderTagSize
	first := pos
	posH := pos
	pos += headerSize
	delta_start := pos
	for _, k := range keys {
		v := p[k]
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeString)
		pos = access.WriteString(buf, pos, k)
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, v.HeaderType())
		pos = v.Write(buf, pos)
	}
	// Add the end-of-container marker.
	//corect first arg with absolute
	_ = access.WriteTypeHeader(buf, first, headerSize, typetags.TypeString)
	_ = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeEnd)
	return pos
}

// PackMap packs a map of Packable values. This is the unsorted version.
type PackMap map[string]access.Packable

// ValueSize returns the size of the packed map's content.
func (p PackMap) ValueSize() int {
	size := 0
	for k, v := range p {
		// Add the size of the key and the size of the packed value.
		size += len(k) + v.ValueSize()
	}
	return size + len(p)*2*access.HeaderTagSize + access.HeaderTagSize
}

// HeaderType returns the type of the header for a map.
func (p PackMap) HeaderType() typetags.Type {
	return typetags.TypeMap
}

// Add packs the map into a byte buffer. This version does not sort keys.
func (p PackMap) Write(buf []byte, pos int) int {
	headerSize := len(p)*2*access.HeaderTagSize + access.HeaderTagSize
	first := pos
	posH := pos
	pos += headerSize
	delta_start := pos
	for k, v := range p {
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeString)
		pos = access.WriteString(buf, pos, k)
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, v.HeaderType())
		pos = v.Write(buf, pos)
	}
	// Add the end-of-container marker.
	//correct first arg with absolute
	_ = access.WriteTypeHeader(buf, first, headerSize, typetags.TypeString)
	_ = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeEnd)
	return pos
}

// PackMapStr packs a map of string values. This is the unsorted version.
type PackMapStr map[string]string

// ValueSize returns the size of the packed map's content.
func (p PackMapStr) ValueSize() int {
	size := 0
	for k, v := range p {
		// Add the size of the key and the size of the packed value.
		size += len(k) + len(v)
	}
	return size + len(p)*2*access.HeaderTagSize + access.HeaderTagSize
}

// HeaderType returns the type of the header for a map.
func (p PackMapStr) HeaderType() typetags.Type {
	return typetags.TypeMap
}

// Add packs the map into a byte buffer. This version does not sort keys.
func (p PackMapStr) Write(buf []byte, pos int) int {
	headerSize := len(p)*2*access.HeaderTagSize + access.HeaderTagSize
	first := pos
	posH := pos
	pos += headerSize
	delta_start := pos
	for k, v := range p {
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeString)
		pos = access.WriteString(buf, pos, k)
		posH = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeString)
		pos = access.WriteString(buf, pos, v)
	}
	// Add the end-of-container marker.
	//correct first arg with absolute
	_ = access.WriteTypeHeader(buf, first, headerSize, typetags.TypeString)
	_ = access.WriteTypeHeader(buf, posH, pos-delta_start, typetags.TypeEnd)
	return pos
}

// PackMapStrInt32 packs a map of int32 values. This is the unsorted version.
type PackMapStrInt32 map[string]int32

// ValueSize returns the size of the packed map's content.
func (p PackMapStrInt32) ValueSize() int {
	size := 0
	for k := range p {
		size += len(k)
	}
	size += len(p)*(2*access.HeaderTagSize+4) + access.HeaderTagSize
	return size
}

// HeaderType returns the type of the header for a map.
func (p PackMapStrInt32) HeaderType() typetags.Type {
	return typetags.TypeMap
}

// Write packs the map into a byte buffer. This version does not sort keys.
func (p PackMapStrInt32) Write(buf []byte, pos int) int {
	headerSize := len(p)*2*access.HeaderTagSize + access.HeaderTagSize
	first := pos
	posH := pos
	pos += headerSize
	deltaStart := pos

	for k, v := range p {
		posH = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeString)
		pos = access.WriteString(buf, pos, k)

		posH = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeInteger)
		pos = access.WriteInt32(buf, pos, v)
	}

	_ = access.WriteTypeHeader(buf, first, headerSize, typetags.TypeString)
	_ = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeEnd)
	return pos
}

// PackMapStrInt64 packs a map of int64 values. This is the unsorted version.
type PackMapStrInt64 map[string]int64

// ValueSize returns the size of the packed map's content.
func (p PackMapStrInt64) ValueSize() int {
	size := 0
	for k := range p {
		size += len(k)
	}
	size += len(p)*(2*access.HeaderTagSize+8) + access.HeaderTagSize
	return size
}

// HeaderType returns the type of the header for a map.
func (p PackMapStrInt64) HeaderType() typetags.Type {
	return typetags.TypeMap
}

// Write packs the map into a byte buffer. This version does not sort keys.
func (p PackMapStrInt64) Write(buf []byte, pos int) int {
	headerSize := len(p)*2*access.HeaderTagSize + access.HeaderTagSize
	first := pos
	posH := pos
	pos += headerSize
	deltaStart := pos

	for k, v := range p {
		posH = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeString)
		pos = access.WriteString(buf, pos, k)

		posH = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeInteger)
		pos = access.WriteInt64(buf, pos, v)
	}

	_ = access.WriteTypeHeader(buf, first, headerSize, typetags.TypeString)
	_ = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeEnd)
	return pos
}

// Pair for Packable values
type PackPair struct {
	Key   string
	Value access.Packable
}

func PP(key string, value access.Packable) PackPair {
	return PackPair{Key: key, Value: value}
}

// PackableMapOrdered packs Packable values using OrderedMap to preserve insertion order.
type PackableMapOrdered struct {
	om *typetags.OrderedMap[access.Packable]
}

// PackMapOrdered creates a new PackableMapOrdered, optionally initialized with pairs.
func PackMapOrdered(pairs ...PackPair) *PackableMapOrdered {
	om := typetags.NewOrderedMap[access.Packable]()
	for _, p := range pairs {
		om.Set(p.Key, p.Value)
	}
	return &PackableMapOrdered{om: om}
}

// Set adds or updates a key/value pair.
func (p *PackableMapOrdered) Set(key string, val access.Packable) {
	p.om.Set(key, val)
}

// ValueSize returns the size of the packed map's content.
func (p *PackableMapOrdered) ValueSize() int {
	size := 0
	for k, v := range p.om.ItemsIter() {
		size += len(k) + v.ValueSize()
	}
	return size + len(p.om.Keys())*2*access.HeaderTagSize + access.HeaderTagSize
}

// HeaderType returns the type of the header for a map.
func (p *PackableMapOrdered) HeaderType() typetags.Type {
	return typetags.TypeMap
}

// Write packs the map into a byte buffer in insertion order.
func (p *PackableMapOrdered) Write(buf []byte, pos int) int {
	headerSize := len(p.om.Keys())*2*access.HeaderTagSize + access.HeaderTagSize
	first := pos
	posH := pos
	pos += headerSize
	deltaStart := pos

	for k, v := range p.om.ItemsIter() {
		posH = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeString)
		pos = access.WriteString(buf, pos, k)
		posH = access.WriteTypeHeader(buf, posH, pos-deltaStart, v.HeaderType())
		pos = v.Write(buf, pos)
	}

	// End-of-container marker
	_ = access.WriteTypeHeader(buf, first, headerSize, typetags.TypeString)
	_ = access.WriteTypeHeader(buf, posH, pos-deltaStart, typetags.TypeEnd)

	return pos
}

func (pack PackMap) PackInto(p *access.PutAccess) {
	size := pack.ValueSize()
	buffer := bPool.Acquire(size)
	pos := 0
	pos = pack.Write(buffer, pos)
	p.AppendTagAndValue(typetags.TypeMap, buffer[:pos])
	bPool.Release(buffer)
}

func (pack PackMapSorted) PackInto(p *access.PutAccess) {
	size := pack.ValueSize()
	buffer := bPool.Acquire(size)
	pos := 0
	pos = pack.Write(buffer, pos)
	p.AppendTagAndValue(typetags.TypeMap, buffer[:pos])
	bPool.Release(buffer)
}

func (pack PackMapStr) PackInto(p *access.PutAccess) {
	size := pack.ValueSize()
	buffer := bPool.Acquire(size)
	pos := 0
	pos = pack.Write(buffer, pos)
	p.AppendTagAndValue(typetags.TypeMap, buffer[:pos])
	bPool.Release(buffer)
}

// PackInto packs the ordered map into the PutAccess buffer.
func (pack *PackableMapOrdered) PackInto(p *access.PutAccess) {
	size := pack.ValueSize()
	buffer := bPool.Acquire(size)
	pos := 0
	pos = pack.Write(buffer, pos)
	p.AppendTagAndValue(typetags.TypeMap, buffer[:pos])
	bPool.Release(buffer)
}
