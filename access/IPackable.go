package access

import "github.com/quickwritereader/PackOS/typetags"

// ⚠️ Allocation Warning:
// When implementing Packable for slice typetags like []byte, be extremely cautious with interface boxing.
// Assigning a slice directly to an interface (e.g. Packable = []byte) can trigger heap allocation,
// even if the slice itself is stack-allocated. This happens because Go copies the slice header into
// a hidden interface wrapper, and escape analysis often forces it onto the heap.
//
// ✅ To avoid this, we wrap *[]byte in a struct (e.g. PackByteArrayRef) and implement Packable on that.
// This keeps the slice header on the stack and avoids hidden allocations during packing.
//
// This pattern is critical for high-throughput, teardown-safe systems where allocation discipline matters.
// Applies equally to other slice typetags (e.g. []int16, []float64) and maps if boxed into interfaces.

type Packable interface {
	HeaderType() typetags.Type
	ValueSize() int
	Write(buf []byte, pos int) int
	// dymanic way of adding to the putAccess
	PackInto(p *PutAccess)
}
