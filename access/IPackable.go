package access

import (
	"github.com/quickwritereader/packos/types"
)

// ⚠️ Allocation Warning:
// When implementing Packable for slice types like []byte, be extremely cautious with interface boxing.
// Assigning a slice directly to an interface (e.g. Packable = []byte) can trigger heap allocation,
// even if the slice itself is stack-allocated. This happens because Go copies the slice header into
// a hidden interface wrapper, and escape analysis often forces it onto the heap.
//
// ✅ To avoid this, we wrap *[]byte in a struct (e.g. PackByteArrayRef) and implement Packable on that.
// This keeps the slice header on the stack and avoids hidden allocations during packing.
//
// This pattern is critical for high-throughput, teardown-safe systems where allocation discipline matters.
// Applies equally to other slice types (e.g. []int16, []float64) and maps if boxed into interfaces.

type Packable interface {
	HeaderType() types.Type
	ValueSize() int
	Write(buf []byte, pos int) int
	// dymanic way of adding to the putAccess
	PackInto(p *PutAccess)
}
