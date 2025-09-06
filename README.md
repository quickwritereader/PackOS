# Paosp
PAOSP is a binary packing protocol with offset-indexed framing, designed for fast composition, random access, and reliable for  RPC or BadgerDB workloads. It supports:
All packing paths emit canonical headers, preserve offset domains, and support recursive structures. PAOSP ensures schema validation, GC neutrality, and reproducible output across variants.

- [x] Canonical encoding
Emits offset-indexed binary frames with consistent headers and type tags.
- [x] Sequential decoding
Supports single-pass traversal with offset tracking and teardown-safe accessors.
- [x] Random access decoding
Enables direct lookup via offset domains without full unpacking.
- [x] Recursive structure support
Handles nested maps, slices, and tagged frames with offset provenance.
- [ ] Simple schema validation
Validates type tags, offsets, and structure boundaries during decode.
- [x] GC neutrality
Avoids retained slices and ensures allocation discipline across packing paths.
- [x] Reproducible output
Guarantees stable binary layout across runs and variants.

## Example


```go
put := paosp.NewPacker()
put.AddInt16(42)                 // 2 bytes
put.AddBool(true)                // 1 byte
put.AddString("go")              // 2-byte length + 2 bytes
put.AddBytes([]byte{0xAA, 0xBB}) // 2-byte length + 2 bytes

actual := put.Pack()

expected := []byte{
	// Headers (5 × 2 bytes)
	0x51, 0x00, // header[0]: absolute offset=10, TypeInt16
	0x15, 0x00, // header[1]: delta=2, TypeBool
	0x1E, 0x00, // header[2]: delta=3, TypeString ("go")
	0x2E, 0x00, // header[3]: delta=5, TypeString (bytes)
	0x38, 0x00, // header[4]: delta=7, TypeEnd

	// Payload (7 bytes)
	0x2A, 0x00, // int16(42)
	0x01,       // bool(true)
	0x67, 0x6F, // "go"
	0xAA, 0xBB, // bytes
}
```