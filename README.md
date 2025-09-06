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
put := paosp.NewPutAccessFromPool()
put.AddInt16(42)                 // 2 bytes
put.AddBool(true)                // 1 byte
put.AddString("go")              // 2-byte length + 2 bytes
put.AddBytes([]byte{0xAA, 0xBB}) // 2-byte length + 2 bytes

actual := put.Pack()
ReleasePutAccess(put)

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

```
    // packing Map sorted way
	actual := PackArgs(
		PackInt16(12345),
		PackSortedMapPackable{
			"meta": PackSortedMapPackable{
				"user": PackBytes([]byte("alice")),
				"role": PackBytes([]byte("admin")),
			},
			"name": PackString("gopher"),
		},
	)

	expected := []byte{
		// Outer Header Block
		0x31, 0x00, // offset = 6, TypeInt16
		0x17, 0x00, // delta = 2, TypeMap
		0xB0, 0x01, // delta = 54, TypeEnd

		// Outer Payload
		0x39, 0x30, // int16(12345)

		// inner1 Header Block
		0x56, 0x00, // "meta"
		0x27, 0x00, // map
		0x06, 0x01, // "name"
		0x26, 0x01, // "gopher"
		0x50, 0x01, // TypeEnd

		// inner1 Payload
		'm', 'e', 't', 'a',

		// inner1.1 Header Block
		0x56, 0x00, // "role"
		0x26, 0x00, // "admin"
		0x4E, 0x00, // "user"
		0x6E, 0x00, // "alice"
		0x90, 0x00, // TypeEnd

		// inner1.1 Payload
		'r', 'o', 'l', 'e',
		'a', 'd', 'm', 'i', 'n',
		'u', 's', 'e', 'r',
		'a', 'l', 'i', 'c', 'e',

		// Remaining inner1 Payload
		'n', 'a', 'm', 'e',
		'g', 'o', 'p', 'h', 'e', 'r',
	}

```