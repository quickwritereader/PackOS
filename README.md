# packos
packos is a binary packing protocol with offset-indexed framing, designed for fast composition, random access, and reliable for  RPC or BadgerDB workloads with **small blobs(<8kb)**(the size limitation maybe lifted to make it suitable for all purpose) 
It supports:
All packing paths emit canonical headers, preserve offset domains, and support recursive structures. PAOSP ensures schema validation, GC neutrality, and reproducible output across variants.

- [x] Canonical encoding
Emits offset-indexed binary frames with consistent headers and type tags.
- [x] Sequential decoding
Supports single-pass traversal with offset tracking and teardown-safe accessors.
- [x] Random access decoding
Enables direct lookup via offset domains without full unpacking.
- [x] Recursive structure support
Handles nested maps, slices, and tagged frames with offset provenance.
- [x] Simple schema validation
Validates type tags, offsets, and structure boundaries during decode.
- [x] GC neutrality
Avoids retained slices and ensures allocation discipline across packing paths.
- [x] Reproducible output
Guarantees stable binary layout across runs and variants.
- [ ] General-purpose container support
Supports arbitrarily large maps, slices, and tagged frames without size limits.
- [ ] Generate big and nested complex structures above 8kb limit


## Encoding Example


```go

put := packos.NewPutAccessFromPool()
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

```go
actual := Pack(
	PackInt16(12345),
	PackMapSorted{
		"meta": PackMapSorted{
			"user": PackByteArray([]byte("alice")),
			"role": PackByteArray([]byte("admin")),
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

## Scheme examples
```go
actual := pack.Pack(
	pack.PackInt16(12345),
	pack.PackFloat32(3.14),
	pack.PackInt64(9876543210),
	pack.PackBool(true),
	pack.PackMapSorted{
		"meta": pack.PackMapSorted{
			"user": pack.PackByteArray([]byte("alice")),
			"role": pack.PackByteArray([]byte("admin")),
		},
		"name": pack.PackString("gopher"),
	},
)

chain := SChain(
	SInt16,
	SFloat32,
	SInt64,
	SBool,
	//will match in written order
	SMap(
		SStringExact("meta"), // key
		SMap(                     // → value
			SStringExact("role"), // key
			SBytes(len("admin")), // → value
			SStringExact("user"),
			SBytes(len("alice")),
		),
		SStringExact("name"),   // key
		SStringLen(len("gopher")), // → value
	),
)

err := ValidateBuffer(actual, chain)

```
```go
actual := pack.Pack(
	pack.PackInt16(12345),
	pack.PackFloat32(3.14),
	pack.PackInt64(9876543210),
	pack.PackBool(true),
	pack.PackMapSorted{
		"meta": pack.PackMapSorted{
			"user": pack.PackByteArray([]byte("alice")), // valid
			"role": pack.PackString("admin"),            // valid
			"age":  pack.PackInt32(30),                  // valid
		},
		"name": pack.PackString("gopher"), // valid
	},
)

chain := SChain(
	SInt16,
	SFloat32,
	SInt64,
	SBool,
	SMap(
		SString.Match("meta"),
		SMapUnordered(map[string]Scheme{
			"age":  SInt32.Range(18, 99),               // declared first
			"user": SBytes(len("alice")),               // declared second
			"role": SString.Pattern(`^(admin|guest)$`), // declared third
		}),
		SString.Match("name"),
		SString.WithWidth(len("gopher")),
	),
)

err := ValidateBuffer(actual, chain)
```

```go

import . "github.com/BranchAndLink/packos/packable"

/////////////////////////////////////////////////////


actual := Pack(
	PackString("2025-09-10"),        // date
	PackInt32(42),                   // range
	PackString("alice@example.com"), // email
	PackString("prefix-hello"),      // prefix
	PackString("world-suffix"),      // suffix
)

chain := SChain(
	SString.Pattern(`^\d{4}-\d{2}-\d{2}$`),                              // date pattern YYYY-MM-DD
	SInt32.Range(1, 100),                                                // int range
	SString.Pattern(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`), // email
	SString.Prefix("prefix-"),                                           // prefix match
	SString.Suffix("-suffix"),                                           // suffix match
)

err := ValidateBuffer(actual, chain)
```