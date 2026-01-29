# PackOS
PackOS is a binary packing protocol with offset-indexed framing, designed for fast composition, random access, and reliable for  RPC or BadgerDB workloads with **small blobs(<8kb)**(the size limitation maybe lifted to make it suitable for all purpose) 
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

put := PackOS.NewPutAccessFromPool()
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

```go
actual := Pack(
PackTuple(
	PackInt32(2025),
	PackBool(false),
	PackString("az"),
),
PackTuple(
	PackInt16(7),
	PackBool(true),
	PackString("go"),
),
)
expected := []byte{
// Outer headers (3 × 2 bytes)
0x34, 0x00, // header[0]: absolute offset = 6,  type=4 → Tuple   @ offset 0 → payload @ offset 6
0x7C, 0x00, // header[1]: delta = 15,           type=4 → Tuple   @ offset 2 → payload @ offset 21 (6 + 15)
0xE0, 0x00, // header[2]: delta = 28,           type=0 → End     @ offset 4 → marks end @ offset 34 (6 + 28)
// Tuple 1 headers (4 × 2 bytes)                                 @ offset 6
0x41, 0x00, // header[0]: absolute offset = 8,  type=1 → Int32   @ offset 6  → inner_offset 8
0x25, 0x00, // header[1]: delta = 4,            type=5 → Bool    @ offset 8  → inner_offset 12 (8 + 4)
0x2E, 0x00, // header[2]: delta = 5,            type=6 → String  @ offset 10 → inner_offset 13 (8 + 5)
0x38, 0x00, // header[3]: delta = 7,            type=0 → End     @ offset 12 → inner_offset 15 (8 + 7)
// Tuple 1 payload (7 bytes)
0xE9, 0x07, 0x00, 0x00, // int32(2025)                           @ offset 14 → inner_offset 8
0x00,       // bool(false)                                       @ offset 18 → inner_offset 12
0x61, 0x7A, // "az"                                              @ offset 19 → inner_offset 13
// Tuple 2 headers (4 × 2 bytes)                                 @ offset 21
0x41, 0x00, // header[0]: absolute offset = 8,  type=1 → Int16   @ offset 21 → inner_offset 8
0x15, 0x00, // header[1]: delta = 2,            type=5 → Bool    @ offset 23 → inner_offset 10 (8 + 2)
0x1E, 0x00, // header[2]: delta = 3,            type=6 → String  @ offset 25 → inner_offset 11 (8 + 3)
0x28, 0x00, // header[3]: delta = 5,            type=0 → End     @ offset 27 → inner_offset 13 (8 + 5)
// Tuple 2 payload (5 bytes)
0x07, 0x00, // int16(7)                                          @ offset 29 → inner_offset 8
0x01,       // bool(true)                                        @ offset 31 → inner_offset 10
0x67, 0x6F, // "go"                                              @ offset 32 → inner_offset 11
//                                              final byte       @ offset 34 → inner_offset 13
}
```  
## Decode examples

```go
// Generic decode
byteResult := pack.Pack(pack.PackTuple(
	pack.PackInt32(42),
	pack.PackString("alpha"),
	// repeated booleans
	pack.PackBool(true),
	pack.PackBool(false),
	pack.PackBool(true),
))
   
generic, err := access.Decode(byteResult)
expected := []any{
	int32(42),
	"alpha", true, false, true, 
}

fmt.Println("Generic Decoded:", generic) 
```

## Schema, builder and schema guided decode examples.
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
	SMapUnordered(map[string]Schema{
		"age":  SInt32.RangeValues(18, 99),               // declared first
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

import . "github.com/quickwritereader/PackOS/packable"

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
SInt32.RangeValues(1, 100),                                                // int range
SString.Pattern(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`), // email
SString.Prefix("prefix-"),                                           // prefix match
SString.Suffix("-suffix"),                                           // suffix match
)

err := ValidateBuffer(actual, chain)
```  


```go  
// Build packed tuple: (42, "alpha", true, false, true)
byteResult := pack.Pack(pack.PackTuple(
pack.PackInt32(42),
pack.PackString("alpha"),
pack.PackBool(true),
pack.PackBool(false),
pack.PackBool(true),
))

// Strict schema: repeated field stays grouped
chainStrict := SChain(
STupleNamedVal(
	[]string{"id", "name", "flags"},
	SInt32,
	SString,
	SRepeat(1, -1, SBool),
),
)

// Flattened schema: repeated field expands inline
chainFlat := SChain(
STupleNamedValFlattened(
	[]string{"id", "name", "flag"},
	SInt32,
	SString,
	SRepeat(1, -1, SBool),
),
)

// Decode with strict schema
decodedStrict, err := DecodeBuffer(byteResult, chainStrict)
assert.NoError(t, err, "Strict named tuple decode should succeed")

expectedStrict := map[string]any{
"id":    int32(42),
"name":  "alpha",
"flags": []any{true, false, true},
}
fmt.Println("DecodedStrict:", decodedStrict) 
// Decode with flattened schema
decodedFlat, err := DecodeBuffer(byteResult, chainFlat) 

expectedFlat := map[string]any{
"id":     int32(42),
"name":   "alpha",
"flag_0": true,
"flag_1": false,
"flag_2": true,
}
fmt.Println("DecodedFlat:", decodedFlat)
```

```go
// Encode with schema example
expected := pack.Pack(
	pack.PackTuple(
		pack.PackInt32(2025),
		pack.PackBool(false),
		pack.PackString("az"),
	),
	pack.PackTuple(
		pack.PackInt16(7),
		pack.PackBool(true),
		pack.PackString("go"),
	),
)

// Schema with named tuples
chain := SchemaNamedChain{
	SchemaChain: SChain(
		STupleNamed(
			[]string{"year", "flag", "code"},
			SInt32,
			SBool,
			SStringLen(len("az")),
		),
		STupleNamed(
			[]string{"num", "flag", "lang"},
			SInt16,
			SBool,
			SStringLen(len("go")),
		),
	),
	FieldNames: []string{"firstTuple", "secondTuple"},
}

// Value to encode: map with named fields
val := map[string]any{
	"firstTuple": map[string]any{
		"year": int32(2025),
		"flag": false,
		"code": "az",
	},
	"secondTuple": map[string]any{
		"num":  int16(7),
		"flag": true,
		"lang": "go",
	},
}

actual, err := EncodeValueNamed(val, chain) 
fmt.Println("Encoded Named:", actual)
```

```go
// Define schema in JSON form
schemaJSON := SchemaJSON{
Type: "repeat",
Min:  PtrToInt64(1),
Max:  nil,
Schema: []SchemaJSON{
	{
		Type: "tuple",
		Schema: []SchemaJSON{
			{Type: "int32"},
			{Type: "bool"},
			{Type: "string"},
		},
	},
	{
		Type: "tuple",
		Schema: []SchemaJSON{
			{Type: "int16"},
			{Type: "bool"},
			{Type: "string"},
		},
	},
},
}

// Build schema from JSON
built := BuildSchema(&schemaJSON)

// Manually constructed schema
expected := SRepeat(1, -1,
STuple(
	SInt32,
	SBool,
	SString,
),
STuple(
	SInt16,
	SBool,
	SString,
),
)

```

