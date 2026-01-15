package usage

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/quickwritereader/PackOS/access"
	"github.com/quickwritereader/PackOS/scheme"
	. "github.com/quickwritereader/PackOS/scheme"
	"github.com/quickwritereader/PackOS/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testJson = `{"meta":{"version":"1.0.0","author":"Copilot","timestamp":"2025-12-15T11:21:00Z","description":"Large JSON for testing decode and pack length comparison"},"users":[{"id":1,"name":"Alice","roles":["admin","editor","viewer"],"settings":{"theme":"dark","notifications":true,"languages":["en","fr","de","es"]},"activity":[{"date":"2025-01-01","action":"login","ip":"192.168.0.1"},{"date":"2025-01-02","action":"upload","file":"report.pdf"},{"date":"2025-01-03","action":"logout"}]},{"id":2,"name":"Bob","roles":["viewer"],"settings":{"theme":"light","notifications":false,"languages":["en","ru"]},"activity":[{"date":"2025-02-10","action":"login","ip":"10.0.0.2"},{"date":"2025-02-11","action":"download","file":"data.csv"}]}],"projects":[{"projectId":"P100","title":"AI Research","status":"active","members":[1,2],"tasks":[{"taskId":"T1","title":"Data Collection","completed":false},{"taskId":"T2","title":"Model Training","completed":true},{"taskId":"T3","title":"Evaluation","completed":false}]},{"projectId":"P200","title":"Web Development","status":"archived","members":[2],"tasks":[{"taskId":"T10","title":"Frontend Design","completed":true},{"taskId":"T11","title":"Backend API","completed":true},{"taskId":"T12","title":"Deployment","completed":true}]}],"logs":{"system":[{"level":"info","message":"System started","time":"2025-01-01T00:00:00Z"},{"level":"warn","message":"High memory usage","time":"2025-01-05T12:00:00Z"},{"level":"error","message":"Disk failure","time":"2025-01-10T18:30:00Z"}],"application":[{"level":"debug","message":"User clicked button","time":"2025-02-01T09:15:00Z"},{"level":"info","message":"File uploaded","time":"2025-02-02T10:00:00Z"}]},"data":{"matrix":[[1,2,3,4,5],[6,7,8,9,10],[11,12,13,14,15],[16,17,18,19,20]],"nested":{"alpha":{"beta":{"gamma":{"delta":"deep value","epsilon":[true,false,null,"string",12345]}}}},"largeArray":[{"index":0,"value":"A"},{"index":1,"value":"B"},{"index":2,"value":"C"},{"index":3,"value":"D"},{"index":4,"value":"E"},{"index":5,"value":"F"},{"index":6,"value":"G"},{"index":7,"value":"H"},{"index":8,"value":"I"},{"index":9,"value":"J"},{"index":10,"value":"K"},{"index":11,"value":"L"},{"index":12,"value":"M"},{"index":13,"value":"N"},{"index":14,"value":"O"},{"index":15,"value":"P"},{"index":16,"value":"Q"},{"index":17,"value":"R"},{"index":18,"value":"S"},{"index":19,"value":"T"},{"index":20,"value":"U"},{"index":21,"value":"V"},{"index":22,"value":"W"},{"index":23,"value":"X"},{"index":24,"value":"Y"},{"index":25,"value":"Z"}]}}`

// DecodeToGenericMap unmarshals a JSON blob into map[string]interface{}.

// Returns a fully generic structure (maps, slices, primitives).
func DecodeToGenericMap(data []byte) (map[string]interface{}, error) {
	var root interface{}
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	// Ensure the root is an object
	obj, ok := root.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected JSON object at root, got %T", root)
	}

	return obj, nil
}

// Safe wrapper: initialize JsonObject once, handle error internally
var JsonObject = func() map[string]interface{} {
	obj, err := DecodeToGenericMap([]byte(testJson))
	if err != nil {
		fmt.Println("failed to decode testJson:", err)
		return map[string]interface{}{}
	}
	return obj
}()

func TestUsage1(t *testing.T) {
	fmt.Fprintln(os.Stdout,
		"Checking whether Packable can compact a map containing []interface{} values, "+
			"even though it was originally designed for strongly typed data.")

	// Original JSON object (already unmarshalled into JsonObject)
	fmt.Println(JsonObject)

	// Encode with Packable
	put := access.NewPutAccess()
	put.AddMapAny(JsonObject, true)
	res := put.Pack()

	fmt.Fprintln(os.Stdout, "Minified Json size:", len(testJson),
		"\nPackable byte size:", len(res))

	// --- Decode back using DecodeTuple ---
	decoded, err := access.Decode(res)
	if err != nil {
		t.Fatalf("failed to decode Packable tuple: %v", err)
	}

	fmt.Fprintln(os.Stdout, "Decoded back tuple:", decoded)

}

func TestUsage1_WithSChain(t *testing.T) {
	fmt.Fprintln(os.Stdout,
		"Checking whether SChain can encode/decode a large JSON object with nested arrays and maps.")

	// Unmarshal the JSON constant into a generic map
	var decodedJSON map[string]any
	err := json.Unmarshal([]byte(testJson), &decodedJSON)
	assert.NoError(t, err, "Unmarshal of testJson should succeed")

	chain := SchemeNamedChain{
		SchemeChain: SChain(
			SType(types.TypeMap),   // meta
			SType(types.TypeTuple), // users
			SType(types.TypeTuple), // projects
			SType(types.TypeMap),   // logs
			SType(types.TypeMap),   // data
		),
		FieldNames: []string{"meta", "users", "projects", "logs", "data"},
	}

	// Encode using the scheme
	encoded, err := EncodeValueNamed(decodedJSON, chain)
	assert.NoError(t, err, "Encoding with SChain should succeed")

	// Decode back using the same scheme
	decodedBack, err := DecodeBufferNamed(encoded, chain)
	assert.NoError(t, err, "Decoding with SChain should succeed")

	// --- Log size comparison ---
	t.Logf("[PACKING LOG] Original JSON length: %d bytes, Packed buffer length: %d bytes", len(testJson), len(encoded))
	// --- Stringwise comparison ---
	// Marshal both to canonical JSON strings
	origJSONBytes, err := json.Marshal(decodedJSON)
	assert.NoError(t, err, "Marshal of original JSON should succeed")

	backJSONBytes, err := json.Marshal(decodedBack)
	assert.NoError(t, err, "Marshal of decoded back JSON should succeed")

	// Compare as strings
	origStr := string(origJSONBytes)
	backStr := string(backJSONBytes)

	fmt.Fprintln(os.Stdout, "Original JSON string:", origStr)
	fmt.Fprintln(os.Stdout, "Decoded back JSON string:", backStr)

	assert.Equal(t, origStr, backStr,
		"Re‑encoded decodedBack should match the original JSON stringwise")
}

func TestUsage1_WithSChainMax(t *testing.T) {
	fmt.Fprintln(os.Stdout,
		"Checking whether SChain can encode/decode a large JSON object with nested arrays and maps.")

	// Unmarshal the JSON constant into a generic map
	var decodedJSON map[string]any
	err := json.Unmarshal([]byte(testJson), &decodedJSON)
	assert.NoError(t, err, "Unmarshal of testJson should succeed")

	chain := SchemeNamedChain{
		SchemeChain: SChain(
			STupleNamed([]string{"version", "author", "timestamp", "description"}, SString, SString, SString, SString), // meta
			// Users section: array of tuples
			STupleValFlatten(SRepeat(1, -1, STupleNamedVal(
				[]string{"id", "name", "roles", "settings", "activity"},
				SInt32,                 // id
				SString,                // name
				SType(types.TypeTuple), // roles
				STupleNamed( // settings is solid
					[]string{"theme", "notifications", "languages"},
					SString, SBool, SType(types.TypeTuple),
				),
				SType(types.TypeTuple), // activity is not solid, so keep as map
			))),
			STupleValFlatten(SRepeat(1, -1, STupleNamedVal(
				[]string{"projectId", "title", "status", "members", "tasks"},
				SString,                // projectId
				SString,                // title
				SString,                // status
				SType(types.TypeTuple), // members (array of ints)
				STupleValFlatten(SRepeat(1, -1, STupleNamed( // tasks
					[]string{"taskId", "title", "completed"},
					SString, // taskId
					SString, // title
					SBool,   // completed
				))),
			))),
			STupleNamedVal(
				[]string{"system", "application"},
				STupleValFlatten(SRepeat(1, -1, STupleNamedVal(
					[]string{"level", "message", "time"},
					SString, // level
					SString, // message
					SString, // time
				))),
				STupleValFlatten(SRepeat(1, -1, STupleNamedVal(
					[]string{"level", "message", "time"},
					SString, // level
					SString, // message
					SString, // time
				))),
			),

			STupleNamedVal(
				[]string{"matrix", "nested", "largeArray"},
				// matrix: 2D array of ints
				STupleValFlatten(SRepeat(1, -1, SType(types.TypeTuple))),
				//nested
				STupleNamedVal(
					[]string{"alpha"},
					STupleNamedVal(
						[]string{"beta"},
						STupleNamedVal(
							[]string{"gamma"},
							STupleNamedVal(
								[]string{"delta", "epsilon"},
								SString,                // delta
								SType(types.TypeTuple), // epsilon (heterogeneous array)
							),
						),
					),
				),

				// largeArray: solid array of {index, value}
				STupleValFlatten(SRepeat(1, -1, STupleNamedVal(
					[]string{"index", "value"},
					SInt8,   // index
					SString, // value
				))),
			),
		),
		FieldNames: []string{"meta", "users", "projects", "logs", "data"},
	}

	// Encode using the scheme
	encoded, err := EncodeValueNamed(decodedJSON, chain)
	assert.NoError(t, err, "Encoding with SChain should succeed")

	// Decode back using the same scheme
	decodedBack, err := DecodeBufferNamed(encoded, chain)
	assert.NoError(t, err, "Decoding with SChain should succeed")

	// --- Log size comparison ---  2509 -> 1411 43.7% reduction
	t.Logf("[PACKING LOG] Original JSON length: %d bytes, Packed buffer length: %d bytes", len(testJson), len(encoded))
	// --- Stringwise comparison ---
	// Marshal both to canonical JSON strings
	origJSONBytes, err := json.Marshal(decodedJSON)
	assert.NoError(t, err, "Marshal of original JSON should succeed")

	backJSONBytes, err := json.Marshal(decodedBack)
	assert.NoError(t, err, "Marshal of decoded back JSON should succeed")

	// Compare as strings
	origStr := string(origJSONBytes)
	backStr := string(backJSONBytes)

	assert.Equal(t, origStr, backStr,
		"Re‑encoded decodedBack should match the original JSON stringwise")
}

func TestDefaultHugoConfigRoundTrip(t *testing.T) {

	SchemeJsonStr := `
{"type":"tuple","variableLength":true,"fieldNames":["baseURL","languageCode","title","theme",
"paginate","permalinks","outputs","menus"],"schema":[{"type":"string","pattern":"^(https?://.*|/)$"},
{"type":"string"},{"type":"string"},{"type":"string"},{"type":"int32","min":1},{"type":"tuple",
"fieldNames":["blog"],"schema":[{"type":"string","pattern":"^/blog/"}]},{"type":"tuple",
"fieldNames":["home"],"schema":[{"type":"tuple","variableLength":true,"flatten":true,"schema":[
{"type":"repeat","min":0,"max":8,"schema":[{"type":"string","pattern":"^(HTML|RSS|JSON|AMP)$"}]}]}]},
{"type":"tuple","fieldNames":["main"],"flatten":false,"variableLength":true,"schema":[{"type":"repeat",
"min":1,"max":1024,"schema":[{"type":"tuple","fieldNames":["identifier","name","url","weight"],
"schema":[{"type":"string"},{"type":"string"},{"type":"string"},{"type":"int32","min":1}]}]}]}]}
	`
	// A minimal but valid Hugo config JSON
	configJSON := `{
		"baseURL": "/",
		"languageCode": "en-us",
		"title": "My Hugo Site",
		"theme": "ananke",
		"paginate": 10,
		"permalinks": {
			"blog": "/blog/:slug/"
		},
		"outputs": {
			"home": ["HTML","RSS","JSON"]
		},
		"menus": {
			"main": [
				{"identifier":"home","name":"Home","url":"/","weight":1},
				{"identifier":"blog","name":"Blog","url":"/blog/","weight":2}
			]
		}
	}`

	var SchemeJson SchemeJSON
	require.NoError(t, json.Unmarshal([]byte(SchemeJsonStr), &SchemeJson), "failed to unmarshal config")

	schain := scheme.SChain(scheme.BuildScheme(SchemeJson))

	// Decode into generic map
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(configJSON), &decoded), "failed to unmarshal config")

	// Encode using your scheme
	encoded, err := scheme.EncodeValue(decoded, schain)
	require.NoError(t, err, "scheme encode failed")

	// Decode back using your scheme
	roundTrip, err := scheme.DecodeBuffer(encoded, schain)
	require.NoError(t, err, "scheme decode failed")

	// Marshal both original and round-trip to canonical JSON
	origJSON, err := json.Marshal(decoded)
	require.NoError(t, err, "failed to marshal original")

	roundTripJSON, err := json.Marshal(roundTrip)
	require.NoError(t, err, "failed to marshal roundTrip")

	// Compare the JSON strings
	assert.JSONEq(t, string(origJSON), string(roundTripJSON), "round-trip mismatch")

}

func TestDefaultHugoConfigRoundTripMultiCheck(t *testing.T) {

	SchemeJsonStr := `
	{"type":"tuple","variableLength":true,"fieldNames":["baseURL","languageCode","title","theme",
	"paginate","permalinks","outputs","menus"],"schema":[{"type":"string","pattern":"^(https?://.*|/)$"},
	{"type":"string"},{"type":"string"},{"type":"string"},{"type":"int32","min":1},{"type":"tuple",
	"fieldNames":["blog"],"schema":[{"type":"string","pattern":"^/blog/"}]},
	{
	"type": "tuple",
	"fieldNames": ["home", "section", "page", "taxonomy", "term"],
	"schema": [
		{ "type": "multicheck", "fieldNames": ["HTML", "RSS", "JSON", "AMP"] },
		{ "type": "multicheck", "fieldNames": ["HTML", "RSS", "JSON", "AMP"] },
		{ "type": "multicheck", "fieldNames": ["HTML", "RSS", "JSON", "AMP"] },
		{ "type": "multicheck", "fieldNames": ["HTML", "RSS", "JSON", "AMP"] },
		{ "type": "multicheck", "fieldNames": ["HTML", "RSS", "JSON", "AMP"] }
	]
	},
	{"type":"tuple","fieldNames":["main"],"flatten":false,"variableLength":true,"schema":[{"type":"repeat",
	"min":1,"max":1024,"schema":[{"type":"tuple","fieldNames":["identifier","name","url","weight"],
	"schema":[{"type":"string"},{"type":"string"},{"type":"string"},{"type":"int32","min":1}]}]}]}]}
	`
	// A minimal but valid Hugo config JSON
	configJSON := `{
		"baseURL": "/",
		"languageCode": "en-us",
		"title": "My Hugo Site",
		"theme": "ananke",
		"paginate": 10,
		"permalinks": {
			"blog": "/blog/:slug/"
		},
		"outputs": {
			"home": ["HTML", "RSS", "JSON", "AMP"],
			"section": ["HTML", "RSS", "JSON"],
			"page": ["HTML",  "JSON","AMP"],
			"taxonomy": ["HTML", "RSS"],
			"term": ["HTML", "RSS", "JSON"]
		}, 
		"menus": {
			"main": [
				{"identifier":"home","name":"Home","url":"/","weight":1},
				{"identifier":"blog","name":"Blog","url":"/blog/","weight":2}
			]
		}
	}`

	var SchemeJson SchemeJSON
	require.NoError(t, json.Unmarshal([]byte(SchemeJsonStr), &SchemeJson), "failed to unmarshal config")

	schain := scheme.SChain(scheme.BuildScheme(SchemeJson))

	// Decode into generic map
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(configJSON), &decoded), "failed to unmarshal config")

	// Encode using your scheme
	encoded, err := scheme.EncodeValue(decoded, schain)
	require.NoError(t, err, "scheme encode failed")

	// Decode back using your scheme
	roundTrip, err := scheme.DecodeBuffer(encoded, schain)
	require.NoError(t, err, "scheme decode failed")

	// Marshal both original and round-trip to canonical JSON
	origJSON, err := json.Marshal(decoded)
	require.NoError(t, err, "failed to marshal original")

	roundTripJSON, err := json.Marshal(roundTrip)
	require.NoError(t, err, "failed to marshal roundTrip")

	// Compare the JSON strings
	assert.JSONEq(t, string(origJSON), string(roundTripJSON), "round-trip mismatch")

}
