package usage

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/quickwritereader/PackOS/access"
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
