package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetAndGet(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("a", 1)
	om.Set("b", "two")

	v, ok := om.Get("a")
	require.True(t, ok)
	assert.Equal(t, 1, v)

	v, ok = om.Get("b")
	require.True(t, ok)
	assert.Equal(t, "two", v)

	_, ok = om.Get("c")
	assert.False(t, ok, "expected missing key")
}

func TestUpdateValue(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("x", 10)
	om.Set("x", 20)

	v, ok := om.Get("x")
	require.True(t, ok)
	assert.Equal(t, 20, v)
}

func TestDelete(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("a", 1)
	om.Set("b", 2)
	om.Delete("a")

	_, ok := om.Get("a")
	assert.False(t, ok, "expected 'a' deleted")

	v, ok := om.Get("b")
	require.True(t, ok)
	assert.Equal(t, 2, v)
}

func TestKeysValuesItems(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("a", 1)
	om.Set("b", 2)
	om.Set("c", 3)

	keys := om.Keys()
	assert.Equal(t, []string{"a", "b", "c"}, keys)

	values := om.Values()
	assert.Equal(t, []interface{}{1, 2, 3}, values)

	items := om.Items()
	expected := []PairAny{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "c", Value: 3},
	}
	assert.Equal(t, expected, items)

}

func TestMoveToEnd(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("a", 1)
	om.Set("b", 2)
	om.Set("c", 3)

	err := om.MoveToEnd("b", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "c", "b"}, om.Keys())

	err = om.MoveToEnd("c", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"c", "a", "b"}, om.Keys())

	err = om.MoveToEnd("x", true)
	assert.Error(t, err, "expected error for missing key")
}

func TestMarshalUnmarshalJSON(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("a", 1)
	om.Set("b", "two")

	data, err := json.Marshal(om)
	require.NoError(t, err)

	var om2 OrderedMapAny
	err = json.Unmarshal(data, &om2)
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b"}, om2.Keys())

	v, ok := om2.Get("b")
	require.True(t, ok)
	assert.Equal(t, "two", v)
}

func TestIterators(t *testing.T) {
	om := NewOrderedMapAny()
	om.Set("a", 1)
	om.Set("b", 2)

	expectedKeys := []string{"a", "b"}
	expectedVals := []int{1, 2}

	// KeysIter
	i := 0
	for k := range om.KeysIter() {
		assert.Equal(t, expectedKeys[i], k)
		i++
	}
	assert.Equal(t, len(expectedKeys), i)

	// ValuesIter
	i = 0
	for v := range om.ValuesIter() {
		assert.Equal(t, expectedVals[i], v)
		i++
	}
	assert.Equal(t, len(expectedVals), i)

	// ItemsIter
	i = 0
	for k, v := range om.ItemsIter() {
		assert.Equal(t, expectedKeys[i], k)
		assert.Equal(t, expectedVals[i], v)
		i++
	}
	assert.Equal(t, len(expectedKeys), i)
}

func TestMarshalJSON(t *testing.T) {
	om := NewOrderedMapAny(
		PairAny{"a", 1},
		PairAny{"b", "two"},
		PairAny{"c", true},
	)

	data, err := json.Marshal(om)
	require.NoError(t, err)

	// JSON should be in insertion order
	expected := `{"a":1,"b":"two","c":true}`
	assert.JSONEq(t, expected, string(data))
}

func TestUnmarshalJSON(t *testing.T) {
	jsonData := `{"x":42,"y":"hello","z":false}`

	var om OrderedMapAny
	err := json.Unmarshal([]byte(jsonData), &om)
	require.NoError(t, err)

	// Keys should preserve order
	assert.Equal(t, []string{"x", "y", "z"}, om.Keys())

	// Values should match
	v, ok := om.Get("x")
	require.True(t, ok)
	assert.Equal(t, float64(42), v) // JSON numbers decode as float64

	v, ok = om.Get("y")
	require.True(t, ok)
	assert.Equal(t, "hello", v)

	v, ok = om.Get("z")
	require.True(t, ok)
	assert.Equal(t, false, v)
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	om := NewOrderedMapAny(
		PairAny{"first", 123},
		PairAny{"second", "abc"},
	)

	data, err := json.Marshal(om)
	require.NoError(t, err)

	var om2 OrderedMapAny
	err = json.Unmarshal(data, &om2)
	require.NoError(t, err)

	// Keys preserved
	assert.Equal(t, []string{"first", "second"}, om2.Keys())

	// Values preserved
	v, ok := om2.Get("first")
	require.True(t, ok)
	assert.Equal(t, float64(123), v) // JSON numbers decode as float64

	v, ok = om2.Get("second")
	require.True(t, ok)
	assert.Equal(t, "abc", v)
}
