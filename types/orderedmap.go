package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// NOTE: Portions of this code were generated with AI assistance under human guidance.

// Go 1.23 iterator package

// Pair represents a key/value pair for initialization
type Pair[V any] struct {
	Key   string
	Value V
}

func OP[V any](k string, v V) Pair[V] {
	return Pair[V]{Key: k, Value: v}
}

// Alias for Pair[any]
type PairAny = Pair[any]

// OPAny is a helper to construct a Pair[any] inline.
func OPAny(k string, v any) PairAny {
	return PairAny{Key: k, Value: v}
}

// node now contains key directly
type node[V any] struct {
	key   string
	value V
	prev  *node[V]
	next  *node[V]
}

// OrderedMap is a generic ordered map keyed by string
type OrderedMap[V any] struct {
	data map[string]*node[V] // key → node
	head *node[V]
	tail *node[V]
}

// NewOrderedMap creates a new OrderedMap, optionally initialized with pairs.
func NewOrderedMap[V any](pairs ...Pair[V]) *OrderedMap[V] {
	om := &OrderedMap[V]{
		data: make(map[string]*node[V]),
	}
	for _, p := range pairs {
		om.Set(p.Key, p.Value)
	}
	return om
}

// Alias for OrderedMap with any values
type OrderedMapAny = OrderedMap[any]

// NewOrderedMapAny creates an OrderedMap[any] initialized with pairs.
func NewOrderedMapAny(pairs ...PairAny) *OrderedMapAny {
	om := &OrderedMapAny{
		data: make(map[string]*node[any]),
	}
	for _, p := range pairs {
		om.Set(p.Key, p.Value)
	}
	return om
}

// Length
func (om *OrderedMap[V]) Len() int {
	return len(om.data)
}

// Set inserts or updates a key
func (om *OrderedMap[V]) Set(key string, value V) {
	if n, ok := om.data[key]; ok {
		n.value = value
		return
	}
	n := &node[V]{key: key, value: value}
	om.data[key] = n
	if om.tail == nil {
		om.head, om.tail = n, n
	} else {
		n.prev = om.tail
		om.tail.next = n
		om.tail = n
	}
}

// Get retrieves a value
func (om *OrderedMap[V]) Get(key string) (V, bool) {
	n, ok := om.data[key]
	if !ok {
		var zero V
		return zero, false
	}
	return n.value, true
}

func GetAs[U any](om *OrderedMapAny, key string) U {
	v, ok := om.Get(key) // returns any
	if !ok {
		var zero U
		return zero
	}
	u, ok := v.(U)
	if !ok {
		var zero U
		return zero
	}
	return u
}

// Delete removes a key
func (om *OrderedMap[V]) Delete(key string) {
	n, ok := om.data[key]
	if !ok {
		return
	}
	delete(om.data, key)
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		om.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		om.tail = n.prev
	}
}

// Keys returns keys in insertion order
func (om *OrderedMap[V]) Keys() []string {
	keys := []string{}
	for n := om.head; n != nil; n = n.next {
		keys = append(keys, n.key)
	}
	return keys
}

// Values returns values in insertion order
func (om *OrderedMap[V]) Values() []V {
	values := []V{}
	for n := om.head; n != nil; n = n.next {
		values = append(values, n.value)
	}
	return values
}

// Items returns key/value pairs in insertion order
func (om *OrderedMap[V]) Items() []Pair[V] {
	items := []Pair[V]{}
	for n := om.head; n != nil; n = n.next {
		items = append(items, Pair[V]{Key: n.key, Value: n.value})
	}
	return items
}

// MoveToEnd moves a key to front or back
func (om *OrderedMap[V]) MoveToEnd(key string, last bool) error {
	n, ok := om.data[key]
	if !ok {
		return errors.New("key not found")
	}
	// detach
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		om.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		om.tail = n.prev
	}
	// attach
	if last {
		n.prev, n.next = om.tail, nil
		if om.tail != nil {
			om.tail.next = n
		}
		om.tail = n
		if om.head == nil {
			om.head = n
		}
	} else {
		n.prev, n.next = nil, om.head
		if om.head != nil {
			om.head.prev = n
		}
		om.head = n
		if om.tail == nil {
			om.tail = n
		}
	}
	return nil
}

func (om *OrderedMap[V]) Equal(other *OrderedMap[V]) bool {
	if om.Len() != other.Len() {
		return false
	}
	n1, n2 := om.head, other.head
	for n1 != nil && n2 != nil {
		if n1.key != n2.key {
			return false
		}
		if !reflect.DeepEqual(n1.value, n2.value) {
			return false
		}
		n1, n2 = n1.next, n2.next
	}
	return true
}

// MarshalJSON encodes as JSON object in insertion order
func (om *OrderedMap[V]) MarshalJSON() ([]byte, error) {
	buf := []byte{'{'}
	i := 0
	for n := om.head; n != nil; n = n.next {
		keyBytes, err := json.Marshal(n.key)
		if err != nil {
			return nil, err
		}
		valBytes, err := json.Marshal(n.value)
		if err != nil {
			return nil, err
		}
		buf = append(buf, keyBytes...)
		buf = append(buf, ':')
		buf = append(buf, valBytes...)
		if i < len(om.data)-1 {
			buf = append(buf, ',')
		}
		i++
	}
	buf = append(buf, '}')
	return buf, nil
}

// UnmarshalJSON decodes JSON object preserving order
func (om *OrderedMap[V]) UnmarshalJSON(data []byte) error {
	*om = *NewOrderedMap[V]()
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return fmt.Errorf("expected {")
	}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := t.(string)
		if !ok {
			return fmt.Errorf("expected string key")
		}
		var val V
		if err := dec.Decode(&val); err != nil {
			return err
		}
		om.Set(key, val)
	}
	t, err = dec.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '}' {
		return fmt.Errorf("expected }")
	}
	return nil
}

// KeysIter returns an iterator over keys
func (om *OrderedMap[V]) KeysIter() iter.Seq[string] {
	return func(yield func(string) bool) {
		for n := om.head; n != nil; n = n.next {
			if !yield(n.key) {
				return
			}
		}
	}
}

// ValuesIter returns an iterator over values
func (om *OrderedMap[V]) ValuesIter() iter.Seq[V] {
	return func(yield func(V) bool) {
		for n := om.head; n != nil; n = n.next {
			if !yield(n.value) {
				return
			}
		}
	}
}

// ItemsIter returns an iterator over key/value pairs
func (om *OrderedMap[V]) ItemsIter() iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		for n := om.head; n != nil; n = n.next {
			if !yield(n.key, n.value) {
				return
			}
		}
	}
}

// ConvertUnorderedToOrdered takes a plain map[string]any and a desired key order,
// and returns an OrderedMapAny with keys in that order.
func ConvertUnorderedToOrdered(m map[string]any, order []string) *OrderedMapAny {
	om := NewOrderedMapAny()
	for _, k := range order {
		if v, ok := m[k]; ok {
			om.Set(k, v)
		}
	}
	return om
}
