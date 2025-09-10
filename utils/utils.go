package utils

import (
	"sort"
)

func SortKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func HasPrefix(b []byte, prefix string) bool {
	return len(b) >= len(prefix) && string(b[:len(prefix)]) == prefix
}

func HasSuffix(b []byte, suffix string) bool {
	return len(b) >= len(suffix) && string(b[len(b)-len(suffix):]) == suffix
}
