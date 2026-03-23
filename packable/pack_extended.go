package packable

import (
	"github.com/quickwritereader/PackOS/access"
)

// PackExtended packs data with automatic segment creation
func PackExtended(pivot int, args ...access.Packable) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)

	for _, arg := range args {
		put.AddPackable(arg)
	}

	return put.PackExtended()
}

// PackExtendedWithMapStr packs map[string]string with extended container support
func PackExtendedWithMapStr(pivot int, m map[string]string) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)

	// Add as map of strings directly
	put.AddMapStr(m)

	return put.PackExtended()
}

// PackExtendedWithBytesMap packs map[string][]byte with extended container support
func PackExtendedWithBytesMap(pivot int, m map[string][]byte) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)

	// Add as map of bytes directly
	put.AddMap(m)

	return put.PackExtended()
}

// PackExtendedWithAnyMap packs map[string]any with extended container support
func PackExtendedWithAnyMap(pivot int, m map[string]any, useNumeric bool) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)

	if err := put.AddMapAny(m, useNumeric); err != nil {
		return nil, err
	}

	return put.PackExtended()
}

// PackExtendedWithPackableMap packs map[string]access.Packable with extended container support
func PackExtendedWithPackableMap(pivot int, m map[string]access.Packable) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)

	// Add as map of Packable values using AddMapAny
	// Convert map[string]access.Packable to map[string]any
	anyMap := make(map[string]any, len(m))
	for k, v := range m {
		anyMap[k] = v
	}

	if err := put.AddMapAny(anyMap, true); err != nil {
		return nil, err
	}

	return put.PackExtended()
}

// PackInt64Array packs an int64 array with automatic segmentation
func PackInt64Array(pivot int, values []int64) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)
	put.AddIntegerArray(values)
	return put.PackExtended()
}

// PackFloat64Array packs a float64 array with automatic segmentation
func PackFloat64Array(pivot int, values []float64) ([]byte, error) {
	put := access.NewExtendedPutAccess(pivot)
	put.AddFloatArray(values)
	return put.PackExtended()
}
