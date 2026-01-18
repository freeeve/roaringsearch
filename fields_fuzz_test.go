package roaringsearch

import (
	"bytes"
	"testing"
)

// FuzzBitmapFilterRead tests deserialization of bitmap filter data
func FuzzBitmapFilterRead(f *testing.F) {
	// Add seed corpus - minimal valid data
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}) // 0 fields

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadBitmapFilter(bytes.NewReader(data))
		// No panic = success
	})
}

// FuzzSortColumnRead tests deserialization of sort column data
func FuzzSortColumnRead(f *testing.F) {
	// Add seed corpus - minimal valid data
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}) // 0 entries

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadSortColumn[int64](bytes.NewReader(data))
		// No panic = success
	})
}
