package roaringsearch

import (
	"bytes"
	"testing"
)

// FuzzIndexReadFrom tests deserialization of index data
func FuzzIndexReadFrom(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("FTSR\x02\x00\x03\x00\x00\x00\x00\x00")) // valid header, 0 ngrams

	f.Fuzz(func(t *testing.T, data []byte) {
		idx := NewIndex(3)
		_, _ = idx.ReadFrom(bytes.NewReader(data))
		// No panic = success
	})
}
