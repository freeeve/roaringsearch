package roaringsearch

import (
	"math/rand"
	"testing"
)

func BenchmarkNormalizerComparison(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Generate realistic ASCII documents
	docs := make([]string, 1000)
	for i := range docs {
		docs[i] = generateDocument(rng, 5, 20)
	}

	b.Run("Current", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			NormalizeLowercaseAlphanumeric(docs[i%len(docs)])
		}
	})

	b.Run("ASCIIFast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			normalizeFastASCII(docs[i%len(docs)])
		}
	})
}

// Test implementation
func normalizeFastASCII(s string) string {
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			buf = append(buf, c+32)
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			buf = append(buf, c)
		}
	}
	return string(buf)
}
