package roaringsearch

import (
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func BenchmarkDedupMethods(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Generate test documents with varying sizes
	docs := make([][]uint64, 1000)
	for i := range docs {
		// Simulate n-gram keys for a document (5-50 n-grams)
		numKeys := 5 + rng.Intn(46)
		docs[i] = make([]uint64, numKeys)
		for j := range docs[i] {
			docs[i][j] = rng.Uint64()
		}
	}

	b.Run("Slice", func(b *testing.B) {
		seen := make([]uint64, 0, 64)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			seen = seen[:0]
			for _, key := range docs[i%len(docs)] {
				found := false
				for _, k := range seen {
					if k == key {
						found = true
						break
					}
				}
				if !found {
					seen = append(seen, key)
				}
			}
		}
	})

	b.Run("Map", func(b *testing.B) {
		seen := make(map[uint64]struct{}, 64)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for k := range seen {
				delete(seen, k)
			}
			for _, key := range docs[i%len(docs)] {
				if _, ok := seen[key]; !ok {
					seen[key] = struct{}{}
				}
			}
		}
	})

	b.Run("Roaring64", func(b *testing.B) {
		seen := roaring64.New()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			seen.Clear()
			for _, key := range docs[i%len(docs)] {
				if !seen.Contains(key) {
					seen.Add(key)
				}
			}
		}
	})

	b.Run("Roaring64_CheckedAdd", func(b *testing.B) {
		seen := roaring64.New()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			seen.Clear()
			for _, key := range docs[i%len(docs)] {
				seen.CheckedAdd(key) // Returns true if newly added
			}
		}
	})
}

// Test with realistic document sizes
func BenchmarkDedupRealistic(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Generate realistic documents
	docs := make([]string, 1000)
	for i := range docs {
		docs[i] = generateDocument(rng, 5, 20)
	}

	// Pre-compute runes
	runesDocs := make([][]rune, len(docs))
	for i, doc := range docs {
		runesDocs[i] = []rune(NormalizeLowercaseAlphanumeric(doc))
	}

	gramSize := 3

	b.Run("Slice", func(b *testing.B) {
		seen := make([]uint64, 0, 64)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runes := runesDocs[i%len(runesDocs)]
			seen = seen[:0]
			for j := 0; j <= len(runes)-gramSize; j++ {
				key := runeNgramKey(runes[j : j+gramSize])
				found := false
				for _, k := range seen {
					if k == key {
						found = true
						break
					}
				}
				if !found {
					seen = append(seen, key)
				}
			}
		}
	})

	b.Run("Roaring64", func(b *testing.B) {
		seen := roaring64.New()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runes := runesDocs[i%len(runesDocs)]
			seen.Clear()
			for j := 0; j <= len(runes)-gramSize; j++ {
				key := runeNgramKey(runes[j : j+gramSize])
				seen.CheckedAdd(key)
			}
		}
	})
}
