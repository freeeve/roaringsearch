//go:build slow

package roaringsearch

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func buildIndex(numDocs int, seed int64) *Index {
	idx := NewIndex(3)
	rng := rand.New(rand.NewSource(seed))

	batch := idx.BatchSize(numDocs)
	for i := 0; i < numDocs; i++ {
		batch.Add(uint32(i), generateDocument(rng, 5, 20))
	}
	batch.Flush()

	return idx
}

func BenchmarkScaledSearch(b *testing.B) {
	scales := []int{
		100_000,
		1_000_000,
		10_000_000,
		100_000_000,
	}

	queries := []string{"server", "client", "database"}

	for _, numDocs := range scales {
		name := fmt.Sprintf("%dK", numDocs/1000)
		if numDocs >= 1_000_000 {
			name = fmt.Sprintf("%dM", numDocs/1_000_000)
		}

		b.Run(name, func(b *testing.B) {
			b.Logf("Building %s document index...", name)
			start := time.Now()
			idx := buildIndex(numDocs, 42)
			b.Logf("Built in %v, %d n-grams", time.Since(start), idx.NgramCount())

			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			b.Logf("Memory: %.2f MB", float64(m.Alloc)/(1024*1024))

			b.Run("Search", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx.Search(queries[i%len(queries)])
				}
			})

			b.Run("SearchWithLimit100", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx.SearchWithLimit(queries[i%len(queries)], 100)
				}
			})

			b.Run("SearchCount", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx.SearchCount(queries[i%len(queries)])
				}
			})

			b.Run("SearchCallback100", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					idx.SearchCallback(queries[i%len(queries)], func(docID uint32) bool {
						count++
						return count < 100
					})
				}
			})
		})
	}
}

func TestScaledSearchResults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scaled test in short mode")
	}

	scales := []int{
		100_000,
		1_000_000,
		10_000_000,
		100_000_000,
	}

	queries := []string{"server", "client", "database"}

	for _, numDocs := range scales {
		name := fmt.Sprintf("%dK", numDocs/1000)
		if numDocs >= 1_000_000 {
			name = fmt.Sprintf("%dM", numDocs/1_000_000)
		}

		t.Run(name, func(t *testing.T) {
			t.Logf("Building %s document index...", name)
			start := time.Now()
			idx := buildIndex(numDocs, 42)
			buildTime := time.Since(start)
			t.Logf("Built in %v, %d n-grams", buildTime, idx.NgramCount())

			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			t.Logf("Memory: %.2f MB", float64(m.Alloc)/(1024*1024))

			// Benchmark each search type
			for _, query := range queries {
				// Search
				start = time.Now()
				results := idx.Search(query)
				searchTime := time.Since(start)

				// SearchWithLimit
				start = time.Now()
				_ = idx.SearchWithLimit(query, 100)
				limitTime := time.Since(start)

				// SearchCount
				start = time.Now()
				count := idx.SearchCount(query)
				countTime := time.Since(start)

				t.Logf("Query %q: %d results, Search=%v, Limit100=%v, Count=%v",
					query, len(results), searchTime, limitTime, countTime)

				if count != uint64(len(results)) {
					t.Errorf("count mismatch: %d vs %d", count, len(results))
				}
			}
		})
	}
}
