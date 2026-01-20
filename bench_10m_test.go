//go:build slow

package roaringsearch

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const (
	numDocs10M = 10_000_000
)

var cached10MIndex *Index
var cached10MIndexPath string

func getOrCreate10MIndex(b *testing.B) *Index {
	if cached10MIndex != nil {
		return cached10MIndex
	}

	b.Helper()
	b.Logf("Building 10M document index...")

	start := time.Now()
	idx := NewIndex(3)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < numDocs10M; i++ {
		doc := generateDocument(rng, 5, 20)
		idx.Add(uint32(i), doc)

		if i > 0 && i%1_000_000 == 0 {
			b.Logf("  Indexed %dM documents...", i/1_000_000)
		}
	}

	b.Logf("Index built in %v, %d unique n-grams", time.Since(start), idx.NgramCount())
	cached10MIndex = idx
	return idx
}

func getOrCreate10MIndexFile(b *testing.B) string {
	if cached10MIndexPath != "" {
		if _, err := os.Stat(cached10MIndexPath); err == nil {
			return cached10MIndexPath
		}
	}

	b.Helper()
	idx := getOrCreate10MIndex(b)

	tmpDir := os.TempDir()
	path := filepath.Join(tmpDir, "bench_10m.sear")

	b.Logf("Saving index to %s...", path)
	start := time.Now()
	if err := idx.SaveToFile(path); err != nil {
		b.Fatalf("Failed to save index: %v", err)
	}
	b.Logf("Index saved in %v", time.Since(start))

	if fi, err := os.Stat(path); err == nil {
		b.Logf("Index file size: %.2f MB", float64(fi.Size())/(1024*1024))
	}

	cached10MIndexPath = path
	return path
}

// ============================================================================
// Benchmarks: Search (AND)
// ============================================================================

func BenchmarkSearch10M_ShortQuery(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"the", "server", "john"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(queries[i%len(queries)])
	}
}

func BenchmarkSearch10M_MediumQuery(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"server client", "john database", "network protocol"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(queries[i%len(queries)])
	}
}

func BenchmarkSearch10M_LongQuery(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{
		"server client database network",
		"john michael david william",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(queries[i%len(queries)])
	}
}

func BenchmarkSearch10M_HighSelectivity(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"xylophone", "quizzical", "zephyr"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(queries[i%len(queries)])
	}
}

func BenchmarkSearch10M_NoMatch(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"zzzzz", "xxxxx", "qqqqq"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(queries[i%len(queries)])
	}
}

// ============================================================================
// Benchmarks: SearchAny (OR)
// ============================================================================

func BenchmarkSearchAny10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"server client", "john database", "network protocol"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchAny(queries[i%len(queries)])
	}
}

// ============================================================================
// Benchmarks: SearchThreshold
// ============================================================================

func BenchmarkSearchThreshold10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	query := "server client database network"

	b.Run("Threshold1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.SearchThreshold(query, 1)
		}
	})

	b.Run("Threshold2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.SearchThreshold(query, 2)
		}
	})

	b.Run("Threshold4", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.SearchThreshold(query, 4)
		}
	})
}

// ============================================================================
// Benchmarks: SearchCount (no result allocation)
// ============================================================================

func BenchmarkSearchCount10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"server", "client", "database"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchCount(queries[i%len(queries)])
	}
}

// ============================================================================
// Benchmarks: CachedIndex
// ============================================================================

func BenchmarkCachedSearch10M_ColdCache(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	path := getOrCreate10MIndexFile(b)
	queries := []string{"server", "client", "database", "network", "john"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cached, err := OpenCachedIndex(path, WithCacheSize(100))
		if err != nil {
			b.Fatalf("Failed to open cached index: %v", err)
		}
		cached.Search(queries[i%len(queries)])
	}
}

func BenchmarkCachedSearch10M_WarmCache(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	path := getOrCreate10MIndexFile(b)
	cached, err := OpenCachedIndex(path, WithCacheSize(1000))
	if err != nil {
		b.Fatalf("Failed to open cached index: %v", err)
	}

	queries := []string{"server", "client", "database"}

	// Warm up cache
	for _, q := range queries {
		cached.Search(q)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cached.Search(queries[i%len(queries)])
	}
}

// ============================================================================
// Benchmarks: Serialization
// ============================================================================

func BenchmarkSerialize10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.sear", i))
		if err := idx.SaveToFile(path); err != nil {
			b.Fatalf("Failed to save: %v", err)
		}
	}
}

func BenchmarkDeserialize10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	path := getOrCreate10MIndexFile(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LoadFromFile(path)
		if err != nil {
			b.Fatalf("Failed to load: %v", err)
		}
	}
}

// ============================================================================
// Benchmarks: Concurrent Access
// ============================================================================

func BenchmarkSearchConcurrent10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"server", "client", "database", "network", "john"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx.Search(queries[i%len(queries)])
			i++
		}
	})
}

// ============================================================================
// Benchmarks: Compare Methods
// ============================================================================

func BenchmarkCompareSearchMethods10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	idx := getOrCreate10MIndex(b)
	queries := []string{"server", "client", "database"}

	b.Run("Search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.Search(queries[i%len(queries)])
		}
	})

	b.Run("SearchWithLimit_100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.SearchWithLimit(queries[i%len(queries)], 100)
		}
	})

	b.Run("SearchWithLimit_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.SearchWithLimit(queries[i%len(queries)], 1000)
		}
	})

	b.Run("SearchCount", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.SearchCount(queries[i%len(queries)])
		}
	})

	b.Run("SearchCallback_All", func(b *testing.B) {
		var count int
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count = 0
			idx.SearchCallback(queries[i%len(queries)], func(docID uint32) bool {
				count++
				return true
			})
		}
		_ = count
	})

	b.Run("SearchCallback_First100", func(b *testing.B) {
		var count int
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count = 0
			idx.SearchCallback(queries[i%len(queries)], func(docID uint32) bool {
				count++
				return count < 100
			})
		}
		_ = count
	})
}

// ============================================================================
// Benchmark: Memory Usage
// ============================================================================

func BenchmarkMemoryUsage10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10M benchmark in short mode")
	}

	var m runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m)
	baselineAlloc := m.Alloc

	idx := getOrCreate10MIndex(b)

	runtime.GC()
	runtime.ReadMemStats(&m)
	afterIndexAlloc := m.Alloc

	b.Logf("Memory for 10M index: %.2f MB",
		float64(afterIndexAlloc-baselineAlloc)/(1024*1024))
	b.Logf("N-grams: %d", idx.NgramCount())
	b.Logf("Bytes per n-gram: %.2f",
		float64(afterIndexAlloc-baselineAlloc)/float64(idx.NgramCount()))
}

// ============================================================================
// Test: Result Size Analysis
// ============================================================================

func TestSearchResultSizes10M(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}

	idx := NewIndex(3)
	rng := rand.New(rand.NewSource(42))

	numDocs := 10_000_000
	t.Logf("Building index with %d documents...", numDocs)
	start := time.Now()

	for i := 0; i < numDocs; i++ {
		doc := generateDocument(rng, 5, 20)
		idx.Add(uint32(i), doc)
	}

	t.Logf("Index built in %v, %d unique n-grams", time.Since(start), idx.NgramCount())

	queries := map[string]string{
		"common_short":  "the",
		"common_medium": "the and",
		"tech_short":    "server",
		"tech_medium":   "server client",
		"name_short":    "john",
		"rare_word":     "xylophone",
		"no_match":      "zzzzz",
	}

	t.Log("\nSearch (AND) results:")
	for name, query := range queries {
		start := time.Now()
		results := idx.Search(query)
		elapsed := time.Since(start)
		t.Logf("  %-15s: %8d results in %v", name, len(results), elapsed)
	}
}
