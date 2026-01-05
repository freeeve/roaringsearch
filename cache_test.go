package roaringsearch

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestCachedIndexBasic(t *testing.T) {
	// Create and save an index
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "hello there")
	idx.Add(3, "world peace")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "cached.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Open as cached index
	cached, err := OpenCachedIndex(path, WithCacheSize(10))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Verify metadata
	if cached.GramSize() != 3 {
		t.Errorf("gram size = %d, want 3", cached.GramSize())
	}

	if cached.NgramCount() != idx.NgramCount() {
		t.Errorf("ngram count = %d, want %d", cached.NgramCount(), idx.NgramCount())
	}

	// Cache should be empty initially
	if cached.CacheSize() != 0 {
		t.Errorf("initial cache size = %d, want 0", cached.CacheSize())
	}

	// Search should work and populate cache
	results := cached.Search("hello")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })

	expected := []uint32{1, 2}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Search(hello) = %v, want %v", results, expected)
	}

	// Cache should now have entries
	if cached.CacheSize() == 0 {
		t.Error("cache should have entries after search")
	}
}

func TestCachedIndexLRUEviction(t *testing.T) {
	// Create index with many n-grams
	idx := NewIndex(3)
	idx.Add(1, "abcdefghijklmnopqrstuvwxyz") // 24 unique 3-grams

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "lru.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Open with small cache
	cached, err := OpenCachedIndex(path, WithCacheSize(5))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Search for different terms to fill cache
	cached.Search("abc")
	cached.Search("def")
	cached.Search("ghi")
	cached.Search("jkl")
	cached.Search("mno")
	cached.Search("pqr") // This should trigger eviction

	// Cache should not exceed max size
	if cached.CacheSize() > 5 {
		t.Errorf("cache size = %d, want <= 5", cached.CacheSize())
	}
}

func TestCachedIndexSearchAny(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "apple")
	idx.Add(2, "banana")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "any.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path)
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	results := cached.SearchAny("app")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("SearchAny(app) = %v, want [1]", results)
	}
}

func TestCachedIndexSearchThreshold(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "hello there")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "threshold.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path)
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	result := cached.SearchThreshold("hello", 2)
	if len(result.DocIDs) != 2 {
		t.Errorf("expected 2 results, got %d", len(result.DocIDs))
	}
}

func TestCachedIndexClearCache(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "clear.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path)
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Populate cache
	cached.Search("hello")
	if cached.CacheSize() == 0 {
		t.Error("cache should have entries")
	}

	// Clear cache
	cached.ClearCache()
	if cached.CacheSize() != 0 {
		t.Errorf("cache size after clear = %d, want 0", cached.CacheSize())
	}

	// Search should still work (reloads from disk)
	results := cached.Search("hello")
	if len(results) == 0 {
		t.Error("search should work after cache clear")
	}
}

func TestCachedIndexPreload(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "preload.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path)
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Preload by searching (populates cache)
	cached.Search("hello")

	if cached.CacheSize() < 3 {
		t.Errorf("cache size after search = %d, want >= 3", cached.CacheSize())
	}
}

func TestHasNgram(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sear")
	idx.SaveToFile(path)

	cached, err := OpenCachedIndex(path)
	if err != nil {
		t.Fatalf("failed to open cached index: %v", err)
	}

	if !cached.HasNgram("hel") {
		t.Error("expected HasNgram(hel) to return true")
	}

	if cached.HasNgram("zzz") {
		t.Error("expected HasNgram(zzz) to return false")
	}
}

func BenchmarkCachedSearch(b *testing.B) {
	// Create and save index
	idx := NewIndex(3)
	for i := 0; i < 10000; i++ {
		idx.Add(uint32(i), "The quick brown fox jumps over the lazy dog")
	}

	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.sear")
	idx.SaveToFile(path)

	cached, _ := OpenCachedIndex(path, WithCacheSize(100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cached.Search("brown fox")
	}
}
