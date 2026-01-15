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

func TestCachedIndexMoveToFront(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "foo bar baz")
	idx.Add(3, "test data here")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "lru_move.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path, WithCacheSize(100))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Search for different terms to populate cache
	cached.Search("hello")
	cached.Search("foo")
	cached.Search("test")

	// Search for "hello" again - this should trigger moveToFront
	cached.Search("hello")

	// Search should still work correctly
	results := cached.Search("hello")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("Search after moveToFront = %v, want [1]", results)
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

func TestWithCachedNormalizer(t *testing.T) {
	idx := NewIndex(3, WithNormalizer(NormalizeLowercase))
	idx.Add(1, "Hello World")
	idx.Add(2, "HELLO THERE")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "normalizer.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Open with custom normalizer
	cached, err := OpenCachedIndex(path, WithCachedNormalizer(NormalizeLowercase))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Search should work with normalizer
	results := cached.Search("HELLO")
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestPreloadKeys(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "hello there")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "preloadkeys.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path, WithCacheSize(100))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Get some valid keys from the index
	var keys []uint64
	for key := range cached.ngramIndex {
		keys = append(keys, key)
		if len(keys) >= 3 {
			break
		}
	}

	// Preload keys
	if err := cached.PreloadKeys(keys); err != nil {
		t.Errorf("PreloadKeys failed: %v", err)
	}

	// Cache should now have entries
	if cached.CacheSize() < len(keys) {
		t.Errorf("cache size = %d, want >= %d", cached.CacheSize(), len(keys))
	}

	// Preload with invalid key should still work (keys not in index are ignored)
	if err := cached.PreloadKeys([]uint64{99999999}); err != nil {
		t.Errorf("PreloadKeys with invalid key failed: %v", err)
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

func TestCachedIndexSearchEdgeCases(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "hello there")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "edge.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cached, err := OpenCachedIndex(path)
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Short query
	results := cached.Search("he")
	if results != nil {
		t.Errorf("short query should return nil, got %v", results)
	}

	// Empty query
	results = cached.Search("")
	if results != nil {
		t.Errorf("empty query should return nil, got %v", results)
	}

	// Query not in index
	results = cached.Search("xyz")
	if results != nil {
		t.Errorf("missing query should return nil, got %v", results)
	}

	// SearchAny with short query
	results = cached.SearchAny("he")
	if results != nil {
		t.Errorf("SearchAny short query should return nil, got %v", results)
	}

	// SearchAny with missing query
	results = cached.SearchAny("xyz")
	if results != nil {
		t.Errorf("SearchAny missing query should return nil, got %v", results)
	}

	// SearchThreshold with short query
	result := cached.SearchThreshold("he", 1)
	if result.DocIDs != nil {
		t.Errorf("SearchThreshold short query should return nil, got %v", result.DocIDs)
	}

	// HasNgram with short ngram
	if cached.HasNgram("he") {
		t.Error("HasNgram should return false for short ngram")
	}
}

func TestCachedIndexEviction(t *testing.T) {
	// Create index with many unique ngrams
	idx := NewIndex(3)
	idx.Add(1, "alpha beta gamma delta")
	idx.Add(2, "epsilon zeta eta theta")
	idx.Add(3, "iota kappa lambda mu")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "evict.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Small cache to force eviction
	cached, err := OpenCachedIndex(path, WithCacheSize(3))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Search multiple times to fill and evict cache
	cached.Search("alpha")
	cached.Search("epsilon")
	cached.Search("iota")
	cached.Search("beta")  // Should trigger eviction
	cached.Search("gamma") // Should trigger eviction

	// Verify searches still work
	results := cached.Search("alpha")
	if len(results) != 1 {
		t.Errorf("Search after eviction failed: got %v", results)
	}
}

func TestCachedIndexSingleEntryEviction(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "foo bar baz")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "single.sear")
	idx.SaveToFile(path)

	// Cache size 1 - every new search evicts
	cached, _ := OpenCachedIndex(path, WithCacheSize(1))

	cached.Search("hello")
	if cached.CacheSize() != 1 {
		t.Errorf("cache size after first search = %d, want 1", cached.CacheSize())
	}

	cached.Search("foo")
	if cached.CacheSize() != 1 {
		t.Errorf("cache size after second search = %d, want 1", cached.CacheSize())
	}

	// Re-search first term (should reload from disk)
	results := cached.Search("hello")
	if len(results) != 1 {
		t.Errorf("re-search after eviction failed: got %v", results)
	}
}

func TestCachedIndexSearchAnyPartialMatch(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")
	idx.Add(2, "goodbye world")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "partial.sear")
	idx.SaveToFile(path)

	cached, _ := OpenCachedIndex(path)

	// SearchAny with partial match - "hello xyz" has some ngrams that exist
	results := cached.SearchAny("hello xyz")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("SearchAny partial match = %v, want [1]", results)
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
