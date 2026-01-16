package roaringsearch

import (
	"fmt"
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

func TestWithMemoryBudget(t *testing.T) {
	idx := NewIndex(3)
	// Add docs with varying content to create different sized bitmaps
	for i := 1; i <= 100; i++ {
		idx.Add(uint32(i), "hello world test data for memory budget")
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "memory.sear")
	idx.SaveToFile(path)

	// Open with 1KB memory budget
	cached, err := OpenCachedIndex(path, WithMemoryBudget(1024))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Initial memory should be 0
	if cached.MemoryUsage() != 0 {
		t.Errorf("initial memory usage = %d, want 0", cached.MemoryUsage())
	}

	// Search to populate cache
	cached.Search("hello")
	cached.Search("world")
	cached.Search("test")

	// Memory should be tracked
	if cached.MemoryUsage() == 0 {
		t.Error("memory usage should be > 0 after searches")
	}

	// Memory should stay under budget
	if cached.MemoryUsage() > 1024 {
		t.Errorf("memory usage %d exceeds budget 1024", cached.MemoryUsage())
	}

	// Clear should reset memory
	cached.ClearCache()
	if cached.MemoryUsage() != 0 {
		t.Errorf("memory usage after clear = %d, want 0", cached.MemoryUsage())
	}
}

func TestMemoryBudgetRespected(t *testing.T) {
	// Create index with many unique ngrams to generate many bitmaps
	idx := NewIndex(3)
	words := []string{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
		"golf", "hotel", "india", "juliet", "kilo", "lima",
		"mike", "november", "oscar", "papa", "quebec", "romeo",
		"sierra", "tango", "uniform", "victor", "whiskey", "xray",
		"yankee", "zulu", "one", "two", "three", "four", "five",
	}

	// Add many documents with different word combinations
	docID := uint32(1)
	for i := 0; i < len(words); i++ {
		for j := 0; j < len(words); j++ {
			if i != j {
				idx.Add(docID, words[i]+" "+words[j])
				docID++
			}
		}
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "budget_test.sear")
	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Set a small memory budget (10KB)
	memoryBudget := int64(10 * 1024)
	cached, err := OpenCachedIndex(path, WithMemoryBudget(memoryBudget))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	// Track max memory seen
	var maxMemory uint64

	// Perform many searches to stress the cache
	for _, word := range words {
		cached.Search(word)

		usage := cached.MemoryUsage()
		if usage > maxMemory {
			maxMemory = usage
		}

		// Verify budget is never exceeded
		if usage > uint64(memoryBudget) {
			t.Errorf("memory budget exceeded: usage=%d, budget=%d", usage, memoryBudget)
		}
	}

	// Do another round to ensure eviction is working
	for _, word := range words {
		cached.SearchAny(word + " test")

		usage := cached.MemoryUsage()
		if usage > uint64(memoryBudget) {
			t.Errorf("memory budget exceeded on second pass: usage=%d, budget=%d", usage, memoryBudget)
		}
	}

	t.Logf("Memory budget: %d bytes, max observed: %d bytes, final: %d bytes",
		memoryBudget, maxMemory, cached.MemoryUsage())
}

func TestMemoryBudgetWithLargeBitmaps(t *testing.T) {
	// Create index with large sparse bitmaps (more realistic)
	idx := NewIndex(3)

	// Create 50 unique words, each appearing in random subsets of 10K docs
	words := make([]string, 50)
	for i := range words {
		words[i] = fmt.Sprintf("word%03d", i)
	}

	// Add 10K docs with random word combinations
	for docID := uint32(1); docID <= 10000; docID++ {
		// Each doc gets 3-5 random words
		numWords := 3 + int(docID%3)
		var text string
		for j := 0; j < numWords; j++ {
			wordIdx := (int(docID) * (j + 1)) % len(words)
			text += words[wordIdx] + " "
		}
		idx.Add(docID, text)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large_budget_test.sear")
	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// 50KB budget - should force frequent eviction
	memoryBudget := int64(50 * 1024)
	cached, err := OpenCachedIndex(path, WithMemoryBudget(memoryBudget))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	t.Logf("Index has %d ngrams", cached.NgramCount())

	// Hammer with 200 queries
	for i := 0; i < 200; i++ {
		word := words[i%len(words)]
		cached.Search(word)

		usage := cached.MemoryUsage()
		if usage > uint64(memoryBudget) {
			t.Errorf("query %d: memory budget exceeded: usage=%d, budget=%d", i, usage, memoryBudget)
		}
	}

	t.Logf("After 200 queries: cache has %d entries, memory: %d/%d bytes",
		cached.CacheSize(), cached.MemoryUsage(), memoryBudget)

	// Verify eviction happened (cache shouldn't have all ngrams)
	if cached.CacheSize() >= cached.NgramCount() {
		t.Errorf("eviction not working: cache has %d entries but index has %d ngrams",
			cached.CacheSize(), cached.NgramCount())
	}
}

func TestMemoryBudgetLargeBitmaps(t *testing.T) {
	// Simulate a real index with 100K docs and common ngrams
	idx := NewIndex(3)

	// Add 100K docs - common words appear in many docs creating large bitmaps
	commonWords := []string{"the", "and", "for", "with", "this", "that", "from", "have", "been", "were"}
	for docID := uint32(1); docID <= 100000; docID++ {
		// Each doc has 2 common words + unique content
		text := commonWords[int(docID)%len(commonWords)] + " " +
			commonWords[(int(docID)+3)%len(commonWords)] + " " +
			fmt.Sprintf("unique%d content%d", docID, docID*7)
		idx.Add(docID, text)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large_bitmaps.sear")
	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// 1MB budget
	memoryBudget := int64(1024 * 1024)
	cached, err := OpenCachedIndex(path, WithMemoryBudget(memoryBudget))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	t.Logf("Index has %d ngrams", cached.NgramCount())

	// Query common words (large bitmaps)
	for i := 0; i < 100; i++ {
		word := commonWords[i%len(commonWords)]
		_ = cached.Search(word) // discard result

		usage := cached.MemoryUsage()
		if usage > uint64(memoryBudget) {
			t.Errorf("query %d (%s): memory %d exceeds budget %d",
				i, word, usage, memoryBudget)
		}
	}

	t.Logf("After 100 queries: cache=%d entries, memory=%d/%d bytes (%.1f%%)",
		cached.CacheSize(), cached.MemoryUsage(), memoryBudget,
		float64(cached.MemoryUsage())/float64(memoryBudget)*100)
}

func TestMemoryBudgetManyUniqueQueries(t *testing.T) {
	// This test searches for MANY unique terms to force constant cache eviction
	idx := NewIndex(3)

	// Create 1000 unique words
	words := make([]string, 1000)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i)
	}

	// Add 50K docs with random word combinations
	for docID := uint32(1); docID <= 50000; docID++ {
		// Each doc gets 5 words
		text := ""
		for j := 0; j < 5; j++ {
			wordIdx := (int(docID)*7 + j*13) % len(words)
			text += words[wordIdx] + " "
		}
		idx.Add(docID, text)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "many_queries.sear")
	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Small budget to force eviction: 100KB
	memoryBudget := int64(100 * 1024)
	cached, err := OpenCachedIndex(path, WithMemoryBudget(memoryBudget))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	totalNgrams := cached.NgramCount()
	t.Logf("Index has %d ngrams, budget %d bytes", totalNgrams, memoryBudget)

	var maxUsage uint64
	var evictionCount int
	prevCacheSize := 0

	// Search for ALL unique words - forces loading many different ngrams
	for i, word := range words {
		_ = cached.Search(word)

		usage := cached.MemoryUsage()
		if usage > maxUsage {
			maxUsage = usage
		}

		if usage > uint64(memoryBudget) {
			t.Errorf("query %d (%s): memory %d exceeds budget %d",
				i, word, usage, memoryBudget)
		}

		// Track evictions
		if cached.CacheSize() < prevCacheSize {
			evictionCount++
		}
		prevCacheSize = cached.CacheSize()
	}

	t.Logf("After %d unique queries:", len(words))
	t.Logf("  Max memory: %d bytes (%.1f%% of budget)", maxUsage, float64(maxUsage)/float64(memoryBudget)*100)
	t.Logf("  Final cache: %d entries", cached.CacheSize())
	t.Logf("  Evictions detected: %d", evictionCount)

	// Verify eviction is happening
	if evictionCount == 0 {
		t.Error("expected evictions to occur with small budget and many queries")
	}
}

func TestMemoryBudgetOversizedBitmap(t *testing.T) {
	// Test that a bitmap larger than the budget doesn't break eviction
	idx := NewIndex(3)

	// Create one very common word that appears in ALL docs (huge bitmap)
	// and many rare words
	for docID := uint32(1); docID <= 50000; docID++ {
		text := "common " + fmt.Sprintf("rare%05d unique%05d", docID, docID*3)
		idx.Add(docID, text)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "oversized.sear")
	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Very small budget - likely smaller than the "common" bitmap
	memoryBudget := int64(1024) // 1KB
	cached, err := OpenCachedIndex(path, WithMemoryBudget(memoryBudget))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	t.Logf("Index has %d ngrams, budget %d bytes", cached.NgramCount(), memoryBudget)

	// Search for "common" - this bitmap is huge (50K docs)
	results := cached.Search("common")
	t.Logf("Search 'common' returned %d results", len(results))

	// Memory should NOT exceed budget even with oversized bitmap
	usage := cached.MemoryUsage()
	if usage > uint64(memoryBudget) {
		t.Errorf("memory %d exceeds budget %d after oversized bitmap", usage, memoryBudget)
	}

	// Search for rare terms - these should fit in cache
	for i := 1; i <= 10; i++ {
		word := fmt.Sprintf("rare%05d", i)
		_ = cached.Search(word)

		usage = cached.MemoryUsage()
		if usage > uint64(memoryBudget) {
			t.Errorf("memory %d exceeds budget %d after query %d", usage, memoryBudget, i)
		}
	}

	t.Logf("Final: cache=%d entries, memory=%d/%d bytes",
		cached.CacheSize(), cached.MemoryUsage(), memoryBudget)
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
