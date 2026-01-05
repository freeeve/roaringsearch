package roaringsearch

import (
	"sort"
	"testing"
)

func TestIndexBasicSearch(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "Hello World")
	idx.Add(2, "Hello there")
	idx.Add(3, "World peace")

	if idx.NgramCount() == 0 {
		t.Error("expected ngrams in index")
	}

	if idx.GramSize() != 3 {
		t.Errorf("expected gram size 3, got %d", idx.GramSize())
	}

	// AND search for "hello" - should match docs 1 and 2
	results := idx.Search("hello")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	expected := []uint32{1, 2}
	if len(results) != len(expected) {
		t.Errorf("Search(hello) = %v, want %v", results, expected)
	}

	// SearchCount
	count := idx.SearchCount("hello")
	if count != 2 {
		t.Errorf("SearchCount(hello) = %d, want 2", count)
	}
}

func TestIndexNoMatch(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	results := idx.Search("zzzzz")
	if results != nil {
		t.Errorf("expected nil for no match, got %v", results)
	}

	count := idx.SearchCount("zzzzz")
	if count != 0 {
		t.Errorf("expected 0 for no match, got %d", count)
	}
}

func TestIndexShortQuery(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	results := idx.Search("he")
	if results != nil {
		t.Errorf("expected nil for short query, got %v", results)
	}
}

func TestJapaneseSearch(t *testing.T) {
	// Use gram size 2 for Japanese (collision-free with PackRunes)
	idx := NewIndex(2)

	idx.Add(1, "東京は日本の首都です")        // Tokyo is the capital of Japan
	idx.Add(2, "京都は美しい街です")          // Kyoto is a beautiful city
	idx.Add(3, "Hello World こんにちは") // Mixed English and Japanese

	// Search for "東京" (Tokyo) - should match doc 1
	results := idx.Search("東京")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("Search(東京) = %v, want [1]", results)
	}

	// Search for "京都" - should match only doc 2 (doc 1 has 東京, not 京都)
	results = idx.Search("京都")
	if len(results) != 1 || results[0] != 2 {
		t.Errorf("Search(京都) = %v, want [2]", results)
	}

	// Search count
	count := idx.SearchCount("東京")
	if count != 1 {
		t.Errorf("SearchCount(東京) = %d, want 1", count)
	}

	// Mixed language search - need 2+ chars for gramSize 2
	results = idx.Search("こんにちは")
	if len(results) != 1 || results[0] != 3 {
		t.Errorf("Search(こんにちは) = %v, want [3]", results)
	}
}

func TestMixedLanguageIndex(t *testing.T) {
	// Test that ASCII and Unicode documents coexist correctly
	// Use gramSize 2 so "世界" (2 chars) can be searched
	idx := NewIndex(2)

	idx.Add(1, "hello world")
	idx.Add(2, "Hello 世界") // hello world in mixed
	idx.Add(3, "世界和平")     // world peace in Chinese

	// Search for "hello" should find both docs 1 and 2
	results := idx.Search("hello")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	if len(results) != 2 {
		t.Errorf("Search(hello) = %v, want [1, 2]", results)
	}

	// Search for "世界" should find docs 2 and 3
	results = idx.Search("世界")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	if len(results) != 2 {
		t.Errorf("Search(世界) = %v, want [2, 3]", results)
	}
}

func TestSearchAnyCount(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello world")
	idx.Add(2, "hello there")
	idx.Add(3, "goodbye world")

	count := idx.SearchAnyCount("hello")
	if count != 2 {
		t.Errorf("SearchAnyCount(hello) = %d, want 2", count)
	}
}

func TestHasNgram(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	tmpDir := t.TempDir()
	path := tmpDir + "/test.sear"
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

func TestLoadFromFileWithOptions(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	tmpDir := t.TempDir()
	path := tmpDir + "/test.sear"
	idx.SaveToFile(path)

	// Test with custom normalizer
	loaded, err := LoadFromFileWithOptions(path, WithNormalizer(NormalizeLowercase))
	if err != nil {
		t.Fatalf("LoadFromFileWithOptions failed: %v", err)
	}

	if loaded.NgramCount() != idx.NgramCount() {
		t.Errorf("ngram count mismatch: got %d, want %d", loaded.NgramCount(), idx.NgramCount())
	}
}

func TestGramSizeClamping(t *testing.T) {
	// Test that gram size is clamped to valid range
	idx := NewIndex(0)
	if idx.GramSize() != 3 {
		t.Errorf("gram size 0 should default to 3, got %d", idx.GramSize())
	}

	idx = NewIndex(-1)
	if idx.GramSize() != 3 {
		t.Errorf("gram size -1 should default to 3, got %d", idx.GramSize())
	}

	idx = NewIndex(10)
	if idx.GramSize() != 8 {
		t.Errorf("gram size 10 should clamp to 8, got %d", idx.GramSize())
	}

	idx = NewIndex(5)
	if idx.GramSize() != 5 {
		t.Errorf("gram size 5 should be 5, got %d", idx.GramSize())
	}
}
