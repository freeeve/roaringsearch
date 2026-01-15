package roaringsearch

import (
	"reflect"
	"sort"
	"testing"
)

func TestIndexBasic(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "Hello World")
	idx.Add(2, "Hello there")
	idx.Add(3, "World peace")

	if idx.NgramCount() == 0 {
		t.Error("expected ngrams in index")
	}

	// AND search for "hello" - should match docs 1 and 2
	results := idx.Search("hello")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	expected := []uint32{1, 2}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Search(hello) = %v, want %v", results, expected)
	}

	// AND search for "world" - should match docs 1 and 3
	results = idx.Search("world")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	expected = []uint32{1, 3}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Search(world) = %v, want %v", results, expected)
	}
}

func TestSearchAny(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "apple")
	idx.Add(2, "banana")
	idx.Add(3, "cherry")

	// OR search - should find docs with any matching ngram
	results := idx.SearchAny("app")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("SearchAny(app) = %v, want [1]", results)
	}

	// Search for something not in index
	results = idx.SearchAny("xyz")
	if results != nil {
		t.Errorf("SearchAny(xyz) = %v, want nil", results)
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

func TestSearchThreshold(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello world")   // ngrams: hel, ell, llo, owo, wor, orl, rld
	idx.Add(2, "hello there")   // ngrams: hel, ell, llo, oth, the, her, ere
	idx.Add(3, "goodbye world") // ngrams: goo, ood, odb, dby, bye, ewo, wor, orl, rld

	// Search "hello" (hel, ell, llo) with threshold 2
	result := idx.SearchThreshold("hello", 2)

	// Both doc 1 and 2 should match (both have hel, ell, llo)
	if len(result.DocIDs) != 2 {
		t.Errorf("expected 2 results, got %d: %v", len(result.DocIDs), result.DocIDs)
	}

	// Scores should be 3 for both (all 3 ngrams match)
	for _, docID := range result.DocIDs {
		if result.Scores[docID] != 3 {
			t.Errorf("expected score 3 for doc %d, got %d", docID, result.Scores[docID])
		}
	}
}

func TestRemove(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello")
	idx.Add(2, "hello")

	results := idx.Search("hello")
	if len(results) != 2 {
		t.Errorf("expected 2 results before remove, got %d", len(results))
	}

	idx.Remove(1)

	results = idx.Search("hello")
	if len(results) != 1 || results[0] != 2 {
		t.Errorf("expected [2] after remove, got %v", results)
	}
}

func TestClear(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello")
	idx.Add(2, "world")

	if idx.NgramCount() == 0 {
		t.Error("expected ngrams before clear")
	}

	idx.Clear()

	if idx.NgramCount() != 0 {
		t.Errorf("expected 0 ngrams after clear, got %d", idx.NgramCount())
	}
}

func TestCustomNormalizer(t *testing.T) {
	// Custom normalizer that removes vowels
	removeVowels := func(s string) string {
		result := make([]rune, 0, len(s))
		for _, r := range s {
			switch r {
			case 'a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U':
				continue
			default:
				result = append(result, r)
			}
		}
		return string(result)
	}

	idx := NewIndex(3, WithNormalizer(removeVowels))

	idx.Add(1, "hello") // becomes "hll" - only 1 ngram

	results := idx.Search("hll")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("custom normalizer search failed: got %v", results)
	}
}

func TestJapaneseText(t *testing.T) {
	idx := NewIndex(2) // Use 2-grams for Japanese

	idx.Add(1, "東京都")
	idx.Add(2, "京都府")
	idx.Add(3, "大阪府")

	// Search for "京都" - should match docs 1 and 2
	results := idx.Search("京都")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })

	expected := []uint32{1, 2}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Japanese search = %v, want %v", results, expected)
	}
}

func TestMixedLanguageIndex(t *testing.T) {
	// Test that ASCII and Unicode documents coexist correctly
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

func TestEmptyQuery(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello")

	// Empty query should return nil
	results := idx.Search("")
	if results != nil {
		t.Errorf("empty query should return nil, got %v", results)
	}

	results = idx.SearchAny("")
	if results != nil {
		t.Errorf("empty query should return nil, got %v", results)
	}

	result := idx.SearchThreshold("", 1)
	if result.DocIDs != nil {
		t.Errorf("empty query should return nil, got %v", result.DocIDs)
	}
}

func TestShortQuery(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello")

	// Query shorter than gram size should return nil
	results := idx.Search("he")
	if results != nil {
		t.Errorf("short query should return nil, got %v", results)
	}
}

func TestSearchWithLimit(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello world")
	idx.Add(2, "hello there")
	idx.Add(3, "hello everyone")
	idx.Add(4, "hello friend")
	idx.Add(5, "hello neighbor")

	// Get only first 2 results
	results := idx.SearchWithLimit("hello", 2)
	if len(results) != 2 {
		t.Errorf("SearchWithLimit(hello, 2) returned %d results, want 2", len(results))
	}

	// Limit of 0 should return nil
	results = idx.SearchWithLimit("hello", 0)
	if results != nil {
		t.Errorf("SearchWithLimit with limit 0 should return nil, got %v", results)
	}

	// Limit of -1 should return nil
	results = idx.SearchWithLimit("hello", -1)
	if results != nil {
		t.Errorf("SearchWithLimit with negative limit should return nil, got %v", results)
	}

	// Query not in index should return nil
	results = idx.SearchWithLimit("xyz", 10)
	if results != nil {
		t.Errorf("SearchWithLimit for missing query should return nil, got %v", results)
	}

	// Short query should return nil
	results = idx.SearchWithLimit("he", 10)
	if results != nil {
		t.Errorf("SearchWithLimit with short query should return nil, got %v", results)
	}

	// Limit larger than results should return all matches
	results = idx.SearchWithLimit("hello", 100)
	if len(results) != 5 {
		t.Errorf("SearchWithLimit(hello, 100) returned %d results, want 5", len(results))
	}
}

func TestSearchCallback(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello world")
	idx.Add(2, "hello there")
	idx.Add(3, "hello everyone")

	// Collect all results via callback
	var collected []uint32
	completed := idx.SearchCallback("hello", func(docID uint32) bool {
		collected = append(collected, docID)
		return true // continue
	})

	if !completed {
		t.Error("SearchCallback should return true when callback always returns true")
	}
	if len(collected) != 3 {
		t.Errorf("SearchCallback collected %d results, want 3", len(collected))
	}

	// Early termination - stop after first result
	collected = nil
	completed = idx.SearchCallback("hello", func(docID uint32) bool {
		collected = append(collected, docID)
		return false // stop
	})

	if completed {
		t.Error("SearchCallback should return false when callback returns false")
	}
	if len(collected) != 1 {
		t.Errorf("SearchCallback with early termination collected %d results, want 1", len(collected))
	}

	// Query not in index
	collected = nil
	completed = idx.SearchCallback("xyz", func(docID uint32) bool {
		collected = append(collected, docID)
		return true
	})
	if !completed {
		t.Error("SearchCallback for missing query should return true")
	}
	if len(collected) != 0 {
		t.Errorf("SearchCallback for missing query collected %d results, want 0", len(collected))
	}

	// Short query
	completed = idx.SearchCallback("he", func(docID uint32) bool {
		return true
	})
	if !completed {
		t.Error("SearchCallback with short query should return true")
	}
}

func TestSearchCount(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello world")
	idx.Add(2, "hello there")
	idx.Add(3, "goodbye world")

	count := idx.SearchCount("hello")
	if count != 2 {
		t.Errorf("SearchCount(hello) = %d, want 2", count)
	}

	count = idx.SearchCount("world")
	if count != 2 {
		t.Errorf("SearchCount(world) = %d, want 2", count)
	}

	// Query not in index
	count = idx.SearchCount("xyz")
	if count != 0 {
		t.Errorf("SearchCount(xyz) = %d, want 0", count)
	}

	// Short query
	count = idx.SearchCount("he")
	if count != 0 {
		t.Errorf("SearchCount with short query = %d, want 0", count)
	}
}

func TestUnicodeTrigrams(t *testing.T) {
	// Test Unicode trigrams (uses hashRunes internally for n>2 Unicode)
	idx := NewIndex(3)

	idx.Add(1, "東京都庁")
	idx.Add(2, "京都府庁")
	idx.Add(3, "大阪府庁")

	// Search for "京都府" - should only match doc 2
	results := idx.Search("京都府")
	if len(results) != 1 || results[0] != 2 {
		t.Errorf("Search(京都府) = %v, want [2]", results)
	}

	// Search for "府庁" - too short for trigrams, but should work with 2-gram
	idx2 := NewIndex(2)
	idx2.Add(1, "東京都庁")
	idx2.Add(2, "京都府庁")
	idx2.Add(3, "大阪府庁")

	results = idx2.Search("府庁")
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	if len(results) != 2 {
		t.Errorf("Search(府庁) with 2-grams = %v, want [2, 3]", results)
	}
}

func TestSearchDuplicateNgrams(t *testing.T) {
	// Test query with repeated ngrams (e.g., "aaa" has duplicate "aa" bigrams)
	idx := NewIndex(2)
	idx.Add(1, "aaa bbb")
	idx.Add(2, "xyz ccc")

	// "aaa" generates duplicate "aa" ngrams - should handle dedup
	results := idx.Search("aaa")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("Search with duplicate ngrams = %v, want [1]", results)
	}
}

func TestRemoveNonexistent(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	// Remove doc that doesn't exist - should not panic
	idx.Remove(999)

	// Original doc should still be searchable
	results := idx.Search("hello")
	if len(results) != 1 {
		t.Errorf("Search after removing nonexistent = %v, want [1]", results)
	}
}

func TestSearchWithLimitDuplicateNgrams(t *testing.T) {
	idx := NewIndex(2)
	idx.Add(1, "aaa")
	idx.Add(2, "bbb")

	// Query with duplicate ngrams + limit
	results := idx.SearchWithLimit("aaa", 5)
	if len(results) != 1 {
		t.Errorf("SearchWithLimit with dup ngrams = %v, want [1]", results)
	}
}

func TestSearchCountDuplicateNgrams(t *testing.T) {
	idx := NewIndex(2)
	idx.Add(1, "aaa")
	idx.Add(2, "bbb")

	count := idx.SearchCount("aaa")
	if count != 1 {
		t.Errorf("SearchCount with dup ngrams = %d, want 1", count)
	}
}

func TestSearchCallbackDuplicateNgrams(t *testing.T) {
	idx := NewIndex(2)
	idx.Add(1, "aaa")
	idx.Add(2, "bbb")

	var results []uint32
	idx.SearchCallback("aaa", func(docID uint32) bool {
		results = append(results, docID)
		return true
	})
	if len(results) != 1 {
		t.Errorf("SearchCallback with dup ngrams = %v, want [1]", results)
	}
}

func TestSearchCallbackStopMidway(t *testing.T) {
	idx := NewIndex(3)
	for i := 1; i <= 10; i++ {
		idx.Add(uint32(i), "hello world test")
	}

	// Stop after collecting 5
	var collected []uint32
	idx.SearchCallback("hello", func(docID uint32) bool {
		collected = append(collected, docID)
		return len(collected) < 5
	})

	if len(collected) != 5 {
		t.Errorf("callback stopped at %d, want 5", len(collected))
	}
}

func TestGramSizeClamping(t *testing.T) {
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

func TestAddBatchN(t *testing.T) {
	// Test empty batch
	idx := NewIndex(3)
	idx.AddBatchN(nil, 4)
	if idx.NgramCount() != 0 {
		t.Error("empty batch should not add ngrams")
	}

	// Test single doc batch
	idx = NewIndex(3)
	idx.AddBatchN([]Document{{ID: 1, Text: "hello world"}}, 1)
	if idx.NgramCount() == 0 {
		t.Error("single doc batch should add ngrams")
	}

	// Test with explicit worker count
	idx = NewIndex(3)
	docs := []Document{
		{ID: 1, Text: "hello world"},
		{ID: 2, Text: "foo bar baz"},
		{ID: 3, Text: "test data here"},
	}
	idx.AddBatchN(docs, 2)
	results := idx.Search("hello")
	if len(results) != 1 {
		t.Errorf("AddBatchN search failed: got %v", results)
	}

	// Test with workers > docs (should clamp)
	idx = NewIndex(3)
	idx.AddBatchN(docs, 100)
	if idx.NgramCount() == 0 {
		t.Error("AddBatchN with many workers should still work")
	}

	// Test with workers = 0 (should default to NumCPU)
	idx = NewIndex(3)
	idx.AddBatchN(docs, 0)
	if idx.NgramCount() == 0 {
		t.Error("AddBatchN with 0 workers should default to NumCPU")
	}
}

func TestAddBatchNBigrams(t *testing.T) {
	// Test AddBatchN with bigrams (gramSize <= 2) to cover that code path
	idx := NewIndex(2)
	docs := []Document{
		{ID: 1, Text: "hello world"},
		{ID: 2, Text: "hi there"},
		{ID: 3, Text: "hey you"},
	}
	idx.AddBatchN(docs, 2)

	results := idx.Search("he")
	if len(results) != 3 {
		t.Errorf("bigram AddBatchN search failed: got %v, want 3 results", results)
	}
}

func TestAddBatchNUnicode(t *testing.T) {
	// Test AddBatchN with Unicode to cover fallback path
	idx := NewIndex(2) // Use bigrams for short CJK text
	docs := []Document{
		{ID: 1, Text: "東京都"},
		{ID: 2, Text: "京都府"},
		{ID: 3, Text: "大阪府"},
	}
	idx.AddBatchN(docs, 2)

	results := idx.Search("京都")
	if len(results) != 2 {
		t.Errorf("Unicode AddBatchN search failed: got %v", results)
	}
}

func BenchmarkAdd(b *testing.B) {
	idx := NewIndex(3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add(uint32(i), "The quick brown fox jumps over the lazy dog")
	}
}

func BenchmarkSearch(b *testing.B) {
	idx := NewIndex(3)

	// Add some documents
	for i := 0; i < 10000; i++ {
		idx.Add(uint32(i), "The quick brown fox jumps over the lazy dog")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search("brown fox")
	}
}

func BenchmarkSearchThreshold(b *testing.B) {
	idx := NewIndex(3)

	// Add some documents
	for i := 0; i < 10000; i++ {
		idx.Add(uint32(i), "The quick brown fox jumps over the lazy dog")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchThreshold("brown fox", 2)
	}
}
