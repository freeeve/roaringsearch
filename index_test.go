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

	idx.Add(1, "hello world")    // ngrams: hel, ell, llo, owo, wor, orl, rld
	idx.Add(2, "hello there")    // ngrams: hel, ell, llo, oth, the, her, ere
	idx.Add(3, "goodbye world")  // ngrams: goo, ood, odb, dby, bye, ewo, wor, orl, rld

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
