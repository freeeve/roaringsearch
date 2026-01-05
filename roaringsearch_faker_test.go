package roaringsearch

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-faker/faker/v4"
)

// generateFakeDocuments creates n fake documents with realistic text
func generateFakeDocuments(n int) []string {
	docs := make([]string, n)
	for i := 0; i < n; i++ {
		// Mix of different content types
		switch i % 5 {
		case 0:
			docs[i] = faker.Sentence()
		case 1:
			docs[i] = faker.Paragraph()
		case 2:
			docs[i] = fmt.Sprintf("%s %s - %s", faker.Name(), faker.Email(), faker.Sentence())
		case 3:
			addr := faker.GetRealAddress()
			docs[i] = fmt.Sprintf("%s, %s %s", addr.Address, addr.City, addr.State)
		case 4:
			docs[i] = faker.Paragraph() + " " + faker.Paragraph()
		}
	}
	return docs
}

func TestFakerBasicSearch(t *testing.T) {
	idx := NewIndex(3)

	// Generate 1000 fake documents
	docs := generateFakeDocuments(1000)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	t.Logf("Indexed %d documents, %d unique n-grams", len(docs), idx.NgramCount())

	// Search for common English words that should appear
	commonWords := []string{"the", "and", "for", "with"}
	for _, word := range commonWords {
		results := idx.SearchAny(word)
		if len(results) > 0 {
			t.Logf("SearchAny(%q) found %d documents", word, len(results))
		}
	}
}

func TestFakerNameSearch(t *testing.T) {
	idx := NewIndex(3)

	// Index names
	names := make([]string, 500)
	for i := 0; i < 500; i++ {
		names[i] = faker.Name()
		idx.Add(uint32(i), names[i])
	}

	t.Logf("Indexed %d names, %d unique n-grams", len(names), idx.NgramCount())

	// Search for a specific name (pick one we know exists)
	targetName := names[42]
	results := idx.Search(targetName)

	found := false
	for _, id := range results {
		if id == 42 {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("failed to find indexed name %q", targetName)
	}

	// Fuzzy search - search for partial name
	parts := strings.Fields(targetName)
	if len(parts) > 0 {
		firstName := parts[0]
		if len(firstName) >= 3 {
			results := idx.Search(firstName)
			t.Logf("Search for first name %q found %d matches", firstName, len(results))
		}
	}
}

func TestFakerThresholdSearch(t *testing.T) {
	idx := NewIndex(3)

	// Generate documents with some common phrases
	docs := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		docs[i] = faker.Paragraph()
		idx.Add(uint32(i), docs[i])
	}

	// Pick a document and search with threshold
	targetDoc := docs[100]
	words := strings.Fields(targetDoc)

	if len(words) >= 3 {
		query := strings.Join(words[:3], " ")
		result := idx.SearchThreshold(query, 2)

		t.Logf("Threshold search for %q found %d docs", query, len(result.DocIDs))

		// The original document should be in results with high score
		if score, ok := result.Scores[100]; ok {
			t.Logf("Target document score: %d", score)
		}
	}
}

func TestFakerLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test in short mode")
	}

	idx := NewIndex(3)

	// Index 10,000 documents
	numDocs := 10000
	docs := generateFakeDocuments(numDocs)

	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	t.Logf("Indexed %d documents", numDocs)
	t.Logf("Unique n-grams: %d", idx.NgramCount())

	// Run multiple searches
	searchTerms := []string{
		"john",
		"street",
		"company",
		"email",
		"the quick",
	}

	for _, term := range searchTerms {
		results := idx.SearchAny(term)
		t.Logf("SearchAny(%q): %d results", term, len(results))
	}
}

func TestFakerCachedIndex(t *testing.T) {
	// Create and populate index
	idx := NewIndex(3)

	docs := generateFakeDocuments(5000)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	// Save to file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "faker.sear")

	if err := idx.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	t.Logf("Saved index with %d n-grams", idx.NgramCount())

	// Open as cached index with small cache
	cached, err := OpenCachedIndex(path, WithCacheSize(50))
	if err != nil {
		t.Fatalf("OpenCachedIndex failed: %v", err)
	}

	t.Logf("Opened cached index, cache size limit: 50")

	// Run searches that will stress the cache
	searchTerms := []string{
		"john", "mary", "street", "avenue", "company",
		"email", "phone", "city", "country", "hello",
		"world", "test", "data", "information", "service",
	}

	for _, term := range searchTerms {
		results := cached.SearchAny(term)
		t.Logf("SearchAny(%q): %d results, cache size: %d",
			term, len(results), cached.CacheSize())
	}

	// Cache should not exceed limit
	if cached.CacheSize() > 50 {
		t.Errorf("cache exceeded limit: %d > 50", cached.CacheSize())
	}
}

func TestFakerMultiLanguageSimulation(t *testing.T) {
	idx := NewIndex(2) // 2-grams work better for CJK

	// Simulate multi-language content (faker doesn't do CJK, so we mix in some manually)
	docs := []string{
		faker.Sentence(),
		"東京都渋谷区",
		faker.Sentence(),
		"大阪市北区梅田",
		faker.Sentence(),
		"京都府京都市",
		faker.Paragraph(),
		"北海道札幌市",
	}

	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	// Search Japanese
	results := idx.Search("東京")
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("Japanese search failed: got %v", results)
	}

	// Search for shared character
	results = idx.Search("京都")
	if len(results) != 2 {
		t.Errorf("Expected 2 results for 京都, got %d", len(results))
	}
}

func TestFakerRemoveAndReindex(t *testing.T) {
	idx := NewIndex(3)

	// Initial indexing
	docs := generateFakeDocuments(100)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	initialCount := idx.NgramCount()
	t.Logf("Initial n-gram count: %d", initialCount)

	// Remove half the documents
	for i := 0; i < 50; i++ {
		idx.Remove(uint32(i))
	}

	afterRemoveCount := idx.NgramCount()
	t.Logf("After removing 50 docs: %d n-grams", afterRemoveCount)

	// N-gram count should decrease (some n-grams may have been unique to removed docs)
	if afterRemoveCount > initialCount {
		t.Error("n-gram count should not increase after removal")
	}

	// Re-index with new documents
	newDocs := generateFakeDocuments(50)
	for i, doc := range newDocs {
		idx.Add(uint32(i), doc) // Reuse IDs 0-49
	}

	finalCount := idx.NgramCount()
	t.Logf("After reindexing: %d n-grams", finalCount)
}

// Benchmarks with realistic data

func BenchmarkFakerAdd(b *testing.B) {
	docs := generateFakeDocuments(b.N)
	idx := NewIndex(3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add(uint32(i), docs[i%len(docs)])
	}
}

func BenchmarkFakerSearch(b *testing.B) {
	idx := NewIndex(3)
	docs := generateFakeDocuments(10000)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	// Generate search queries from actual content
	queries := make([]string, 100)
	for i := 0; i < 100; i++ {
		words := strings.Fields(docs[i*100])
		if len(words) >= 2 {
			queries[i] = strings.Join(words[:2], " ")
		} else {
			queries[i] = docs[i*100]
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(queries[i%len(queries)])
	}
}

func BenchmarkFakerSearchAny(b *testing.B) {
	idx := NewIndex(3)
	docs := generateFakeDocuments(10000)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	queries := []string{"the", "and", "john", "street", "company"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchAny(queries[i%len(queries)])
	}
}

func BenchmarkFakerCachedSearch(b *testing.B) {
	// Setup: create and save index
	idx := NewIndex(3)
	docs := generateFakeDocuments(10000)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.sear")
	idx.SaveToFile(path)

	cached, _ := OpenCachedIndex(path, WithCacheSize(100))

	queries := []string{"the", "and", "john", "street", "company"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cached.SearchAny(queries[i%len(queries)])
	}
}

func BenchmarkFakerSerialize(b *testing.B) {
	idx := NewIndex(3)
	docs := generateFakeDocuments(10000)
	for i, doc := range docs {
		idx.Add(uint32(i), doc)
	}

	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.sear", i))
		idx.SaveToFile(path)
	}
}
