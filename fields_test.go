package roaringsearch

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestBitmapFilterBasic(t *testing.T) {
	filter := NewBitmapFilter()

	// Add some documents with categories
	filter.Set(1, "media_type", "books")
	filter.Set(2, "media_type", "books")
	filter.Set(3, "media_type", "movies")
	filter.Set(4, "media_type", "books")
	filter.Set(5, "media_type", "movies")

	// Test category filtering
	booksBitmap := filter.Get("media_type", "books")
	if booksBitmap == nil {
		t.Fatal("expected books bitmap")
	}
	if booksBitmap.GetCardinality() != 3 {
		t.Errorf("books count = %d, want 3", booksBitmap.GetCardinality())
	}

	moviesBitmap := filter.Get("media_type", "movies")
	if moviesBitmap.GetCardinality() != 2 {
		t.Errorf("movies count = %d, want 2", moviesBitmap.GetCardinality())
	}

	// Test non-existent category
	if filter.Get("media_type", "music") != nil {
		t.Error("expected nil for non-existent category")
	}

	// Test non-existent field
	if filter.Get("nonexistent", "books") != nil {
		t.Error("expected nil for non-existent field")
	}
}

func TestBitmapFilterMultipleFields(t *testing.T) {
	filter := NewBitmapFilter()

	// Add documents with multiple category fields
	filter.Set(1, "media_type", "book")
	filter.Set(1, "language", "english")

	filter.Set(2, "media_type", "book")
	filter.Set(2, "language", "spanish")

	filter.Set(3, "media_type", "movie")
	filter.Set(3, "language", "english")

	// Filter by media_type AND language
	books := filter.Get("media_type", "book")
	english := filter.Get("language", "english")
	englishBooks := roaring.And(books, english)

	if englishBooks.GetCardinality() != 1 {
		t.Errorf("english books = %d, want 1", englishBooks.GetCardinality())
	}
	if !englishBooks.Contains(1) {
		t.Error("english books should contain doc 1")
	}
}

func TestSortColumnSortByValue(t *testing.T) {
	col := NewSortColumn[uint16]()

	col.Set(1, 100)
	col.Set(2, 50)
	col.Set(3, 200)
	col.Set(4, 150)

	// Sort descending
	results := col.Sort([]uint32{1, 2, 3, 4}, false, 0)

	expected := []uint32{3, 4, 1, 2} // 200, 150, 100, 50
	for i, r := range results {
		if r.DocID != expected[i] {
			t.Errorf("results[%d].DocID = %d, want %d", i, r.DocID, expected[i])
		}
	}

	// Sort ascending
	results = col.Sort([]uint32{1, 2, 3, 4}, true, 0)
	expected = []uint32{2, 1, 4, 3} // 50, 100, 150, 200
	for i, r := range results {
		if r.DocID != expected[i] {
			t.Errorf("asc results[%d].DocID = %d, want %d", i, r.DocID, expected[i])
		}
	}

	// Test with limit
	results = col.Sort([]uint32{1, 2, 3, 4}, false, 2)
	if len(results) != 2 {
		t.Errorf("limit results len = %d, want 2", len(results))
	}
	if results[0].DocID != 3 || results[1].DocID != 4 {
		t.Errorf("limit results = %v, want [3, 4]", results)
	}
}

func TestBitmapFilterGetAny(t *testing.T) {
	filter := NewBitmapFilter()

	filter.Set(1, "media_type", "books")
	filter.Set(2, "media_type", "movies")
	filter.Set(3, "media_type", "music")
	filter.Set(4, "media_type", "books")

	// OR filter: books OR movies
	combined := filter.GetAny("media_type", []string{"books", "movies"})
	if combined.GetCardinality() != 3 {
		t.Errorf("combined count = %d, want 3", combined.GetCardinality())
	}
	if !combined.Contains(1) || !combined.Contains(2) || !combined.Contains(4) {
		t.Error("combined should contain docs 1, 2, 4")
	}
}

func TestBitmapFilterCounts(t *testing.T) {
	filter := NewBitmapFilter()

	filter.Set(1, "media_type", "books")
	filter.Set(2, "media_type", "books")
	filter.Set(3, "media_type", "movies")

	counts := filter.Counts("media_type")

	if counts["books"] != 2 {
		t.Errorf("books count = %d, want 2", counts["books"])
	}
	if counts["movies"] != 1 {
		t.Errorf("movies count = %d, want 1", counts["movies"])
	}
}

func TestBitmapFilterRemove(t *testing.T) {
	filter := NewBitmapFilter()

	filter.Set(1, "media_type", "books")
	filter.Set(2, "media_type", "books")

	if filter.Get("media_type", "books").GetCardinality() != 2 {
		t.Error("expected 2 books before remove")
	}

	filter.Remove(1)

	if filter.Get("media_type", "books").GetCardinality() != 1 {
		t.Error("expected 1 book after remove")
	}
}

func TestSortColumnGenericTypes(t *testing.T) {
	// Test with float64
	floatCol := NewSortColumn[float64]()
	floatCol.Set(1, 3.14)
	floatCol.Set(2, 2.71)
	floatCol.Set(3, 1.41)

	results := floatCol.Sort([]uint32{1, 2, 3}, false, 0)
	if results[0].Value != 3.14 {
		t.Errorf("float sort failed: got %v", results[0].Value)
	}

	// Test with int64
	intCol := NewSortColumn[int64]()
	intCol.Set(1, -100)
	intCol.Set(2, 50)
	intCol.Set(3, 0)

	intResults := intCol.Sort([]uint32{1, 2, 3}, true, 0)
	if intResults[0].Value != -100 {
		t.Errorf("int sort failed: got %v", intResults[0].Value)
	}

	// Test with string
	strCol := NewSortColumn[string]()
	strCol.Set(1, "banana")
	strCol.Set(2, "apple")
	strCol.Set(3, "cherry")

	strResults := strCol.Sort([]uint32{1, 2, 3}, true, 0)
	if strResults[0].Value != "apple" {
		t.Errorf("string sort failed: got %v", strResults[0].Value)
	}
}

func TestBitmapFilterCategories(t *testing.T) {
	filter := NewBitmapFilter()

	filter.Set(1, "media_type", "book")
	filter.Set(2, "media_type", "movie")
	filter.Set(3, "media_type", "music")

	categories := filter.Categories("media_type")
	if len(categories) != 3 {
		t.Errorf("expected 3 categories, got %d", len(categories))
	}

	// Non-existent field
	categories = filter.Categories("nonexistent")
	if categories != nil {
		t.Error("expected nil for non-existent field")
	}
}

func TestSortColumnSortBitmapNil(t *testing.T) {
	col := NewSortColumn[uint16]()
	col.Set(1, 100)

	// Nil bitmap
	results := col.SortBitmap(nil, false, 10)
	if results != nil {
		t.Error("expected nil for nil bitmap")
	}

	// Empty bitmap
	results = col.SortBitmap(roaring.New(), false, 10)
	if results != nil {
		t.Error("expected nil for empty bitmap")
	}
}

func TestBitmapFilterPersistence(t *testing.T) {
	filter := NewBitmapFilter()

	// Add documents with multiple category fields
	filter.Set(1, "media_type", "book")
	filter.Set(1, "language", "english")

	filter.Set(2, "media_type", "movie")
	filter.Set(2, "language", "spanish")

	filter.Set(1000, "media_type", "music")
	filter.Set(1000, "language", "english")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "filter.idx")

	// Save
	if err := filter.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Load
	loaded, err := LoadBitmapFilter(path)
	if err != nil {
		t.Fatalf("LoadBitmapFilter failed: %v", err)
	}

	// Verify categories
	if loaded.Get("media_type", "book").GetCardinality() != 1 {
		t.Error("loaded book count mismatch")
	}
	if loaded.Get("media_type", "movie").GetCardinality() != 1 {
		t.Error("loaded movie count mismatch")
	}
	if loaded.Get("language", "english").GetCardinality() != 2 {
		t.Error("loaded english count mismatch")
	}

	// Verify multi-field filter still works
	books := loaded.Get("media_type", "book")
	english := loaded.Get("language", "english")
	englishBooks := roaring.And(books, english)
	if englishBooks.GetCardinality() != 1 || !englishBooks.Contains(1) {
		t.Error("multi-field filter failed after load")
	}
}

func TestSortColumnPersistence(t *testing.T) {
	col := NewSortColumn[uint16]()

	col.Set(1, 100)
	col.Set(2, 200)
	col.Set(1000, 500)

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "column.idx")

	// Save
	if err := col.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Load
	loaded, err := LoadSortColumn[uint16](path)
	if err != nil {
		t.Fatalf("LoadSortColumn failed: %v", err)
	}

	// Verify values via sorting
	results := loaded.Sort([]uint32{1, 2, 1000}, false, 0)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[0].DocID != 1000 || results[0].Value != 500 {
		t.Errorf("results[0] = %+v, want {DocID:1000, Value:500}", results[0])
	}
	if results[1].DocID != 2 || results[1].Value != 200 {
		t.Errorf("results[1] = %+v, want {DocID:2, Value:200}", results[1])
	}
}

func TestFilterAndSort(t *testing.T) {
	// Test combined category filtering and value sorting
	filter := NewBitmapFilter()
	col := NewSortColumn[uint16]()

	// Books
	filter.Set(1, "media_type", "books")
	col.Set(1, 100)
	filter.Set(2, "media_type", "books")
	col.Set(2, 50)
	filter.Set(3, "media_type", "books")
	col.Set(3, 200)

	// Movies
	filter.Set(4, "media_type", "movies")
	col.Set(4, 150)
	filter.Set(5, "media_type", "movies")
	col.Set(5, 75)

	// Music
	filter.Set(6, "media_type", "music")
	col.Set(6, 300)

	// Get books bitmap, then sort
	booksBitmap := filter.Get("media_type", "books")
	results := col.SortBitmapDesc(booksBitmap, 0)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Should be sorted: 200, 100, 50
	if results[0].DocID != 3 || results[0].Value != 200 {
		t.Errorf("results[0] = %+v, want {DocID:3, Value:200}", results[0])
	}
	if results[1].DocID != 1 || results[1].Value != 100 {
		t.Errorf("results[1] = %+v, want {DocID:1, Value:100}", results[1])
	}
	if results[2].DocID != 2 || results[2].Value != 50 {
		t.Errorf("results[2] = %+v, want {DocID:2, Value:50}", results[2])
	}
}

func TestMultiFieldFilterAndSort(t *testing.T) {
	filter := NewBitmapFilter()
	col := NewSortColumn[uint16]()

	// Doc 1: book, english
	filter.Set(1, "media_type", "book")
	filter.Set(1, "language", "english")
	col.Set(1, 100)

	// Doc 2: book, spanish
	filter.Set(2, "media_type", "book")
	filter.Set(2, "language", "spanish")
	col.Set(2, 200)

	// Doc 3: movie, english
	filter.Set(3, "media_type", "movie")
	filter.Set(3, "language", "english")
	col.Set(3, 150)

	// Filter by media_type AND language
	books := filter.Get("media_type", "book")
	english := filter.Get("language", "english")
	englishBooks := roaring.And(books, english)

	// Sort filtered results
	results := col.SortBitmapDesc(englishBooks, 10)
	if len(results) != 1 || results[0].DocID != 1 {
		t.Errorf("results = %+v, want [{DocID:1, Value:100}]", results)
	}
}

func BenchmarkFilterAndSort100M(b *testing.B) {
	filter := NewBitmapFilter()
	col := NewSortColumn[uint16]()

	// 100M docs across 12 categories with uint16 values
	categories := []string{
		"electronics", "books", "clothing", "home", "sports",
		"toys", "automotive", "garden", "health", "beauty",
		"grocery", "pets",
	}

	b.Log("Building index with 100M documents...")
	for i := uint32(1); i <= 100_000_000; i++ {
		cat := categories[int(i)%len(categories)]
		value := uint16(i * 7 % 65536)
		filter.Set(i, "category", cat)
		col.Set(i, value)
	}

	// Report memory usage
	var totalBitmapBytes uint64
	for _, fieldMap := range filter.Fields {
		for _, bm := range fieldMap {
			totalBitmapBytes += bm.GetSizeInBytes()
		}
	}
	valuesBytes := uint64(len(col.Values) * 2) // uint16 = 2 bytes
	b.Logf("Memory: bitmaps=%d bytes (%.2f MB), values=%d bytes (%.2f MB), total=%.2f MB",
		totalBitmapBytes, float64(totalBitmapBytes)/(1024*1024),
		valuesBytes, float64(valuesBytes)/(1024*1024),
		float64(totalBitmapBytes+valuesBytes)/(1024*1024))

	// Different search result sizes with limit 1000
	sizes := []int{10000, 100000, 1000000}

	for _, size := range sizes {
		searchResults := make([]uint32, size)
		for i := range searchResults {
			searchResults[i] = uint32(i*10 + 1)
		}

		b.Run(fmt.Sprintf("FilterSortLimit1k_%dk", size/1000), func(b *testing.B) {
			booksBitmap := filter.Get("category", "books")
			for i := 0; i < b.N; i++ {
				searchBitmap := roaring.BitmapOf(searchResults...)
				filtered := roaring.And(searchBitmap, booksBitmap)
				_ = col.SortBitmapDesc(filtered, 1000)
			}
		})

		b.Run(fmt.Sprintf("SortOnlyLimit1k_%dk", size/1000), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = col.SortDesc(searchResults, 1000)
			}
		})
	}
}
