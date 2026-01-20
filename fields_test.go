package roaringsearch

import (
	"container/heap"
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

func TestSortColumnGet(t *testing.T) {
	col := NewSortColumn[uint16]()

	col.Set(1, 100)
	col.Set(2, 200)
	col.Set(5, 500)

	// Get existing values
	if v := col.Get(1); v != 100 {
		t.Errorf("Get(1) = %d, want 100", v)
	}
	if v := col.Get(2); v != 200 {
		t.Errorf("Get(2) = %d, want 200", v)
	}
	if v := col.Get(5); v != 500 {
		t.Errorf("Get(5) = %d, want 500", v)
	}

	// Get non-existent (returns zero value)
	if v := col.Get(999); v != 0 {
		t.Errorf("Get(999) = %d, want 0", v)
	}
}

func TestSortColumnSortDesc(t *testing.T) {
	col := NewSortColumn[uint16]()

	col.Set(1, 100)
	col.Set(2, 50)
	col.Set(3, 200)
	col.Set(4, 150)

	results := col.SortDesc([]uint32{1, 2, 3, 4}, 2)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].DocID != 3 || results[0].Value != 200 {
		t.Errorf("results[0] = %+v, want {DocID:3, Value:200}", results[0])
	}
	if results[1].DocID != 4 || results[1].Value != 150 {
		t.Errorf("results[1] = %+v, want {DocID:4, Value:150}", results[1])
	}
}

func TestSortColumnHeapSort(t *testing.T) {
	col := NewSortColumn[uint16]()

	// Create enough docs to trigger heap sort (limit < len/4)
	// Need at least 100 docs with limit < 25
	for i := uint32(1); i <= 100; i++ {
		col.Set(i, uint16(i*7%1000))
	}

	docIDs := make([]uint32, 100)
	for i := range docIDs {
		docIDs[i] = uint32(i + 1)
	}

	// Limit 10 with 100 docs triggers heap sort (10 < 100/4 = 25)
	results := col.SortDesc(docIDs, 10)

	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}

	// Verify results are sorted descending
	for i := 1; i < len(results); i++ {
		if results[i].Value > results[i-1].Value {
			t.Errorf("results not sorted: [%d].Value=%d > [%d].Value=%d",
				i, results[i].Value, i-1, results[i-1].Value)
		}
	}

	// Test ascending heap sort
	resultsAsc := col.Sort(docIDs, true, 10)
	if len(resultsAsc) != 10 {
		t.Fatalf("expected 10 asc results, got %d", len(resultsAsc))
	}

	// Verify results are sorted ascending
	for i := 1; i < len(resultsAsc); i++ {
		if resultsAsc[i].Value < resultsAsc[i-1].Value {
			t.Errorf("asc results not sorted: [%d].Value=%d < [%d].Value=%d",
				i, resultsAsc[i].Value, i-1, resultsAsc[i-1].Value)
		}
	}
}

func TestSortColumnHeapSortPartialFill(t *testing.T) {
	col := NewSortColumn[uint16]()

	// Only 5 docs but request limit of 10 with heap path
	// Need to ensure heap path: create 20 docs, limit 4 (4 < 20/4 = 5)
	for i := uint32(1); i <= 20; i++ {
		col.Set(i, uint16(i*10))
	}

	docIDs := make([]uint32, 20)
	for i := range docIDs {
		docIDs[i] = uint32(i + 1)
	}

	results := col.SortDesc(docIDs, 4)

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	// Top 4 should be docs 20, 19, 18, 17 with values 200, 190, 180, 170
	if results[0].DocID != 20 || results[0].Value != 200 {
		t.Errorf("results[0] = %+v, want {DocID:20, Value:200}", results[0])
	}
}

func TestResultHeapPush(t *testing.T) {
	// Test the Push method directly since heapSort doesn't use heap.Push
	h := &resultHeap[uint16]{
		items: make([]SortedResult[uint16], 0),
		asc:   false,
	}

	// Use heap.Push to add items
	heap.Push(h, SortedResult[uint16]{DocID: 1, Value: 100})
	heap.Push(h, SortedResult[uint16]{DocID: 2, Value: 200})
	heap.Push(h, SortedResult[uint16]{DocID: 3, Value: 50})

	if h.Len() != 3 {
		t.Errorf("heap len = %d, want 3", h.Len())
	}

	// Pop should return smallest first (min-heap for descending sort)
	result := heap.Pop(h).(SortedResult[uint16])
	if result.Value != 50 {
		t.Errorf("first pop value = %d, want 50", result.Value)
	}
}

func TestBitmapFilterBatch(t *testing.T) {
	filter := NewBitmapFilter()

	batch := filter.Batch("media_type")
	batch.Add(1, "book")
	batch.Add(2, "book")
	batch.Add(3, "movie")
	batch.Add(4, "book")
	batch.Add(5, "movie")
	batch.Flush()

	books := filter.Get("media_type", "book")
	if books.GetCardinality() != 3 {
		t.Errorf("books = %d, want 3", books.GetCardinality())
	}

	movies := filter.Get("media_type", "movie")
	if movies.GetCardinality() != 2 {
		t.Errorf("movies = %d, want 2", movies.GetCardinality())
	}

	// Test batch reuse
	batch.Add(6, "music")
	batch.Flush()

	music := filter.Get("media_type", "music")
	if music.GetCardinality() != 1 {
		t.Errorf("music = %d, want 1", music.GetCardinality())
	}

	// Empty flush should not panic
	batch.Flush()
}

func TestSortColumnBatch(t *testing.T) {
	col := NewSortColumn[uint16]()

	batch := col.Batch()
	batch.Add(1, 100)
	batch.Add(2, 50)
	batch.Add(3, 200)
	batch.Add(4, 150)
	batch.Flush()

	// Verify values
	if v := col.Get(1); v != 100 {
		t.Errorf("Get(1) = %d, want 100", v)
	}
	if v := col.Get(2); v != 50 {
		t.Errorf("Get(2) = %d, want 50", v)
	}
	if v := col.Get(3); v != 200 {
		t.Errorf("Get(3) = %d, want 200", v)
	}
	if v := col.Get(4); v != 150 {
		t.Errorf("Get(4) = %d, want 150", v)
	}

	// Test sorting works correctly
	results := col.SortDesc([]uint32{1, 2, 3, 4}, 0)
	if results[0].DocID != 3 || results[0].Value != 200 {
		t.Errorf("results[0] = %+v, want {DocID:3, Value:200}", results[0])
	}

	// Test batch reuse
	batch.Add(5, 300)
	batch.Flush()

	if v := col.Get(5); v != 300 {
		t.Errorf("Get(5) = %d, want 300", v)
	}

	// Empty flush should not panic
	batch.Flush()
}

func TestBitmapFilterAllCounts(t *testing.T) {
	filter := NewBitmapFilter()

	filter.Set(1, "media_type", "book")
	filter.Set(2, "media_type", "book")
	filter.Set(3, "media_type", "movie")
	filter.Set(1, "language", "english")
	filter.Set(2, "language", "spanish")
	filter.Set(3, "language", "english")

	allCounts := filter.AllCounts()

	if len(allCounts) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(allCounts))
	}

	if allCounts["media_type"]["book"] != 2 {
		t.Errorf("media_type.book = %d, want 2", allCounts["media_type"]["book"])
	}
	if allCounts["media_type"]["movie"] != 1 {
		t.Errorf("media_type.movie = %d, want 1", allCounts["media_type"]["movie"])
	}
	if allCounts["language"]["english"] != 2 {
		t.Errorf("language.english = %d, want 2", allCounts["language"]["english"])
	}
	if allCounts["language"]["spanish"] != 1 {
		t.Errorf("language.spanish = %d, want 1", allCounts["language"]["spanish"])
	}
}

func BenchmarkBatch(b *testing.B) {
	const numDocs = 1_000_000
	categories := []string{
		"electronics", "books", "clothing", "home", "sports",
		"toys", "automotive", "garden", "health", "beauty",
		"grocery", "pets",
	}

	b.Run("BitmapFilter/Set", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			filter := NewBitmapFilter()
			for i := uint32(1); i <= numDocs; i++ {
				filter.Set(i, "category", categories[int(i)%len(categories)])
			}
		}
	})

	b.Run("BitmapFilter/Batch", func(b *testing.B) {
		// Pre-compute category strings
		cats := make([]string, numDocs)
		for i := uint32(1); i <= numDocs; i++ {
			cats[i-1] = categories[int(i)%len(categories)]
		}
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter := NewBitmapFilter()
			batch := filter.BatchSize("category", numDocs)
			for i := uint32(1); i <= numDocs; i++ {
				batch.Add(i, cats[i-1])
			}
			batch.Flush()
		}
	})

	b.Run("SortColumn/Set", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			col := NewSortColumn[uint16]()
			for i := uint32(1); i <= numDocs; i++ {
				col.Set(i, uint16(i*7%65536))
			}
		}
	})

	b.Run("SortColumn/Batch", func(b *testing.B) {
		// Pre-compute values
		values := make([]uint16, numDocs)
		for i := uint32(1); i <= numDocs; i++ {
			values[i-1] = uint16(i * 7 % 65536)
		}
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col := NewSortColumn[uint16]()
			batch := col.BatchSize(numDocs)
			for i := uint32(1); i <= numDocs; i++ {
				batch.Add(i, values[i-1])
			}
			batch.Flush()
		}
	})
}
