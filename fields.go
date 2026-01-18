package roaringsearch

import (
	"cmp"
	"container/heap"
	"io"
	"os"
	"slices"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/vmihailenco/msgpack/v5"
)

// BitmapFilter provides fast filtering by multiple category fields using bitmap indexes.
// Each field (e.g., "media_type", "language") can have multiple category values
// (e.g., "book", "movie"), and each category maps to a bitmap of document IDs.
//
// Example:
//
//	filter := NewBitmapFilter()
//	filter.Set(1, "media_type", "book")
//	filter.Set(1, "language", "english")
//	filter.Set(2, "media_type", "movie")
//
//	books := filter.Get("media_type", "book")           // bitmap of books
//	english := filter.Get("language", "english")        // bitmap of english
//	englishBooks := roaring.And(books, english)         // AND filter
type BitmapFilter struct {
	mu     sync.RWMutex
	fields map[string]map[string]*roaring.Bitmap
}

// NewBitmapFilter creates a new bitmap filter.
func NewBitmapFilter() *BitmapFilter {
	return &BitmapFilter{
		fields: make(map[string]map[string]*roaring.Bitmap),
	}
}

// Set assigns a document to a category within a field.
func (c *BitmapFilter) Set(docID uint32, field, category string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(docID, field, category)
}

func (c *BitmapFilter) setLocked(docID uint32, field, category string) {
	fieldMap, ok := c.fields[field]
	if !ok {
		fieldMap = make(map[string]*roaring.Bitmap)
		c.fields[field] = fieldMap
	}

	bm, ok := fieldMap[category]
	if !ok {
		bm = roaring.New()
		fieldMap[category] = bm
	}
	bm.Add(docID)
}

// FilterBatch accumulates entries for efficient batch insertion.
type FilterBatch struct {
	filter     *BitmapFilter
	field      string
	docIDs     []uint32
	categories []string
}

// Batch creates a new batch builder for the given field.
// Use BatchSize for better performance when you know the approximate count.
func (c *BitmapFilter) Batch(field string) *FilterBatch {
	return c.BatchSize(field, 1024)
}

// BatchSize creates a batch builder with pre-allocated capacity.
func (c *BitmapFilter) BatchSize(field string, size int) *FilterBatch {
	return &FilterBatch{
		filter:     c,
		field:      field,
		docIDs:     make([]uint32, 0, size),
		categories: make([]string, 0, size),
	}
}

// Add adds a document with a category to the batch.
func (b *FilterBatch) Add(docID uint32, category string) {
	b.docIDs = append(b.docIDs, docID)
	b.categories = append(b.categories, category)
}

// Flush commits all accumulated entries to the filter.
func (b *FilterBatch) Flush() {
	if len(b.docIDs) == 0 {
		return
	}

	n := len(b.docIDs)

	// Pass 1: build integer index array using linear search (fast for few categories)
	categoryList := make([]string, 0, 16)
	indices := make([]int, n)

	for i, cat := range b.categories {
		// Linear search - faster than map for small category counts
		idx := -1
		for j, existing := range categoryList {
			if existing == cat {
				idx = j
				break
			}
		}
		if idx == -1 {
			idx = len(categoryList)
			categoryList = append(categoryList, cat)
		}
		indices[i] = idx
	}

	numCats := len(categoryList)

	// Pass 2: count per category (fast integer array access)
	counts := make([]int, numCats)
	for _, idx := range indices {
		counts[idx]++
	}

	// Pre-allocate exact-sized groups
	groups := make([][]uint32, numCats)
	for i, count := range counts {
		groups[i] = make([]uint32, 0, count)
	}

	// Pass 3: fill groups (fast integer array access, no reallocation)
	for i, idx := range indices {
		groups[idx] = append(groups[idx], b.docIDs[i])
	}

	b.filter.mu.Lock()
	defer b.filter.mu.Unlock()

	fieldMap, ok := b.filter.fields[b.field]
	if !ok {
		fieldMap = make(map[string]*roaring.Bitmap)
		b.filter.fields[b.field] = fieldMap
	}

	// Create bitmaps
	bitmaps := make([]*roaring.Bitmap, numCats)
	for idx := range groups {
		cat := categoryList[idx]
		bm, ok := fieldMap[cat]
		if !ok {
			bm = roaring.New()
			fieldMap[cat] = bm
		}
		bitmaps[idx] = bm
	}

	// Parallel AddMany if enough categories
	if numCats >= 4 {
		var wg sync.WaitGroup
		for idx, ids := range groups {
			wg.Add(1)
			go func(bm *roaring.Bitmap, docIDs []uint32) {
				defer wg.Done()
				bm.AddMany(docIDs)
			}(bitmaps[idx], ids)
		}
		wg.Wait()
	} else {
		for idx, ids := range groups {
			bitmaps[idx].AddMany(ids)
		}
	}

	// Clear for reuse
	b.docIDs = b.docIDs[:0]
	b.categories = b.categories[:0]
}

// Remove removes a document from all categories across all fields.
func (c *BitmapFilter) Remove(docID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, fieldMap := range c.fields {
		for _, bm := range fieldMap {
			bm.Remove(docID)
		}
	}
}

// Get returns a bitmap of documents in the given category for a field.
// Returns nil if field or category doesn't exist.
func (c *BitmapFilter) Get(field, category string) *roaring.Bitmap {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fieldMap, ok := c.fields[field]
	if !ok {
		return nil
	}
	return fieldMap[category]
}

// GetAny returns a bitmap of documents in ANY of the given categories (OR).
func (c *BitmapFilter) GetAny(field string, categories []string) *roaring.Bitmap {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fieldMap, ok := c.fields[field]
	if !ok {
		return roaring.New()
	}

	result := roaring.New()
	for _, cat := range categories {
		if bm, ok := fieldMap[cat]; ok {
			result.Or(bm)
		}
	}
	return result
}

// Categories returns all category values for a given field.
func (c *BitmapFilter) Categories(field string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fieldMap, ok := c.fields[field]
	if !ok {
		return nil
	}

	cats := make([]string, 0, len(fieldMap))
	for cat := range fieldMap {
		cats = append(cats, cat)
	}
	return cats
}

// Counts returns the number of documents in each category for a field.
func (c *BitmapFilter) Counts(field string) map[string]uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fieldMap, ok := c.fields[field]
	if !ok {
		return nil
	}

	counts := make(map[string]uint64, len(fieldMap))
	for cat, bm := range fieldMap {
		counts[cat] = bm.GetCardinality()
	}
	return counts
}

// AllCounts returns counts for all fields and categories.
func (c *BitmapFilter) AllCounts() map[string]map[string]uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]map[string]uint64, len(c.fields))
	for field, fieldMap := range c.fields {
		counts := make(map[string]uint64, len(fieldMap))
		for cat, bm := range fieldMap {
			counts[cat] = bm.GetCardinality()
		}
		result[field] = counts
	}
	return result
}

// MemoryUsage returns the total memory used by all bitmaps in bytes.
func (c *BitmapFilter) MemoryUsage() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var total uint64
	for _, fieldMap := range c.fields {
		for _, bm := range fieldMap {
			total += bm.GetSizeInBytes()
		}
	}
	return total
}

// bitmapFilterData is the serializable representation.
type bitmapFilterData struct {
	Fields map[string]map[string][]byte `msgpack:"fields"`
}

// SaveToFile saves the bitmap filter to a file atomically.
// Writes to a temp file first, then renames to prevent corruption on crash.
func (c *BitmapFilter) SaveToFile(path string) error {
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := c.Encode(file); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

// Encode writes the bitmap filter to a writer.
func (c *BitmapFilter) Encode(w io.Writer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data := bitmapFilterData{
		Fields: make(map[string]map[string][]byte, len(c.fields)),
	}

	for field, fieldMap := range c.fields {
		data.Fields[field] = make(map[string][]byte, len(fieldMap))
		for cat, bm := range fieldMap {
			bytes, err := bm.ToBytes()
			if err != nil {
				return err
			}
			data.Fields[field][cat] = bytes
		}
	}

	return msgpack.NewEncoder(w).Encode(data)
}

// LoadBitmapFilter loads a bitmap filter from a file.
func LoadBitmapFilter(path string) (*BitmapFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ReadBitmapFilter(file)
}

// ReadBitmapFilter reads a bitmap filter from a reader.
func ReadBitmapFilter(r io.Reader) (*BitmapFilter, error) {
	var data bitmapFilterData
	if err := msgpack.NewDecoder(r).Decode(&data); err != nil {
		return nil, err
	}

	c := &BitmapFilter{
		fields: make(map[string]map[string]*roaring.Bitmap, len(data.Fields)),
	}

	for field, fieldMap := range data.Fields {
		c.fields[field] = make(map[string]*roaring.Bitmap, len(fieldMap))
		for cat, bytes := range fieldMap {
			bm := roaring.New()
			if err := bm.UnmarshalBinary(bytes); err != nil {
				return nil, err
			}
			c.fields[field][cat] = bm
		}
	}

	return c, nil
}

// SortColumn provides a typed columnar array for sorting documents by a value.
// Uses heap-based partial sort for efficient top-K queries.
//
// Example:
//
//	ratings := NewSortColumn[uint16]()
//	ratings.Set(1, 85)
//	ratings.Set(2, 92)
//
//	// Sort all docs
//	results := ratings.Sort([]uint32{1, 2}, false, 10)
//
//	// Sort filtered docs from a bitmap
//	results := ratings.SortBitmapDesc(filteredBitmap, 100)
type SortColumn[T cmp.Ordered] struct {
	mu       sync.RWMutex
	values   []T
	maxDocID uint32
}

// SortedResult holds a document ID and its sort value.
type SortedResult[T cmp.Ordered] struct {
	DocID uint32
	Value T
}

// NewSortColumn creates a new typed sort column.
func NewSortColumn[T cmp.Ordered]() *SortColumn[T] {
	return &SortColumn[T]{
		values: make([]T, 0),
	}
}

// Set sets the value for a document.
func (col *SortColumn[T]) Set(docID uint32, value T) {
	col.mu.Lock()
	defer col.mu.Unlock()
	col.setLocked(docID, value)
}

func (col *SortColumn[T]) setLocked(docID uint32, value T) {
	// Grow array if needed
	if docID >= uint32(len(col.values)) {
		newSize := docID + 1
		if newSize < uint32(len(col.values)*5/4) {
			newSize = uint32(len(col.values) * 5 / 4)
		}
		if newSize < 1024 {
			newSize = 1024
		}
		newValues := make([]T, newSize)
		copy(newValues, col.values)
		col.values = newValues
	}

	col.values[docID] = value

	if docID > col.maxDocID {
		col.maxDocID = docID
	}
}

// SortColumnBatch accumulates entries for efficient batch insertion.
type SortColumnBatch[T cmp.Ordered] struct {
	col    *SortColumn[T]
	docIDs []uint32
	values []T
}

// Batch creates a new batch builder for this column.
// Use BatchSize for better performance when you know the approximate count.
func (col *SortColumn[T]) Batch() *SortColumnBatch[T] {
	return col.BatchSize(1024)
}

// BatchSize creates a batch builder with pre-allocated capacity.
func (col *SortColumn[T]) BatchSize(size int) *SortColumnBatch[T] {
	return &SortColumnBatch[T]{
		col:    col,
		docIDs: make([]uint32, 0, size),
		values: make([]T, 0, size),
	}
}

// Add adds a document with a value to the batch.
func (b *SortColumnBatch[T]) Add(docID uint32, value T) {
	b.docIDs = append(b.docIDs, docID)
	b.values = append(b.values, value)
}

// Flush commits all accumulated entries to the column.
func (b *SortColumnBatch[T]) Flush() {
	if len(b.docIDs) == 0 {
		return
	}

	// Find max docID to pre-allocate
	var maxID uint32
	for _, id := range b.docIDs {
		if id > maxID {
			maxID = id
		}
	}

	b.col.mu.Lock()
	defer b.col.mu.Unlock()

	// Pre-allocate if needed
	if maxID >= uint32(len(b.col.values)) {
		newValues := make([]T, maxID+1)
		copy(newValues, b.col.values)
		b.col.values = newValues
	}

	// Set all values
	for i, id := range b.docIDs {
		b.col.values[id] = b.values[i]
		if id > b.col.maxDocID {
			b.col.maxDocID = id
		}
	}

	// Clear for reuse
	b.docIDs = b.docIDs[:0]
	b.values = b.values[:0]
}

// Get returns the value for a document.
func (col *SortColumn[T]) Get(docID uint32) T {
	col.mu.RLock()
	defer col.mu.RUnlock()

	var zero T
	if docID >= uint32(len(col.values)) {
		return zero
	}
	return col.values[docID]
}

// MemoryUsage returns the memory used by the values array in bytes.
func (col *SortColumn[T]) MemoryUsage() uint64 {
	col.mu.RLock()
	defer col.mu.RUnlock()

	var zero T
	return uint64(len(col.values)) * uint64(unsafe.Sizeof(zero))
}

// Sort sorts document IDs by their value.
// Uses heap-based partial sort when limit is small relative to input.
func (col *SortColumn[T]) Sort(docIDs []uint32, asc bool, limit int) []SortedResult[T] {
	col.mu.RLock()
	defer col.mu.RUnlock()

	return col.sortLocked(docIDs, asc, limit)
}

// SortDesc is a convenience method for descending sort.
func (col *SortColumn[T]) SortDesc(docIDs []uint32, limit int) []SortedResult[T] {
	return col.Sort(docIDs, false, limit)
}

// SortBitmap sorts documents from a bitmap by their value.
func (col *SortColumn[T]) SortBitmap(bm *roaring.Bitmap, asc bool, limit int) []SortedResult[T] {
	if bm == nil || bm.IsEmpty() {
		return nil
	}

	col.mu.RLock()
	defer col.mu.RUnlock()

	return col.sortLocked(bm.ToArray(), asc, limit)
}

// SortBitmapDesc is a convenience method for descending bitmap sort.
func (col *SortColumn[T]) SortBitmapDesc(bm *roaring.Bitmap, limit int) []SortedResult[T] {
	return col.SortBitmap(bm, false, limit)
}

func (col *SortColumn[T]) sortLocked(docIDs []uint32, asc bool, limit int) []SortedResult[T] {
	if len(docIDs) == 0 {
		return nil
	}

	values := col.values

	// Use heap for partial sort when limit is small relative to input
	if limit > 0 && limit < len(docIDs)/4 {
		return col.heapSort(docIDs, values, asc, limit)
	}

	// Full sort
	results := make([]SortedResult[T], len(docIDs))
	for i, docID := range docIDs {
		var value T
		if docID < uint32(len(values)) {
			value = values[docID]
		}
		results[i] = SortedResult[T]{DocID: docID, Value: value}
	}

	if asc {
		slices.SortFunc(results, func(a, b SortedResult[T]) int {
			return cmp.Compare(a.Value, b.Value)
		})
	} else {
		slices.SortFunc(results, func(a, b SortedResult[T]) int {
			return cmp.Compare(b.Value, a.Value)
		})
	}

	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}

	return results
}

func (col *SortColumn[T]) heapSort(docIDs []uint32, values []T, asc bool, limit int) []SortedResult[T] {
	h := &resultHeap[T]{
		items: make([]SortedResult[T], 0, limit),
		asc:   asc,
	}

	for _, docID := range docIDs {
		var value T
		if docID < uint32(len(values)) {
			value = values[docID]
		}

		if h.Len() < limit {
			h.items = append(h.items, SortedResult[T]{DocID: docID, Value: value})
			if h.Len() == limit {
				heap.Init(h)
			}
		} else {
			top := h.items[0]
			better := (asc && value < top.Value) || (!asc && value > top.Value)
			if better {
				h.items[0] = SortedResult[T]{DocID: docID, Value: value}
				heap.Fix(h, 0)
			}
		}
	}

	if h.Len() < limit && h.Len() > 0 {
		heap.Init(h)
	}

	results := make([]SortedResult[T], h.Len())
	for i := len(results) - 1; i >= 0; i-- {
		results[i] = heap.Pop(h).(SortedResult[T])
	}

	return results
}

// resultHeap implements heap.Interface for SortedResult.
type resultHeap[T cmp.Ordered] struct {
	items []SortedResult[T]
	asc   bool
}

func (h *resultHeap[T]) Len() int { return len(h.items) }

func (h *resultHeap[T]) Less(i, j int) bool {
	if h.asc {
		return h.items[i].Value > h.items[j].Value // max-heap for ascending
	}
	return h.items[i].Value < h.items[j].Value // min-heap for descending
}

func (h *resultHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *resultHeap[T]) Push(x any) {
	h.items = append(h.items, x.(SortedResult[T]))
}

func (h *resultHeap[T]) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

// sortColumnData is the serializable representation.
type sortColumnData[T cmp.Ordered] struct {
	Values   []T    `msgpack:"values"`
	MaxDocID uint32 `msgpack:"max_doc_id"`
}

// SaveToFile saves the sort column to a file atomically.
// Writes to a temp file first, then renames to prevent corruption on crash.
func (col *SortColumn[T]) SaveToFile(path string) error {
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := col.Encode(file); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

// Encode writes the sort column to a writer.
func (col *SortColumn[T]) Encode(w io.Writer) error {
	col.mu.RLock()
	defer col.mu.RUnlock()

	data := sortColumnData[T]{
		Values:   col.values[:col.maxDocID+1],
		MaxDocID: col.maxDocID,
	}

	return msgpack.NewEncoder(w).Encode(data)
}

// LoadSortColumn loads a sort column from a file.
func LoadSortColumn[T cmp.Ordered](path string) (*SortColumn[T], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ReadSortColumn[T](file)
}

// ReadSortColumn reads a sort column from a reader.
func ReadSortColumn[T cmp.Ordered](r io.Reader) (*SortColumn[T], error) {
	var data sortColumnData[T]
	if err := msgpack.NewDecoder(r).Decode(&data); err != nil {
		return nil, err
	}

	return &SortColumn[T]{
		values:   data.Values,
		maxDocID: data.MaxDocID,
	}, nil
}
