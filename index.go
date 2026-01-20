package roaringsearch

import (
	"runtime"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// SearchResult holds search results with scoring information.
type SearchResult struct {
	DocIDs []uint32       // Document IDs matching the search
	Scores map[uint32]int // Number of n-grams matched per document
}

// Index is an n-gram based text search index using roaring bitmaps.
// It uses packed byte values as map keys for efficient lookups.
// Supports gram sizes 1-8 (bytes packed into uint64).
type Index struct {
	mu              sync.RWMutex
	gramSize        int
	normalizer      Normalizer
	bitmaps         map[uint64]*roaring.Bitmap
	useASCIFastPath bool // true when using default normalizer
}

// NewIndex creates a new Index with the specified gram size.
// Default normalizer is NormalizeLowercaseAlphanumeric.
// Gram size is clamped to 1-8 (defaults to 3).
func NewIndex(gramSize int, opts ...Option) *Index {
	if gramSize <= 0 {
		gramSize = 3
	}
	if gramSize > 8 {
		gramSize = 8 // Max 8 bytes fit in uint64
	}

	idx := &Index{
		gramSize:        gramSize,
		normalizer:      NormalizeLowercaseAlphanumeric,
		bitmaps:         make(map[uint64]*roaring.Bitmap),
		useASCIFastPath: true, // default normalizer supports fast path
	}

	for _, opt := range opts {
		opt(idx)
	}

	return idx
}

// GramSize returns the n-gram size used by this index.
func (idx *Index) GramSize() int {
	return idx.gramSize
}

// NgramCount returns the number of unique n-grams in the index.
func (idx *Index) NgramCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.bitmaps)
}

// getOrCreateBitmap returns the bitmap for the key, creating it if needed.
func (idx *Index) getOrCreateBitmap(key uint64) *roaring.Bitmap {
	bm, exists := idx.bitmaps[key]
	if !exists {
		bm = roaring.New()
		idx.bitmaps[key] = bm
	}
	return bm
}

// addRuneBasedNgrams indexes a document using rune-based n-gram processing.
func (idx *Index) addRuneBasedNgrams(docID uint32, text string) {
	normalized := idx.normalizer(text)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return
	}

	seen := make([]uint64, 0, len(runes)-idx.gramSize+1)

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])

		if containsKey(seen, key) {
			continue
		}
		seen = append(seen, key)

		idx.getOrCreateBitmap(key).Add(docID)
	}
}

// Add indexes a document with the given ID and text.
// Uses fast ASCII path when possible, falls back to rune-based for Unicode.
func (idx *Index) Add(docID uint32, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.useASCIFastPath {
		keys := make([]uint64, 0, 64)
		keys, ok := normalizeAndKeyASCII(text, idx.gramSize, keys)
		if ok {
			for _, key := range keys {
				idx.getOrCreateBitmap(key).Add(docID)
			}
			return
		}
	}

	idx.addRuneBasedNgrams(docID, text)
}

// addBatch indexes multiple documents efficiently using parallel processing.
func (idx *Index) addBatch(docs []document) {
	idx.addBatchN(docs, 0)
}

// localIndex holds per-worker bitmap data during batch indexing.
type localIndex struct {
	bitmaps map[uint64]*roaring.Bitmap
}

// addKeyToBitmap adds a document ID to the bitmap for the given key.
func (local *localIndex) addKeyToBitmap(key uint64, docID uint32) {
	bm, exists := local.bitmaps[key]
	if !exists {
		bm = roaring.New()
		local.bitmaps[key] = bm
	}
	bm.Add(docID)
}

// processDocASCII processes a document using the fast ASCII path.
func (idx *Index) processDocASCII(doc document, local *localIndex, keys []uint64, buf []byte) ([]uint64, []byte, bool) {
	var ok bool
	keys, buf, ok = normalizeAndKeyASCIIPooled(doc.text, idx.gramSize, keys, buf)
	if !ok {
		return keys, buf, false
	}
	for _, key := range keys {
		local.addKeyToBitmap(key, doc.id)
	}
	return keys, buf, true
}

// processDocUnicode processes a document using rune-based Unicode handling.
func (idx *Index) processDocUnicode(doc document, local *localIndex, seen []uint64) []uint64 {
	normalized := idx.normalizer(doc.text)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return seen
	}

	seen = seen[:0]
	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if !containsKey(seen, key) {
			seen = append(seen, key)
			local.addKeyToBitmap(key, doc.id)
		}
	}
	return seen
}

// containsKey checks if key exists in the slice.
func containsKey(keys []uint64, key uint64) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// addBatchN indexes multiple documents with a specified number of workers.
func (idx *Index) addBatchN(docs []document, workers int) {
	if len(docs) == 0 {
		return
	}
	workers = idx.clampWorkers(workers, len(docs))

	localIndexes := idx.initLocalIndexes(workers, len(docs))

	var wg sync.WaitGroup
	chunkSize := (len(docs) + workers - 1) / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go idx.processChunk(docs, w, chunkSize, &localIndexes[w], &wg)
	}

	wg.Wait()
	idx.mergeLocalIndexes(localIndexes)
}

// clampWorkers adjusts worker count based on document count.
func (idx *Index) clampWorkers(workers, docCount int) int {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if workers > docCount {
		workers = docCount
	}
	if docCount < 100 && workers > 1 {
		workers = 1
	}
	return workers
}

// initLocalIndexes creates per-worker local indexes.
func (idx *Index) initLocalIndexes(workers, docCount int) []localIndex {
	docsPerWorker := (docCount + workers - 1) / workers
	estimatedNgrams := docsPerWorker * 50
	if estimatedNgrams > 10000 {
		estimatedNgrams = 10000
	}

	localIndexes := make([]localIndex, workers)
	for i := range localIndexes {
		localIndexes[i].bitmaps = make(map[uint64]*roaring.Bitmap, estimatedNgrams)
	}
	return localIndexes
}

// processChunk processes a chunk of documents for a worker.
func (idx *Index) processChunk(docs []document, workerID, chunkSize int, local *localIndex, wg *sync.WaitGroup) {
	defer wg.Done()

	start := workerID * chunkSize
	end := start + chunkSize
	if end > len(docs) {
		end = len(docs)
	}
	if start >= len(docs) {
		return
	}

	keys := make([]uint64, 0, 64)
	buf := make([]byte, 0, 256)
	seen := make([]uint64, 0, 64)

	for _, doc := range docs[start:end] {
		if idx.useASCIFastPath {
			var ok bool
			keys, buf, ok = idx.processDocASCII(doc, local, keys, buf)
			if ok {
				continue
			}
		}
		seen = idx.processDocUnicode(doc, local, seen)
	}
}

// mergeLocalIndexes merges all local indexes into the main index.
// Uses parallel pairwise reduction for better performance with many workers.
func (idx *Index) mergeLocalIndexes(localIndexes []localIndex) {
	if len(localIndexes) == 0 {
		return
	}

	// Parallel pairwise reduction: 16 -> 8 -> 4 -> 2 -> 1
	for len(localIndexes) > 1 {
		half := (len(localIndexes) + 1) / 2
		var wg sync.WaitGroup

		for i := 0; i < len(localIndexes)/2; i++ {
			wg.Add(1)
			go func(dst, src int) {
				defer wg.Done()
				mergeTwoLocals(&localIndexes[dst], &localIndexes[src])
			}(i, half+i)
		}
		wg.Wait()
		localIndexes = localIndexes[:half]
	}

	// Final merge into main index - incremental to allow reads between batches
	local := localIndexes[0].bitmaps
	keys := make([]uint64, 0, len(local))
	for k := range local {
		keys = append(keys, k)
	}

	const mergeBatchSize = 1000
	for i := 0; i < len(keys); i += mergeBatchSize {
		end := i + mergeBatchSize
		if end > len(keys) {
			end = len(keys)
		}

		idx.mu.Lock()
		for _, key := range keys[i:end] {
			localBm := local[key]
			if bm, ok := idx.bitmaps[key]; ok {
				bm.Or(localBm)
			} else {
				idx.bitmaps[key] = localBm
			}
			delete(local, key) // free memory as we go
		}
		idx.mu.Unlock()
	}
}

// mergeTwoLocals merges src into dst.
func mergeTwoLocals(dst, src *localIndex) {
	for key, srcBm := range src.bitmaps {
		if dstBm, ok := dst.bitmaps[key]; ok {
			dstBm.Or(srcBm)
		} else {
			dst.bitmaps[key] = srcBm
		}
	}
}

// document represents a document to be indexed (internal use).
type document struct {
	id   uint32
	text string
}

// IndexBatch accumulates documents for efficient batch insertion.
type IndexBatch struct {
	idx  *Index
	docs []document
}

// Batch creates a new batch builder for this index.
// Use BatchSize for better performance when you know the approximate count.
func (idx *Index) Batch() *IndexBatch {
	return idx.BatchSize(1024)
}

// BatchSize creates a batch builder with pre-allocated capacity.
func (idx *Index) BatchSize(size int) *IndexBatch {
	return &IndexBatch{
		idx:  idx,
		docs: make([]document, 0, size),
	}
}

// Add adds a document to the batch.
func (b *IndexBatch) Add(docID uint32, text string) {
	b.docs = append(b.docs, document{id: docID, text: text})
}

// Flush commits all accumulated documents to the index using parallel processing.
func (b *IndexBatch) Flush() {
	if len(b.docs) == 0 {
		return
	}

	b.idx.addBatch(b.docs)

	// Clear for reuse
	b.docs = b.docs[:0]
}

// Remove removes a document from the index.
func (idx *Index) Remove(docID uint32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for key, bm := range idx.bitmaps {
		bm.Remove(docID)
		if bm.IsEmpty() {
			delete(idx.bitmaps, key)
		}
	}
}

// Clear removes all documents from the index.
func (idx *Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.bitmaps = make(map[uint64]*roaring.Bitmap)
}

// Search performs an AND search for documents containing all n-grams of the query.
// Uses rune-based n-gram generation for consistent Unicode support.
func (idx *Index) Search(query string) []uint32 {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bitmaps := make([]*roaring.Bitmap, 0, len(runes)-idx.gramSize+1)
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		bm, ok := idx.bitmaps[key]
		if !ok {
			return nil
		}
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 0 {
		return nil
	}

	if len(bitmaps) == 1 {
		return bitmaps[0].ToArray()
	}

	// Sort by cardinality for better performance
	sort.Slice(bitmaps, func(i, j int) bool {
		return bitmaps[i].GetCardinality() < bitmaps[j].GetCardinality()
	})

	result := roaring.FastAnd(bitmaps...)
	if result == nil || result.IsEmpty() {
		return nil
	}

	return result.ToArray()
}

// SearchWithLimit returns up to limit matching document IDs.
// This can be faster than Search when you only need a subset of results.
func (idx *Index) SearchWithLimit(query string, limit int) []uint32 {
	if limit <= 0 {
		return nil
	}

	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bitmaps := make([]*roaring.Bitmap, 0, len(runes)-idx.gramSize+1)
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		bm, ok := idx.bitmaps[key]
		if !ok {
			return nil
		}
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 0 {
		return nil
	}

	// Sort by cardinality for better performance
	sort.Slice(bitmaps, func(i, j int) bool {
		return bitmaps[i].GetCardinality() < bitmaps[j].GetCardinality()
	})

	// Use iterator-based intersection with early termination
	results := make([]uint32, 0, limit)

	// Start with smallest bitmap and check against others
	smallest := bitmaps[0]
	rest := bitmaps[1:]

	it := smallest.Iterator()
	for it.HasNext() && len(results) < limit {
		docID := it.Next()

		// Check if docID exists in all other bitmaps
		found := true
		for _, bm := range rest {
			if !bm.Contains(docID) {
				found = false
				break
			}
		}

		if found {
			results = append(results, docID)
		}
	}

	if len(results) == 0 {
		return nil
	}

	return results
}

// SearchCallback calls the callback for each matching document ID using fast
// iterator-based intersection with early termination support.
// Returns false if callback returned false, true otherwise.
//
// This is optimized for early termination (first N results) - use it when you
// only need a subset of results without allocating a slice.
// For iterating ALL results, use SearchIterateResults which uses FastAnd.
func (idx *Index) SearchCallback(query string, cb func(docID uint32) bool) bool {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return true
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bitmaps := make([]*roaring.Bitmap, 0, len(runes)-idx.gramSize+1)
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		bm, ok := idx.bitmaps[key]
		if !ok {
			return true
		}
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 0 {
		return true
	}

	// Sort by cardinality for better performance
	sort.Slice(bitmaps, func(i, j int) bool {
		return bitmaps[i].GetCardinality() < bitmaps[j].GetCardinality()
	})

	// Start with smallest bitmap and check against others
	smallest := bitmaps[0]
	rest := bitmaps[1:]

	it := smallest.Iterator()
	for it.HasNext() {
		docID := it.Next()

		// Check if docID exists in all other bitmaps
		found := true
		for _, bm := range rest {
			if !bm.Contains(docID) {
				found = false
				break
			}
		}

		if found {
			if !cb(docID) {
				return false
			}
		}
	}

	return true
}

// SearchCount returns the count of matching documents without allocating a result slice.
func (idx *Index) SearchCount(query string) uint64 {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return 0
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bitmaps := make([]*roaring.Bitmap, 0, len(runes)-idx.gramSize+1)
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		bm, ok := idx.bitmaps[key]
		if !ok {
			return 0
		}
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 0 {
		return 0
	}

	if len(bitmaps) == 1 {
		return bitmaps[0].GetCardinality()
	}

	sort.Slice(bitmaps, func(i, j int) bool {
		return bitmaps[i].GetCardinality() < bitmaps[j].GetCardinality()
	})

	result := roaring.FastAnd(bitmaps...)
	if result == nil {
		return 0
	}
	return result.GetCardinality()
}

// SearchAny returns documents containing any n-gram of the query (OR search).
func (idx *Index) SearchAny(query string) []uint32 {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := roaring.New()
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if bm, ok := idx.bitmaps[key]; ok {
			result.Or(bm)
		}
	}

	if result.IsEmpty() {
		return nil
	}

	return result.ToArray()
}

// SearchAnyCount returns the count of documents matching any n-gram (OR search).
func (idx *Index) SearchAnyCount(query string) uint64 {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return 0
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := roaring.New()
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if bm, ok := idx.bitmaps[key]; ok {
			result.Or(bm)
		}
	}

	return result.GetCardinality()
}

// SearchThreshold returns documents containing at least threshold n-grams of the query.
// Results include scores indicating how many n-grams matched for each document.
func (idx *Index) SearchThreshold(query string, threshold int) SearchResult {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize || threshold <= 0 {
		return SearchResult{}
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Collect unique bitmaps
	bitmaps := make([]*roaring.Bitmap, 0, len(runes)-idx.gramSize+1)
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if bm, ok := idx.bitmaps[key]; ok {
			bitmaps = append(bitmaps, bm)
		}
	}

	if len(bitmaps) == 0 {
		return SearchResult{}
	}

	// Clamp threshold
	if threshold > len(bitmaps) {
		threshold = len(bitmaps)
	}

	// Count matches per document
	counts := make(map[uint32]int)
	for _, bm := range bitmaps {
		it := bm.Iterator()
		for it.HasNext() {
			counts[it.Next()]++
		}
	}

	// Filter by threshold and collect results
	var docIDs []uint32
	scores := make(map[uint32]int)

	for docID, count := range counts {
		if count >= threshold {
			docIDs = append(docIDs, docID)
			scores[docID] = count
		}
	}

	// Sort by score (descending), then by docID (ascending)
	sort.Slice(docIDs, func(i, j int) bool {
		if scores[docIDs[i]] != scores[docIDs[j]] {
			return scores[docIDs[i]] > scores[docIDs[j]]
		}
		return docIDs[i] < docIDs[j]
	})

	return SearchResult{
		DocIDs: docIDs,
		Scores: scores,
	}
}
