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

// Add indexes a document with the given ID and text.
// Uses fast ASCII path when possible, falls back to rune-based for Unicode.
func (idx *Index) Add(docID uint32, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Try fast ASCII path (combines normalization and key generation)
	// Only used with default normalizer
	if idx.useASCIFastPath {
		keys := make([]uint64, 0, 64)
		keys, ok := normalizeAndKeyASCII(text, idx.gramSize, keys)
		if ok {
			for _, key := range keys {
				bm, exists := idx.bitmaps[key]
				if !exists {
					bm = roaring.New()
					idx.bitmaps[key] = bm
				}
				bm.Add(docID)
			}
			return
		}
	}

	// Fallback: rune-based processing for Unicode text or custom normalizer
	normalized := idx.normalizer(text)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return
	}

	// Use slice for deduplication - faster than map for typical doc sizes
	seen := make([]uint64, 0, len(runes)-idx.gramSize+1)

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])

		// Linear scan for duplicates (fast for small N)
		found := false
		for _, k := range seen {
			if k == key {
				found = true
				break
			}
		}
		if found {
			continue
		}
		seen = append(seen, key)

		bm, exists := idx.bitmaps[key]
		if !exists {
			bm = roaring.New()
			idx.bitmaps[key] = bm
		}
		bm.Add(docID)
	}
}

// AddBatch indexes multiple documents efficiently using parallel processing.
// This is significantly faster than calling Add repeatedly for bulk inserts.
// Uses runtime.NumCPU() workers by default.
func (idx *Index) AddBatch(docs []Document) {
	idx.AddBatchN(docs, 0)
}

// AddBatchN indexes multiple documents with a specified number of workers.
// If workers <= 0, defaults to runtime.NumCPU().
func (idx *Index) AddBatchN(docs []Document, workers int) {
	if len(docs) == 0 {
		return
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if workers > len(docs) {
		workers = len(docs)
	}
	// For small batches, use single worker to avoid goroutine overhead
	if len(docs) < 100 && workers > 1 {
		workers = 1
	}

	// Create per-worker local indexes
	// Pre-size maps based on expected unique n-grams (estimate ~50 per doc, capped at 10k)
	type localIndex struct {
		bitmaps map[uint64]*roaring.Bitmap
	}

	docsPerWorker := (len(docs) + workers - 1) / workers
	estimatedNgrams := docsPerWorker * 50
	if estimatedNgrams > 10000 {
		estimatedNgrams = 10000
	}

	localIndexes := make([]localIndex, workers)
	for i := range localIndexes {
		localIndexes[i].bitmaps = make(map[uint64]*roaring.Bitmap, estimatedNgrams)
	}

	// Process documents in parallel
	var wg sync.WaitGroup
	chunkSize := (len(docs) + workers - 1) / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := workerID * chunkSize
			end := start + chunkSize
			if end > len(docs) {
				end = len(docs)
			}
			if start >= len(docs) {
				return
			}

			local := &localIndexes[workerID]
			keys := make([]uint64, 0, 64)
			buf := make([]byte, 0, 256) // Reused buffer for ASCII normalization
			seen := make([]uint64, 0, 64)
			useFastPath := idx.useASCIFastPath

			for _, doc := range docs[start:end] {
				// Try fast ASCII path (only with default normalizer)
				if useFastPath {
					var ok bool
					keys, buf, ok = normalizeAndKeyASCIIPooled(doc.Text, idx.gramSize, keys, buf)
					if ok {
						for _, key := range keys {
							bm, exists := local.bitmaps[key]
							if !exists {
								bm = roaring.New()
								local.bitmaps[key] = bm
							}
							bm.Add(doc.ID)
						}
						continue
					}
				}

				// Fallback: rune-based processing for Unicode or custom normalizer
				normalized := idx.normalizer(doc.Text)
				runes := []rune(normalized)

				if len(runes) < idx.gramSize {
					continue
				}

				seen = seen[:0]

				for i := 0; i <= len(runes)-idx.gramSize; i++ {
					key := runeNgramKey(runes[i : i+idx.gramSize])

					found := false
					for _, k := range seen {
						if k == key {
							found = true
							break
						}
					}
					if found {
						continue
					}
					seen = append(seen, key)

					bm, exists := local.bitmaps[key]
					if !exists {
						bm = roaring.New()
						local.bitmaps[key] = bm
					}
					bm.Add(doc.ID)
				}
			}
		}(w)
	}

	wg.Wait()

	// Merge local indexes into main index
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, local := range localIndexes {
		for key, localBm := range local.bitmaps {
			if bm, ok := idx.bitmaps[key]; ok {
				bm.Or(localBm)
			} else {
				idx.bitmaps[key] = localBm
			}
		}
	}
}

// Document represents a document to be indexed.
type Document struct {
	ID   uint32
	Text string
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

