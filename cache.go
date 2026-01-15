package roaringsearch

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// CachedIndex is a memory-efficient index that keeps only frequently used
// n-gram bitmaps in memory, loading others from disk on demand.
type CachedIndex struct {
	mu         sync.RWMutex
	gramSize   int
	normalizer Normalizer
	filePath   string

	// LRU cache
	cache    map[uint64]*lruEntry
	lruHead  *lruEntry // most recently used
	lruTail  *lruEntry // least recently used
	maxCache int

	// Index of n-gram positions in file for lazy loading
	ngramIndex map[uint64]ngramLocation
}

type lruEntry struct {
	key    uint64
	bitmap *roaring.Bitmap
	prev   *lruEntry
	next   *lruEntry
}

type ngramLocation struct {
	offset int64  // offset in file where bitmap data starts
	size   uint32 // size of bitmap data
}

// CachedIndexOption configures a CachedIndex.
type CachedIndexOption func(*CachedIndex)

// WithCacheSize sets the maximum number of bitmaps to keep in memory.
// Default is 1000.
func WithCacheSize(n int) CachedIndexOption {
	return func(idx *CachedIndex) {
		if n > 0 {
			idx.maxCache = n
		}
	}
}

// WithCachedNormalizer sets the normalizer for the cached index.
func WithCachedNormalizer(n Normalizer) CachedIndexOption {
	return func(idx *CachedIndex) {
		idx.normalizer = n
	}
}

// OpenCachedIndex opens an index file for cached access.
// Only metadata is loaded initially; bitmaps are loaded on demand.
func OpenCachedIndex(path string, opts ...CachedIndexOption) (*CachedIndex, error) {
	idx := &CachedIndex{
		filePath:   path,
		normalizer: NormalizeLowercaseAlphanumeric,
		cache:      make(map[uint64]*lruEntry),
		ngramIndex: make(map[uint64]ngramLocation),
		maxCache:   1000,
	}

	for _, opt := range opts {
		opt(idx)
	}

	if err := idx.loadIndex(); err != nil {
		return nil, err
	}

	return idx, nil
}

// loadIndex reads the file and builds an index of n-gram locations without loading bitmaps.
func (idx *CachedIndex) loadIndex() error {
	f, err := os.Open(idx.filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// Read header
	header := make([]byte, 8)
	if _, err := io.ReadFull(f, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if string(header[0:4]) != magicBytes {
		return ErrInvalidMagic
	}

	fileVersion := binary.LittleEndian.Uint16(header[4:6])
	if fileVersion != version {
		return ErrInvalidVersion
	}

	idx.gramSize = int(binary.LittleEndian.Uint16(header[6:8]))

	// Read n-gram count
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, countBuf); err != nil {
		return fmt.Errorf("read ngram count: %w", err)
	}
	ngramCount := binary.LittleEndian.Uint32(countBuf)

	// Build index of n-gram locations
	// Format: key(8) + size(4) + bitmap_data(size)
	currentOffset := int64(12) // header(8) + count(4)

	keyBuf := make([]byte, 8)
	sizeBuf := make([]byte, 4)

	for i := uint32(0); i < ngramCount; i++ {
		// Read n-gram key
		if _, err := io.ReadFull(f, keyBuf); err != nil {
			return fmt.Errorf("read ngram key: %w", err)
		}
		key := binary.LittleEndian.Uint64(keyBuf)
		currentOffset += 8

		// Read bitmap size
		if _, err := io.ReadFull(f, sizeBuf); err != nil {
			return fmt.Errorf("read bitmap size: %w", err)
		}
		bmSize := binary.LittleEndian.Uint32(sizeBuf)
		currentOffset += 4

		// Record location (offset where bitmap data starts)
		idx.ngramIndex[key] = ngramLocation{
			offset: currentOffset,
			size:   bmSize,
		}

		// Skip bitmap data
		if _, err := f.Seek(int64(bmSize), io.SeekCurrent); err != nil {
			return fmt.Errorf("skip bitmap: %w", err)
		}
		currentOffset += int64(bmSize)
	}

	return nil
}

// GramSize returns the n-gram size.
func (idx *CachedIndex) GramSize() int {
	return idx.gramSize
}

// NgramCount returns the number of unique n-grams in the index.
func (idx *CachedIndex) NgramCount() int {
	return len(idx.ngramIndex)
}

// CacheSize returns the current number of bitmaps in cache.
func (idx *CachedIndex) CacheSize() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.cache)
}

// getBitmap retrieves a bitmap, loading from disk if necessary.
func (idx *CachedIndex) getBitmap(key uint64) (*roaring.Bitmap, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check cache first
	if entry, ok := idx.cache[key]; ok {
		idx.moveToFront(entry)
		return entry.bitmap, true
	}

	// Check if n-gram exists
	loc, ok := idx.ngramIndex[key]
	if !ok {
		return nil, false
	}

	// Load from disk
	bm, err := idx.loadBitmap(loc)
	if err != nil {
		return nil, false
	}

	// Add to cache
	idx.addToCache(key, bm)

	return bm, true
}

func (idx *CachedIndex) loadBitmap(loc ngramLocation) (*roaring.Bitmap, error) {
	f, err := os.Open(idx.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(loc.offset, io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, loc.size)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}

	bm := roaring.New()
	if _, err := bm.ReadFrom(bytes.NewReader(data)); err != nil {
		return nil, err
	}

	return bm, nil
}

func (idx *CachedIndex) addToCache(key uint64, bm *roaring.Bitmap) {
	// Evict if at capacity
	for len(idx.cache) >= idx.maxCache && idx.lruTail != nil {
		idx.evictLRU()
	}

	entry := &lruEntry{
		key:    key,
		bitmap: bm,
	}

	idx.cache[key] = entry
	idx.addToFront(entry)
}

func (idx *CachedIndex) addToFront(entry *lruEntry) {
	entry.prev = nil
	entry.next = idx.lruHead

	if idx.lruHead != nil {
		idx.lruHead.prev = entry
	}
	idx.lruHead = entry

	if idx.lruTail == nil {
		idx.lruTail = entry
	}
}

func (idx *CachedIndex) moveToFront(entry *lruEntry) {
	if entry == idx.lruHead {
		return
	}

	// Remove from current position
	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
	if entry == idx.lruTail {
		idx.lruTail = entry.prev
	}

	// Add to front
	idx.addToFront(entry)
}

func (idx *CachedIndex) evictLRU() {
	if idx.lruTail == nil {
		return
	}

	entry := idx.lruTail
	delete(idx.cache, entry.key)

	if entry.prev != nil {
		entry.prev.next = nil
	}
	idx.lruTail = entry.prev

	if idx.lruHead == entry {
		idx.lruHead = nil
	}
}

// ClearCache removes all bitmaps from memory.
func (idx *CachedIndex) ClearCache() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.cache = make(map[uint64]*lruEntry)
	idx.lruHead = nil
	idx.lruTail = nil
}

// generateKeys generates unique n-gram keys from a query.
func (idx *CachedIndex) generateKeys(query string) []uint64 {
	normalized := idx.normalizer(query)
	runes := []rune(normalized)

	if len(runes) < idx.gramSize {
		return nil
	}

	keys := make([]uint64, 0, len(runes)-idx.gramSize+1)
	seen := make(map[uint64]struct{})

	for i := 0; i <= len(runes)-idx.gramSize; i++ {
		key := runeNgramKey(runes[i : i+idx.gramSize])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	return keys
}

// Search performs an AND search - documents containing ALL n-grams.
func (idx *CachedIndex) Search(query string) []uint32 {
	keys := idx.generateKeys(query)
	if len(keys) == 0 {
		return nil
	}

	bitmaps := make([]*roaring.Bitmap, 0, len(keys))

	for _, key := range keys {
		bm, ok := idx.getBitmap(key)
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

// SearchAny performs an OR search - documents containing ANY n-gram.
func (idx *CachedIndex) SearchAny(query string) []uint32 {
	keys := idx.generateKeys(query)
	if len(keys) == 0 {
		return nil
	}

	result := roaring.New()

	for _, key := range keys {
		if bm, ok := idx.getBitmap(key); ok {
			result.Or(bm)
		}
	}

	if result.IsEmpty() {
		return nil
	}

	return result.ToArray()
}

// SearchThreshold returns documents matching at least minMatches n-grams.
func (idx *CachedIndex) SearchThreshold(query string, minMatches int) SearchResult {
	keys := idx.generateKeys(query)
	if len(keys) == 0 || minMatches <= 0 {
		return SearchResult{}
	}

	if minMatches > len(keys) {
		minMatches = len(keys)
	}

	counts := make(map[uint32]int)

	for _, key := range keys {
		if bm, ok := idx.getBitmap(key); ok {
			it := bm.Iterator()
			for it.HasNext() {
				docID := it.Next()
				counts[docID]++
			}
		}
	}

	var docIDs []uint32
	scores := make(map[uint32]int)

	for docID, count := range counts {
		if count >= minMatches {
			docIDs = append(docIDs, docID)
			scores[docID] = count
		}
	}

	// Sort by score desc, then docID asc
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

// HasNgram checks if an n-gram exists in the index without loading it.
func (idx *CachedIndex) HasNgram(ngram string) bool {
	runes := []rune(ngram)
	if len(runes) != idx.gramSize {
		return false
	}
	key := runeNgramKey(runes)
	_, ok := idx.ngramIndex[key]
	return ok
}

// PreloadKeys loads specific n-gram keys into cache.
func (idx *CachedIndex) PreloadKeys(keys []uint64) error {
	var errs []error

	for _, key := range keys {
		if _, ok := idx.getBitmap(key); !ok {
			if _, exists := idx.ngramIndex[key]; exists {
				errs = append(errs, fmt.Errorf("failed to load key: %d", key))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
