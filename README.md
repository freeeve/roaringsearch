# roaringsearch

[![CI](https://github.com/freeeve/roaringsearch/actions/workflows/ci.yml/badge.svg)](https://github.com/freeeve/roaringsearch/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/freeeve/roaringsearch/badge.svg?branch=main)](https://coveralls.io/github/freeeve/roaringsearch?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/freeeve/roaringsearch)](https://goreportcard.com/report/github.com/freeeve/roaringsearch)
[![Go Reference](https://pkg.go.dev/badge/github.com/freeeve/roaringsearch.svg)](https://pkg.go.dev/github.com/freeeve/roaringsearch)

A high-performance n-gram text search library using roaring bitmaps for Go.

Designed to run on memory-constrained environments like AWS t4g.micro (1GB RAM) while still supporting large indexes via disk-backed caching with configurable memory budgets.

## Features

- N-gram based text indexing (configurable gram size 1-8)
- Full Unicode support (Japanese, Chinese, Arabic, etc.)
- Multiple search modes: AND, OR, threshold-based
- Disk-backed index with memory-budgeted LRU cache
- Configurable memory limits for predictable resource usage
- Thread-safe for concurrent reads

## Installation

```bash
go get github.com/freeeve/roaringsearch
```

## Quick Start

```go
package main

import (
    "fmt"
    rs "github.com/freeeve/roaringsearch"
)

func main() {
    // Create a new index with trigrams
    idx := rs.NewIndex(3)

    // Add documents
    idx.Add(1, "Hello World")
    idx.Add(2, "Hello there")
    idx.Add(3, "World peace")

    // AND search - documents containing ALL query n-grams
    results := idx.Search("hello")
    fmt.Println("Search 'hello':", results) // [1, 2]

    // OR search - documents containing ANY query n-gram
    results = idx.SearchAny("hello world")
    fmt.Println("SearchAny 'hello world':", results) // [1, 2, 3]

    // Early termination - get first N results fast
    results = idx.SearchWithLimit("hello", 1)
    fmt.Println("SearchWithLimit:", results) // [1] or [2]

    // Threshold search with scores
    result := idx.SearchThreshold("hello world", 2)
    fmt.Println("Threshold:", result.DocIDs, result.Scores)
}
```

## API

### Index

```go
// Create index with gram size (1-8, default 3)
idx := rs.NewIndex(3)
idx := rs.NewIndex(3, rs.WithNormalizer(rs.NormalizeLowercase))

// Index operations
idx.Add(docID uint32, text string)    // Single document
idx.AddBatch(docs []Document)          // Bulk insert (4x faster, auto-parallel)
idx.Remove(docID uint32)
idx.Clear()

// Search methods
idx.Search(query string) []uint32              // AND search
idx.SearchAny(query string) []uint32           // OR search
idx.SearchWithLimit(query string, n int) []uint32  // First N results (fast)
idx.SearchCallback(query string, fn func(uint32) bool) // Zero-alloc iteration
idx.SearchThreshold(query string, min int) SearchResult // Fuzzy matching
idx.SearchCount(query string) uint64           // Count only
idx.SearchAnyCount(query string) uint64

// Metadata
idx.GramSize() int
idx.NgramCount() int
```

### Disk-backed Index

For large indexes that don't fit in memory, use the disk-backed `CachedIndex` with a memory budget:

```go
// Save to disk
idx.SaveToFile("index.sear")

// Load fully into memory (only for small indexes)
idx, _ := rs.LoadFromFile("index.sear")

// Open with LRU cache limited by bitmap count
cached, _ := rs.OpenCachedIndex("index.sear", rs.WithCacheSize(1000))
cached.Search("query")
cached.ClearCache()

// Open with memory budget (recommended for predictable memory usage)
cached, _ := rs.OpenCachedIndex("index.sear", rs.WithMemoryBudget(100*1024*1024)) // 100MB
cached.MemoryUsage() // returns current bytes used
```

### Memory Management

For memory-constrained environments (e.g., t4g.micro with 1GB RAM), combine `WithMemoryBudget` with Go's `GOMEMLIMIT`:

```bash
# Set Go runtime soft memory limit to 800MB, leaving room for OS
export GOMEMLIMIT=800MiB
```

```go
// Use ~500MB for index cache, leaving room for app overhead
cached, _ := rs.OpenCachedIndex("large.sear", rs.WithMemoryBudget(500*1024*1024))
```

The memory budget controls only the bitmap cache. Actual process memory will be higher due to:
- Go runtime overhead
- Query processing buffers
- Your application's memory usage

A conservative rule: set `WithMemoryBudget` to ~50-60% of `GOMEMLIMIT`.

### Normalizers

```go
// Default: lowercase + remove non-alphanumeric
rs.NormalizeLowercaseAlphanumeric

// Lowercase only (preserves punctuation)
rs.NormalizeLowercase

// Custom normalizer
rs.WithNormalizer(func(s string) string {
    return strings.ToLower(s)
})
```

## Unicode Support

The library handles Unicode text natively. For CJK languages, use smaller gram sizes:

```go
idx := rs.NewIndex(2) // Bigrams work well for Japanese/Chinese

idx.Add(1, "東京は日本の首都です")
idx.Add(2, "京都は美しい街です")

results := idx.Search("東京") // [1]
results = idx.Search("京都")  // [2]
```

## Benchmarks

**Apple M3 Max (16 cores), trigrams**

### Benchmark Data

Documents are randomly generated with:
- **5-20 words** per document (avg ~12 words, ~60 characters)
- **~90 unique vocabulary words** from 4 pools: common words, tech terms, names, rare words
- **~50 unique trigrams** per document on average

### Scaling by Index Size

| Documents | Build Time | Memory | Search | SearchWithLimit(100) | SearchCount |
|-----------|------------|--------|--------|---------------------|-------------|
| 100K | 137ms | 11 MB | 42-109µs | 10-28µs | 30-60µs |
| 1M | 1.3s | 109 MB | 0.5-1ms | 15-33µs | 340-580µs |
| 10M | 12.8s | 718 MB | 3-5ms | 21-36µs | 2.3-4.5ms |
| 100M | 3m 16s | 6.7 GB | 75-146ms | 87-161µs | 34-44ms |

### Key Takeaways

- **SearchWithLimit stays sub-millisecond even at 100M docs** - 87-161µs vs 75-146ms for full search
- **Use `SearchWithLimit` for pagination** - 500-1000x faster than full search at scale
- **Longer queries are faster** - more n-grams = more selective intersection
- **No-match queries are instant** - early termination on first missing n-gram
- **Memory scales linearly** - ~70 MB per million documents

### Indexing Performance (1M docs)

| Method | Time | Speedup |
|--------|------|---------|
| Add (sequential) | 7.5s | baseline |
| AddBatch (16 workers) | 1.3s | **5.8x** |

Use `AddBatch` for bulk indexing:

```go
docs := make([]rs.Document, numDocs)
for i := range docs {
    docs[i] = rs.Document{ID: uint32(i), Text: texts[i]}
}
idx.AddBatch(docs) // Automatically uses all CPU cores
```

See `bench_scale_test.go` and `bench_10m_test.go` for full benchmarks.

## License

MIT
