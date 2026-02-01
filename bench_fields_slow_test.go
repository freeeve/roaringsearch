//go:build slow

package roaringsearch

import (
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

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
	totalBitmapBytes := filter.MemoryUsage()
	valuesBytes := col.MemoryUsage()
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
