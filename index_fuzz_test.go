package roaringsearch

import (
	"testing"
)

// FuzzSearch tests search with arbitrary queries
func FuzzSearch(f *testing.F) {
	// Add seed corpus
	f.Add("hello")
	f.Add("test query")
	f.Add("")
	f.Add("a")
	f.Add("ab")

	f.Fuzz(func(t *testing.T, query string) {
		idx := NewIndex(3)
		idx.Add(1, "hello world")
		idx.Add(2, "testing search")
		_ = idx.Search(query)
		// No panic = success
	})
}

// FuzzAddAndSearch tests adding and searching arbitrary content
func FuzzAddAndSearch(f *testing.F) {
	// Add seed corpus
	f.Add("content to index", "search query")
	f.Add("", "")
	f.Add("short", "short")

	f.Fuzz(func(t *testing.T, content, query string) {
		idx := NewIndex(3)
		idx.Add(1, content)
		_ = idx.Search(query)
		// No panic = success
	})
}
