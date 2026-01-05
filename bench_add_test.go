package roaringsearch

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func BenchmarkAddMethods(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Generate test documents
	numDocs := 100000
	docs := make([]Document, numDocs)
	for i := range docs {
		docs[i] = Document{
			ID:   uint32(i),
			Text: generateDocument(rng, 5, 20),
		}
	}

	b.Run("Add_Sequential", func(b *testing.B) {
		idx := NewIndex(3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.Add(docs[i%numDocs].ID, docs[i%numDocs].Text)
		}
	})

	b.Run("AddBatch", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := NewIndex(3)
			idx.AddBatch(docs)
		}
		b.ReportMetric(float64(numDocs), "docs/op")
	})
}

func BenchmarkAddScaled(b *testing.B) {
	scales := []int{10000, 100000, 1000000}

	for _, numDocs := range scales {
		name := fmt.Sprintf("%dK", numDocs/1000)

		rng := rand.New(rand.NewSource(42))
		docs := make([]Document, numDocs)
		for i := range docs {
			docs[i] = Document{
				ID:   uint32(i),
				Text: generateDocument(rng, 5, 20),
			}
		}

		b.Run(name+"/Sequential", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				idx := NewIndex(3)
				for _, doc := range docs {
					idx.Add(doc.ID, doc.Text)
				}
			}
			b.ReportMetric(float64(numDocs), "docs/op")
		})

		b.Run(name+"/AddBatch", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				idx := NewIndex(3)
				idx.AddBatch(docs)
			}
			b.ReportMetric(float64(numDocs), "docs/op")
		})
	}
}

func TestAddMethodsCorrectness(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	docs := make([]Document, 1000)
	for i := range docs {
		docs[i] = Document{
			ID:   uint32(i),
			Text: generateDocument(rng, 5, 20),
		}
	}

	// Build with sequential Add
	idx1 := NewIndex(3)
	for _, doc := range docs {
		idx1.Add(doc.ID, doc.Text)
	}

	// Build with AddBatch (parallel)
	idx2 := NewIndex(3)
	idx2.AddBatch(docs)

	// Compare n-gram counts
	if idx1.NgramCount() != idx2.NgramCount() {
		t.Errorf("AddBatch n-gram count mismatch: %d vs %d", idx1.NgramCount(), idx2.NgramCount())
	}

	// Compare search results
	queries := []string{"server", "client", "the"}
	for _, q := range queries {
		r1 := idx1.Search(q)
		r2 := idx2.Search(q)

		if len(r1) != len(r2) {
			t.Errorf("AddBatch search mismatch for %q: %d vs %d", q, len(r1), len(r2))
		}
	}
}

func TestAddBatchLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test")
	}

	rng := rand.New(rand.NewSource(42))
	numDocs := 1_000_000

	docs := make([]Document, numDocs)
	for i := range docs {
		docs[i] = Document{
			ID:   uint32(i),
			Text: generateDocument(rng, 5, 20),
		}
	}

	t.Run("Sequential", func(t *testing.T) {
		idx := NewIndex(3)
		start := time.Now()
		for _, doc := range docs {
			idx.Add(doc.ID, doc.Text)
		}
		t.Logf("Sequential: %v for %d docs", time.Since(start), numDocs)
	})

	t.Run("AddBatch", func(t *testing.T) {
		idx := NewIndex(3)
		start := time.Now()
		idx.AddBatch(docs)
		t.Logf("AddBatch (%d workers): %v for %d docs", runtime.NumCPU(), time.Since(start), numDocs)
	})
}
