package roaringsearch

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// testDoc is a simple struct for test data
type testDoc struct {
	id   uint32
	text string
}

func BenchmarkAddMethods(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Generate test documents
	numDocs := 100000
	docs := make([]testDoc, numDocs)
	for i := range docs {
		docs[i] = testDoc{
			id:   uint32(i),
			text: generateDocument(rng, 5, 20),
		}
	}

	b.Run("Add_Sequential", func(b *testing.B) {
		idx := NewIndex(3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.Add(docs[i%numDocs].id, docs[i%numDocs].text)
		}
	})

	b.Run("Batch", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := NewIndex(3)
			batch := idx.BatchSize(numDocs)
			for _, doc := range docs {
				batch.Add(doc.id, doc.text)
			}
			batch.Flush()
		}
		b.ReportMetric(float64(numDocs), benchMetricDocsOp)
	})
}

func BenchmarkAddScaled(b *testing.B) {
	scales := []int{10000, 100000, 1000000}

	for _, numDocs := range scales {
		name := fmt.Sprintf("%dK", numDocs/1000)

		rng := rand.New(rand.NewSource(42))
		docs := make([]testDoc, numDocs)
		for i := range docs {
			docs[i] = testDoc{
				id:   uint32(i),
				text: generateDocument(rng, 5, 20),
			}
		}

		b.Run(name+"/Sequential", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				idx := NewIndex(3)
				for _, doc := range docs {
					idx.Add(doc.id, doc.text)
				}
			}
			b.ReportMetric(float64(numDocs), benchMetricDocsOp)
		})

		b.Run(name+"/Batch", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				idx := NewIndex(3)
				batch := idx.BatchSize(numDocs)
				for _, doc := range docs {
					batch.Add(doc.id, doc.text)
				}
				batch.Flush()
			}
			b.ReportMetric(float64(numDocs), benchMetricDocsOp)
		})
	}
}

func TestBatchCorrectness(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	docs := make([]testDoc, 1000)
	for i := range docs {
		docs[i] = testDoc{
			id:   uint32(i),
			text: generateDocument(rng, 5, 20),
		}
	}

	// Build with sequential Add
	idx1 := NewIndex(3)
	for _, doc := range docs {
		idx1.Add(doc.id, doc.text)
	}

	// Build with Batch (parallel)
	idx2 := NewIndex(3)
	batch := idx2.BatchSize(len(docs))
	for _, doc := range docs {
		batch.Add(doc.id, doc.text)
	}
	batch.Flush()

	// Compare n-gram counts
	if idx1.NgramCount() != idx2.NgramCount() {
		t.Errorf("Batch n-gram count mismatch: %d vs %d", idx1.NgramCount(), idx2.NgramCount())
	}

	// Compare search results
	queries := []string{"server", "client", "the"}
	for _, q := range queries {
		r1 := idx1.Search(q)
		r2 := idx2.Search(q)

		if len(r1) != len(r2) {
			t.Errorf("Batch search mismatch for %q: %d vs %d", q, len(r1), len(r2))
		}
	}
}

func TestBatchLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test")
	}

	rng := rand.New(rand.NewSource(42))
	numDocs := 1_000_000

	docs := make([]testDoc, numDocs)
	for i := range docs {
		docs[i] = testDoc{
			id:   uint32(i),
			text: generateDocument(rng, 5, 20),
		}
	}

	t.Run("Sequential", func(t *testing.T) {
		idx := NewIndex(3)
		start := time.Now()
		for _, doc := range docs {
			idx.Add(doc.id, doc.text)
		}
		t.Logf("Sequential: %v for %d docs", time.Since(start), numDocs)
	})

	t.Run("Batch", func(t *testing.T) {
		idx := NewIndex(3)
		batch := idx.BatchSize(numDocs)
		start := time.Now()
		for _, doc := range docs {
			batch.Add(doc.id, doc.text)
		}
		batch.Flush()
		t.Logf("Batch (%d workers): %v for %d docs", runtime.NumCPU(), time.Since(start), numDocs)
	})
}
