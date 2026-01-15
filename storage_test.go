package roaringsearch

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestSerialization(t *testing.T) {
	idx := NewIndex(3)

	idx.Add(1, "hello world")
	idx.Add(2, "hello there")
	idx.Add(3, "world peace")

	// Save to temp file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sear")

	err := idx.SaveToFile(path)
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("file was not created")
	}

	// Load from file
	idx2, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	// Verify gram size
	if idx2.GramSize() != idx.GramSize() {
		t.Errorf("gram size mismatch: got %d, want %d", idx2.GramSize(), idx.GramSize())
	}

	// Verify ngram count
	if idx2.NgramCount() != idx.NgramCount() {
		t.Errorf("ngram count mismatch: got %d, want %d", idx2.NgramCount(), idx.NgramCount())
	}

	// Verify search results
	results1 := idx.Search("hello")
	results2 := idx2.Search("hello")
	sort.Slice(results1, func(i, j int) bool { return results1[i] < results1[j] })
	sort.Slice(results2, func(i, j int) bool { return results2[i] < results2[j] })

	if !reflect.DeepEqual(results1, results2) {
		t.Errorf("search results mismatch: got %v, want %v", results2, results1)
	}
}

func TestLoadFromFileWithOptions(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello world")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sear")
	idx.SaveToFile(path)

	loaded, err := LoadFromFileWithOptions(path, WithNormalizer(NormalizeLowercase))
	if err != nil {
		t.Fatalf("LoadFromFileWithOptions failed: %v", err)
	}

	if loaded.NgramCount() != idx.NgramCount() {
		t.Errorf("ngram count mismatch: got %d, want %d", loaded.NgramCount(), idx.NgramCount())
	}
}

func TestSaveToFileError(t *testing.T) {
	idx := NewIndex(3)
	idx.Add(1, "hello")

	// Try to save to an invalid path
	err := idx.SaveToFile("/nonexistent/directory/test.sear")
	if err == nil {
		t.Error("SaveToFile should fail for invalid path")
	}
}

func TestLoadFromFileError(t *testing.T) {
	// Try to load from nonexistent file
	_, err := LoadFromFile("/nonexistent/file.sear")
	if err == nil {
		t.Error("LoadFromFile should fail for nonexistent file")
	}

	// Try to load from invalid file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.sear")
	os.WriteFile(path, []byte("invalid data"), 0644)

	_, err = LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile should fail for invalid file format")
	}
}

func TestOpenCachedIndexError(t *testing.T) {
	// Try to open nonexistent file
	_, err := OpenCachedIndex("/nonexistent/file.sear")
	if err == nil {
		t.Error("OpenCachedIndex should fail for nonexistent file")
	}

	// Try to open invalid file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.sear")
	os.WriteFile(path, []byte("invalid data"), 0644)

	_, err = OpenCachedIndex(path)
	if err == nil {
		t.Error("OpenCachedIndex should fail for invalid file format")
	}
}
