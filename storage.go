package roaringsearch

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/RoaringBitmap/roaring"
)

const (
	magicBytes = "FTSR"
	version    = 2 // Version 2 uses uint64 keys
)

var (
	ErrInvalidMagic   = errors.New("invalid magic bytes")
	ErrInvalidVersion = errors.New("unsupported version")
)

// WriteTo writes the index to the provided writer.
func (idx *Index) WriteTo(w io.Writer) (int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var written int64

	// Write header: magic (4) + version (2) + gram size (2) = 8 bytes
	header := make([]byte, 8)
	copy(header[0:4], magicBytes)
	binary.LittleEndian.PutUint16(header[4:6], version)
	binary.LittleEndian.PutUint16(header[6:8], uint16(idx.gramSize))

	n, err := w.Write(header)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write header: %w", err)
	}

	// Write n-gram count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(idx.bitmaps)))
	n, err = w.Write(countBuf)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write ngram count: %w", err)
	}

	// Write each n-gram key and its bitmap
	keyBuf := make([]byte, 8)
	sizeBuf := make([]byte, 4)

	for key, bm := range idx.bitmaps {
		// N-gram key (8 bytes)
		binary.LittleEndian.PutUint64(keyBuf, key)
		n, err = w.Write(keyBuf)
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("write ngram key: %w", err)
		}

		// Serialize bitmap to buffer first to get size
		bmBytes, err := bm.ToBytes()
		if err != nil {
			return written, fmt.Errorf("serialize bitmap: %w", err)
		}

		// Bitmap size (4 bytes)
		binary.LittleEndian.PutUint32(sizeBuf, uint32(len(bmBytes)))
		n, err = w.Write(sizeBuf)
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("write bitmap size: %w", err)
		}

		// Bitmap data
		n, err = w.Write(bmBytes)
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("write bitmap: %w", err)
		}
	}

	return written, nil
}

// ReadFrom reads the index from the provided reader.
// Note: This replaces the current index contents. The normalizer is preserved.
func (idx *Index) ReadFrom(r io.Reader) (int64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var read int64

	// Read header
	header := make([]byte, 8)
	n, err := io.ReadFull(r, header)
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("read header: %w", err)
	}

	// Verify magic
	if string(header[0:4]) != magicBytes {
		return read, ErrInvalidMagic
	}

	// Check version
	fileVersion := binary.LittleEndian.Uint16(header[4:6])
	if fileVersion != version {
		return read, ErrInvalidVersion
	}

	// Read gram size
	idx.gramSize = int(binary.LittleEndian.Uint16(header[6:8]))

	// Read n-gram count
	countBuf := make([]byte, 4)
	n, err = io.ReadFull(r, countBuf)
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("read ngram count: %w", err)
	}
	ngramCount := binary.LittleEndian.Uint32(countBuf)

	// Clear and reinitialize bitmaps
	idx.bitmaps = make(map[uint64]*roaring.Bitmap, ngramCount)

	// Read each n-gram and its bitmap
	keyBuf := make([]byte, 8)
	sizeBuf := make([]byte, 4)

	for i := uint32(0); i < ngramCount; i++ {
		// Read n-gram key
		n, err = io.ReadFull(r, keyBuf)
		read += int64(n)
		if err != nil {
			return read, fmt.Errorf("read ngram key: %w", err)
		}
		key := binary.LittleEndian.Uint64(keyBuf)

		// Read bitmap size
		n, err = io.ReadFull(r, sizeBuf)
		read += int64(n)
		if err != nil {
			return read, fmt.Errorf("read bitmap size: %w", err)
		}
		bmSize := binary.LittleEndian.Uint32(sizeBuf)

		// Read bitmap data
		bmBytes := make([]byte, bmSize)
		n, err = io.ReadFull(r, bmBytes)
		read += int64(n)
		if err != nil {
			return read, fmt.Errorf("read bitmap: %w", err)
		}

		// Deserialize bitmap
		bm := roaring.New()
		_, err = bm.ReadFrom(bytes.NewReader(bmBytes))
		if err != nil {
			return read, fmt.Errorf("deserialize bitmap: %w", err)
		}

		idx.bitmaps[key] = bm
	}

	return read, nil
}

// SaveToFile saves the index to a file atomically.
// Writes to a temp file first, then renames to prevent corruption on crash.
func (idx *Index) SaveToFile(path string) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	_, err = idx.WriteTo(f)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync temp file: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

// LoadFromFile loads an index from a file.
// Returns a new Index with the default normalizer.
func LoadFromFile(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	idx := NewIndex(3) // gram size will be overwritten by ReadFrom
	_, err = idx.ReadFrom(f)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

// LoadFromFileWithOptions loads an index from a file with custom options.
func LoadFromFileWithOptions(path string, opts ...Option) (*Index, error) {
	idx, err := LoadFromFile(path)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(idx)
	}

	return idx, nil
}
