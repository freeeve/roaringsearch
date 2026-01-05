package roaringsearch

import (
	"strings"
	"unicode"
)

// Normalizer transforms text before n-gram generation.
type Normalizer func(s string) string

// NormalizeLowercase converts the string to lowercase.
func NormalizeLowercase(s string) string {
	return strings.ToLower(s)
}

// NormalizeLowercaseAlphanumeric converts to lowercase and removes non-alphanumeric characters.
// This is the default normalizer.
func NormalizeLowercaseAlphanumeric(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// normalizeAndKeyASCII normalizes ASCII text and generates n-gram keys directly.
// Returns keys slice and true if successful, nil and false if text contains non-ASCII.
// Key encoding must match runeNgramKey: 32-bit per char for n<=2, 8-bit for n>2.
func normalizeAndKeyASCII(s string, gramSize int, keys []uint64) ([]uint64, bool) {
	// Normalize in place to a byte buffer
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c > 127 {
			return nil, false // Non-ASCII, fall back
		}
		if c >= 'A' && c <= 'Z' {
			buf = append(buf, c+32)
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			buf = append(buf, c)
		}
	}

	if len(buf) < gramSize {
		return keys[:0], true
	}

	// Generate keys directly from bytes
	// Must match runeNgramKey encoding: 32-bit for n<=2, 8-bit for n>2
	keys = keys[:0]
	for i := 0; i <= len(buf)-gramSize; i++ {
		var key uint64
		if gramSize <= 2 {
			// 32-bit packing to match packRunes
			for j := 0; j < gramSize; j++ {
				key = (key << 32) | uint64(buf[i+j])
			}
		} else {
			// 8-bit packing for gramSize 3-8
			for j := 0; j < gramSize; j++ {
				key = (key << 8) | uint64(buf[i+j])
			}
		}

		// Check for duplicate (linear scan)
		found := false
		for _, k := range keys {
			if k == key {
				found = true
				break
			}
		}
		if !found {
			keys = append(keys, key)
		}
	}

	return keys, true
}

// normalizeAndKeyASCIIPooled is like normalizeAndKeyASCII but uses a provided buffer.
// Returns (keys, buf, ok) where buf is the potentially grown buffer for pool return.
func normalizeAndKeyASCIIPooled(s string, gramSize int, keys []uint64, buf []byte) ([]uint64, []byte, bool) {
	// Normalize in place to byte buffer
	buf = buf[:0]
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c > 127 {
			return nil, buf, false // Non-ASCII, fall back
		}
		if c >= 'A' && c <= 'Z' {
			buf = append(buf, c+32)
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			buf = append(buf, c)
		}
	}

	if len(buf) < gramSize {
		return keys[:0], buf, true
	}

	// Generate keys directly from bytes
	keys = keys[:0]
	n := len(buf) - gramSize

	if gramSize <= 2 {
		// 32-bit packing for gram sizes 1-2
		for i := 0; i <= n; i++ {
			var key uint64
			for j := 0; j < gramSize; j++ {
				key = (key << 32) | uint64(buf[i+j])
			}
			// Dedup with linear scan (fast for small N)
			found := false
			for _, k := range keys {
				if k == key {
					found = true
					break
				}
			}
			if !found {
				keys = append(keys, key)
			}
		}
	} else {
		// 8-bit packing for gram sizes 3-8
		for i := 0; i <= n; i++ {
			var key uint64
			for j := 0; j < gramSize; j++ {
				key = (key << 8) | uint64(buf[i+j])
			}
			found := false
			for _, k := range keys {
				if k == key {
					found = true
					break
				}
			}
			if !found {
				keys = append(keys, key)
			}
		}
	}

	return keys, buf, true
}

// packRunes packs up to 2 runes into a uint64 key.
// Each rune uses 32 bits, so max 2 runes fit in 64 bits.
// This is collision-free for n <= 2, suitable for CJK languages.
func packRunes(runes []rune) uint64 {
	var key uint64
	for _, r := range runes {
		key = (key << 32) | uint64(r)
	}
	return key
}

// hashRunes generates a uint64 hash for a rune slice.
// Used for Unicode n-grams with n > 2 where collision-free packing isn't possible.
// Uses FNV-1a algorithm for fast, reasonable distribution.
func hashRunes(runes []rune) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, r := range runes {
		h ^= uint64(r & 0xFF)
		h *= prime64
		h ^= uint64((r >> 8) & 0xFF)
		h *= prime64
		h ^= uint64((r >> 16) & 0xFF)
		h *= prime64
		h ^= uint64((r >> 24) & 0xFF)
		h *= prime64
	}
	return h
}

// runeNgramKey returns the appropriate key for a rune n-gram.
// For n <= 2: collision-free packing
// For n 3-8 with ASCII-only: collision-free packing
// For n > 2 with Unicode: hash-based (small collision risk)
func runeNgramKey(runes []rune) uint64 {
	n := len(runes)
	if n <= 2 {
		return packRunes(runes)
	}

	// For gramSize 3-8, check if ASCII and use fast packing
	if n <= 8 {
		var key uint64
		for _, r := range runes {
			if r > 127 {
				// Has Unicode - fall back to hash
				return hashRunes(runes)
			}
			key = (key << 8) | uint64(r)
		}
		return key
	}

	return hashRunes(runes)
}
