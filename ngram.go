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

// normalizeASCIIToBuf normalizes ASCII text to a byte buffer.
// Returns the buffer and true if successful, or the buffer and false if non-ASCII found.
func normalizeASCIIToBuf(s string, buf []byte) ([]byte, bool) {
	buf = buf[:0]
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c > 127 {
			return buf, false
		}
		if c >= 'A' && c <= 'Z' {
			buf = append(buf, c+32)
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			buf = append(buf, c)
		}
	}
	return buf, true
}

// packBytesToKey packs bytes into a uint64 key.
// Uses 32-bit packing for gramSize <= 2, 8-bit for gramSize > 2.
func packBytesToKey(buf []byte, start, gramSize int) uint64 {
	var key uint64
	if gramSize <= 2 {
		for j := 0; j < gramSize; j++ {
			key = (key << 32) | uint64(buf[start+j])
		}
	} else {
		for j := 0; j < gramSize; j++ {
			key = (key << 8) | uint64(buf[start+j])
		}
	}
	return key
}

// appendKeyDedup appends key to keys if not already present.
func appendKeyDedup(keys []uint64, key uint64) []uint64 {
	for _, k := range keys {
		if k == key {
			return keys
		}
	}
	return append(keys, key)
}

// normalizeAndKeyASCII normalizes ASCII text and generates n-gram keys directly.
// Returns keys slice and true if successful, nil and false if text contains non-ASCII.
// Key encoding must match runeNgramKey: 32-bit per char for n<=2, 8-bit for n>2.
func normalizeAndKeyASCII(s string, gramSize int, keys []uint64) ([]uint64, bool) {
	buf := make([]byte, 0, len(s))
	buf, ok := normalizeASCIIToBuf(s, buf)
	if !ok {
		return nil, false
	}

	if len(buf) < gramSize {
		return keys[:0], true
	}

	keys = keys[:0]
	for i := 0; i <= len(buf)-gramSize; i++ {
		key := packBytesToKey(buf, i, gramSize)
		keys = appendKeyDedup(keys, key)
	}

	return keys, true
}

// normalizeAndKeyASCIIPooled is like normalizeAndKeyASCII but uses a provided buffer.
// Returns (keys, buf, ok) where buf is the potentially grown buffer for pool return.
func normalizeAndKeyASCIIPooled(s string, gramSize int, keys []uint64, buf []byte) ([]uint64, []byte, bool) {
	buf, ok := normalizeASCIIToBuf(s, buf)
	if !ok {
		return nil, buf, false
	}

	if len(buf) < gramSize {
		return keys[:0], buf, true
	}

	keys = keys[:0]
	for i := 0; i <= len(buf)-gramSize; i++ {
		key := packBytesToKey(buf, i, gramSize)
		keys = appendKeyDedup(keys, key)
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
