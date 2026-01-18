package roaringsearch

import (
	"testing"
)

// FuzzNormalize tests text normalization with arbitrary input
func FuzzNormalize(f *testing.F) {
	// Add seed corpus
	f.Add("Hello World")
	f.Add("UPPERCASE")
	f.Add("café résumé")
	f.Add("日本語テスト")
	f.Add("")
	f.Add("   spaces   ")

	f.Fuzz(func(t *testing.T, input string) {
		_ = NormalizeLowercase(input)
		// No panic = success
	})
}
