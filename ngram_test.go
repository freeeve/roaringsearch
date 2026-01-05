package roaringsearch

import "testing"

func TestNormalizers(t *testing.T) {
	tests := []struct {
		name       string
		normalizer Normalizer
		input      string
		expected   string
	}{
		{"lowercase", NormalizeLowercase, "Hello World!", "hello world!"},
		{"alphanumeric", NormalizeLowercaseAlphanumeric, "Hello World!", "helloworld"},
		{"alphanumeric with numbers", NormalizeLowercaseAlphanumeric, "Test123!", "test123"},
		{"unicode lowercase", NormalizeLowercase, "ÜBER", "über"},
		{"japanese preserved", NormalizeLowercaseAlphanumeric, "日本語テスト", "日本語テスト"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.normalizer(tt.input)
			if result != tt.expected {
				t.Errorf("normalizer(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func BenchmarkNormalizers(b *testing.B) {
	text := "The Quick Brown Fox Jumps Over The Lazy Dog! 123"

	b.Run("Lowercase", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			NormalizeLowercase(text)
		}
	})

	b.Run("LowercaseAlphanumeric", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			NormalizeLowercaseAlphanumeric(text)
		}
	})
}
