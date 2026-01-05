package roaringsearch

// Option configures an Index.
type Option func(*Index)

// WithNormalizer sets the text normalizer for n-gram generation.
// Default is NormalizeLowercaseAlphanumeric.
// Note: Custom normalizers disable the ASCII fast path optimization.
func WithNormalizer(n Normalizer) Option {
	return func(idx *Index) {
		idx.normalizer = n
		idx.useASCIFastPath = false // custom normalizer requires full processing
	}
}
