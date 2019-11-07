static inline __m128i
enc_reshuffle (__m128i in)
{
	// Slice into 32-bit chunks and operate on all chunks in parallel.
	// All processing is done within the 32-bit chunk. First, shuffle:
	// before: [eeeeeeff|ccdddddd|bbbbcccc|aaaaaabb]
	// after:  [00000000|aaaaaabb|bbbbcccc|ccdddddd]
	in = _mm_shuffle_epi8(in, _mm_set_epi8(
		-1, 9, 10, 11,
		-1, 6,  7,  8,
		-1, 3,  4,  5,
		-1, 0,  1,  2));

	// merged  = [0000aaaa|aabbbbbb|bbbbcccc|ccdddddd]
	const __m128i merged = _mm_blend_epi16(_mm_slli_epi32(in, 4), in, 0x55);

	// bd      = [00000000|00bbbbbb|00000000|00dddddd]
	const __m128i bd = _mm_and_si128(merged, _mm_set1_epi32(0x003F003F));

	// ac      = [00aaaaaa|00000000|00cccccc|00000000]
	const __m128i ac = _mm_and_si128(_mm_slli_epi32(merged, 2), _mm_set1_epi32(0x3F003F00));

	// indices = [00aaaaaa|00bbbbbb|00cccccc|00dddddd]
	const __m128i indices = _mm_or_si128(ac, bd);

	// return  = [00dddddd|00cccccc|00bbbbbb|00aaaaaa]
	return _mm_bswap_epi32(indices);
}
