// If we have AVX2 support, pick off 24 bytes at a time for as long as we can.
// But because we read 32 bytes at a time, ensure we have enough room to do a
// full 32-byte read without segfaulting:

if (srclen >= 32) {
	const uint8_t* const o_orig = o;

	// first load is done at c-0 not to get a segfault
	__m256i inputvector = _mm256_loadu_si256((__m256i *)(c - 0));

	// shift by 4 bytes, as required by enc_reshuffle
	inputvector = _mm256_permutevar8x32_epi32(inputvector, _mm256_setr_epi32(0, 0, 1, 2, 3, 4, 5, 6));

	for (;;) {
		inputvector = enc_reshuffle(inputvector);
		inputvector = enc_translate(inputvector);
		_mm256_storeu_si256((__m256i *)o, inputvector);
		c += 24;
		o += 32;
		srclen -= 24;
		if(srclen < 28) {
			break;
		}
		// Load at c-4, as required by enc_reshuffle
		inputvector = _mm256_loadu_si256((__m256i *)(c - 4));
	}
	outl += (size_t)(o - o_orig);
}
