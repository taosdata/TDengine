// If we have AVX2 support, pick off 32 bytes at a time for as long as we can,
// but make sure that we quit before seeing any == markers at the end of the
// string. Also, because we write 8 zeroes at the end of the output, ensure
// that there are at least 11 valid bytes of input data remaining to close the
// gap. 32 + 2 + 11 = 45 bytes:
while (srclen >= 45)
{
	// Load string:
	__m256i str = _mm256_loadu_si256((__m256i *)c);

	// see ssse3/dec_loop.c for an explanation of how the code works.

	const __m256i lut_lo = _mm256_setr_epi8(
		0x15, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
		0x11, 0x11, 0x13, 0x1A, 0x1B, 0x1B, 0x1B, 0x1A,
		0x15, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
		0x11, 0x11, 0x13, 0x1A, 0x1B, 0x1B, 0x1B, 0x1A);

	const __m256i lut_hi = _mm256_setr_epi8(
		0x10, 0x10, 0x01, 0x02, 0x04, 0x08, 0x04, 0x08,
		0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10,
		0x10, 0x10, 0x01, 0x02, 0x04, 0x08, 0x04, 0x08,
		0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10);

	const __m256i lut_roll = _mm256_setr_epi8(
		0,  16,  19,   4, -65, -65, -71, -71,
		0,   0,   0,   0,   0,   0,   0,   0,
		0,  16,  19,   4, -65, -65, -71, -71,
		0,   0,   0,   0,   0,   0,   0,   0);

	const __m256i mask_2F = _mm256_set1_epi8(0x2f);

	// lookup
	const __m256i hi_nibbles  = _mm256_and_si256(_mm256_srli_epi32(str, 4), mask_2F);
	const __m256i lo_nibbles  = _mm256_and_si256(str, mask_2F);
	const __m256i hi          = _mm256_shuffle_epi8(lut_hi, hi_nibbles);
	const __m256i lo          = _mm256_shuffle_epi8(lut_lo, lo_nibbles);
	const __m256i eq_2F       = _mm256_cmpeq_epi8(str, mask_2F);
	const __m256i roll        = _mm256_shuffle_epi8(lut_roll, _mm256_add_epi8(eq_2F, hi_nibbles));

	if (!_mm256_testz_si256(lo, hi)) {
		break;
	}

	// Now simply add the delta values to the input:
	str = _mm256_add_epi8(str, roll);

	// Reshuffle the input to packed 12-byte output format:
	str = dec_reshuffle(str);

	// Store back:
	_mm256_storeu_si256((__m256i *)o, str);

	c += 32;
	o += 24;
	outl += 24;
	srclen -= 32;
}
