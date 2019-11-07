// If we have SSSE3 support, pick off 12 bytes at a time for as long as we can.
// But because we read 16 bytes at a time, ensure we have enough room to do a
// full 16-byte read without segfaulting:
while (srclen >= 16)
{
	// Load string:
	__m128i str = _mm_loadu_si128((__m128i *)c);

	// Reshuffle:
	str = enc_reshuffle(str);

	// Translate reshuffled bytes to the Base64 alphabet:
	str = enc_translate(str);

	// Store:
	_mm_storeu_si128((__m128i *)o, str);

	c += 12;	// 3 * 4 bytes of input
	o += 16;	// 4 * 4 bytes of output
	outl += 16;
	srclen -= 12;
}
