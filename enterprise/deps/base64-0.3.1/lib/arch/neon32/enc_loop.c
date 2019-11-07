// If we have ARM NEON support, pick off 48 bytes at a time:
while (srclen >= 48)
{
	uint8x16x3_t str;
	uint8x16x4_t res;

	// Load 48 bytes and deinterleave:
	str = vld3q_u8((uint8_t *)c);

	// Reshuffle:
	res = enc_reshuffle(str);

	// Translate reshuffled bytes to the Base64 alphabet:
	res = enc_translate(res);

	// Interleave and store result:
	vst4q_u8((uint8_t *)o, res);

	c += 48;	// 3 * 16 bytes of input
	o += 64;	// 4 * 16 bytes of output
	outl += 64;
	srclen -= 48;
}
