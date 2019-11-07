// If we have ARM NEON support, pick off 48 bytes at a time:
while (srclen >= 48)
{
	uint8x16x3_t str;
	uint8x16x4_t res;

	// Load 48 bytes and deinterleave:
	str = vld3q_u8((uint8_t *)c);

	// Divide bits of three input bytes over four output bytes:
	res.val[0] = vshrq_n_u8(str.val[0], 2);
	res.val[1] = vshrq_n_u8(str.val[1], 4) | vshlq_n_u8(str.val[0], 4);
	res.val[2] = vshrq_n_u8(str.val[2], 6) | vshlq_n_u8(str.val[1], 2);
	res.val[3] = str.val[2];

	// Clear top two bits:
	res.val[0] &= vdupq_n_u8(0x3F);
	res.val[1] &= vdupq_n_u8(0x3F);
	res.val[2] &= vdupq_n_u8(0x3F);
	res.val[3] &= vdupq_n_u8(0x3F);

	// The bits have now been shifted to the right locations;
	// translate their values 0..63 to the Base64 alphabet.
	// Use a 64-byte table lookup:
	res.val[0] = vqtbl4q_u8(tbl_enc, res.val[0]);
	res.val[1] = vqtbl4q_u8(tbl_enc, res.val[1]);
	res.val[2] = vqtbl4q_u8(tbl_enc, res.val[2]);
	res.val[3] = vqtbl4q_u8(tbl_enc, res.val[3]);

	// Interleave and store result:
	vst4q_u8((uint8_t *)o, res);

	c += 48;	// 3 * 16 bytes of input
	o += 64;	// 4 * 16 bytes of output
	outl += 64;
	srclen -= 48;
}
