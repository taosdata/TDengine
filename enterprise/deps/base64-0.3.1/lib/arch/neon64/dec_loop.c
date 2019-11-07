// If we have NEON support, pick off 64 bytes at a time for as long as we can.
// Unlike the SSE codecs, we don't write trailing zero bytes to output, so we
// don't need to check if we have enough remaining input to cover them:
while (srclen >= 64)
{
	const uint8x16_t offset = vdupq_n_u8(63U);
	uint8x16x4_t dec1, dec2;
	uint8x16x3_t dec;

	// Load 64 bytes and deinterleave:
	uint8x16x4_t str = vld4q_u8((uint8_t *)c);

	// Get indices for 2nd LUT
	dec2.val[0] = vqsubq_u8(str.val[0], offset);
	dec2.val[1] = vqsubq_u8(str.val[1], offset);
	dec2.val[2] = vqsubq_u8(str.val[2], offset);
	dec2.val[3] = vqsubq_u8(str.val[3], offset);

	// Get values from 1st LUT
	dec1.val[0] = vqtbl4q_u8(tbl_dec1, str.val[0]);
	dec1.val[1] = vqtbl4q_u8(tbl_dec1, str.val[1]);
	dec1.val[2] = vqtbl4q_u8(tbl_dec1, str.val[2]);
	dec1.val[3] = vqtbl4q_u8(tbl_dec1, str.val[3]);

	// Get values from 2nd LUT
	dec2.val[0] = vqtbx4q_u8(dec2.val[0], tbl_dec2, dec2.val[0]);
	dec2.val[1] = vqtbx4q_u8(dec2.val[1], tbl_dec2, dec2.val[1]);
	dec2.val[2] = vqtbx4q_u8(dec2.val[2], tbl_dec2, dec2.val[2]);
	dec2.val[3] = vqtbx4q_u8(dec2.val[3], tbl_dec2, dec2.val[3]);

	// Get final values
	str.val[0] = vorrq_u8(dec1.val[0], dec2.val[0]);
	str.val[1] = vorrq_u8(dec1.val[1], dec2.val[1]);
	str.val[2] = vorrq_u8(dec1.val[2], dec2.val[2]);
	str.val[3] = vorrq_u8(dec1.val[3], dec2.val[3]);

	// Check for invalid input, any value larger than 63:
	uint8x16_t classified = CMPGT(str.val[0], 63);
	classified = vorrq_u8(classified, CMPGT(str.val[1], 63));
	classified = vorrq_u8(classified, CMPGT(str.val[2], 63));
	classified = vorrq_u8(classified, CMPGT(str.val[3], 63));

	// check that all bits are zero:
	if (vmaxvq_u8(classified) != 0U) {
		break;
	}

	// Compress four bytes into three:
	dec.val[0] = vshlq_n_u8(str.val[0], 2) | vshrq_n_u8(str.val[1], 4);
	dec.val[1] = vshlq_n_u8(str.val[1], 4) | vshrq_n_u8(str.val[2], 2);
	dec.val[2] = vshlq_n_u8(str.val[2], 6) | str.val[3];

	// Interleave and store decoded result:
	vst3q_u8((uint8_t *)o, dec);

	c += 64;
	o += 48;
	outl += 48;
	srclen -= 64;
}
