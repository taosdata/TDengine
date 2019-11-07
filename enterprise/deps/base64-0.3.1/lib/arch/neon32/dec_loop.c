// If we have NEON support, pick off 64 bytes at a time for as long as we can.
// Unlike the SSE codecs, we don't write trailing zero bytes to output, so we
// don't need to check if we have enough remaining input to cover them:
while (srclen >= 64)
{
	uint8x16x3_t dec;

	// Load 64 bytes and deinterleave:
	uint8x16x4_t str = vld4q_u8((uint8_t *)c);

	// see ssse3/dec_loop.c for an explanation of how the code works.

	const uint8x16_t lut_lo = {
		0x15, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
		0x11, 0x11, 0x13, 0x1A, 0x1B, 0x1B, 0x1B, 0x1A
	};
	const uint8x16_t lut_hi = {
		0x10, 0x10, 0x01, 0x02, 0x04, 0x08, 0x04, 0x08,
		0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10
	};

	const uint8x16_t lut_roll = {
		0,  16,  19,   4, (uint8_t)-65, (uint8_t)-65, (uint8_t)-71, (uint8_t)-71,
		0,   0,   0,   0,   0,   0,   0,   0
	};

	const uint8x16_t mask_F = vdupq_n_u8(0xf);
	const uint8x16_t mask_2F = vdupq_n_u8(0x2f);

	uint8x16_t classified;

	{
		const uint8x16_t hi_nibbles  = vshrq_n_u8(str.val[0], 4);
		const uint8x16_t lo_nibbles  = vandq_u8(str.val[0], mask_F);
		const uint8x16_t eq_2F = vceqq_u8(str.val[0], mask_2F);

		const uint8x16_t hi = vqtbl1q_u8(lut_hi, hi_nibbles);
		const uint8x16_t lo = vqtbl1q_u8(lut_lo, lo_nibbles);

		const uint8x16_t delta = vqtbl1q_u8(lut_roll, vaddq_u8(eq_2F, hi_nibbles));
		classified = vandq_u8(lo, hi);
		// Now simply add the delta values to the input:
		str.val[0] = vaddq_u8(str.val[0], delta);
	}
	{
		const uint8x16_t hi_nibbles  = vshrq_n_u8(str.val[1], 4);
		const uint8x16_t lo_nibbles  = vandq_u8(str.val[1], mask_F);
		const uint8x16_t eq_2F = vceqq_u8(str.val[1], mask_2F);

		const uint8x16_t hi = vqtbl1q_u8(lut_hi, hi_nibbles);
		const uint8x16_t lo = vqtbl1q_u8(lut_lo, lo_nibbles);

		const uint8x16_t delta = vqtbl1q_u8(lut_roll, vaddq_u8(eq_2F, hi_nibbles));
		classified = vorrq_u8(classified, vandq_u8(lo, hi));
		// Now simply add the delta values to the input:
		str.val[1] = vaddq_u8(str.val[1], delta);
	}
	{
		const uint8x16_t hi_nibbles  = vshrq_n_u8(str.val[2], 4);
		const uint8x16_t lo_nibbles  = vandq_u8(str.val[2], mask_F);
		const uint8x16_t eq_2F = vceqq_u8(str.val[2], mask_2F);

		const uint8x16_t hi = vqtbl1q_u8(lut_hi, hi_nibbles);
		const uint8x16_t lo = vqtbl1q_u8(lut_lo, lo_nibbles);

		const uint8x16_t delta = vqtbl1q_u8(lut_roll, vaddq_u8(eq_2F, hi_nibbles));
		classified = vorrq_u8(classified, vandq_u8(lo, hi));
		// Now simply add the delta values to the input:
		str.val[2] = vaddq_u8(str.val[2], delta);
	}
	{
		const uint8x16_t hi_nibbles  = vshrq_n_u8(str.val[3], 4);
		const uint8x16_t lo_nibbles  = vandq_u8(str.val[3], mask_F);
		const uint8x16_t eq_2F = vceqq_u8(str.val[3], mask_2F);

		const uint8x16_t hi = vqtbl1q_u8(lut_hi, hi_nibbles);
		const uint8x16_t lo = vqtbl1q_u8(lut_lo, lo_nibbles);

		const uint8x16_t delta = vqtbl1q_u8(lut_roll, vaddq_u8(eq_2F, hi_nibbles));
		classified = vorrq_u8(classified, vandq_u8(lo, hi));
		// Now simply add the delta values to the input:
		str.val[3] = vaddq_u8(str.val[3], delta);
	}

	// Check for invalid input: if any of the delta values are zero,
	// fall back on bytewise code to do error checking and reporting:
	// Extract both 32-bit halves; check that all bits are zero:
	if (vgetq_lane_u32((uint32x4_t)classified, 0) != 0
	 || vgetq_lane_u32((uint32x4_t)classified, 1) != 0
	 || vgetq_lane_u32((uint32x4_t)classified, 2) != 0
	 || vgetq_lane_u32((uint32x4_t)classified, 3) != 0) {
		break;
	}

	// Compress four bytes into three:
	dec.val[0] = vorrq_u8(vshlq_n_u8(str.val[0], 2), vshrq_n_u8(str.val[1], 4));
	dec.val[1] = vorrq_u8(vshlq_n_u8(str.val[1], 4), vshrq_n_u8(str.val[2], 2));
	dec.val[2] = vorrq_u8(vshlq_n_u8(str.val[2], 6), str.val[3]);

	// Interleave and store decoded result:
	vst3q_u8((uint8_t *)o, dec);

	c += 64;
	o += 48;
	outl += 48;
	srclen -= 64;
}
