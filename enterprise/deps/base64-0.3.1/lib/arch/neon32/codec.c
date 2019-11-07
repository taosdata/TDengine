#if (defined(__ARM_NEON) && !defined(__ARM_NEON__))
#define __ARM_NEON__
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include "../../../include/libbase64.h"
#include "../../codecs.h"

#if (defined(__arm__) && defined(__ARM_NEON__) && HAVE_NEON32)

#include <arm_neon.h>

#define CMPGT(s,n)	vcgtq_u8((s), vdupq_n_u8(n))
#define CMPEQ(s,n)	vceqq_u8((s), vdupq_n_u8(n))
#define REPLACE(s,n)	vandq_u8((s), vdupq_n_u8(n))
#define RANGE(s,a,b)	vandq_u8(vcgeq_u8((s), vdupq_n_u8(a)), vcleq_u8((s), vdupq_n_u8(b)))

static inline uint8x16x4_t
enc_reshuffle (uint8x16x3_t in)
{
	uint8x16x4_t out;

	// Divide bits of three input bytes over four output bytes:
	out.val[0] = vshrq_n_u8(in.val[0], 2);
	out.val[1] = vorrq_u8(vshrq_n_u8(in.val[1], 4), vshlq_n_u8(in.val[0], 4));
	out.val[2] = vorrq_u8(vshrq_n_u8(in.val[2], 6), vshlq_n_u8(in.val[1], 2));
	out.val[3] = in.val[2];

	// Clear top two bits:
	out.val[0] = vandq_u8(out.val[0], vdupq_n_u8(0x3F));
	out.val[1] = vandq_u8(out.val[1], vdupq_n_u8(0x3F));
	out.val[2] = vandq_u8(out.val[2], vdupq_n_u8(0x3F));
	out.val[3] = vandq_u8(out.val[3], vdupq_n_u8(0x3F));

	return out;
}

static inline uint8x16_t
vqtbl1q_u8 (uint8x16_t lut, uint8x16_t indices)
{
	uint8x8x2_t lut2;
	uint8x8x2_t result;

	lut2.val[0] = vget_low_u8(lut);
	lut2.val[1] = vget_high_u8(lut);

	result.val[0] = vtbl2_u8(lut2, vget_low_u8(indices));
	result.val[1] = vtbl2_u8(lut2, vget_high_u8(indices));

	return vcombine_u8(result.val[0], result.val[1]);
}

static inline uint8x16x4_t
enc_translate (uint8x16x4_t in)
{
	// LUT contains Absolute offset for all ranges:
	const uint8x16_t lut = {
		 65U,  71U, 252U, 252U,
		252U, 252U, 252U, 252U,
		252U, 252U, 252U, 252U,
		237U, 240U,   0U,   0U
	};

	const uint8x16_t offset = vdupq_n_u8(51);

	uint8x16x4_t indices, mask, delta, out;

	// Translate values 0..63 to the Base64 alphabet. There are five sets:
	// #  From      To         Abs    Index  Characters
	// 0  [0..25]   [65..90]   +65        0  ABCDEFGHIJKLMNOPQRSTUVWXYZ
	// 1  [26..51]  [97..122]  +71        1  abcdefghijklmnopqrstuvwxyz
	// 2  [52..61]  [48..57]    -4  [2..11]  0123456789
	// 3  [62]      [43]       -19       12  +
	// 4  [63]      [47]       -16       13  /

	// Create LUT indices from input:
	// the index for range #0 is right, others are 1 less than expected:
	indices.val[0] = vqsubq_u8(in.val[0], offset);
	indices.val[1] = vqsubq_u8(in.val[1], offset);
	indices.val[2] = vqsubq_u8(in.val[2], offset);
	indices.val[3] = vqsubq_u8(in.val[3], offset);

	// mask is 0xFF (-1) for range #[1..4] and 0x00 for range #0:
	mask.val[0] = CMPGT(in.val[0], 25);
	mask.val[1] = CMPGT(in.val[1], 25);
	mask.val[2] = CMPGT(in.val[2], 25);
	mask.val[3] = CMPGT(in.val[3], 25);

	// substract -1, so add 1 to indices for range #[1..4], All indices are now correct:
	indices.val[0] = vsubq_u8(indices.val[0], mask.val[0]);
	indices.val[1] = vsubq_u8(indices.val[1], mask.val[1]);
	indices.val[2] = vsubq_u8(indices.val[2], mask.val[2]);
	indices.val[3] = vsubq_u8(indices.val[3], mask.val[3]);

	// lookup delta values
	delta.val[0] = vqtbl1q_u8(lut, indices.val[0]);
	delta.val[1] = vqtbl1q_u8(lut, indices.val[1]);
	delta.val[2] = vqtbl1q_u8(lut, indices.val[2]);
	delta.val[3] = vqtbl1q_u8(lut, indices.val[3]);

	// add delta values:
	out.val[0] = vaddq_u8(in.val[0], delta.val[0]);
	out.val[1] = vaddq_u8(in.val[1], delta.val[1]);
	out.val[2] = vaddq_u8(in.val[2], delta.val[2]);
	out.val[3] = vaddq_u8(in.val[3], delta.val[3]);

	return out;
}

#endif

// Stride size is so large on these NEON 32-bit functions
// (48 bytes encode, 32 bytes decode) that we inline the
// uint32 codec to stay performant on smaller inputs.

BASE64_ENC_FUNCTION(neon32)
{
#if (defined(__arm__) && defined(__ARM_NEON__) && HAVE_NEON32)
	#include "../generic/enc_head.c"
	#include "enc_loop.c"
	#include "../generic/32/enc_loop.c"
	#include "../generic/enc_tail.c"
#else
	BASE64_ENC_STUB
#endif
}

BASE64_DEC_FUNCTION(neon32)
{
#if (defined(__arm__) && defined(__ARM_NEON__) && HAVE_NEON32)
	#include "../generic/dec_head.c"
	#include "dec_loop.c"
	#include "../generic/32/dec_loop.c"
	#include "../generic/dec_tail.c"
#else
	BASE64_DEC_STUB
#endif
}
