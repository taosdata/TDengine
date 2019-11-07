#if (defined(__ARM_NEON) && !defined(__ARM_NEON__))
#define __ARM_NEON__
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include "../../../include/libbase64.h"
#include "../../codecs.h"

#if (defined(__aarch64__) && defined(__ARM_NEON__) && HAVE_NEON64)
#include <arm_neon.h>

#define CMPGT(s,n)	vcgtq_u8((s), vdupq_n_u8(n))

static inline uint8x16x4_t
load_64byte_table (const uint8_t *p)
{
#if defined(__GNUC__) && !defined(__clang__)
	// As of October 2016, GCC does not support the 'vld1q_u8_x4()' intrinsic.
	uint8x16x4_t ret;
	ret.val[0] = vld1q_u8(p +  0);
	ret.val[1] = vld1q_u8(p + 16);
	ret.val[2] = vld1q_u8(p + 32);
	ret.val[3] = vld1q_u8(p + 48);
	return ret;
#else
	return vld1q_u8_x4(p);
#endif
}

// Encoding
// Use a 64-byte lookup to do the encoding.
// Reuse existing base64_table_enc table.

// Decoding
// The input consists of five valid character sets in the Base64 alphabet,
// which we need to map back to the 6-bit values they represent.
// There are three ranges, two singles, and then there's the rest.
//
//   #  From       To        LUT  Characters
//   1  [0..42]    [255]      #1  invalid input
//   2  [43]       [62]       #1  +
//   3  [44..46]   [255]      #1  invalid input
//   4  [47]       [63]       #1  /
//   5  [48..57]   [52..61]   #1  0..9
//   6  [58..63]   [255]      #1  invalid input
//   7  [64]       [255]      #2  invalid input
//   8  [65..90]   [0..25]    #2  A..Z
//   9  [91..96]   [255]      #2 invalid input
//  10  [97..122]  [26..51]   #2  a..z
//  11  [123..126] [255]      #2 invalid input
// (12) Everything else => invalid input

// First LUT will use VTBL instruction (out of range indices are set to 0 in destination).
static const uint8_t base64_dec_lut1[] =
{
	255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U,
	255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U,
	255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U, 255U,  62U, 255U, 255U, 255U,  63U,
	 52U,  53U,  54U,  55U,  56U,  57U,  58U,  59U,  60U,  61U, 255U, 255U, 255U, 255U, 255U, 255U
};
// Second LUT will use VTBX instruction (out of range indices will be unchanged in destination).
// Input [64..126] will be mapped to index [1..63] in this LUT. Index 0 means that value comes from LUT #1.
static const uint8_t base64_dec_lut2[] =
{
	  0U, 255U,   0U,   1U,   2U,   3U,   4U,   5U,   6U,   7U,   8U,   9U,  10U,  11U,  12U,  13U,
	 14U,  15U,  16U,  17U,  18U,  19U,  20U,  21U,  22U,  23U,  24U,  25U, 255U, 255U, 255U, 255U,
	255U, 255U,  26U,  27U,  28U,  29U,  30U,  31U,  32U,  33U,  34U,  35U,  36U,  37U,  38U,  39U,
	 40U,  41U,  42U,  43U,  44U,  45U,  46U,  47U,  48U,  49U,  50U,  51U, 255U, 255U, 255U, 255U
};

// All input values in range for the first look-up will be 0U in the second look-up result.
// All input values out of range for the first look-up will be 0U in the first look-up result.
// Thus, the two results can be ORed without conflicts.
// Invalid characters that are in the valid range for either look-up will be set to 255U in the combined result.
// Other invalid characters will just be passed through with the second look-up result (using VTBX instruction).
// Since the second LUT is 64 bytes, those passed through values are guaranteed to have a value greater than 63U.
// Therefore, valid characters will be mapped to the valid [0..63] range and all invalid characters will be mapped
// to values greater than 63.

#endif

// Stride size is so large on these NEON 64-bit functions
// (48 bytes encode, 64 bytes decode) that we inline the
// uint64 codec to stay performant on smaller inputs.

BASE64_ENC_FUNCTION(neon64)
{
#if (defined(__aarch64__) && defined(__ARM_NEON__) && HAVE_NEON64)
	const uint8x16x4_t tbl_enc = load_64byte_table(base64_table_enc);

	#include "../generic/enc_head.c"
	#include "enc_loop.c"
	#include "../generic/64/enc_loop.c"
	#include "../generic/enc_tail.c"
#else
	BASE64_ENC_STUB
#endif
}

BASE64_DEC_FUNCTION(neon64)
{
#if (defined(__aarch64__) && defined(__ARM_NEON__) && HAVE_NEON64)
	const uint8x16x4_t tbl_dec1 = load_64byte_table(base64_dec_lut1);
	const uint8x16x4_t tbl_dec2 = load_64byte_table(base64_dec_lut2);

	#include "../generic/dec_head.c"
	#include "dec_loop.c"
	#include "../generic/32/dec_loop.c"
	#include "../generic/dec_tail.c"
#else
	BASE64_DEC_STUB
#endif
}
