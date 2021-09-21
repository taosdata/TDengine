/*
 * Copyright (C) 1999-2001 Free Software Foundation, Inc.
 * This file is part of the GNU LIBICONV Library.
 *
 * The GNU LIBICONV Library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * The GNU LIBICONV Library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with the GNU LIBICONV Library; see the file COPYING.LIB.
 * If not, write to the Free Software Foundation, Inc., 51 Franklin Street,
 * Fifth Floor, Boston, MA 02110-1301, USA.
 */

/*
 * JOHAB Hangul
 *
 * Ken Lunde writes in his "CJKV Information Processing" book, p. 114:
 * "Hangul can be composed of two or three jamo (some jamo are considered
 *  compound). Johab uses 19 initial jamo (consonants), 21 medial jamo (vowels)
 *  and 27 final jamo (consonants; 28 when you include the "fill" character
 *  for Hangul containing only two jamo). Multiplying these numbers results in
 *  11172."
 *
 * Structure of the Johab encoding (see p. 181-184):
 *   bit 15 = 1
 *   bit 14..10 = initial jamo, only 19+1 out of 32 possible values are used
 *   bit 9..5 = medial jamo, only 21+1 out of 32 possible values are used
 *   bit 4..0 = final jamo, only 27+1 out of 32 possible values are used
 * 
 * Structure of the Unicode encoding:
 * grep '^0x\([8-C]...\|D[0-7]..\)' unicode.org-mappings/EASTASIA/KSC/JOHAB.TXT
 * You see that all characters there are marked "HANGUL LETTER" or "HANGUL
 * SYLLABLE". If you eliminate the "HANGUL LETTER"s, the table is sorted
 * in ascending order according to Johab encoding and according to the Unicode
 * encoding. Now look a little more carefully, and you see that the following
 * formula holds:
 *     unicode == 0xAC00
 *                + 21 * 28 * (jamo_initial_index[(johab >> 10) & 31] - 1)
 *                + 28 * (jamo_medial_index[(johab >> 5) & 31] - 1)
 *                + jamo_final_index[johab & 31]
 * where the index tables are defined as below.
 */

/* Tables mapping 5-bit groups to jamo letters. */
/* Note that Jamo XX = UHC 0xA4A0+XX = Unicode 0x3130+XX */
#define NONE 0xfd
#define FILL 0xff
static const unsigned char jamo_initial[32] = {
  NONE, FILL, 0x01, 0x02, 0x04, 0x07, 0x08, 0x09,
  0x11, 0x12, 0x13, 0x15, 0x16, 0x17, 0x18, 0x19,
  0x1a, 0x1b, 0x1c, 0x1d, 0x1e, NONE, NONE, NONE,
  NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE,
};
static const unsigned char jamo_medial[32] = {
  NONE, NONE, FILL, 0x1f, 0x20, 0x21, 0x22, 0x23,
  NONE, NONE, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
  NONE, NONE, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
  NONE, NONE, 0x30, 0x31, 0x32, 0x33, NONE, NONE,
};
static const unsigned char jamo_final[32] = {
  NONE, FILL, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
  0x07, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11, NONE, 0x12, 0x14, 0x15, 0x16, 0x17,
  0x18, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, NONE, NONE,
};
/* Same as jamo_final, except that it excludes characters already
   contained in jamo_initial. 11 characters instead of 27. */
static const unsigned char jamo_final_notinitial[32] = {
  NONE, NONE, NONE, NONE, 0x03, NONE, 0x05, 0x06,
  NONE, NONE, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, NONE, NONE, NONE, 0x14, NONE, NONE, NONE,
  NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE,
};

/* Tables mapping 5-bit groups to packed indices. */
#define none -1
#define fill 0
static const signed char jamo_initial_index[32] = {
  none, fill, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
  0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
  0x0f, 0x10, 0x11, 0x12, 0x13, none, none, none,
  none, none, none, none, none, none, none, none,
};
static const signed char jamo_medial_index[32] = {
  none, none, fill, 0x01, 0x02, 0x03, 0x04, 0x05,
  none, none, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
  none, none, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11,
  none, none, 0x12, 0x13, 0x14, 0x15, none, none,
};
static const signed char jamo_final_index[32] = {
  none, fill, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
  0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
  0x0f, 0x10, none, 0x11, 0x12, 0x13, 0x14, 0x15,
  0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, none, none,
};

static int
johab_hangul_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c1 = s[0];
  if ((c1 >= 0x84 && c1 <= 0xd3)) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if ((c2 >= 0x41 && c2 < 0x7f) || (c2 >= 0x81 && c2 < 0xff)) {
        unsigned int johab = (c1 << 8) | c2;
        unsigned int bitspart1 = (johab >> 10) & 31;
        unsigned int bitspart2 = (johab >> 5) & 31;
        unsigned int bitspart3 = johab & 31;
        int index1 = jamo_initial_index[bitspart1];
        int index2 = jamo_medial_index[bitspart2];
        int index3 = jamo_final_index[bitspart3];
        /* Exclude "none" values. */
        if (index1 >= 0 && index2 >= 0 && index3 >= 0) {
          /* Deal with "fill" values in initial or medial position. */
          if (index1 == fill) {
            if (index2 == fill) {
              unsigned char jamo3 = jamo_final_notinitial[bitspart3];
              if (jamo3 != NONE) {
                *pwc = (ucs4_t) 0x3130 + jamo3;
                return 2;
              }
            } else if (index3 == fill) {
              unsigned char jamo2 = jamo_medial[bitspart2];
              if (jamo2 != NONE && jamo2 != FILL) {
                *pwc = (ucs4_t) 0x3130 + jamo2;
                return 2;
              }
            }
            /* Syllables composed only of medial and final don't exist. */
          } else if (index2 == fill) {
            if (index3 == fill) {
              unsigned char jamo1 = jamo_initial[bitspart1];
              if (jamo1 != NONE && jamo1 != FILL) {
                *pwc = (ucs4_t) 0x3130 + jamo1;
                return 2;
              }
            }
            /* Syllables composed only of initial and final don't exist. */
          } else {
             /* index1 and index2 are not fill, but index3 may be fill. */
             /* Nothing more to exclude. All 11172 code points are valid. */
             *pwc = 0xac00 + ((index1 - 1) * 21 + (index2 - 1)) * 28 + index3;
             return 2;
          }
        }
      }
      return RET_ILSEQ;
    }
    return RET_TOOFEW(0);
  }
  return RET_ILSEQ;
}

/* 51 Jamo: 19 initial, 21 medial, 11 final not initial. */
static const unsigned short johab_hangul_page31[51] = {
          0x8841, 0x8c41, 0x8444, 0x9041, 0x8446, 0x8447, 0x9441, /*0x30-0x37*/
  0x9841, 0x9c41, 0x844a, 0x844b, 0x844c, 0x844d, 0x844e, 0x844f, /*0x38-0x3f*/
  0x8450, 0xa041, 0xa441, 0xa841, 0x8454, 0xac41, 0xb041, 0xb441, /*0x40-0x47*/
  0xb841, 0xbc41, 0xc041, 0xc441, 0xc841, 0xcc41, 0xd041, 0x8461, /*0x48-0x4f*/
  0x8481, 0x84a1, 0x84c1, 0x84e1, 0x8541, 0x8561, 0x8581, 0x85a1, /*0x50-0x57*/
  0x85c1, 0x85e1, 0x8641, 0x8661, 0x8681, 0x86a1, 0x86c1, 0x86e1, /*0x58-0x5f*/
  0x8741, 0x8761, 0x8781, 0x87a1,                                 /*0x60-0x67*/
};

/* Tables mapping packed indices to 5-bit groups. */
/* index1+1 = jamo_initial_index[bitspart1]  <==>
   bitspart1 = jamo_initial_index_inverse[index1] */
static const char jamo_initial_index_inverse[19] = {
              0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11, 0x12, 0x13, 0x14,
};
/* index2+1 = jamo_medial_index[bitspart2]  <==>
   bitspart2 = jamo_medial_index_inverse[index2] */
static const char jamo_medial_index_inverse[21] = {
                    0x03, 0x04, 0x05, 0x06, 0x07,
              0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
              0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
              0x1a, 0x1b, 0x1c, 0x1d,
};
/* index3 = jamo_final_index[bitspart3]  <==>
   bitspart3 = jamo_final_index_inverse[index3] */
static const char jamo_final_index_inverse[28] = {
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11,       0x13, 0x14, 0x15, 0x16, 0x17,
  0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
};

static int
johab_hangul_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (n >= 2) {
    if (wc >= 0x3131 && wc < 0x3164) {
      unsigned short c = johab_hangul_page31[wc-0x3131];
      r[0] = (c >> 8); r[1] = (c & 0xff);
      return 2;
    } else if (wc >= 0xac00 && wc < 0xd7a4) {
      unsigned int index1;
      unsigned int index2;
      unsigned int index3;
      unsigned short c;
      unsigned int tmp = wc - 0xac00;
      index3 = tmp % 28; tmp = tmp / 28;
      index2 = tmp % 21; tmp = tmp / 21;
      index1 = tmp;
      c = (((((1 << 5)
              | jamo_initial_index_inverse[index1]) << 5)
            | jamo_medial_index_inverse[index2]) << 5)
          | jamo_final_index_inverse[index3];
      r[0] = (c >> 8); r[1] = (c & 0xff);
      return 2;
    }
    return RET_ILUNI;
  }
  return RET_TOOSMALL;
}

/*
 * Decomposition of JOHAB Hangul in one to three Johab Jamo elements.
 */

/* Decompose wc into r[0..2], and return the number of resulting Jamo elements.
   Return RET_ILUNI if decomposition is not possible. */

static int johab_hangul_decompose (conv_t conv, ucs4_t* r, ucs4_t wc)
{
  unsigned char buf[2];
  int ret = johab_hangul_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    unsigned int hangul = (buf[0] << 8) | buf[1];
    unsigned char jamo1 = jamo_initial[(hangul >> 10) & 31];
    unsigned char jamo2 = jamo_medial[(hangul >> 5) & 31];
    unsigned char jamo3 = jamo_final[hangul & 31];
    if ((hangul >> 15) != 1) abort();
    if (jamo1 != NONE && jamo2 != NONE && jamo3 != NONE) {
      /* They are not all three == FILL because that would correspond to
         johab = 0x8441, which doesn't exist. */
      ucs4_t* p = r;
      if (jamo1 != FILL)
        *p++ = 0x3130 + jamo1;
      if (jamo2 != FILL)
        *p++ = 0x3130 + jamo2;
      if (jamo3 != FILL)
        *p++ = 0x3130 + jamo3;
      return p-r;
    }
  }
  return RET_ILUNI;
}

#undef fill
#undef none
#undef FILL
#undef NONE
