/*
 * Copyright (C) 1999-2001, 2004 Free Software Foundation, Inc.
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
 * CP1255
 */

#include "flushwc.h"

/* Combining characters used in Hebrew encoding CP1255. */

/* Relevant combining characters:
   0x05b4, 0x05b7, 0x05b8, 0x05b9, 0x05bc, 0x05bf, 0x05c1, 0x05c2. */

/* Composition tables for each of the relevant combining characters. */
static const struct { unsigned short base; unsigned short composed; } cp1255_comp_table_data[] = {
#define cp1255_comp_table05b4_idx 0
#define cp1255_comp_table05b4_len 1
  { 0x05D9, 0xFB1D },
#define cp1255_comp_table05b7_idx (cp1255_comp_table05b4_idx+cp1255_comp_table05b4_len)
#define cp1255_comp_table05b7_len 2
  { 0x05D0, 0xFB2E },
  { 0x05F2, 0xFB1F },
#define cp1255_comp_table05b8_idx (cp1255_comp_table05b7_idx+cp1255_comp_table05b7_len)
#define cp1255_comp_table05b8_len 1
  { 0x05D0, 0xFB2F },
#define cp1255_comp_table05b9_idx (cp1255_comp_table05b8_idx+cp1255_comp_table05b8_len)
#define cp1255_comp_table05b9_len 1
  { 0x05D5, 0xFB4B },
#define cp1255_comp_table05bc_idx (cp1255_comp_table05b9_idx+cp1255_comp_table05b9_len)
#define cp1255_comp_table05bc_len 24
  { 0x05D0, 0xFB30 },
  { 0x05D1, 0xFB31 },
  { 0x05D2, 0xFB32 },
  { 0x05D3, 0xFB33 },
  { 0x05D4, 0xFB34 },
  { 0x05D5, 0xFB35 },
  { 0x05D6, 0xFB36 },
  { 0x05D8, 0xFB38 },
  { 0x05D9, 0xFB39 },
  { 0x05DA, 0xFB3A },
  { 0x05DB, 0xFB3B },
  { 0x05DC, 0xFB3C },
  { 0x05DE, 0xFB3E },
  { 0x05E0, 0xFB40 },
  { 0x05E1, 0xFB41 },
  { 0x05E3, 0xFB43 },
  { 0x05E4, 0xFB44 },
  { 0x05E6, 0xFB46 },
  { 0x05E7, 0xFB47 },
  { 0x05E8, 0xFB48 },
  { 0x05E9, 0xFB49 },
  { 0x05EA, 0xFB4A },
  { 0xFB2A, 0xFB2C },
  { 0xFB2B, 0xFB2D },
#define cp1255_comp_table05bf_idx (cp1255_comp_table05bc_idx+cp1255_comp_table05bc_len)
#define cp1255_comp_table05bf_len 3
  { 0x05D1, 0xFB4C },
  { 0x05DB, 0xFB4D },
  { 0x05E4, 0xFB4E },
#define cp1255_comp_table05c1_idx (cp1255_comp_table05bf_idx+cp1255_comp_table05bf_len)
#define cp1255_comp_table05c1_len 2
  { 0x05E9, 0xFB2A },
  { 0xFB49, 0xFB2C },
#define cp1255_comp_table05c2_idx (cp1255_comp_table05c1_idx+cp1255_comp_table05c1_len)
#define cp1255_comp_table05c2_len 2
  { 0x05E9, 0xFB2B },
  { 0xFB49, 0xFB2D },
};
static const struct { unsigned int len; unsigned int idx; } cp1255_comp_table[] = {
  { cp1255_comp_table05b4_len, cp1255_comp_table05b4_idx },
  { cp1255_comp_table05b7_len, cp1255_comp_table05b7_idx },
  { cp1255_comp_table05b8_len, cp1255_comp_table05b8_idx },
  { cp1255_comp_table05b9_len, cp1255_comp_table05b9_idx },
  { cp1255_comp_table05bc_len, cp1255_comp_table05bc_idx },
  { cp1255_comp_table05bf_len, cp1255_comp_table05bf_idx },
  { cp1255_comp_table05c1_len, cp1255_comp_table05c1_idx },
  { cp1255_comp_table05c2_len, cp1255_comp_table05c2_idx },
};

/* Decomposition table for the relevant Unicode characters. */
struct cp1255_decomp { unsigned short composed; unsigned short base; int comb1 : 8; signed int comb2 : 8; };
static const struct cp1255_decomp cp1255_decomp_table[] = {
  { 0xFB1D, 0x05D9, 0, -1 },
  { 0xFB1F, 0x05F2, 1, -1 },
  { 0xFB2A, 0x05E9, 6, -1 },
  { 0xFB2B, 0x05E9, 7, -1 },
  { 0xFB2C, 0x05E9, 4, 6 },
  { 0xFB2D, 0x05E9, 4, 7 },
  { 0xFB2E, 0x05D0, 1, -1 },
  { 0xFB2F, 0x05D0, 2, -1 },
  { 0xFB30, 0x05D0, 4, -1 },
  { 0xFB31, 0x05D1, 4, -1 },
  { 0xFB32, 0x05D2, 4, -1 },
  { 0xFB33, 0x05D3, 4, -1 },
  { 0xFB34, 0x05D4, 4, -1 },
  { 0xFB35, 0x05D5, 4, -1 },
  { 0xFB36, 0x05D6, 4, -1 },
  { 0xFB38, 0x05D8, 4, -1 },
  { 0xFB39, 0x05D9, 4, -1 },
  { 0xFB3A, 0x05DA, 4, -1 },
  { 0xFB3B, 0x05DB, 4, -1 },
  { 0xFB3C, 0x05DC, 4, -1 },
  { 0xFB3E, 0x05DE, 4, -1 },
  { 0xFB40, 0x05E0, 4, -1 },
  { 0xFB41, 0x05E1, 4, -1 },
  { 0xFB43, 0x05E3, 4, -1 },
  { 0xFB44, 0x05E4, 4, -1 },
  { 0xFB46, 0x05E6, 4, -1 },
  { 0xFB47, 0x05E7, 4, -1 },
  { 0xFB48, 0x05E8, 4, -1 },
  { 0xFB49, 0x05E9, 4, -1 },
  { 0xFB4A, 0x05EA, 4, -1 },
  { 0xFB4B, 0x05D5, 3, -1 },
  { 0xFB4C, 0x05D1, 5, -1 },
  { 0xFB4D, 0x05DB, 5, -1 },
  { 0xFB4E, 0x05E4, 5, -1 },
};

static const unsigned char cp1255_comb_table[] = {
  0xc4, 0xc7, 0xc8, 0xc9, 0xcc, 0xcf, 0xd1, 0xd2,
};

static const unsigned short cp1255_2uni[128] = {
  /* 0x80 */
  0x20ac, 0xfffd, 0x201a, 0x0192, 0x201e, 0x2026, 0x2020, 0x2021,
  0x02c6, 0x2030, 0xfffd, 0x2039, 0xfffd, 0xfffd, 0xfffd, 0xfffd,
  /* 0x90 */
  0xfffd, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014,
  0x02dc, 0x2122, 0xfffd, 0x203a, 0xfffd, 0xfffd, 0xfffd, 0xfffd,
  /* 0xa0 */
  0x00a0, 0x00a1, 0x00a2, 0x00a3, 0x20aa, 0x00a5, 0x00a6, 0x00a7,
  0x00a8, 0x00a9, 0x00d7, 0x00ab, 0x00ac, 0x00ad, 0x00ae, 0x00af,
  /* 0xb0 */
  0x00b0, 0x00b1, 0x00b2, 0x00b3, 0x00b4, 0x00b5, 0x00b6, 0x00b7,
  0x00b8, 0x00b9, 0x00f7, 0x00bb, 0x00bc, 0x00bd, 0x00be, 0x00bf,
  /* 0xc0 */
  0x05b0, 0x05b1, 0x05b2, 0x05b3, 0x05b4, 0x05b5, 0x05b6, 0x05b7,
  0x05b8, 0x05b9, 0xfffd, 0x05bb, 0x05bc, 0x05bd, 0x05be, 0x05bf,
  /* 0xd0 */
  0x05c0, 0x05c1, 0x05c2, 0x05c3, 0x05f0, 0x05f1, 0x05f2, 0x05f3,
  0x05f4, 0xfffd, 0xfffd, 0xfffd, 0xfffd, 0xfffd, 0xfffd, 0xfffd,
  /* 0xe0 */
  0x05d0, 0x05d1, 0x05d2, 0x05d3, 0x05d4, 0x05d5, 0x05d6, 0x05d7,
  0x05d8, 0x05d9, 0x05da, 0x05db, 0x05dc, 0x05dd, 0x05de, 0x05df,
  /* 0xf0 */
  0x05e0, 0x05e1, 0x05e2, 0x05e3, 0x05e4, 0x05e5, 0x05e6, 0x05e7,
  0x05e8, 0x05e9, 0x05ea, 0xfffd, 0xfffd, 0x200e, 0x200f, 0xfffd,
};

/* In the CP1255 to Unicode direction, the state contains a buffered
   character, or 0 if none. */

static int
cp1255_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  unsigned short wc;
  unsigned short last_wc;
  if (c < 0x80) {
    wc = c;
  } else {
    wc = cp1255_2uni[c-0x80];
    if (wc == 0xfffd)
      return RET_ILSEQ;
  }
  last_wc = conv->istate;
  if (last_wc) {
    if (wc >= 0x05b0 && wc < 0x05c5) {
      /* See whether last_wc and wc can be combined. */
      unsigned int k;
      unsigned int i1, i2;
      switch (wc) {
        case 0x05b4: k = 0; break;
        case 0x05b7: k = 1; break;
        case 0x05b8: k = 2; break;
        case 0x05b9: k = 3; break;
        case 0x05bc: k = 4; break;
        case 0x05bf: k = 5; break;
        case 0x05c1: k = 6; break;
        case 0x05c2: k = 7; break;
        default: goto not_combining;
      }
      i1 = cp1255_comp_table[k].idx;
      i2 = i1 + cp1255_comp_table[k].len-1;
      if (last_wc >= cp1255_comp_table_data[i1].base
          && last_wc <= cp1255_comp_table_data[i2].base) {
        unsigned int i;
        for (;;) {
          i = (i1+i2)>>1;
          if (last_wc == cp1255_comp_table_data[i].base)
            break;
          if (last_wc < cp1255_comp_table_data[i].base) {
            if (i1 == i)
              goto not_combining;
            i2 = i;
          } else {
            if (i1 != i)
              i1 = i;
            else {
              i = i2;
              if (last_wc == cp1255_comp_table_data[i].base)
                break;
              goto not_combining;
            }
          }
        }
        last_wc = cp1255_comp_table_data[i].composed;
        if (last_wc == 0xfb2a || last_wc == 0xfb2b || last_wc == 0xfb49) {
          /* Buffer the combined character. */
          conv->istate = last_wc;
          return RET_TOOFEW(1);
        } else {
          /* Output the combined character. */
          conv->istate = 0;
          *pwc = (ucs4_t) last_wc;
          return 1;
        }
      }
    }
  not_combining:
    /* Output the buffered character. */
    conv->istate = 0;
    *pwc = (ucs4_t) last_wc;
    return 0; /* Don't advance the input pointer. */
  }
  if ((wc >= 0x05d0 && wc <= 0x05ea && ((0x07db5f7f >> (wc - 0x05d0)) & 1))
      || wc == 0x05f2) {
    /* wc is a possible match in cp1255_comp_table_data. Buffer it. */
    conv->istate = wc;
    return RET_TOOFEW(1);
  } else {
    /* Output wc immediately. */
    *pwc = (ucs4_t) wc;
    return 1;
  }
}

#define cp1255_flushwc normal_flushwc

static const unsigned char cp1255_page00[88] = {
  0xa0, 0xa1, 0xa2, 0xa3, 0x00, 0xa5, 0xa6, 0xa7, /* 0xa0-0xa7 */
  0xa8, 0xa9, 0x00, 0xab, 0xac, 0xad, 0xae, 0xaf, /* 0xa8-0xaf */
  0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, /* 0xb0-0xb7 */
  0xb8, 0xb9, 0x00, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf, /* 0xb8-0xbf */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xc0-0xc7 */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xc8-0xcf */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, /* 0xd0-0xd7 */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xd8-0xdf */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xe0-0xe7 */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xe8-0xef */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xba, /* 0xf0-0xf7 */
};
static const unsigned char cp1255_page02[32] = {
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0x00, /* 0xc0-0xc7 */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xc8-0xcf */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xd0-0xd7 */
  0x00, 0x00, 0x00, 0x00, 0x98, 0x00, 0x00, 0x00, /* 0xd8-0xdf */
};
static const unsigned char cp1255_page05[72] = {
  0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, /* 0xb0-0xb7 */
  0xc8, 0xc9, 0x00, 0xcb, 0xcc, 0xcd, 0xce, 0xcf, /* 0xb8-0xbf */
  0xd0, 0xd1, 0xd2, 0xd3, 0x00, 0x00, 0x00, 0x00, /* 0xc0-0xc7 */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xc8-0xcf */
  0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, /* 0xd0-0xd7 */
  0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef, /* 0xd8-0xdf */
  0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, /* 0xe0-0xe7 */
  0xf8, 0xf9, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0xe8-0xef */
  0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0x00, 0x00, 0x00, /* 0xf0-0xf7 */
};
static const unsigned char cp1255_page20[56] = {
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0xfe, /* 0x08-0x0f */
  0x00, 0x00, 0x00, 0x96, 0x97, 0x00, 0x00, 0x00, /* 0x10-0x17 */
  0x91, 0x92, 0x82, 0x00, 0x93, 0x94, 0x84, 0x00, /* 0x18-0x1f */
  0x86, 0x87, 0x95, 0x00, 0x00, 0x00, 0x85, 0x00, /* 0x20-0x27 */
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0x28-0x2f */
  0x89, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0x30-0x37 */
  0x00, 0x8b, 0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, /* 0x38-0x3f */
};

static int
cp1255_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char c = 0;
  if (wc < 0x0080) {
    *r = wc;
    return 1;
  }
  else if (wc >= 0x00a0 && wc < 0x00f8)
    c = cp1255_page00[wc-0x00a0];
  else if (wc == 0x0192)
    c = 0x83;
  else if (wc >= 0x02c0 && wc < 0x02e0)
    c = cp1255_page02[wc-0x02c0];
  else if (wc >= 0x05b0 && wc < 0x05f8)
    c = cp1255_page05[wc-0x05b0];
  else if (wc >= 0x2008 && wc < 0x2040)
    c = cp1255_page20[wc-0x2008];
  else if (wc == 0x20aa)
    c = 0xa4;
  else if (wc == 0x20ac)
    c = 0x80;
  else if (wc == 0x2122)
    c = 0x99;
  if (c != 0) {
    *r = c;
    return 1;
  }
  /* Try canonical decomposition. */
  {
    /* Binary search through cp1255_decomp_table. */
    unsigned int i1 = 0;
    unsigned int i2 = sizeof(cp1255_decomp_table)/sizeof(cp1255_decomp_table[0])-1;
    if (wc >= cp1255_decomp_table[i1].composed
        && wc <= cp1255_decomp_table[i2].composed) {
      unsigned int i;
      for (;;) {
        /* Here i2 - i1 > 0. */
        i = (i1+i2)>>1;
        if (wc == cp1255_decomp_table[i].composed)
          break;
        if (wc < cp1255_decomp_table[i].composed) {
          if (i1 == i)
            return RET_ILUNI;
          /* Here i1 < i < i2. */
          i2 = i;
        } else {
          /* Here i1 <= i < i2. */
          if (i1 != i)
            i1 = i;
          else {
            /* Here i2 - i1 = 1. */
            i = i2;
            if (wc == cp1255_decomp_table[i].composed)
              break;
            else
              return RET_ILUNI;
          }
        }
      }
      /* Found a canonical decomposition. */
      wc = cp1255_decomp_table[i].base;
      /* wc is one of 0x05d0..0x05d6, 0x05d8..0x05dc, 0x05de, 0x05e0..0x05e1,
         0x05e3..0x05e4, 0x05e6..0x05ea, 0x05f2. */
      c = cp1255_page05[wc-0x05b0];
      if (cp1255_decomp_table[i].comb2 < 0) {
        if (n < 2)
          return RET_TOOSMALL;
        r[0] = c;
        r[1] = cp1255_comb_table[cp1255_decomp_table[i].comb1];
        return 2;
      } else {
        if (n < 3)
          return RET_TOOSMALL;
        r[0] = c;
        r[1] = cp1255_comb_table[cp1255_decomp_table[i].comb1];
        r[2] = cp1255_comb_table[cp1255_decomp_table[i].comb2];
        return 3;
      }
    }
  }
  return RET_ILUNI;
}
