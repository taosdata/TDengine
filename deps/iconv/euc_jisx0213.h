/*
 * Copyright (C) 1999-2002 Free Software Foundation, Inc.
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
 * EUC-JISX0213
 */

/* The structure of EUC-JISX0213 is as follows:

   0x00..0x7F: ASCII

   0x8E{A1..FE}: JISX0201 Katakana, with prefix 0x8E, offset by +0x80.

   0x8F{A1..FE}{A1..FE}: JISX0213 plane 2, with prefix 0x8F, offset by +0x8080.

   0x{A1..FE}{A1..FE}: JISX0213 plane 1, offset by +0x8080.

   Note that some JISX0213 characters are not contained in Unicode 3.2
   and are therefore best represented as sequences of Unicode characters.
*/

#include "jisx0213.h"
#include "flushwc.h"

static int
euc_jisx0213_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  ucs4_t last_wc = conv->istate;
  if (last_wc) {
    /* Output the buffered character. */
    conv->istate = 0;
    *pwc = last_wc;
    return 0; /* Don't advance the input pointer. */
  } else {
    unsigned char c = *s;
    if (c < 0x80) {
      /* Plain ASCII character. */
      *pwc = (ucs4_t) c;
      return 1;
    } else {
      if ((c >= 0xa1 && c <= 0xfe) || c == 0x8e || c == 0x8f) {
        /* Two or three byte character. */
        if (n >= 2) {
          unsigned char c2 = s[1];
          if (c2 >= 0xa1 && c2 <= 0xfe) {
            if (c == 0x8e) {
              /* Half-width katakana. */
              if (c2 <= 0xdf) {
                *pwc = c2 + 0xfec0;
                return 2;
              }
            } else {
              ucs4_t wc;
              if (c == 0x8f) {
                /* JISX 0213 plane 2. */
                if (n >= 3) {
                  unsigned char c3 = s[2];
                  wc = jisx0213_to_ucs4(0x200-0x80+c2,c3^0x80);
                } else
                  return RET_TOOFEW(0);
              } else {
                /* JISX 0213 plane 1. */
                wc = jisx0213_to_ucs4(0x100-0x80+c,c2^0x80);
              }
              if (wc) {
                if (wc < 0x80) {
                  /* It's a combining character. */
                  ucs4_t wc1 = jisx0213_to_ucs_combining[wc - 1][0];
                  ucs4_t wc2 = jisx0213_to_ucs_combining[wc - 1][1];
                  /* We cannot output two Unicode characters at once. So,
                     output the first character and buffer the second one. */
                  *pwc = wc1;
                  conv->istate = wc2;
                } else
                  *pwc = wc;
                return (c == 0x8f ? 3 : 2);
              }
            }
          }
        } else
          return RET_TOOFEW(0);
      }
      return RET_ILSEQ;
    }
  }
}

#define euc_jisx0213_flushwc normal_flushwc

/* Composition tables for each of the relevant combining characters.  */
static const struct { unsigned short base; unsigned short composed; } euc_jisx0213_comp_table_data[] = {
#define euc_jisx0213_comp_table02e5_idx 0
#define euc_jisx0213_comp_table02e5_len 1
  { 0xabe4, 0xabe5 }, /* 0x12B65 = 0x12B64 U+02E5 */
#define euc_jisx0213_comp_table02e9_idx (euc_jisx0213_comp_table02e5_idx+euc_jisx0213_comp_table02e5_len)
#define euc_jisx0213_comp_table02e9_len 1
  { 0xabe0, 0xabe6 }, /* 0x12B66 = 0x12B60 U+02E9 */
#define euc_jisx0213_comp_table0300_idx (euc_jisx0213_comp_table02e9_idx+euc_jisx0213_comp_table02e9_len)
#define euc_jisx0213_comp_table0300_len 5
  { 0xa9dc, 0xabc4 }, /* 0x12B44 = 0x1295C U+0300 */
  { 0xabb8, 0xabc8 }, /* 0x12B48 = 0x12B38 U+0300 */
  { 0xabb7, 0xabca }, /* 0x12B4A = 0x12B37 U+0300 */
  { 0xabb0, 0xabcc }, /* 0x12B4C = 0x12B30 U+0300 */
  { 0xabc3, 0xabce }, /* 0x12B4E = 0x12B43 U+0300 */
#define euc_jisx0213_comp_table0301_idx (euc_jisx0213_comp_table0300_idx+euc_jisx0213_comp_table0300_len)
#define euc_jisx0213_comp_table0301_len 4
  { 0xabb8, 0xabc9 }, /* 0x12B49 = 0x12B38 U+0301 */
  { 0xabb7, 0xabcb }, /* 0x12B4B = 0x12B37 U+0301 */
  { 0xabb0, 0xabcd }, /* 0x12B4D = 0x12B30 U+0301 */
  { 0xabc3, 0xabcf }, /* 0x12B4F = 0x12B43 U+0301 */
#define euc_jisx0213_comp_table309a_idx (euc_jisx0213_comp_table0301_idx+euc_jisx0213_comp_table0301_len)
#define euc_jisx0213_comp_table309a_len 14
  { 0xa4ab, 0xa4f7 }, /* 0x12477 = 0x1242B U+309A */
  { 0xa4ad, 0xa4f8 }, /* 0x12478 = 0x1242D U+309A */
  { 0xa4af, 0xa4f9 }, /* 0x12479 = 0x1242F U+309A */
  { 0xa4b1, 0xa4fa }, /* 0x1247A = 0x12431 U+309A */
  { 0xa4b3, 0xa4fb }, /* 0x1247B = 0x12433 U+309A */
  { 0xa5ab, 0xa5f7 }, /* 0x12577 = 0x1252B U+309A */
  { 0xa5ad, 0xa5f8 }, /* 0x12578 = 0x1252D U+309A */
  { 0xa5af, 0xa5f9 }, /* 0x12579 = 0x1252F U+309A */
  { 0xa5b1, 0xa5fa }, /* 0x1257A = 0x12531 U+309A */
  { 0xa5b3, 0xa5fb }, /* 0x1257B = 0x12533 U+309A */
  { 0xa5bb, 0xa5fc }, /* 0x1257C = 0x1253B U+309A */
  { 0xa5c4, 0xa5fd }, /* 0x1257D = 0x12544 U+309A */
  { 0xa5c8, 0xa5fe }, /* 0x1257E = 0x12548 U+309A */
  { 0xa6f5, 0xa6f8 }, /* 0x12678 = 0x12675 U+309A */
};

static int
euc_jisx0213_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  int count = 0;
  unsigned short lasttwo = conv->ostate;

  if (lasttwo) {
    /* Attempt to combine the last character with this one. */
    unsigned int idx;
    unsigned int len;

    if (wc == 0x02e5)
      idx = euc_jisx0213_comp_table02e5_idx,
      len = euc_jisx0213_comp_table02e5_len;
    else if (wc == 0x02e9)
      idx = euc_jisx0213_comp_table02e9_idx,
      len = euc_jisx0213_comp_table02e9_len;
    else if (wc == 0x0300)
      idx = euc_jisx0213_comp_table0300_idx,
      len = euc_jisx0213_comp_table0300_len;
    else if (wc == 0x0301)
      idx = euc_jisx0213_comp_table0301_idx,
      len = euc_jisx0213_comp_table0301_len;
    else if (wc == 0x309a)
      idx = euc_jisx0213_comp_table309a_idx,
      len = euc_jisx0213_comp_table309a_len;
    else
      goto not_combining;

    do
      if (euc_jisx0213_comp_table_data[idx].base == lasttwo)
        break;
    while (++idx, --len > 0);

    if (len > 0) {
      /* Output the combined character. */
      if (n >= 2) {
        lasttwo = euc_jisx0213_comp_table_data[idx].composed;
        r[0] = (lasttwo >> 8) & 0xff;
        r[1] = lasttwo & 0xff;
        conv->ostate = 0;
        return 2;
      } else
        return RET_TOOSMALL;
    }

  not_combining:
    /* Output the buffered character. */
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = (lasttwo >> 8) & 0xff;
    r[1] = lasttwo & 0xff;
    r += 2;
    count = 2;
  }

  if (wc < 0x80) {
    /* Plain ASCII character. */
    if (n > count) {
      r[0] = (unsigned char) wc;
      conv->ostate = 0;
      return count+1;
    } else
      return RET_TOOSMALL;
  } else if (wc >= 0xff61 && wc <= 0xff9f) {
    /* Half-width katakana. */
    if (n >= count+2) {
      r[0] = 0x8e;
      r[1] = wc - 0xfec0;
      conv->ostate = 0;
      return count+2;
    } else
      return RET_TOOSMALL;
  } else {
    unsigned short jch = ucs4_to_jisx0213(wc);
    if (jch != 0) {
      if (jch & 0x0080) {
        /* A possible match in comp_table_data. We have to buffer it. */
        /* We know it's a JISX 0213 plane 1 character. */
        if (jch & 0x8000) abort();
        conv->ostate = jch | 0x8080;
        return count+0;
      }
      if (jch & 0x8000) {
        /* JISX 0213 plane 2. */
        if (n >= count+3) {
          r[0] = 0x8f;
          r[1] = (jch >> 8) | 0x80;
          r[2] = (jch & 0xff) | 0x80;
          conv->ostate = 0;
          return count+3;
        } else
          return RET_TOOSMALL;
      } else {
        /* JISX 0213 plane 1. */
        if (n >= count+2) {
          r[0] = (jch >> 8) | 0x80;
          r[1] = (jch & 0xff) | 0x80;
          conv->ostate = 0;
          return count+2;
        } else
          return RET_TOOSMALL;
      }
    }
    return RET_ILUNI;
  }
}

static int
euc_jisx0213_reset (conv_t conv, unsigned char *r, int n)
{
  state_t lasttwo = conv->ostate;

  if (lasttwo) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = (lasttwo >> 8) & 0xff;
    r[1] = lasttwo & 0xff;
    /* conv->ostate = 0; will be done by the caller */
    return 2;
  } else
    return 0;
}
