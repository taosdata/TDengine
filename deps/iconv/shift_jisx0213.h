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
 * SHIFT_JISX0213
 */

/* The structure of Shift_JISX0213 is as follows:

   0x00..0x7F: ISO646-JP, an ASCII variant

   0x{A1..DF}: JISX0201 Katakana.

   0x{81..9F,E0..EF}{40..7E,80..FC}: JISX0213 plane 1.

   0x{F0..FC}{40..7E,80..FC}: JISX0213 plane 2, with irregular row mapping.

   Note that some JISX0213 characters are not contained in Unicode 3.2
   and are therefore best represented as sequences of Unicode characters.
*/

#include "jisx0213.h"
#include "flushwc.h"

static int
shift_jisx0213_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
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
      /* Plain ISO646-JP character. */
      if (c == 0x5c)
        *pwc = (ucs4_t) 0x00a5;
      else if (c == 0x7e)
        *pwc = (ucs4_t) 0x203e;
      else
        *pwc = (ucs4_t) c;
      return 1;
    } else if (c >= 0xa1 && c <= 0xdf) {
      *pwc = c + 0xfec0;
      return 1;
    } else {
      if ((c >= 0x81 && c <= 0x9f) || (c >= 0xe0 && c <= 0xfc)) {
        /* Two byte character. */
        if (n >= 2) {
          unsigned char c2 = s[1];
          if ((c2 >= 0x40 && c2 <= 0x7e) || (c2 >= 0x80 && c2 <= 0xfc)) {
            unsigned int c1;
            ucs4_t wc;
            /* Convert to row and column. */
            if (c < 0xe0)
              c -= 0x81;
            else
              c -= 0xc1;
            if (c2 < 0x80)
              c2 -= 0x40;
            else
              c2 -= 0x41;
            /* Now 0 <= c <= 0x3b, 0 <= c2 <= 0xbb. */
            c1 = 2 * c;
            if (c2 >= 0x5e)
              c2 -= 0x5e, c1++;
            c2 += 0x21;
            if (c1 >= 0x5e) {
              /* Handling of JISX 0213 plane 2 rows. */
              if (c1 >= 0x67)
                c1 += 230;
              else if (c1 >= 0x63 || c1 == 0x5f)
                c1 += 168;
              else
                c1 += 162;
            }
            wc = jisx0213_to_ucs4(0x121+c1,c2);
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
              return 2;
            }
          }
        } else
          return RET_TOOFEW(0);
      }
      return RET_ILSEQ;
    }
  }
}

#define shift_jisx0213_flushwc normal_flushwc

/* Composition tables for each of the relevant combining characters.  */
static const struct { unsigned short base; unsigned short composed; } shift_jisx0213_comp_table_data[] = {
#define shift_jisx0213_comp_table02e5_idx 0
#define shift_jisx0213_comp_table02e5_len 1
  { 0x8684, 0x8685 }, /* 0x12B65 = 0x12B64 U+02E5 */
#define shift_jisx0213_comp_table02e9_idx (shift_jisx0213_comp_table02e5_idx+shift_jisx0213_comp_table02e5_len)
#define shift_jisx0213_comp_table02e9_len 1
  { 0x8680, 0x8686 }, /* 0x12B66 = 0x12B60 U+02E9 */
#define shift_jisx0213_comp_table0300_idx (shift_jisx0213_comp_table02e9_idx+shift_jisx0213_comp_table02e9_len)
#define shift_jisx0213_comp_table0300_len 5
  { 0x857b, 0x8663 }, /* 0x12B44 = 0x1295C U+0300 */
  { 0x8657, 0x8667 }, /* 0x12B48 = 0x12B38 U+0300 */
  { 0x8656, 0x8669 }, /* 0x12B4A = 0x12B37 U+0300 */
  { 0x864f, 0x866b }, /* 0x12B4C = 0x12B30 U+0300 */
  { 0x8662, 0x866d }, /* 0x12B4E = 0x12B43 U+0300 */
#define shift_jisx0213_comp_table0301_idx (shift_jisx0213_comp_table0300_idx+shift_jisx0213_comp_table0300_len)
#define shift_jisx0213_comp_table0301_len 4
  { 0x8657, 0x8668 }, /* 0x12B49 = 0x12B38 U+0301 */
  { 0x8656, 0x866a }, /* 0x12B4B = 0x12B37 U+0301 */
  { 0x864f, 0x866c }, /* 0x12B4D = 0x12B30 U+0301 */
  { 0x8662, 0x866e }, /* 0x12B4F = 0x12B43 U+0301 */
#define shift_jisx0213_comp_table309a_idx (shift_jisx0213_comp_table0301_idx+shift_jisx0213_comp_table0301_len)
#define shift_jisx0213_comp_table309a_len 14
  { 0x82a9, 0x82f5 }, /* 0x12477 = 0x1242B U+309A */
  { 0x82ab, 0x82f6 }, /* 0x12478 = 0x1242D U+309A */
  { 0x82ad, 0x82f7 }, /* 0x12479 = 0x1242F U+309A */
  { 0x82af, 0x82f8 }, /* 0x1247A = 0x12431 U+309A */
  { 0x82b1, 0x82f9 }, /* 0x1247B = 0x12433 U+309A */
  { 0x834a, 0x8397 }, /* 0x12577 = 0x1252B U+309A */
  { 0x834c, 0x8398 }, /* 0x12578 = 0x1252D U+309A */
  { 0x834e, 0x8399 }, /* 0x12579 = 0x1252F U+309A */
  { 0x8350, 0x839a }, /* 0x1257A = 0x12531 U+309A */
  { 0x8352, 0x839b }, /* 0x1257B = 0x12533 U+309A */
  { 0x835a, 0x839c }, /* 0x1257C = 0x1253B U+309A */
  { 0x8363, 0x839d }, /* 0x1257D = 0x12544 U+309A */
  { 0x8367, 0x839e }, /* 0x1257E = 0x12548 U+309A */
  { 0x83f3, 0x83f6 }, /* 0x12678 = 0x12675 U+309A */
};

static int
shift_jisx0213_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  int count = 0;
  unsigned short lasttwo = conv->ostate;

  if (lasttwo) {
    /* Attempt to combine the last character with this one. */
    unsigned int idx;
    unsigned int len;

    if (wc == 0x02e5)
      idx = shift_jisx0213_comp_table02e5_idx,
      len = shift_jisx0213_comp_table02e5_len;
    else if (wc == 0x02e9)
      idx = shift_jisx0213_comp_table02e9_idx,
      len = shift_jisx0213_comp_table02e9_len;
    else if (wc == 0x0300)
      idx = shift_jisx0213_comp_table0300_idx,
      len = shift_jisx0213_comp_table0300_len;
    else if (wc == 0x0301)
      idx = shift_jisx0213_comp_table0301_idx,
      len = shift_jisx0213_comp_table0301_len;
    else if (wc == 0x309a)
      idx = shift_jisx0213_comp_table309a_idx,
      len = shift_jisx0213_comp_table309a_len;
    else
      goto not_combining;

    do
      if (shift_jisx0213_comp_table_data[idx].base == lasttwo)
        break;
    while (++idx, --len > 0);

    if (len > 0) {
      /* Output the combined character. */
      if (n >= 2) {
        lasttwo = shift_jisx0213_comp_table_data[idx].composed;
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

  if (wc < 0x80 && wc != 0x5c && wc != 0x7e) {
    /* Plain ISO646-JP character. */
    if (n > count) {
      r[0] = (unsigned char) wc;
      conv->ostate = 0;
      return count+1;
    } else
      return RET_TOOSMALL;
  } else if (wc == 0x00a5) {
    if (n > count) {
      r[0] = 0x5c;
      conv->ostate = 0;
      return count+1;
    } else
      return RET_TOOSMALL;
  } else if (wc == 0x203e) {
    if (n > count) {
      r[0] = 0x7e;
      conv->ostate = 0;
      return count+1;
    } else
      return RET_TOOSMALL;
  } else if (wc >= 0xff61 && wc <= 0xff9f) {
    /* Half-width katakana. */
    if (n > count) {
      r[0] = wc - 0xfec0;
      conv->ostate = 0;
      return count+1;
    } else
      return RET_TOOSMALL;
  } else {
    unsigned int s1, s2;
    unsigned short jch = ucs4_to_jisx0213(wc);
    if (jch != 0) {
      /* Convert it to shifted representation. */
      s1 = jch >> 8;
      s2 = jch & 0x7f;
      s1 -= 0x21;
      s2 -= 0x21;
      if (s1 >= 0x5e) {
        /* Handling of JISX 0213 plane 2 rows. */
        if (s1 >= 0xcd) /* rows 0x26E..0x27E */
          s1 -= 102;
        else if (s1 >= 0x8b || s1 == 0x87) /* rows 0x228, 0x22C..0x22F */
          s1 -= 40;
        else /* rows 0x221, 0x223..0x225 */
          s1 -= 34;
        /* Now 0x5e <= s1 <= 0x77. */
      }
      if (s1 & 1)
        s2 += 0x5e;
      s1 = s1 >> 1;
      if (s1 < 0x1f)
        s1 += 0x81;
      else
        s1 += 0xc1;
      if (s2 < 0x3f)
        s2 += 0x40;
      else
        s2 += 0x41;
      if (jch & 0x0080) {
        /* A possible match in comp_table_data. We have to buffer it. */
        /* We know it's a JISX 0213 plane 1 character. */
        if (jch & 0x8000) abort();
        conv->ostate = (s1 << 8) | s2;
        return count+0;
      }
      /* Output the shifted representation. */
      if (n >= count+2) {
        r[0] = s1;
        r[1] = s2;
        conv->ostate = 0;
        return count+2;
      } else
        return RET_TOOSMALL;
    }
    return RET_ILUNI;
  }
}

static int
shift_jisx0213_reset (conv_t conv, unsigned char *r, int n)
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
