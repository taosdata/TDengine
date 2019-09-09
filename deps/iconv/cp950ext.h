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
 * CP950 extensions
 */

static const unsigned short cp950ext_2uni_pagef9[157-116] = {
  /* 0xf9 */
                                  0x7881, 0x92b9, 0x88cf, 0x58bb,
  0x6052, 0x7ca7, 0x5afa, 0x2554, 0x2566, 0x2557, 0x2560, 0x256c,
  0x2563, 0x255a, 0x2569, 0x255d, 0x2552, 0x2564, 0x2555, 0x255e,
  0x256a, 0x2561, 0x2558, 0x2567, 0x255b, 0x2553, 0x2565, 0x2556,
  0x255f, 0x256b, 0x2562, 0x2559, 0x2568, 0x255c, 0x2551, 0x2550,
  0x256d, 0x256e, 0x2570, 0x256f, 0x2593,
};

static int
cp950ext_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c1 = s[0];
  if ((c1 == 0xf9)) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if ((c2 >= 0x40 && c2 < 0x7f) || (c2 >= 0xa1 && c2 < 0xff)) {
        unsigned int i = 157 * (c1 - 0xa1) + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
        unsigned short wc = 0xfffd;
        {
          if (i >= 13932 && i < 13973)
            wc = cp950ext_2uni_pagef9[i-13932];
        }
        if (wc != 0xfffd) {
          *pwc = (ucs4_t) wc;
          return 2;
        }
      }
      return RET_ILSEQ;
    }
    return RET_TOOFEW(0);
  }
  return RET_ILSEQ;
}

static const unsigned short cp950ext_2charset[41] = {
  0xf9f9, 0xf9f8, 0xf9e6, 0xf9ef, 0xf9dd, 0xf9e8, 0xf9f1, 0xf9df,
  0xf9ec, 0xf9f5, 0xf9e3, 0xf9ee, 0xf9f7, 0xf9e5, 0xf9e9, 0xf9f2,
  0xf9e0, 0xf9eb, 0xf9f4, 0xf9e2, 0xf9e7, 0xf9f0, 0xf9de, 0xf9ed,
  0xf9f6, 0xf9e4, 0xf9ea, 0xf9f3, 0xf9e1, 0xf9fa, 0xf9fb, 0xf9fd,
  0xf9fc, 0xf9fe, 0xf9d9, 0xf9dc, 0xf9da, 0xf9d6, 0xf9db, 0xf9d8,
  0xf9d7,
};

static const Summary16 cp950ext_uni2indx_page25[10] = {
  /* 0x2500 */
  {    0, 0x0000 }, {    0, 0x0000 }, {    0, 0x0000 }, {    0, 0x0000 },
  {    0, 0x0000 }, {    0, 0xffff }, {   16, 0xffff }, {   32, 0x0001 },
  {   33, 0x0000 }, {   33, 0x0008 },
};
static const Summary16 cp950ext_uni2indx_page58[12] = {
  /* 0x5800 */
  {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0000 },
  {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0000 },
  {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0800 },
};
static const Summary16 cp950ext_uni2indx_page5a[16] = {
  /* 0x5a00 */
  {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 },
  {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 },
  {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 },
  {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0400 },
};
static const Summary16 cp950ext_uni2indx_page60[6] = {
  /* 0x6000 */
  {   36, 0x0000 }, {   36, 0x0000 }, {   36, 0x0000 }, {   36, 0x0000 },
  {   36, 0x0000 }, {   36, 0x0004 },
};
static const Summary16 cp950ext_uni2indx_page78[9] = {
  /* 0x7800 */
  {   37, 0x0000 }, {   37, 0x0000 }, {   37, 0x0000 }, {   37, 0x0000 },
  {   37, 0x0000 }, {   37, 0x0000 }, {   37, 0x0000 }, {   37, 0x0000 },
  {   37, 0x0002 },
};
static const Summary16 cp950ext_uni2indx_page7c[11] = {
  /* 0x7c00 */
  {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 },
  {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 },
  {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0080 },
};
static const Summary16 cp950ext_uni2indx_page88[13] = {
  /* 0x8800 */
  {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 },
  {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 },
  {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 },
  {   39, 0x8000 },
};
static const Summary16 cp950ext_uni2indx_page92[12] = {
  /* 0x9200 */
  {   40, 0x0000 }, {   40, 0x0000 }, {   40, 0x0000 }, {   40, 0x0000 },
  {   40, 0x0000 }, {   40, 0x0000 }, {   40, 0x0000 }, {   40, 0x0000 },
  {   40, 0x0000 }, {   40, 0x0000 }, {   40, 0x0000 }, {   40, 0x0200 },
};

static int
cp950ext_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (n >= 2) {
    const Summary16 *summary = NULL;
    if (wc >= 0x2500 && wc < 0x25a0)
      summary = &cp950ext_uni2indx_page25[(wc>>4)-0x250];
    else if (wc >= 0x5800 && wc < 0x58c0)
      summary = &cp950ext_uni2indx_page58[(wc>>4)-0x580];
    else if (wc >= 0x5a00 && wc < 0x5b00)
      summary = &cp950ext_uni2indx_page5a[(wc>>4)-0x5a0];
    else if (wc >= 0x6000 && wc < 0x6060)
      summary = &cp950ext_uni2indx_page60[(wc>>4)-0x600];
    else if (wc >= 0x7800 && wc < 0x7890)
      summary = &cp950ext_uni2indx_page78[(wc>>4)-0x780];
    else if (wc >= 0x7c00 && wc < 0x7cb0)
      summary = &cp950ext_uni2indx_page7c[(wc>>4)-0x7c0];
    else if (wc >= 0x8800 && wc < 0x88d0)
      summary = &cp950ext_uni2indx_page88[(wc>>4)-0x880];
    else if (wc >= 0x9200 && wc < 0x92c0)
      summary = &cp950ext_uni2indx_page92[(wc>>4)-0x920];
    if (summary) {
      unsigned short used = summary->used;
      unsigned int i = wc & 0x0f;
      if (used & ((unsigned short) 1 << i)) {
        unsigned short c;
        /* Keep in `used' only the bits 0..i-1. */
        used &= ((unsigned short) 1 << i) - 1;
        /* Add `summary->indx' and the number of bits set in `used'. */
        used = (used & 0x5555) + ((used & 0xaaaa) >> 1);
        used = (used & 0x3333) + ((used & 0xcccc) >> 2);
        used = (used & 0x0f0f) + ((used & 0xf0f0) >> 4);
        used = (used & 0x00ff) + (used >> 8);
        c = cp950ext_2charset[summary->indx + used];
        r[0] = (c >> 8); r[1] = (c & 0xff);
        return 2;
      }
    }
    return RET_ILUNI;
  }
  return RET_TOOSMALL;
}
