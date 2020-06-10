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
 * CP936 extensions
 */

static const unsigned short cp936ext_2uni_pagea6[181-159] = {
  /* 0xa6 */
                                                          0xfe35,
  0xfe36, 0xfe39, 0xfe3a, 0xfe3f, 0xfe40, 0xfe3d, 0xfe3e, 0xfe41,
  0xfe42, 0xfe43, 0xfe44, 0xfffd, 0xfffd, 0xfe3b, 0xfe3c, 0xfe37,
  0xfe38, 0xfe31, 0xfffd, 0xfe33, 0xfe34,
};
static const unsigned short cp936ext_2uni_pagea8[128-122] = {
  /* 0xa8 */
                  0x0251, 0xfffd, 0x0144, 0x0148, 0xfffd, 0x0261,
};

static int
cp936ext_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c1 = s[0];
  if ((c1 == 0xa6) || (c1 == 0xa8)) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if ((c2 >= 0x40 && c2 < 0x7f) || (c2 >= 0x80 && c2 < 0xff)) {
        unsigned int i = 190 * (c1 - 0x81) + (c2 - (c2 >= 0x80 ? 0x41 : 0x40));
        unsigned short wc = 0xfffd;
        if (i < 7410) {
          if (i >= 7189 && i < 7211)
            wc = cp936ext_2uni_pagea6[i-7189];
        } else {
          if (i >= 7532 && i < 7538)
            wc = cp936ext_2uni_pagea8[i-7532];
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

static const unsigned short cp936ext_page01[16] = {
  0x0000, 0x0000, 0x0000, 0x0000, 0xa8bd, 0x0000, 0x0000, 0x0000, /*0x40-0x47*/
  0xa8be, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, /*0x48-0x4f*/
};
static const unsigned short cp936ext_page02[24] = {
  0x0000, 0xa8bb, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, /*0x50-0x57*/
  0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, /*0x58-0x5f*/
  0x0000, 0xa8c0, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, /*0x60-0x67*/
};
static const unsigned short cp936ext_pagefe[24] = {
  0x0000, 0xa6f2, 0x0000, 0xa6f4, 0xa6f5, 0xa6e0, 0xa6e1, 0xa6f0, /*0x30-0x37*/
  0xa6f1, 0xa6e2, 0xa6e3, 0xa6ee, 0xa6ef, 0xa6e6, 0xa6e7, 0xa6e4, /*0x38-0x3f*/
  0xa6e5, 0xa6e8, 0xa6e9, 0xa6ea, 0xa6eb, 0x0000, 0x0000, 0x0000, /*0x40-0x47*/
};

static int
cp936ext_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (n >= 2) {
    unsigned short c = 0;
    if (wc >= 0x0140 && wc < 0x0150)
      c = cp936ext_page01[wc-0x0140];
    else if (wc >= 0x0250 && wc < 0x0268)
      c = cp936ext_page02[wc-0x0250];
    else if (wc >= 0xfe30 && wc < 0xfe48)
      c = cp936ext_pagefe[wc-0xfe30];
    if (c != 0) {
      r[0] = (c >> 8); r[1] = (c & 0xff);
      return 2;
    }
    return RET_ILUNI;
  }
  return RET_TOOSMALL;
}
