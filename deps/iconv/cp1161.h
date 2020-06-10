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
 * CP1161
 */

static const unsigned short cp1161_2uni[96] = {
  /* 0xa0 */
  0x0e48, 0x0e01, 0x0e02, 0x0e03, 0x0e04, 0x0e05, 0x0e06, 0x0e07,
  0x0e08, 0x0e09, 0x0e0a, 0x0e0b, 0x0e0c, 0x0e0d, 0x0e0e, 0x0e0f,
  /* 0xb0 */
  0x0e10, 0x0e11, 0x0e12, 0x0e13, 0x0e14, 0x0e15, 0x0e16, 0x0e17,
  0x0e18, 0x0e19, 0x0e1a, 0x0e1b, 0x0e1c, 0x0e1d, 0x0e1e, 0x0e1f,
  /* 0xc0 */
  0x0e20, 0x0e21, 0x0e22, 0x0e23, 0x0e24, 0x0e25, 0x0e26, 0x0e27,
  0x0e28, 0x0e29, 0x0e2a, 0x0e2b, 0x0e2c, 0x0e2d, 0x0e2e, 0x0e2f,
  /* 0xd0 */
  0x0e30, 0x0e31, 0x0e32, 0x0e33, 0x0e34, 0x0e35, 0x0e36, 0x0e37,
  0x0e38, 0x0e39, 0x0e3a, 0x0e49, 0x0e4a, 0x0e4b, 0x20ac, 0x0e3f,
  /* 0xe0 */
  0x0e40, 0x0e41, 0x0e42, 0x0e43, 0x0e44, 0x0e45, 0x0e46, 0x0e47,
  0x0e48, 0x0e49, 0x0e4a, 0x0e4b, 0x0e4c, 0x0e4d, 0x0e4e, 0x0e4f,
  /* 0xf0 */
  0x0e50, 0x0e51, 0x0e52, 0x0e53, 0x0e54, 0x0e55, 0x0e56, 0x0e57,
  0x0e58, 0x0e59, 0x0e5a, 0x0e5b, 0x00a2, 0x00ac, 0x00a6, 0x00a0,
};

static int
cp1161_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  if (c < 0x80) {
    *pwc = (ucs4_t) c;
    return 1;
  }
  else if (c < 0xa0) {
  }
  else {
    *pwc = (ucs4_t) cp1161_2uni[c-0xa0];
    return 1;
  }
  return RET_ILSEQ;
}

static const unsigned char cp1161_page00[16] = {
  0xff, 0x00, 0xfc, 0x00, 0x00, 0x00, 0xfe, 0x00, /* 0xa0-0xa7 */
  0x00, 0x00, 0x00, 0x00, 0xfd, 0x00, 0x00, 0x00, /* 0xa8-0xaf */
};

static int
cp1161_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char c = 0;
  if (wc < 0x0080) {
    *r = wc;
    return 1;
  }
  else if (wc >= 0x00a0 && wc < 0x00b0)
    c = cp1161_page00[wc-0x00a0];
  else if (wc >= 0x0e48 && wc < 0x0e4c)
    c = wc-0x0d60;
  else if (wc >= 0x0e00 && wc < 0x0e60)
    c = cp874_page0e[wc-0x0e00];
  else if (wc == 0x20ac)
    c = 0xde;
  if (c != 0) {
    *r = c;
    return 1;
  }
  return RET_ILUNI;
}
