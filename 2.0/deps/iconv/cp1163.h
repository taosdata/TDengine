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
 * CP1163
 */

static int
cp1163_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  if (c < 0xa0)
    *pwc = (ucs4_t) c;
  else if (c == 0xa4)
    *pwc = 0x20ac;
  else
    *pwc = (ucs4_t) cp1129_2uni[c-0xa0];
  return 1;
}

static const unsigned char cp1163_page20[8] = {
  0x00, 0x00, 0x00, 0xfe, 0xa4, 0x00, 0x00, 0x00, /* 0xa8-0xaf */
};

static int
cp1163_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char c = 0;
  if (wc < 0x00a0 || (wc < 0x00a8 && wc != 0x00a4) || wc == 0x00d0) {
    *r = wc;
    return 1;
  }
  else if (wc >= 0x00a8 && wc < 0x01b8)
    c = cp1129_page00[wc-0x00a8];
  else if (wc >= 0x0300 && wc < 0x0328)
    c = cp1129_page03[wc-0x0300];
  else if (wc == 0x203e)
    c = 0xaf;
  else if (wc >= 0x20a8 && wc < 0x20b0)
    c = cp1163_page20[wc-0x20a8];
  if (c != 0) {
    *r = c;
    return 1;
  }
  return RET_ILUNI;
}
