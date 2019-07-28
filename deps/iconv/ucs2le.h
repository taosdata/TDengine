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
 * UCS-2LE = UCS-2 little endian
 */

static int
ucs2le_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  if (n >= 2) {
    if (s[1] >= 0xd8 && s[1] < 0xe0) {
      return RET_ILSEQ;
    } else {
      *pwc = s[0] + (s[1] << 8);
      return 2;
    }
  }
  return RET_TOOFEW(0);
}

static int
ucs2le_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (wc < 0x10000 && !(wc >= 0xd800 && wc < 0xe000)) {
    if (n >= 2) {
      r[0] = (unsigned char) wc;
      r[1] = (unsigned char) (wc >> 8);
      return 2;
    } else
      return RET_TOOSMALL;
  }
  return RET_ILUNI;
}
