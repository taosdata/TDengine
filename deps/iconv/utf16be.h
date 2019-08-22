/*
 * Copyright (C) 1999-2001, 2008 Free Software Foundation, Inc.
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
 * UTF-16BE
 */

/* Specification: RFC 2781 */

static int
utf16be_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  int count = 0;
  if (n >= 2) {
    ucs4_t wc = (s[0] << 8) + s[1];
    if (wc >= 0xd800 && wc < 0xdc00) {
      if (n >= 4) {
        ucs4_t wc2 = (s[2] << 8) + s[3];
        if (!(wc2 >= 0xdc00 && wc2 < 0xe000))
          goto ilseq;
        *pwc = 0x10000 + ((wc - 0xd800) << 10) + (wc2 - 0xdc00);
        return count+4;
      }
    } else if (wc >= 0xdc00 && wc < 0xe000) {
      goto ilseq;
    } else {
      *pwc = wc;
      return count+2;
    }
  }
  return RET_TOOFEW(count);

ilseq:
  return RET_SHIFT_ILSEQ(count);
}

static int
utf16be_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (!(wc >= 0xd800 && wc < 0xe000)) {
    if (wc < 0x10000) {
      if (n >= 2) {
        r[0] = (unsigned char) (wc >> 8);
        r[1] = (unsigned char) wc;
        return 2;
      } else
        return RET_TOOSMALL;
    }
    else if (wc < 0x110000) {
      if (n >= 4) {
        ucs4_t wc1 = 0xd800 + ((wc - 0x10000) >> 10);
        ucs4_t wc2 = 0xdc00 + ((wc - 0x10000) & 0x3ff);
        r[0] = (unsigned char) (wc1 >> 8);
        r[1] = (unsigned char) wc1;
        r[2] = (unsigned char) (wc2 >> 8);
        r[3] = (unsigned char) wc2;
        return 4;
      } else
        return RET_TOOSMALL;
    }
  }
  return RET_ILUNI;
}
