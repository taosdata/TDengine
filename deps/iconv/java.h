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
 * JAVA
 * This is ISO 8859-1 with \uXXXX escape sequences, denoting Unicode BMP
 * characters. Consecutive pairs of \uXXXX escape sequences in the surrogate
 * range, as in UTF-16, denote Unicode characters outside the BMP.
 */

static int
java_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c;
  ucs4_t wc, wc2;
  int i;

  c = s[0];
  if (c != '\\') {
    *pwc = c;
    return 1;
  }
  if (n < 2)
    return RET_TOOFEW(0);
  if (s[1] != 'u')
    goto simply_backslash;
  wc = 0;
  for (i = 2; i < 6; i++) {
    if (n <= i)
      return RET_TOOFEW(0);
    c = s[i];
    if (c >= '0' && c <= '9')
      c -= '0';
    else if (c >= 'A' && c <= 'Z')
      c -= 'A'-10;
    else if (c >= 'a' && c <= 'z')
      c -= 'a'-10;
    else
      goto simply_backslash;
    wc |= (ucs4_t) c << (4 * (5-i));
  }
  if (!(wc >= 0xd800 && wc < 0xe000)) {
    *pwc = wc;
    return 6;
  }
  if (wc >= 0xdc00)
    goto simply_backslash;
  if (n < 7)
    return RET_TOOFEW(0);
  if (s[6] != '\\')
    goto simply_backslash;
  if (n < 8)
    return RET_TOOFEW(0);
  if (s[7] != 'u')
    goto simply_backslash;
  wc2 = 0;
  for (i = 8; i < 12; i++) {
    if (n <= i)
      return RET_TOOFEW(0);
    c = s[i];
    if (c >= '0' && c <= '9')
      c -= '0';
    else if (c >= 'A' && c <= 'Z')
      c -= 'A'-10;
    else if (c >= 'a' && c <= 'z')
      c -= 'a'-10;
    else
      goto simply_backslash;
    wc2 |= (ucs4_t) c << (4 * (11-i));
  }
  if (!(wc2 >= 0xdc00 && wc2 < 0xe000))
    goto simply_backslash;
  *pwc = 0x10000 + ((wc - 0xd800) << 10) + (wc2 - 0xdc00);
  return 12;
simply_backslash:
  *pwc = '\\';
  return 1;
}

static int
java_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (wc < 0x80) {
    *r = wc;
    return 1;
  } else if (wc < 0x10000) {
    if (n >= 6) {
      unsigned int i;
      r[0] = '\\';
      r[1] = 'u';
      i = (wc >> 12) & 0x0f; r[2] = (i < 10 ? '0'+i : 'a'-10+i);
      i = (wc >> 8) & 0x0f;  r[3] = (i < 10 ? '0'+i : 'a'-10+i);
      i = (wc >> 4) & 0x0f;  r[4] = (i < 10 ? '0'+i : 'a'-10+i);
      i = wc & 0x0f;         r[5] = (i < 10 ? '0'+i : 'a'-10+i);
      return 6;
    } else
      return RET_TOOSMALL;
  } else if (wc < 0x110000) {
    if (n >= 12) {
      ucs4_t wc1 = 0xd800 + ((wc - 0x10000) >> 10);
      ucs4_t wc2 = 0xdc00 + ((wc - 0x10000) & 0x3ff);
      unsigned int i;
      r[0] = '\\';
      r[1] = 'u';
      i = (wc1 >> 12) & 0x0f; r[2] = (i < 10 ? '0'+i : 'a'-10+i);
      i = (wc1 >> 8) & 0x0f;  r[3] = (i < 10 ? '0'+i : 'a'-10+i);
      i = (wc1 >> 4) & 0x0f;  r[4] = (i < 10 ? '0'+i : 'a'-10+i);
      i = wc1 & 0x0f;         r[5] = (i < 10 ? '0'+i : 'a'-10+i);
      r[6] = '\\';
      r[7] = 'u';
      i = (wc2 >> 12) & 0x0f; r[8] = (i < 10 ? '0'+i : 'a'-10+i);
      i = (wc2 >> 8) & 0x0f;  r[9] = (i < 10 ? '0'+i : 'a'-10+i);
      i = (wc2 >> 4) & 0x0f; r[10] = (i < 10 ? '0'+i : 'a'-10+i);
      i = wc2 & 0x0f;        r[11] = (i < 10 ? '0'+i : 'a'-10+i);
      return 12;
    } else
      return RET_TOOSMALL;
  }
  return RET_ILUNI;
}
