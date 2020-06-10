/*
 * Copyright (C) 1999-2000 Free Software Foundation, Inc.
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
 * UCS-4-SWAPPED = UCS-4-INTERNAL with inverted endianness
 */

static int
ucs4swapped_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  /* This function assumes that 'unsigned int' has exactly 32 bits. */
  if (sizeof(unsigned int) != 4) abort();

  if (n >= 4) {
    unsigned int x = *(const unsigned int *)s;
    x = (x >> 24) | ((x >> 8) & 0xff00) | ((x & 0xff00) << 8) | (x << 24);
    *pwc = x;
    return 4;
  }
  return RET_TOOFEW(0);
}

static int
ucs4swapped_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  /* This function assumes that 'unsigned int' has exactly 32 bits. */
  if (sizeof(unsigned int) != 4) abort();

  if (n >= 4) {
    unsigned int x = wc;
    x = (x >> 24) | ((x >> 8) & 0xff00) | ((x & 0xff00) << 8) | (x << 24);
    *(unsigned int *)r = x;
    return 4;
  } else
    return RET_TOOSMALL;
}
