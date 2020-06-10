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
 * GB/T 12345-1990
 */

/*
 * GB/T 12345-1990 is a traditional chinese counterpart of GB 2312-1986.
 * According to the unicode.org tables:
 * 2146 characters have been changed to their traditional counterpart,
 * 103 characters have been added, no characters have been removed.
 * Therefore we use an auxiliary table, which contains only the changes.
 */

#include "gb12345ext.h"

static int
gb12345_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  int ret;

  /* The gb12345ext table overrides some entries in the gb2312 table. */
  /* Try the GB12345 extensions -> Unicode table. */
  ret = gb12345ext_mbtowc(conv,pwc,s,n);
  if (ret != RET_ILSEQ)
    return ret;
  /* Try the GB2312 -> Unicode table. */
  ret = gb2312_mbtowc(conv,pwc,s,n);
  return ret;
}

static int
gb12345_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  int ret;

  /* The gb12345ext table overrides some entries in the gb2312 table. */
  /* Try the Unicode -> GB12345 extensions table. */
  ret = gb12345ext_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;
  /* Try the Unicode -> GB2312 table, and check that the resulting GB2312
     byte sequence is not overridden by the GB12345 extensions table. */
  ret = gb2312_wctomb(conv,r,wc,n);
  if (ret == 2 && gb12345ext_mbtowc(conv,&wc,r,2) == 2)
    return RET_ILUNI;
  else
    return ret;
}
