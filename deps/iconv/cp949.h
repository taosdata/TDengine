/*
 * Copyright (C) 1999-2001, 2005, 2007 Free Software Foundation, Inc.
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
 * CP949 is EUC-KR, extended with UHC (Unified Hangul Code).
 *
 * Some variants of CP949 (in JDK, Windows-2000, ICU) also add:
 *
 * 2. Private area mappings:
 *
 *        code           Unicode
 *    0xC9{A1..FE}   U+E000..U+E05D
 *    0xFE{A1..FE}   U+E05E..U+E0BB
 *
 * We add them too because, although there are backward compatibility problems
 * when a character from a private area is moved to an official Unicode code
 * point, they are useful for some people in practice.
 */

#include "uhc_1.h"
#include "uhc_2.h"

static int
cp949_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  /* Code set 0 (ASCII) */
  if (c < 0x80)
    return ascii_mbtowc(conv,pwc,s,n);
  /* UHC part 1 */
  if (c >= 0x81 && c <= 0xa0)
    return uhc_1_mbtowc(conv,pwc,s,n);
  if (c >= 0xa1 && c < 0xff) {
    if (n < 2)
      return RET_TOOFEW(0);
    {
      unsigned char c2 = s[1];
      if (c2 < 0xa1)
        /* UHC part 2 */
        return uhc_2_mbtowc(conv,pwc,s,n);
      else if (c2 < 0xff && !(c == 0xa2 && c2 == 0xe8)) {
        /* Code set 1 (KS C 5601-1992, now KS X 1001:1998) */
        unsigned char buf[2];
        int ret;
        buf[0] = c-0x80; buf[1] = c2-0x80;
        ret = ksc5601_mbtowc(conv,pwc,buf,2);
        if (ret != RET_ILSEQ)
          return ret;
        /* User-defined characters */
        if (c == 0xc9) {
          *pwc = 0xe000 + (c2 - 0xa1);
          return 2;
        }
        if (c == 0xfe) {
          *pwc = 0xe05e + (c2 - 0xa1);
          return 2;
        }
      }
    }
  }
  return RET_ILSEQ;
}

static int
cp949_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char buf[2];
  int ret;

  /* Code set 0 (ASCII) */
  ret = ascii_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  /* Code set 1 (KS C 5601-1992, now KS X 1001:1998) */
  if (wc != 0x327e) {
    ret = ksc5601_wctomb(conv,buf,wc,2);
    if (ret != RET_ILUNI) {
      if (ret != 2) abort();
      if (n < 2)
        return RET_TOOSMALL;
      r[0] = buf[0]+0x80;
      r[1] = buf[1]+0x80;
      return 2;
    }
  }

  /* UHC */
  if (wc >= 0xac00 && wc < 0xd7a4) {
    if (wc < 0xc8a5)
      return uhc_1_wctomb(conv,r,wc,n);
    else
      return uhc_2_wctomb(conv,r,wc,n);
  }

  /* User-defined characters */
  if (wc >= 0xe000 && wc < 0xe0bc) {
    if (n < 2)
      return RET_TOOSMALL;
    if (wc < 0xe05e) {
      r[0] = 0xc9;
      r[1] = wc - 0xe000 + 0xa1;
    } else {
      r[0] = 0xfe;
      r[1] = wc - 0xe05e + 0xa1;
    }
    return 2;
  }

  return RET_ILUNI;
}
