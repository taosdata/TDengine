/*
 * Copyright (C) 1999-2001, 2005 Free Software Foundation, Inc.
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
 * ISO-IR-165
 */

/*
 * ISO-IR-165 is an extension of GB 2312, consisting of:
 * 1. GB 6345.1-86 corrections:
 *    Two corrections to GB 2312, at 0x2367 and 0x6F71.
 * 2. GB 6345.1-86 additions:
 *    - 6 new full-width pinyin characters in row 0x28.
 *    - ISO646-CN in row 0x2A.
 *    - 32 half-width pinyin characters in row 0x2B.
 * 3. GB 8565.2-88 additions:
 *    - 50 characters in row 0x2D.
 *    - 92 characters in row 0x2E.
 *    - 93 characters in row 0x2F.
 *    - 470 characters in rows 0x7A-0x7E.
 * 4. ISO-IR-165 additions:
 *    - 22 characters in row 0x26.
 *    - 94 characters in row 0x2C.
 *    - 44 new characters in row 0x2D.
 *    - 1 new character in row 0x2F.
 *
 * The conversion table was created from the following sources:
 * Ad 1. The 0x2367 correction is already integrated in the unicode.org
 *       GB2312.TXT table. The 0x6F71 mapping is the same in the unicode.org
 *       GB2312.TXT and UNIHAN.TXT table and in Koichi Yasuoka's Uni2GB table,
 *       so we assume it's correct.
 * The unicode.org UNIHAN.TXT table about GB 8565 is not usable: it has
 * extraneous code points at rows 0x28, 0x2C, 0x2D. Note also that it does
 * not list the 69 non-hanzi in row 0x2F. Moreover, it has the characters
 * 0x2F7A-0x2F7D shifted down by one to 0x2F79-0x2F7C.
 * Therefore we take the GB8565 and ISO-IR-165 data from Koichi Yasuoka's
 * Uni2GB table.
 * Ad 1. Yasuoka maps 0x2367 to U+0261 (small script g) and 0x2840 to U+FF47
 *       (full-width small normal g). While coherent with ISO-IR's 165.pdf,
 *       this disagrees with Ken Lunde's book: He says that ISO-IR-165
 *       includes the GB6345 correction, i.e. maps 0x2367 to U+FF47 or U+0067
 *       and _not_ to U+0261 (small script g).
 *       To overcome the confusion, we just map both 0x2367 and 0x2840 to
 *       U+FF47.
 * Ad 2. Row 0x28: Add a mapping from 0x283F to U+01F9.
 *       Row 0x2A: Mapping is well-known, also present in Koichi Yasuoka's
 *                 table.
 *       Row 0x2B: Typed in by hand from appendix E in Ken Lunde's book.
 *       When converting from Unicode to ISO-IR-165, prefer the half-width
 *       range 0x2B{21..40} to the full-width range 0x28{21..40}.
 * Ad 3. Rows 0x2D, 0x2E: Both Koichi Yasuoka's Uni2GB table and the UNIHAN.TXT
 *                 data for GB 8565 agree here.
 *       Row 0x2F: Taken from Koichi Yasuoka's Uni2GB table.
 *       Rows 0x7A-0x7E: Koichi Yasuoka's Uni2GB table and the UNIHAN.TXT
 *                 data for GB 8565 agree here mostly. Differences:
 *                 0x7C38 -> U+6F26 or U+527A ? We choose U+6F26.
 *                 0x7C5A -> U+7A40 or U+6996 ? We choose U+6996.
 * Ad 4. Row 0x26: Mapping unknown.
 *       Rows 0x2C, 0x2D: Both Koichi Yasuoka's Uni2GB table and the UNIHAN.TXT
 *                 data for GB 8565 (!) agree here.
 *       Row 0x2F: Taken from Koichi Yasuoka's Uni2GB table.
 */

#include "isoir165ext.h"

static int
isoir165_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  int ret;

  /* Map full-width pinyin (row 0x28) like half-width pinyin (row 0x2B). */
  if (s[0] == 0x28) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if (c2 >= 0x21 && c2 <= 0x40) {
        unsigned char buf[2];
        buf[0] = 0x2b;
        buf[1] = c2;
        ret = isoir165ext_mbtowc(conv,pwc,buf,2);
        if (ret != RET_ILSEQ)
          return ret;
      }
    }
  }
  /* Try the GB2312 -> Unicode table. */
  ret = gb2312_mbtowc(conv,pwc,s,n);
  if (ret != RET_ILSEQ)
    return ret;
  /* Row 0x2A is GB_1988-80. */
  if (s[0] == 0x2a) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if (c2 >= 0x21 && c2 < 0x7f) {
        int ret = iso646_cn_mbtowc(conv,pwc,s+1,1);
        if (ret != 1) abort();
        return 2;
      }
      return RET_ILSEQ;
    }
    return RET_TOOFEW(0);
  }
  /* Try the ISO-IR-165 extensions -> Unicode table. */
  ret = isoir165ext_mbtowc(conv,pwc,s,n);
  return ret;
}

static int
isoir165_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char buf[2];
  int ret;

  /* Try the Unicode -> GB2312 table. */
  ret = gb2312_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (!(buf[0] == 0x28 && buf[1] >= 0x21 && buf[1] <= 0x40)) {
      if (n >= 2) {
        r[0] = buf[0];
        r[1] = buf[1];
        return 2;
      }
      return RET_TOOSMALL;
    }
  }
  /* Row 0x2A is GB_1988-80. */
  ret = iso646_cn_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] >= 0x21 && buf[0] < 0x7f) {
      if (n >= 2) {
        r[0] = 0x2a;
        r[1] = buf[0];
        return 2;
      }
      return RET_TOOSMALL;
    }
  }
  /* Try the Unicode -> ISO-IR-165 extensions table. */
  ret = isoir165ext_wctomb(conv,r,wc,n);
  return ret;
}
