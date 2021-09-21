/*
 * Copyright (C) 1999-2002, 2005 Free Software Foundation, Inc.
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
 * CP932
 */

/*
 * Microsoft CP932 is a slightly extended version of SHIFT_JIS.
 * The differences between the EASTASIA/JIS/SHIFTJIS.TXT and the
 * VENDORS/MICSFT/WINDOWS/CP932.TXT tables found on ftp.unicode.org are
 * as follows:
 *
 * 1. CP932 uses ASCII, not JISX0201 Roman.
 *
 * 2. Some characters in the JISX0208 range are defined differently:
 *
 *     code   SHIFTJIS.TXT                   CP932.TXT
 *    0x815F  0x005C # REVERSE SOLIDUS       0xFF3C # FULLWIDTH REVERSE SOLIDUS
 *    0x8160  0x301C # WAVE DASH             0xFF5E # FULLWIDTH TILDE
 *    0x8161  0x2016 # DOUBLE VERTICAL LINE  0x2225 # PARALLEL TO
 *    0x817C  0x2212 # MINUS SIGN            0xFF0D # FULLWIDTH HYPHEN-MINUS
 *    0x8191  0x00A2 # CENT SIGN             0xFFE0 # FULLWIDTH CENT SIGN
 *    0x8192  0x00A3 # POUND SIGN            0xFFE1 # FULLWIDTH POUND SIGN
 *    0x81CA  0x00AC # NOT SIGN              0xFFE2 # FULLWIDTH NOT SIGN
 *
 *    We don't implement the latter 6 of these changes, only the first one.
 *    SHIFTJIS.TXT makes more sense. However, as a compromise with user
 *    expectation, we implement the middle 5 of these changes in the
 *    Unicode to CP932 direction. We don't implement the last one at all,
 *    because it would collide with the mapping of 0xFA54.
 *
 * 3. A few new rows. See cp932ext.h.
 *
 * Many variants of CP932 (in GNU libc, JDK, OSF/1, Windows-2000, ICU) also
 * add:
 *
 * 4. Private area mappings:
 *
 *              code                 Unicode
 *    0x{F0..F9}{40..7E,80..FC}  U+E000..U+E757
 *
 * We add them too because, although there are backward compatibility problems
 * when a character from a private area is moved to an official Unicode code
 * point, they are useful for some people in practice.
 */

#include "cp932ext.h"

/*
   Conversion between SJIS codes (s1,s2) and JISX0208 codes (c1,c2):
   Example. (s1,s2) = 0x8140, (c1,c2) = 0x2121.
   0x81 <= s1 <= 0x9F || 0xE0 <= s1 <= 0xEA,
   0x40 <= s2 <= 0x7E || 0x80 <= s2 <= 0xFC,
   0x21 <= c1 <= 0x74, 0x21 <= c2 <= 0x7E.
   Invariant:
     94*2*(s1 < 0xE0 ? s1-0x81 : s1-0xC1) + (s2 < 0x80 ? s2-0x40 : s2-0x41)
     = 94*(c1-0x21)+(c2-0x21)
   Conversion (s1,s2) -> (c1,c2):
     t1 := (s1 < 0xE0 ? s1-0x81 : s1-0xC1)
     t2 := (s2 < 0x80 ? s2-0x40 : s2-0x41)
     c1 := 2*t1 + (t2 < 0x5E ? 0 : 1) + 0x21
     c2 := (t2 < 0x5E ? t2 : t2-0x5E) + 0x21
   Conversion (c1,c2) -> (s1,s2):
     t1 := (c1 - 0x21) >> 1
     t2 := ((c1 - 0x21) & 1) * 0x5E + (c2 - 0x21)
     s1 := (t1 < 0x1F ? t1+0x81 : t1+0xC1)
     s2 := (t2 < 0x3F ? t2+0x40 : t2+0x41)
 */

static int
cp932_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  if (c < 0x80)
    return ascii_mbtowc(conv,pwc,s,n);
  else if (c >= 0xa1 && c <= 0xdf)
    return jisx0201_mbtowc(conv,pwc,s,n);
  else {
    unsigned char s1, s2;
    s1 = c;
    if ((s1 >= 0x81 && s1 <= 0x9f && s1 != 0x87) || (s1 >= 0xe0 && s1 <= 0xea)) {
      if (n < 2)
        return RET_TOOFEW(0);
      s2 = s[1];
      if ((s2 >= 0x40 && s2 <= 0x7e) || (s2 >= 0x80 && s2 <= 0xfc)) {
        unsigned char t1 = (s1 < 0xe0 ? s1-0x81 : s1-0xc1);
        unsigned char t2 = (s2 < 0x80 ? s2-0x40 : s2-0x41);
        unsigned char buf[2];
        buf[0] = 2*t1 + (t2 < 0x5e ? 0 : 1) + 0x21;
        buf[1] = (t2 < 0x5e ? t2 : t2-0x5e) + 0x21;
        return jisx0208_mbtowc(conv,pwc,buf,2);
      }
    } else if ((s1 == 0x87) || (s1 >= 0xed && s1 <= 0xee) || (s1 >= 0xfa)) {
      if (n < 2)
        return RET_TOOFEW(0);
      return cp932ext_mbtowc(conv,pwc,s,2);
    } else if (s1 >= 0xf0 && s1 <= 0xf9) {
      /* User-defined range. See
       * Ken Lunde's "CJKV Information Processing", table 4-66, p. 206. */
      if (n < 2)
        return RET_TOOFEW(0);
      s2 = s[1];
      if ((s2 >= 0x40 && s2 <= 0x7e) || (s2 >= 0x80 && s2 <= 0xfc)) {
        *pwc = 0xe000 + 188*(s1 - 0xf0) + (s2 < 0x80 ? s2-0x40 : s2-0x41);
        return 2;
      }
    }
    return RET_ILSEQ;
  }
}

static int
cp932_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char buf[2];
  int ret;

  /* Try ASCII. */
  ret = ascii_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    unsigned char c;
    if (ret != 1) abort();
    c = buf[0];
    if (c < 0x80) {
      r[0] = c;
      return 1;
    }
  }

  /* Try JIS X 0201-1976 Katakana. */
  ret = jisx0201_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    unsigned char c;
    if (ret != 1) abort();
    c = buf[0];
    if (c >= 0xa1 && c <= 0xdf) {
      r[0] = c;
      return 1;
    }
  }

  /* Try JIS X 0208-1990. */
  ret = jisx0208_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    unsigned char c1, c2;
    if (ret != 2) abort();
    if (n < 2)
      return RET_TOOSMALL;
    c1 = buf[0];
    c2 = buf[1];
    if ((c1 >= 0x21 && c1 <= 0x74) && (c2 >= 0x21 && c2 <= 0x7e)) {
      unsigned char t1 = (c1 - 0x21) >> 1;
      unsigned char t2 = (((c1 - 0x21) & 1) ? 0x5e : 0) + (c2 - 0x21);
      r[0] = (t1 < 0x1f ? t1+0x81 : t1+0xc1);
      r[1] = (t2 < 0x3f ? t2+0x40 : t2+0x41);
      return 2;
    }
  }

  /* Try CP932 extensions. */
  ret = cp932ext_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = buf[0];
    r[1] = buf[1];
    return 2;
  }

  /* User-defined range. See
   * Ken Lunde's "CJKV Information Processing", table 4-66, p. 206. */
  if (wc >= 0xe000 && wc < 0xe758) {
    unsigned char c1, c2;
    if (n < 2)
      return RET_TOOSMALL;
    c1 = (unsigned int) (wc - 0xe000) / 188;
    c2 = (unsigned int) (wc - 0xe000) % 188;
    r[0] = c1+0xf0;
    r[1] = (c2 < 0x3f ? c2+0x40 : c2+0x41);
    return 2;
  }

  /* Irreversible mappings.  */
  if (wc == 0xff5e) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x81;
    r[1] = 0x60;
    return 2;
  }
  if (wc == 0x2225) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x81;
    r[1] = 0x61;
    return 2;
  }
  if (wc == 0xff0d) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x81;
    r[1] = 0x7c;
    return 2;
  }
  if (wc == 0xffe0) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x81;
    r[1] = 0x91;
    return 2;
  }
  if (wc == 0xffe1) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x81;
    r[1] = 0x92;
    return 2;
  }

  return RET_ILUNI;
}
