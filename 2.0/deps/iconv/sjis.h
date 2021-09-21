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
 * SHIFT_JIS
 */

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
sjis_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  if (c < 0x80 || (c >= 0xa1 && c <= 0xdf))
    return jisx0201_mbtowc(conv,pwc,s,n);
  else {
    unsigned char s1, s2;
    s1 = c;
    if ((s1 >= 0x81 && s1 <= 0x9f) || (s1 >= 0xe0 && s1 <= 0xea)) {
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
sjis_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char buf[2];
  int ret;

  /* Try JIS X 0201-1976. */
  ret = jisx0201_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    unsigned char c;
    if (ret != 1) abort();
    c = buf[0];
    if (c < 0x80 || (c >= 0xa1 && c <= 0xdf)) {
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

  return RET_ILUNI;
}
