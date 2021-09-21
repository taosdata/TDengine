/*
 * Copyright (C) 2001, 2005 Free Software Foundation, Inc.
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
 * DEC-HANYU
 */

static int
dec_hanyu_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  /* Code set 0 (ASCII) */
  if (c < 0x80)
    return ascii_mbtowc(conv,pwc,s,n);
  /* Code set 1 (CNS 11643-1992 Plane 1),
     Code set 2 (CNS 11643-1992 Plane 2),
     Code set 3 (CNS 11643-1992 Plane 3) */
  if (c >= 0xa1 && c < 0xff) {
    if (n < 2)
      return RET_TOOFEW(0);
    {
      unsigned char c2 = s[1];
      if (c == 0xc2 && c2 == 0xcb) {
        if (n < 4)
          return RET_TOOFEW(0);
        if (s[2] >= 0xa1 && s[2] < 0xff && s[3] >= 0xa1 && s[3] < 0xff) {
          unsigned char buf[2];
          int ret;
          buf[0] = s[2]-0x80; buf[1] = s[3]-0x80;
          ret = cns11643_3_mbtowc(conv,pwc,buf,2);
          if (ret != RET_ILSEQ) {
            if (ret != 2) abort();
            return 4;
          }
        }
      } else if (c2 >= 0xa1 && c2 < 0xff) {
        if (c != 0xc2 || c2 < 0xc2) {
          unsigned char buf[2];
          buf[0] = c-0x80; buf[1] = c2-0x80;
          return cns11643_1_mbtowc(conv,pwc,buf,2);
        }
      } else if (c2 >= 0x21 && c2 < 0x7f) {
        unsigned char buf[2];
        buf[0] = c-0x80; buf[1] = c2;
        return cns11643_2_mbtowc(conv,pwc,buf,2);
      }
    }
  }
  return RET_ILSEQ;
}

static int
dec_hanyu_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char buf[3];
  int ret;

  /* Code set 0 (ASCII) */
  ret = ascii_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  ret = cns11643_wctomb(conv,buf,wc,3);
  if (ret != RET_ILUNI) {
    if (ret != 3) abort();

    /* Code set 1 (CNS 11643-1992 Plane 1) */
    if (buf[0] == 1 && (buf[1] != 0x42 || buf[2] < 0x42)) {
      if (n < 2)
        return RET_TOOSMALL;
      r[0] = buf[1]+0x80;
      r[1] = buf[2]+0x80;
      return 2;
    }

    /* Code set 2 (CNS 11643-1992 Plane 2) */
    if (buf[0] == 2) {
      if (n < 2)
        return RET_TOOSMALL;
      r[0] = buf[1]+0x80;
      r[1] = buf[2];
      return 2;
    }

    /* Code set 3 (CNS 11643-1992 Plane 3) */
    if (buf[0] == 3) {
      if (n < 4)
        return RET_TOOSMALL;
      r[0] = 0xc2;
      r[1] = 0xcb;
      r[2] = buf[1]+0x80;
      r[3] = buf[2]+0x80;
      return 4;
    }
  }

  return RET_ILUNI;
}
