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
 * EUC-TW
 */

static int
euc_tw_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  /* Code set 0 (ASCII) */
  if (c < 0x80)
    return ascii_mbtowc(conv,pwc,s,n);
  /* Code set 1 (CNS 11643-1992 Plane 1) */
  if (c >= 0xa1 && c < 0xff) {
    if (n < 2)
      return RET_TOOFEW(0);
    {
      unsigned char c2 = s[1];
      if (c2 >= 0xa1 && c2 < 0xff) {
        unsigned char buf[2];
        buf[0] = c-0x80; buf[1] = c2-0x80;
        return cns11643_1_mbtowc(conv,pwc,buf,2);
      } else
        return RET_ILSEQ;
    }
  }
  /* Code set 2 (CNS 11643-1992 Planes 1-16) */
  if (c == 0x8e) {
    if (n < 4)
      return RET_TOOFEW(0);
    {
      unsigned char c2 = s[1];
      if (c2 >= 0xa1 && c2 <= 0xb0) {
        unsigned char c3 = s[2];
        unsigned char c4 = s[3];
        if (c3 >= 0xa1 && c3 < 0xff && c4 >= 0xa1 && c4 < 0xff) {
          unsigned char buf[2];
          int ret;
          buf[0] = c3-0x80; buf[1] = c4-0x80;
          switch (c2-0xa0) {
            case 1: ret = cns11643_1_mbtowc(conv,pwc,buf,2); break;
            case 2: ret = cns11643_2_mbtowc(conv,pwc,buf,2); break;
            case 3: ret = cns11643_3_mbtowc(conv,pwc,buf,2); break;
            case 4: ret = cns11643_4_mbtowc(conv,pwc,buf,2); break;
            case 5: ret = cns11643_5_mbtowc(conv,pwc,buf,2); break;
            case 6: ret = cns11643_6_mbtowc(conv,pwc,buf,2); break;
            case 7: ret = cns11643_7_mbtowc(conv,pwc,buf,2); break;
            case 15: ret = cns11643_15_mbtowc(conv,pwc,buf,2); break;
            default: return RET_ILSEQ;
          }
          if (ret == RET_ILSEQ)
            return RET_ILSEQ;
          if (ret != 2) abort();
          return 4;
        }
      }
    }
  }
  return RET_ILSEQ;
}

static int
euc_tw_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
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
    if (buf[0] == 1) {
      if (n < 2)
        return RET_TOOSMALL;
      r[0] = buf[1]+0x80;
      r[1] = buf[2]+0x80;
      return 2;
    }

    /* Code set 2 (CNS 11643-1992 Planes 1-16) */
    if (n < 4)
      return RET_TOOSMALL;
    r[0] = 0x8e;
    r[1] = buf[0]+0xa0;
    r[2] = buf[1]+0x80;
    r[3] = buf[2]+0x80;
    return 4;
  }

  return RET_ILUNI;
}
