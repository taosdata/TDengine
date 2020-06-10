/*
 * Copyright (C) 1999-2002, 2006 Free Software Foundation, Inc.
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
 * BIG5-HKSCS:2004
 */

/*
 * BIG5-HKSCS:2004 can be downloaded from
 *   http://www.info.gov.hk/digital21/eng/hkscs/download.html
 *   http://www.info.gov.hk/digital21/eng/hkscs/index.html
 *
 * It extends BIG5-HKSCS:2001 through 123 characters.
 *
 * It extends BIG5 (without the rows 0xC6..0xC7) through the ranges
 *
 *   0x{87..8D}{40..7E,A1..FE}      880 characters
 *   0x{8E..A0}{40..7E,A1..FE}     2898 characters
 *   0x{C6..C8}{40..7E,A1..FE}      359 characters
 *   0xF9{D6..FE}                    41 characters
 *   0x{FA..FE}{40..7E,A1..FE}      763 characters
 *
 * Note that some HKSCS characters are not contained in Unicode 3.2
 * and are therefore best represented as sequences of Unicode characters:
 *   0x8862  U+00CA U+0304  LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND MACRON
 *   0x8864  U+00CA U+030C  LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND CARON
 *   0x88A3  U+00EA U+0304  LATIN SMALL LETTER E WITH CIRCUMFLEX AND MACRON
 *   0x88A5  U+00EA U+030C  LATIN SMALL LETTER E WITH CIRCUMFLEX AND CARON
 */

#include "hkscs2004.h"
#include "flushwc.h"

static int
big5hkscs2004_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  ucs4_t last_wc = conv->istate;
  if (last_wc) {
    /* Output the buffered character. */
    conv->istate = 0;
    *pwc = last_wc;
    return 0; /* Don't advance the input pointer. */
  } else {
    unsigned char c = *s;
    /* Code set 0 (ASCII) */
    if (c < 0x80)
      return ascii_mbtowc(conv,pwc,s,n);
    /* Code set 1 (BIG5 extended) */
    if (c >= 0xa1 && c < 0xff) {
      if (n < 2)
        return RET_TOOFEW(0);
      {
        unsigned char c2 = s[1];
        if ((c2 >= 0x40 && c2 < 0x7f) || (c2 >= 0xa1 && c2 < 0xff)) {
          if (!((c == 0xc6 && c2 >= 0xa1) || c == 0xc7)) {
            int ret = big5_mbtowc(conv,pwc,s,2);
            if (ret != RET_ILSEQ)
              return ret;
          }
        }
      }
    }
    {
      int ret = hkscs1999_mbtowc(conv,pwc,s,n);
      if (ret != RET_ILSEQ)
        return ret;
    }
    {
      int ret = hkscs2001_mbtowc(conv,pwc,s,n);
      if (ret != RET_ILSEQ)
        return ret;
    }
    {
      int ret = hkscs2004_mbtowc(conv,pwc,s,n);
      if (ret != RET_ILSEQ)
        return ret;
    }
    if (c == 0x88) {
      if (n < 2)
        return RET_TOOFEW(0);
      {
        unsigned char c2 = s[1];
        if (c2 == 0x62 || c2 == 0x64 || c2 == 0xa3 || c2 == 0xa5) {
          /* It's a composed character. */
          ucs4_t wc1 = ((c2 >> 3) << 2) + 0x009a; /* = 0x00ca or 0x00ea */
          ucs4_t wc2 = ((c2 & 6) << 2) + 0x02fc; /* = 0x0304 or 0x030c */
          /* We cannot output two Unicode characters at once. So,
             output the first character and buffer the second one. */
          *pwc = wc1;
          conv->istate = wc2;
          return 2;
        }
      }
    }
    return RET_ILSEQ;
  }
}

#define big5hkscs2004_flushwc normal_flushwc

static int
big5hkscs2004_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  int count = 0;
  unsigned char last = conv->ostate;

  if (last) {
    /* last is = 0x66 or = 0xa7. */
    if (wc == 0x0304 || wc == 0x030c) {
      /* Output the combined character. */
      if (n >= 2) {
        r[0] = 0x88;
        r[1] = last + ((wc & 24) >> 2) - 4; /* = 0x62 or 0x64 or 0xa3 or 0xa5 */
        conv->ostate = 0;
        return 2;
      } else
        return RET_TOOSMALL;
    }

    /* Output the buffered character. */
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x88;
    r[1] = last;
    r += 2;
    count = 2;
  }

  /* Code set 0 (ASCII) */
  if (wc < 0x0080) {
    /* Plain ASCII character. */
    if (n > count) {
      r[0] = (unsigned char) wc;
      conv->ostate = 0;
      return count+1;
    } else
      return RET_TOOSMALL;
  } else {
    unsigned char buf[2];
    int ret;

    /* Code set 1 (BIG5 extended) */
    ret = big5_wctomb(conv,buf,wc,2);
    if (ret != RET_ILUNI) {
      if (ret != 2) abort();
      if (!((buf[0] == 0xc6 && buf[1] >= 0xa1) || buf[0] == 0xc7)) {
        if (n >= count+2) {
          r[0] = buf[0];
          r[1] = buf[1];
          conv->ostate = 0;
          return count+2;
        } else
          return RET_TOOSMALL;
      }
    }
    ret = hkscs1999_wctomb(conv,buf,wc,2);
    if (ret != RET_ILUNI) {
      if (ret != 2) abort();
      if ((wc & ~0x0020) == 0x00ca) {
        /* A possible first character of a multi-character sequence. We have to
           buffer it. */
        if (!(buf[0] == 0x88 && (buf[1] == 0x66 || buf[1] == 0xa7))) abort();
        conv->ostate = buf[1]; /* = 0x66 or = 0xa7 */
        return count+0;
      }
      if (n >= count+2) {
        r[0] = buf[0];
        r[1] = buf[1];
        conv->ostate = 0;
        return count+2;
      } else
        return RET_TOOSMALL;
    }
    ret = hkscs2001_wctomb(conv,buf,wc,2);
    if (ret != RET_ILUNI) {
      if (ret != 2) abort();
      if (n >= count+2) {
        r[0] = buf[0];
        r[1] = buf[1];
        conv->ostate = 0;
        return count+2;
      } else
        return RET_TOOSMALL;
    }
    ret = hkscs2004_wctomb(conv,buf,wc,2);
    if (ret != RET_ILUNI) {
      if (ret != 2) abort();
      if (n >= count+2) {
        r[0] = buf[0];
        r[1] = buf[1];
        conv->ostate = 0;
        return count+2;
      } else
        return RET_TOOSMALL;
    }
    return RET_ILUNI;
  }
}

static int
big5hkscs2004_reset (conv_t conv, unsigned char *r, int n)
{
  unsigned char last = conv->ostate;

  if (last) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = 0x88;
    r[1] = last;
    /* conv->ostate = 0; will be done by the caller */
    return 2;
  } else
    return 0;
}
