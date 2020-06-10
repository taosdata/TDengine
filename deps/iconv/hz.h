/*
 * Copyright (C) 1999-2001, 2008 Free Software Foundation, Inc.
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
 * If not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */

/*
 * HZ
 */

/* Specification: RFC 1842, RFC 1843 */

/*
 * The state is 1 in GB mode, 0 in ASCII mode.
 */

static int
hz_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  state_t state = conv->istate;
  unsigned int count = 0;
  unsigned char c;
  for (;;) {
    c = *s;
    if (c == '~') {
      if (n < count+2)
        goto none;
      c = s[1];
      if (state == 0) {
        if (c == '~') {
          *pwc = (ucs4_t) '~';
          conv->istate = state;
          return count+2;
        }
        if (c == '{') {
          state = 1;
          s += 2; count += 2;
          if (n < count+1)
            goto none;
          continue;
        }
        if (c == '\n') {
          s += 2; count += 2;
          if (n < count+1)
            goto none;
          continue;
        }
      } else {
        if (c == '}') {
          state = 0;
          s += 2; count += 2;
          if (n < count+1)
            goto none;
          continue;
        }
      }
      goto ilseq;
    }
    break;
  }
  if (state == 0) {
    *pwc = (ucs4_t) c;
    conv->istate = state;
    return count+1;
  } else {
    int ret;
    if (n < count+2)
      goto none;
    ret = gb2312_mbtowc(conv,pwc,s,2);
    if (ret == RET_ILSEQ)
      goto ilseq;
    if (ret != 2) abort();
    conv->istate = state;
    return count+2;
  }

none:
  conv->istate = state;
  return RET_TOOFEW(count);

ilseq:
  conv->istate = state;
  return RET_SHIFT_ILSEQ(count);
}

static int
hz_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  state_t state = conv->ostate;
  unsigned char buf[2];
  int ret;

  /* Code set 0 (ASCII or GB 1988-89) */
  ret = ascii_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] < 0x80) {
      int count = (state ? 3 : 1);
      if (n < count)
        return RET_TOOSMALL;
      if (state) {
        r[0] = '~';
        r[1] = '}';
        r += 2;
        state = 0;
      }
      r[0] = buf[0];
      conv->ostate = state;
      return count;
    }
  }

  /* Code set 1 (GB 2312-1980) */
  ret = gb2312_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (buf[0] < 0x80 && buf[1] < 0x80) {
      int count = (state ? 2 : 4);
      if (n < count)
        return RET_TOOSMALL;
      if (!state) {
        r[0] = '~';
        r[1] = '{';
        r += 2;
        state = 1;
      }
      r[0] = buf[0];
      r[1] = buf[1];
      conv->ostate = state;
      return count;
    }
  }

  return RET_ILUNI;
}

static int
hz_reset (conv_t conv, unsigned char *r, int n)
{
  state_t state = conv->ostate;
  if (state) {
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = '~';
    r[1] = '}';
    /* conv->ostate = 0; will be done by the caller */
    return 2;
  } else
    return 0;
}
