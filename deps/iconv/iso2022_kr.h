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
 * If not, write to the Free Software Foundation, Inc., 51 Franklin Street,
 * Fifth Floor, Boston, MA 02110-1301, USA.
 */

/*
 * ISO-2022-KR
 */

/* Specification: RFC 1557 */

/* Note: CJK.INF says the SO designator needs to appear only once at the
   beginning of a text, but to decrease the risk of ambiguities, when
   producing ISO-2022-KR, we repeat the designator in every line containing
   SO characters. RFC 1557 does not mandate this. */

#define ESC 0x1b
#define SO  0x0e
#define SI  0x0f

/*
 * The state is composed of one of the following values
 */
#define STATE_ASCII          0
#define STATE_TWOBYTE        1
/*
 * and one of the following values, << 8
 */
#define STATE2_NONE                0
#define STATE2_DESIGNATED_KSC5601  1

#define SPLIT_STATE \
  unsigned int state1 = state & 0xff, state2 = state >> 8
#define COMBINE_STATE \
  state = (state2 << 8) | state1

static int
iso2022_kr_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  state_t state = conv->istate;
  SPLIT_STATE;
  int count = 0;
  unsigned char c;
  for (;;) {
    c = *s;
    if (c == ESC) {
      if (n < count+4)
        goto none;
      if (s[1] == '$') {
        if (s[2] == ')') {
          if (s[3] == 'C') {
            state2 = STATE2_DESIGNATED_KSC5601;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
        }
      }
      goto ilseq;
    }
    if (c == SO) {
      if (state2 != STATE2_DESIGNATED_KSC5601)
        goto ilseq;
      state1 = STATE_TWOBYTE;
      s++; count++;
      if (n < count+1)
        goto none;
      continue;
    }
    if (c == SI) {
      state1 = STATE_ASCII;
      s++; count++;
      if (n < count+1)
        goto none;
      continue;
    }
    break;
  }
  switch (state1) {
    case STATE_ASCII:
      if (c < 0x80) {
        int ret = ascii_mbtowc(conv,pwc,s,1);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 1) abort();
#if 0 /* Accept ISO-2022-KR according to CJK.INF. */
        if (*pwc == 0x000a || *pwc == 0x000d)
          state2 = STATE2_NONE;
#endif
        COMBINE_STATE;
        conv->istate = state;
        return count+1;
      } else
        goto ilseq;
    case STATE_TWOBYTE:
      if (n < count+2)
        goto none;
      if (state2 != STATE2_DESIGNATED_KSC5601) abort();
      if (s[0] < 0x80 && s[1] < 0x80) {
        int ret = ksc5601_mbtowc(conv,pwc,s,2);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 2) abort();
        COMBINE_STATE;
        conv->istate = state;
        return count+2;
      } else
        goto ilseq;
    default: abort();
  }

none:
  COMBINE_STATE;
  conv->istate = state;
  return RET_TOOFEW(count);

ilseq:
  COMBINE_STATE;
  conv->istate = state;
  return RET_SHIFT_ILSEQ(count);
}

static int
iso2022_kr_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  unsigned char buf[2];
  int ret;

  /* Try ASCII. */
  ret = ascii_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] < 0x80) {
      int count = (state1 == STATE_ASCII ? 1 : 2);
      if (n < count)
        return RET_TOOSMALL;
      if (state1 != STATE_ASCII) {
        r[0] = SI;
        r += 1;
        state1 = STATE_ASCII;
      }
      r[0] = buf[0];
      if (wc == 0x000a || wc == 0x000d)
        state2 = STATE2_NONE;
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }
  }

  /* Try KS C 5601-1992. */
  ret = ksc5601_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (buf[0] < 0x80 && buf[1] < 0x80) {
      int count = (state2 == STATE2_DESIGNATED_KSC5601 ? 0 : 4) + (state1 == STATE_TWOBYTE ? 0 : 1) + 2;
      if (n < count)
        return RET_TOOSMALL;
      if (state2 != STATE2_DESIGNATED_KSC5601) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = ')';
        r[3] = 'C';
        r += 4;
        state2 = STATE2_DESIGNATED_KSC5601;
      }
      if (state1 != STATE_TWOBYTE) {
        r[0] = SO;
        r += 1;
        state1 = STATE_TWOBYTE;
      }
      r[0] = buf[0];
      r[1] = buf[1];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }
  }

  return RET_ILUNI;
}

static int
iso2022_kr_reset (conv_t conv, unsigned char *r, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  (void)state2;
  if (state1 != STATE_ASCII) {
    if (n < 1)
      return RET_TOOSMALL;
    r[0] = SI;
    /* conv->ostate = 0; will be done by the caller */
    return 1;
  } else
    return 0;
}

#undef COMBINE_STATE
#undef SPLIT_STATE
#undef STATE2_DESIGNATED_KSC5601
#undef STATE2_NONE
#undef STATE_TWOBYTE
#undef STATE_ASCII
