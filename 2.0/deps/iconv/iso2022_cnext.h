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
 * ISO-2022-CN-EXT
 */

/* Specification: RFC 1922 */

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
#define STATE2_NONE                   0
#define STATE2_DESIGNATED_GB2312      1
#define STATE2_DESIGNATED_CNS11643_1  2
#define STATE2_DESIGNATED_ISO_IR_165  3
/*
 * and one of the following values, << 16
 */
#define STATE3_NONE                   0
#define STATE3_DESIGNATED_CNS11643_2  1
/*
 * and one of the following values, << 24
 */
#define STATE4_NONE                   0
#define STATE4_DESIGNATED_CNS11643_3  1
#define STATE4_DESIGNATED_CNS11643_4  2
#define STATE4_DESIGNATED_CNS11643_5  3
#define STATE4_DESIGNATED_CNS11643_6  4
#define STATE4_DESIGNATED_CNS11643_7  5

#define SPLIT_STATE \
  unsigned int state1 = state & 0xff, state2 = (state >> 8) & 0xff, state3 = (state >> 16) & 0xff, state4 = state >> 24
#define COMBINE_STATE \
  state = (state4 << 24) | (state3 << 16) | (state2 << 8) | state1

static int
iso2022_cn_ext_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
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
          if (s[3] == 'A') {
            state2 = STATE2_DESIGNATED_GB2312;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'G') {
            state2 = STATE2_DESIGNATED_CNS11643_1;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'E') {
            state2 = STATE2_DESIGNATED_ISO_IR_165;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
        }
        if (s[2] == '*') {
          if (s[3] == 'H') {
            state3 = STATE3_DESIGNATED_CNS11643_2;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
        }
        if (s[2] == '+') {
          if (s[3] == 'I') {
            state4 = STATE4_DESIGNATED_CNS11643_3;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'J') {
            state4 = STATE4_DESIGNATED_CNS11643_4;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'K') {
            state4 = STATE4_DESIGNATED_CNS11643_5;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'L') {
            state4 = STATE4_DESIGNATED_CNS11643_6;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'M') {
            state4 = STATE4_DESIGNATED_CNS11643_7;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
        }
      }
      if (s[1] == 'N') {
        switch (state3) {
          case STATE3_NONE:
            goto ilseq;
          case STATE3_DESIGNATED_CNS11643_2:
            if (s[2] < 0x80 && s[3] < 0x80) {
              int ret = cns11643_2_mbtowc(conv,pwc,s+2,2);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 2) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+4;
            } else
              goto ilseq;
          default: abort();
        }
      }
      if (s[1] == 'O') {
        switch (state4) {
          case STATE4_NONE:
            goto ilseq;
          case STATE4_DESIGNATED_CNS11643_3:
            if (s[2] < 0x80 && s[3] < 0x80) {
              int ret = cns11643_3_mbtowc(conv,pwc,s+2,2);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 2) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+4;
            } else
              goto ilseq;
          case STATE4_DESIGNATED_CNS11643_4:
            if (s[2] < 0x80 && s[3] < 0x80) {
              int ret = cns11643_4_mbtowc(conv,pwc,s+2,2);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 2) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+4;
            } else
              goto ilseq;
          case STATE4_DESIGNATED_CNS11643_5:
            if (s[2] < 0x80 && s[3] < 0x80) {
              int ret = cns11643_5_mbtowc(conv,pwc,s+2,2);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 2) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+4;
            } else
              goto ilseq;
          case STATE4_DESIGNATED_CNS11643_6:
            if (s[2] < 0x80 && s[3] < 0x80) {
              int ret = cns11643_6_mbtowc(conv,pwc,s+2,2);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 2) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+4;
            } else
              goto ilseq;
          case STATE4_DESIGNATED_CNS11643_7:
            if (s[2] < 0x80 && s[3] < 0x80) {
              int ret = cns11643_7_mbtowc(conv,pwc,s+2,2);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 2) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+4;
            } else
              goto ilseq;
          default: abort();
        }
      }
      goto ilseq;
    }
    if (c == SO) {
      if (state2 != STATE2_DESIGNATED_GB2312 && state2 != STATE2_DESIGNATED_CNS11643_1 && state2 != STATE2_DESIGNATED_ISO_IR_165)
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
        if (*pwc == 0x000a || *pwc == 0x000d) {
          state2 = STATE2_NONE; state3 = STATE3_NONE; state4 = STATE3_NONE;
        }
        COMBINE_STATE;
        conv->istate = state;
        return count+1;
      } else
        goto ilseq;
    case STATE_TWOBYTE:
      if (n < count+2)
        goto none;
      if (s[0] < 0x80 && s[1] < 0x80) {
        int ret;
        switch (state2) {
          case STATE2_NONE:
            goto ilseq;
          case STATE2_DESIGNATED_GB2312:
            ret = gb2312_mbtowc(conv,pwc,s,2); break;
          case STATE2_DESIGNATED_CNS11643_1:
            ret = cns11643_1_mbtowc(conv,pwc,s,2); break;
          case STATE2_DESIGNATED_ISO_IR_165:
            ret = isoir165_mbtowc(conv,pwc,s,2); break;
          default: abort();
        }
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
iso2022_cn_ext_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  unsigned char buf[3];
  int ret;

  /* There is no need to handle Unicode 3.1 tag characters and to look for
     "zh-CN" or "zh-TW" tags, because GB2312 and CNS11643 are disjoint. */

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
      if (wc == 0x000a || wc == 0x000d) {
        state2 = STATE2_NONE; state3 = STATE3_NONE; state4 = STATE3_NONE;
      }
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }
  }

  /* Try GB 2312-1980. */
  ret = gb2312_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (buf[0] < 0x80 && buf[1] < 0x80) {
      int count = (state2 == STATE2_DESIGNATED_GB2312 ? 0 : 4) + (state1 == STATE_TWOBYTE ? 0 : 1) + 2;
      if (n < count)
        return RET_TOOSMALL;
      if (state2 != STATE2_DESIGNATED_GB2312) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = ')';
        r[3] = 'A';
        r += 4;
        state2 = STATE2_DESIGNATED_GB2312;
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

  ret = cns11643_wctomb(conv,buf,wc,3);
  if (ret != RET_ILUNI) {
    if (ret != 3) abort();

    /* Try CNS 11643-1992 Plane 1. */
    if (buf[0] == 1 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state2 == STATE2_DESIGNATED_CNS11643_1 ? 0 : 4) + (state1 == STATE_TWOBYTE ? 0 : 1) + 2;
      if (n < count)
        return RET_TOOSMALL;
      if (state2 != STATE2_DESIGNATED_CNS11643_1) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = ')';
        r[3] = 'G';
        r += 4;
        state2 = STATE2_DESIGNATED_CNS11643_1;
      }
      if (state1 != STATE_TWOBYTE) {
        r[0] = SO;
        r += 1;
        state1 = STATE_TWOBYTE;
      }
      r[0] = buf[1];
      r[1] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

    /* Try CNS 11643-1992 Plane 2. */
    if (buf[0] == 2 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state3 == STATE3_DESIGNATED_CNS11643_2 ? 0 : 4) + 4;
      if (n < count)
        return RET_TOOSMALL;
      if (state3 != STATE3_DESIGNATED_CNS11643_2) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '*';
        r[3] = 'H';
        r += 4;
        state3 = STATE3_DESIGNATED_CNS11643_2;
      }
      r[0] = ESC;
      r[1] = 'N';
      r[2] = buf[1];
      r[3] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

    /* Try CNS 11643-1992 Plane 3. */
    if (buf[0] == 3 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state4 == STATE4_DESIGNATED_CNS11643_3 ? 0 : 4) + 4;
      if (n < count)
        return RET_TOOSMALL;
      if (state4 != STATE4_DESIGNATED_CNS11643_3) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '+';
        r[3] = 'I';
        r += 4;
        state4 = STATE4_DESIGNATED_CNS11643_3;
      }
      r[0] = ESC;
      r[1] = 'O';
      r[2] = buf[1];
      r[3] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

    /* Try CNS 11643-1992 Plane 4. */
    if (buf[0] == 4 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state4 == STATE4_DESIGNATED_CNS11643_4 ? 0 : 4) + 4;
      if (n < count)
        return RET_TOOSMALL;
      if (state4 != STATE4_DESIGNATED_CNS11643_4) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '+';
        r[3] = 'J';
        r += 4;
        state4 = STATE4_DESIGNATED_CNS11643_4;
      }
      r[0] = ESC;
      r[1] = 'O';
      r[2] = buf[1];
      r[3] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

    /* Try CNS 11643-1992 Plane 5. */
    if (buf[0] == 5 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state4 == STATE4_DESIGNATED_CNS11643_5 ? 0 : 4) + 4;
      if (n < count)
        return RET_TOOSMALL;
      if (state4 != STATE4_DESIGNATED_CNS11643_5) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '+';
        r[3] = 'K';
        r += 4;
        state4 = STATE4_DESIGNATED_CNS11643_5;
      }
      r[0] = ESC;
      r[1] = 'O';
      r[2] = buf[1];
      r[3] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

    /* Try CNS 11643-1992 Plane 6. */
    if (buf[0] == 6 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state4 == STATE4_DESIGNATED_CNS11643_6 ? 0 : 4) + 4;
      if (n < count)
        return RET_TOOSMALL;
      if (state4 != STATE4_DESIGNATED_CNS11643_6) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '+';
        r[3] = 'L';
        r += 4;
        state4 = STATE4_DESIGNATED_CNS11643_6;
      }
      r[0] = ESC;
      r[1] = 'O';
      r[2] = buf[1];
      r[3] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

    /* Try CNS 11643-1992 Plane 7. */
    if (buf[0] == 7 && buf[1] < 0x80 && buf[2] < 0x80) {
      int count = (state4 == STATE4_DESIGNATED_CNS11643_7 ? 0 : 4) + 4;
      if (n < count)
        return RET_TOOSMALL;
      if (state4 != STATE4_DESIGNATED_CNS11643_7) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '+';
        r[3] = 'M';
        r += 4;
        state4 = STATE4_DESIGNATED_CNS11643_7;
      }
      r[0] = ESC;
      r[1] = 'O';
      r[2] = buf[1];
      r[3] = buf[2];
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }

  }

  /* Try ISO-IR-165. */
  ret = isoir165_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (buf[0] < 0x80 && buf[1] < 0x80) {
      int count = (state2 == STATE2_DESIGNATED_ISO_IR_165 ? 0 : 4) + (state1 == STATE_TWOBYTE ? 0 : 1) + 2;
      if (n < count)
        return RET_TOOSMALL;
      if (state2 != STATE2_DESIGNATED_ISO_IR_165) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = ')';
        r[3] = 'E';
        r += 4;
        state2 = STATE2_DESIGNATED_ISO_IR_165;
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
iso2022_cn_ext_reset (conv_t conv, unsigned char *r, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  (void)state2;
  (void)state3;
  (void)state4;
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
#undef STATE4_DESIGNATED_CNS11643_7
#undef STATE4_DESIGNATED_CNS11643_6
#undef STATE4_DESIGNATED_CNS11643_5
#undef STATE4_DESIGNATED_CNS11643_4
#undef STATE4_DESIGNATED_CNS11643_3
#undef STATE4_NONE
#undef STATE3_DESIGNATED_CNS11643_2
#undef STATE3_NONE
#undef STATE2_DESIGNATED_ISO_IR_165
#undef STATE2_DESIGNATED_CNS11643_1
#undef STATE2_DESIGNATED_GB2312
#undef STATE2_NONE
#undef STATE_TWOBYTE
#undef STATE_ASCII
