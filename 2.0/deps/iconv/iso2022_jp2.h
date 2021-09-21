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
 * ISO-2022-JP-2
 */

/* Specification: RFC 1554 */
/* ESC '(' 'I' for JISX0201 Katakana is an extension not found in RFC 1554 or
   CJK.INF, but implemented in glibc-2.1 and qt-2.0. */

#define ESC 0x1b

/*
 * The state is composed of one of the following values
 */
#define STATE_ASCII             0
#define STATE_JISX0201ROMAN     1
#define STATE_JISX0201KATAKANA  2
#define STATE_JISX0208          3
#define STATE_JISX0212          4
#define STATE_GB2312            5
#define STATE_KSC5601           6
/*
 * and one of the following values, << 8
 */
#define STATE_G2_NONE           0
#define STATE_G2_ISO8859_1      1
#define STATE_G2_ISO8859_7      2

#define SPLIT_STATE \
  unsigned int state1 = state & 0xff, state2 = state >> 8
#define COMBINE_STATE \
  state = (state2 << 8) | state1

static int
iso2022_jp2_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  state_t state = conv->istate;
  SPLIT_STATE;
  int count = 0;
  unsigned char c;
  for (;;) {
    c = *s;
    if (c == ESC) {
      if (n < count+3)
        goto none;
      if (s[1] == '(') {
        if (s[2] == 'B') {
          state1 = STATE_ASCII;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        if (s[2] == 'J') {
          state1 = STATE_JISX0201ROMAN;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        if (s[2] == 'I') {
          state1 = STATE_JISX0201KATAKANA;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        goto ilseq;
      }
      if (s[1] == '$') {
        if (s[2] == '@' || s[2] == 'B') {
          /* We don't distinguish JIS X 0208-1978 and JIS X 0208-1983. */
          state1 = STATE_JISX0208;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        if (s[2] == 'A') {
          state1 = STATE_GB2312;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        if (s[2] == '(') {
          if (n < count+4)
            goto none;
          if (s[3] == 'D') {
            state1 = STATE_JISX0212;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[3] == 'C') {
            state1 = STATE_KSC5601;
            s += 4; count += 4;
            if (n < count+1)
              goto none;
            continue;
          }
          goto ilseq;
        }
        goto ilseq;
      }
      if (s[1] == '.') {
        if (n < count+3)
          goto none;
        if (s[2] == 'A') {
          state2 = STATE_G2_ISO8859_1;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        if (s[2] == 'F') {
          state2 = STATE_G2_ISO8859_7;
          s += 3; count += 3;
          if (n < count+1)
            goto none;
          continue;
        }
        goto ilseq;
      }
      if (s[1] == 'N') {
        switch (state2) {
          case STATE_G2_NONE:
            goto ilseq;
          case STATE_G2_ISO8859_1:
            if (s[2] < 0x80) {
              unsigned char buf = s[2]+0x80;
              int ret = iso8859_1_mbtowc(conv,pwc,&buf,1);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 1) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+3;
            } else
              goto ilseq;
          case STATE_G2_ISO8859_7:
            if (s[2] < 0x80) {
              unsigned char buf = s[2]+0x80;
              int ret = iso8859_7_mbtowc(conv,pwc,&buf,1);
              if (ret == RET_ILSEQ)
                goto ilseq;
              if (ret != 1) abort();
              COMBINE_STATE;
              conv->istate = state;
              return count+3;
            } else
              goto ilseq;
          default: abort();
        }
      }
      goto ilseq;
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
        if (*pwc == 0x000a || *pwc == 0x000d)
          state2 = STATE_G2_NONE;
        COMBINE_STATE;
        conv->istate = state;
        return count+1;
      } else
        goto ilseq;
    case STATE_JISX0201ROMAN:
      if (c < 0x80) {
        int ret = jisx0201_mbtowc(conv,pwc,s,1);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 1) abort();
        if (*pwc == 0x000a || *pwc == 0x000d)
          state2 = STATE_G2_NONE;
        COMBINE_STATE;
        conv->istate = state;
        return count+1;
      } else
        goto ilseq;
    case STATE_JISX0201KATAKANA:
      if (c < 0x80) {
        unsigned char buf = c+0x80;
        int ret = jisx0201_mbtowc(conv,pwc,&buf,1);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 1) abort();
        COMBINE_STATE;
        conv->istate = state;
        return count+1;
      } else
        goto ilseq;
    case STATE_JISX0208:
      if (n < count+2)
        goto none;
      if (s[0] < 0x80 && s[1] < 0x80) {
        int ret = jisx0208_mbtowc(conv,pwc,s,2);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 2) abort();
        COMBINE_STATE;
        conv->istate = state;
        return count+2;
      } else
        goto ilseq;
    case STATE_JISX0212:
      if (n < count+2)
        goto none;
      if (s[0] < 0x80 && s[1] < 0x80) {
        int ret = jisx0212_mbtowc(conv,pwc,s,2);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 2) abort();
        COMBINE_STATE;
        conv->istate = state;
        return count+2;
      } else
        goto ilseq;
    case STATE_GB2312:
      if (n < count+2)
        goto none;
      if (s[0] < 0x80 && s[1] < 0x80) {
        int ret = gb2312_mbtowc(conv,pwc,s,2);
        if (ret == RET_ILSEQ)
          goto ilseq;
        if (ret != 2) abort();
        COMBINE_STATE;
        conv->istate = state;
        return count+2;
      } else
        goto ilseq;
    case STATE_KSC5601:
      if (n < count+2)
        goto none;
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

#undef COMBINE_STATE
#undef SPLIT_STATE

/*
 * The state can also contain one of the following values, << 16.
 * Values >= STATE_TAG_LANGUAGE are temporary tag parsing states.
 */
#define STATE_TAG_NONE          0
#define STATE_TAG_LANGUAGE      4
#define STATE_TAG_LANGUAGE_j    5
#define STATE_TAG_LANGUAGE_ja   1
#define STATE_TAG_LANGUAGE_k    6
#define STATE_TAG_LANGUAGE_ko   2
#define STATE_TAG_LANGUAGE_z    7
#define STATE_TAG_LANGUAGE_zh   3

#define SPLIT_STATE \
  unsigned int state1 = state & 0xff, state2 = (state >> 8) & 0xff, state3 = state >> 16
#define COMBINE_STATE \
  state = (state3 << 16) | (state2 << 8) | state1

static int
iso2022_jp2_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  unsigned char buf[2];
  int ret;
  /* This defines the conversion preferences depending on the current
     langauge tag. */
  enum conversion { none = 0, european, japanese, chinese, korean, other };
  static const unsigned int conversion_lists[STATE_TAG_LANGUAGE] = {
    /* STATE_TAG_NONE */
    japanese + (european << 3) + (chinese << 6) + (korean << 9) + (other << 12),
    /* STATE_TAG_LANGUAGE_ja */
    japanese + (european << 3) + (chinese << 6) + (korean << 9) + (other << 12),
    /* STATE_TAG_LANGUAGE_ko */
    korean + (european << 3) + (japanese << 6) + (chinese << 9) + (other << 12),
    /* STATE_TAG_LANGUAGE_zh */
    chinese + (european << 3) + (japanese << 6) + (korean << 9) + (other << 12)
  };
  unsigned int conversion_list;

  /* Handle Unicode tag characters (range U+E0000..U+E007F). */
  if ((wc >> 7) == (0xe0000 >> 7)) {
    char c = wc & 0x7f;
    if (c >= 'A' && c <= 'Z')
      c += 'a'-'A';
    switch (c) {
      case 0x01:
        state3 = STATE_TAG_LANGUAGE;
        COMBINE_STATE;
        conv->ostate = state;
        return 0;
      case 'j':
        if (state3 == STATE_TAG_LANGUAGE) {
          state3 = STATE_TAG_LANGUAGE_j;
          COMBINE_STATE;
          conv->ostate = state;
          return 0;
        }
        break;
      case 'a':
        if (state3 == STATE_TAG_LANGUAGE_j) {
          state3 = STATE_TAG_LANGUAGE_ja;
          COMBINE_STATE;
          conv->ostate = state;
          return 0;
        }
        break;
      case 'k':
        if (state3 == STATE_TAG_LANGUAGE) {
          state3 = STATE_TAG_LANGUAGE_k;
          COMBINE_STATE;
          conv->ostate = state;
          return 0;
        }
        break;
      case 'o':
        if (state3 == STATE_TAG_LANGUAGE_k) {
          state3 = STATE_TAG_LANGUAGE_ko;
          COMBINE_STATE;
          conv->ostate = state;
          return 0;
        }
        break;
      case 'z':
        if (state3 == STATE_TAG_LANGUAGE) {
          state3 = STATE_TAG_LANGUAGE_z;
          COMBINE_STATE;
          conv->ostate = state;
          return 0;
        }
        break;
      case 'h':
        if (state3 == STATE_TAG_LANGUAGE_z) {
          state3 = STATE_TAG_LANGUAGE_zh;
          COMBINE_STATE;
          conv->ostate = state;
          return 0;
        }
        break;
      case 0x7f:
        state3 = STATE_TAG_NONE;
        COMBINE_STATE;
        conv->ostate = state;
        return 0;
      default:
        break;
    }
    /* Other tag characters reset the tag parsing state or are ignored. */
    if (state3 >= STATE_TAG_LANGUAGE)
      state3 = STATE_TAG_NONE;
    COMBINE_STATE;
    conv->ostate = state;
    return 0;
  }
  if (state3 >= STATE_TAG_LANGUAGE)
    state3 = STATE_TAG_NONE;

  /* Try ASCII. */
  ret = ascii_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] < 0x80) {
      int count = (state1 == STATE_ASCII ? 1 : 4);
      if (n < count)
        return RET_TOOSMALL;
      if (state1 != STATE_ASCII) {
        r[0] = ESC;
        r[1] = '(';
        r[2] = 'B';
        r += 3;
        state1 = STATE_ASCII;
      }
      r[0] = buf[0];
      if (wc == 0x000a || wc == 0x000d)
        state2 = STATE_G2_NONE;
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }
  }

  conversion_list = conversion_lists[state3];

  do {
    switch (conversion_list & ((1 << 3) - 1)) {

      case european:

        /* Try ISO-8859-1. */
        ret = iso8859_1_wctomb(conv,buf,wc,1);
        if (ret != RET_ILUNI) {
          if (ret != 1) abort();
          if (buf[0] >= 0x80) {
            int count = (state2 == STATE_G2_ISO8859_1 ? 3 : 6);
            if (n < count)
              return RET_TOOSMALL;
            if (state2 != STATE_G2_ISO8859_1) {
              r[0] = ESC;
              r[1] = '.';
              r[2] = 'A';
              r += 3;
              state2 = STATE_G2_ISO8859_1;
            }
            r[0] = ESC;
            r[1] = 'N';
            r[2] = buf[0]-0x80;
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        /* Try ISO-8859-7. */
        ret = iso8859_7_wctomb(conv,buf,wc,1);
        if (ret != RET_ILUNI) {
          if (ret != 1) abort();
          if (buf[0] >= 0x80) {
            int count = (state2 == STATE_G2_ISO8859_7 ? 3 : 6);
            if (n < count)
              return RET_TOOSMALL;
            if (state2 != STATE_G2_ISO8859_7) {
              r[0] = ESC;
              r[1] = '.';
              r[2] = 'F';
              r += 3;
              state2 = STATE_G2_ISO8859_7;
            }
            r[0] = ESC;
            r[1] = 'N';
            r[2] = buf[0]-0x80;
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        break;

      case japanese:

        /* Try JIS X 0201-1976 Roman. */
        ret = jisx0201_wctomb(conv,buf,wc,1);
        if (ret != RET_ILUNI) {
          if (ret != 1) abort();
          if (buf[0] < 0x80) {
            int count = (state1 == STATE_JISX0201ROMAN ? 1 : 4);
            if (n < count)
              return RET_TOOSMALL;
            if (state1 != STATE_JISX0201ROMAN) {
              r[0] = ESC;
              r[1] = '(';
              r[2] = 'J';
              r += 3;
              state1 = STATE_JISX0201ROMAN;
            }
            r[0] = buf[0];
            if (wc == 0x000a || wc == 0x000d)
              state2 = STATE_G2_NONE;
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        /* Try JIS X 0208-1990 in place of JIS X 0208-1978 and
           JIS X 0208-1983. */
        ret = jisx0208_wctomb(conv,buf,wc,2);
        if (ret != RET_ILUNI) {
          if (ret != 2) abort();
          if (buf[0] < 0x80 && buf[1] < 0x80) {
            int count = (state1 == STATE_JISX0208 ? 2 : 5);
            if (n < count)
              return RET_TOOSMALL;
            if (state1 != STATE_JISX0208) {
              r[0] = ESC;
              r[1] = '$';
              r[2] = 'B';
              r += 3;
              state1 = STATE_JISX0208;
            }
            r[0] = buf[0];
            r[1] = buf[1];
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        /* Try JIS X 0212-1990. */
        ret = jisx0212_wctomb(conv,buf,wc,2);
        if (ret != RET_ILUNI) {
          if (ret != 2) abort();
          if (buf[0] < 0x80 && buf[1] < 0x80) {
            int count = (state1 == STATE_JISX0212 ? 2 : 6);
            if (n < count)
              return RET_TOOSMALL;
            if (state1 != STATE_JISX0212) {
              r[0] = ESC;
              r[1] = '$';
              r[2] = '(';
              r[3] = 'D';
              r += 4;
              state1 = STATE_JISX0212;
            }
            r[0] = buf[0];
            r[1] = buf[1];
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        break;

      case chinese:

        /* Try GB 2312-1980. */
        ret = gb2312_wctomb(conv,buf,wc,2);
        if (ret != RET_ILUNI) {
          if (ret != 2) abort();
          if (buf[0] < 0x80 && buf[1] < 0x80) {
            int count = (state1 == STATE_GB2312 ? 2 : 5);
            if (n < count)
              return RET_TOOSMALL;
            if (state1 != STATE_GB2312) {
              r[0] = ESC;
              r[1] = '$';
              r[2] = 'A';
              r += 3;
              state1 = STATE_GB2312;
            }
            r[0] = buf[0];
            r[1] = buf[1];
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        break;

      case korean:

        /* Try KS C 5601-1992. */
        ret = ksc5601_wctomb(conv,buf,wc,2);
        if (ret != RET_ILUNI) {
          if (ret != 2) abort();
          if (buf[0] < 0x80 && buf[1] < 0x80) {
            int count = (state1 == STATE_KSC5601 ? 2 : 6);
            if (n < count)
              return RET_TOOSMALL;
            if (state1 != STATE_KSC5601) {
              r[0] = ESC;
              r[1] = '$';
              r[2] = '(';
              r[3] = 'C';
              r += 4;
              state1 = STATE_KSC5601;
            }
            r[0] = buf[0];
            r[1] = buf[1];
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        break;

      case other:

        /* Try JIS X 0201-1976 Kana. This is not officially part of
           ISO-2022-JP-2, according to RFC 1554. Therefore we try this
           only after all other attempts. */
        ret = jisx0201_wctomb(conv,buf,wc,1);
        if (ret != RET_ILUNI) {
          if (ret != 1) abort();
          if (buf[0] >= 0x80) {
            int count = (state1 == STATE_JISX0201KATAKANA ? 1 : 4);
            if (n < count)
              return RET_TOOSMALL;
            if (state1 != STATE_JISX0201KATAKANA) {
              r[0] = ESC;
              r[1] = '(';
              r[2] = 'I';
              r += 3;
              state1 = STATE_JISX0201KATAKANA;
            }
            r[0] = buf[0]-0x80;
            COMBINE_STATE;
            conv->ostate = state;
            return count;
          }
        }

        break;

      default:
        abort();
    }

    conversion_list = conversion_list >> 3;
  } while (conversion_list != 0);

  return RET_ILUNI;
}

static int
iso2022_jp2_reset (conv_t conv, unsigned char *r, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  (void)state2;
  (void)state3;
  if (state1 != STATE_ASCII) {
    if (n < 3)
      return RET_TOOSMALL;
    r[0] = ESC;
    r[1] = '(';
    r[2] = 'B';
    /* conv->ostate = 0; will be done by the caller */
    return 3;
  } else
    return 0;
}

#undef COMBINE_STATE
#undef SPLIT_STATE
#undef STATE_TAG_LANGUAGE_zh
#undef STATE_TAG_LANGUAGE_z
#undef STATE_TAG_LANGUAGE_ko
#undef STATE_TAG_LANGUAGE_k
#undef STATE_TAG_LANGUAGE_ja
#undef STATE_TAG_LANGUAGE_j
#undef STATE_TAG_LANGUAGE
#undef STATE_TAG_NONE
#undef STATE_G2_ISO8859_7
#undef STATE_G2_ISO8859_1
#undef STATE_G2_NONE
#undef STATE_KSC5601
#undef STATE_GB2312
#undef STATE_JISX0212
#undef STATE_JISX0208
#undef STATE_JISX0201KATAKANA
#undef STATE_JISX0201ROMAN
#undef STATE_ASCII
