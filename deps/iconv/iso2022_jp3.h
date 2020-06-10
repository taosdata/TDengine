/*
 * Copyright (C) 1999-2004, 2008 Free Software Foundation, Inc.
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
 * ISO-2022-JP-3
 */

#include "jisx0213.h"

#define ESC 0x1b

/*
 * The state is composed of one of the following values
 */
#define STATE_ASCII             0  /* Esc ( B */
#define STATE_JISX0201ROMAN     1  /* Esc ( J */
#define STATE_JISX0201KATAKANA  2  /* Esc ( I */
#define STATE_JISX0208          3  /* Esc $ @ or Esc $ B */
#define STATE_JISX02131         4  /* Esc $ ( O or Esc $ ( Q*/
#define STATE_JISX02132         5  /* Esc $ ( P */

/*
 * In the ISO-2022-JP-3 to UCS-4 direction, the state also holds the last
 * character to be output, shifted by 3 bits.
 */

static int
iso2022_jp3_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  ucs4_t last_wc = conv->istate >> 3;
  if (last_wc) {
    /* Output the buffered character. */
    conv->istate &= 7;
    *pwc = last_wc;
    return 0; /* Don't advance the input pointer. */
  } else {
    state_t state = conv->istate;
    int count = 0;
    unsigned char c;
    for (;;) {
      c = *s;
      if (c == ESC) {
        if (n < count+3)
          goto none;
        if (s[1] == '(') {
          if (s[2] == 'B') {
            state = STATE_ASCII;
            s += 3; count += 3;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[2] == 'J') {
            state = STATE_JISX0201ROMAN;
            s += 3; count += 3;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[2] == 'I') {
            state = STATE_JISX0201KATAKANA;
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
            state = STATE_JISX0208;
            s += 3; count += 3;
            if (n < count+1)
              goto none;
            continue;
          }
          if (s[2] == '(') {
            if (n < count+4)
              goto none;
            if (s[3] == 'O' || s[3] == 'Q') {
              state = STATE_JISX02131;
              s += 4; count += 4;
              if (n < count+1)
                goto none;
              continue;
            }
            if (s[3] == 'P') {
              state = STATE_JISX02132;
              s += 4; count += 4;
              if (n < count+1)
                goto none;
              continue;
            }
          }
          goto ilseq;
        }
        goto ilseq;
      }
      break;
    }
    switch (state) {
      case STATE_ASCII:
        if (c < 0x80) {
          int ret = ascii_mbtowc(conv,pwc,s,1);
          if (ret == RET_ILSEQ)
            goto ilseq;
          if (ret != 1) abort();
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
          conv->istate = state;
          return count+2;
        } else
          goto ilseq;
      case STATE_JISX02131:
      case STATE_JISX02132:
        if (n < count+2)
          goto none;
        if (s[0] < 0x80 && s[1] < 0x80) {
          ucs4_t wc = jisx0213_to_ucs4(((state-STATE_JISX02131+1)<<8)+s[0],s[1]);
          if (wc) {
            if (wc < 0x80) {
              /* It's a combining character. */
              ucs4_t wc1 = jisx0213_to_ucs_combining[wc - 1][0];
              ucs4_t wc2 = jisx0213_to_ucs_combining[wc - 1][1];
              /* We cannot output two Unicode characters at once. So,
                 output the first character and buffer the second one. */
              *pwc = wc1;
              conv->istate = (wc2 << 3) | state;
            } else {
              *pwc = wc;
              conv->istate = state;
            }
            return count+2;
          }
        }
        goto ilseq;
      default: abort();
    }
  none:
    conv->istate = state;
    return RET_TOOFEW(count);

  ilseq:
    conv->istate = state;
    return RET_SHIFT_ILSEQ(count);
  }
}

static int
iso2022_jp3_flushwc (conv_t conv, ucs4_t *pwc)
{
  ucs4_t last_wc = conv->istate >> 3;
  if (last_wc) {
    /* Output the buffered character. */
    conv->istate &= 7;
    *pwc = last_wc;
    return 1;
  } else
    return 0;
}

/*
 * In the UCS-4 to ISO-2022-JP-3 direction, the state also holds the last two
 * bytes to be output, shifted by 3 bits, and the STATE_xxxxx value that was
 * effective before this buffered character, shifted by 19 bits.
 */

/* Composition tables for each of the relevant combining characters.  */
static const struct { unsigned short base; unsigned short composed; } iso2022_jp3_comp_table_data[] = {
#define iso2022_jp3_comp_table02e5_idx 0
#define iso2022_jp3_comp_table02e5_len 1
  { 0x2b64, 0x2b65 }, /* 0x12B65 = 0x12B64 U+02E5 */
#define iso2022_jp3_comp_table02e9_idx (iso2022_jp3_comp_table02e5_idx+iso2022_jp3_comp_table02e5_len)
#define iso2022_jp3_comp_table02e9_len 1
  { 0x2b60, 0x2b66 }, /* 0x12B66 = 0x12B60 U+02E9 */
#define iso2022_jp3_comp_table0300_idx (iso2022_jp3_comp_table02e9_idx+iso2022_jp3_comp_table02e9_len)
#define iso2022_jp3_comp_table0300_len 5
  { 0x295c, 0x2b44 }, /* 0x12B44 = 0x1295C U+0300 */
  { 0x2b38, 0x2b48 }, /* 0x12B48 = 0x12B38 U+0300 */
  { 0x2b37, 0x2b4a }, /* 0x12B4A = 0x12B37 U+0300 */
  { 0x2b30, 0x2b4c }, /* 0x12B4C = 0x12B30 U+0300 */
  { 0x2b43, 0x2b4e }, /* 0x12B4E = 0x12B43 U+0300 */
#define iso2022_jp3_comp_table0301_idx (iso2022_jp3_comp_table0300_idx+iso2022_jp3_comp_table0300_len)
#define iso2022_jp3_comp_table0301_len 4
  { 0x2b38, 0x2b49 }, /* 0x12B49 = 0x12B38 U+0301 */
  { 0x2b37, 0x2b4b }, /* 0x12B4B = 0x12B37 U+0301 */
  { 0x2b30, 0x2b4d }, /* 0x12B4D = 0x12B30 U+0301 */
  { 0x2b43, 0x2b4f }, /* 0x12B4F = 0x12B43 U+0301 */
#define iso2022_jp3_comp_table309a_idx (iso2022_jp3_comp_table0301_idx+iso2022_jp3_comp_table0301_len)
#define iso2022_jp3_comp_table309a_len 14
  { 0x242b, 0x2477 }, /* 0x12477 = 0x1242B U+309A */
  { 0x242d, 0x2478 }, /* 0x12478 = 0x1242D U+309A */
  { 0x242f, 0x2479 }, /* 0x12479 = 0x1242F U+309A */
  { 0x2431, 0x247a }, /* 0x1247A = 0x12431 U+309A */
  { 0x2433, 0x247b }, /* 0x1247B = 0x12433 U+309A */
  { 0x252b, 0x2577 }, /* 0x12577 = 0x1252B U+309A */
  { 0x252d, 0x2578 }, /* 0x12578 = 0x1252D U+309A */
  { 0x252f, 0x2579 }, /* 0x12579 = 0x1252F U+309A */
  { 0x2531, 0x257a }, /* 0x1257A = 0x12531 U+309A */
  { 0x2533, 0x257b }, /* 0x1257B = 0x12533 U+309A */
  { 0x253b, 0x257c }, /* 0x1257C = 0x1253B U+309A */
  { 0x2544, 0x257d }, /* 0x1257D = 0x12544 U+309A */
  { 0x2548, 0x257e }, /* 0x1257E = 0x12548 U+309A */
  { 0x2675, 0x2678 }, /* 0x12678 = 0x12675 U+309A */
};

#define SPLIT_STATE \
  unsigned short lasttwo = state >> 3; state_t prevstate = state >> 19; state &= 7
#define COMBINE_STATE \
  state |= (prevstate << 19) | (lasttwo << 3)
#define COMBINE_STATE_NO_LASTTWO \
  /* assume lasttwo == 0, then prevstate is ignored */

static int
iso2022_jp3_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  int count = 0;
  unsigned char buf[2];
  unsigned short jch;
  int ret;
  state_t state = conv->ostate;
  SPLIT_STATE;

  if (lasttwo) {
    /* Attempt to combine the last character with this one. */
    unsigned int idx;
    unsigned int len;

    if (wc == 0x02e5)
      idx = iso2022_jp3_comp_table02e5_idx,
      len = iso2022_jp3_comp_table02e5_len;
    else if (wc == 0x02e9)
      idx = iso2022_jp3_comp_table02e9_idx,
      len = iso2022_jp3_comp_table02e9_len;
    else if (wc == 0x0300)
      idx = iso2022_jp3_comp_table0300_idx,
      len = iso2022_jp3_comp_table0300_len;
    else if (wc == 0x0301)
      idx = iso2022_jp3_comp_table0301_idx,
      len = iso2022_jp3_comp_table0301_len;
    else if (wc == 0x309a)
      idx = iso2022_jp3_comp_table309a_idx,
      len = iso2022_jp3_comp_table309a_len;
    else
      goto not_combining;

    do
      if (iso2022_jp3_comp_table_data[idx].base == lasttwo)
        break;
    while (++idx, --len > 0);

    if (len > 0) {
      /* Output the combined character. */
      /* We know the combined character is in JISX0213 plane 1, but
         the buffered character may have been in JISX0208 or in
         JISX0213 plane 1. */
      count = (state != STATE_JISX02131 ? 4 : 0) + 2;
      if (n < count)
        return RET_TOOSMALL;
      if (state != STATE_JISX02131) {
        r[0] = ESC;
        r[1] = '$';
        r[2] = '(';
        r[3] = 'Q';
        r += 4;
        state = STATE_JISX02131;
      }
      lasttwo = iso2022_jp3_comp_table_data[idx].composed;
      r[0] = (lasttwo >> 8) & 0xff;
      r[1] = lasttwo & 0xff;
      COMBINE_STATE_NO_LASTTWO;
      conv->ostate = state;
      return count;
    }

  not_combining:
    /* Output the buffered character. */
    /* We know it is in JISX0208 or in JISX0213 plane 1. */
    count = (prevstate != state ? 3 : 0) + 2;
    if (n < count)
      return RET_TOOSMALL;
    if (prevstate != state) {
      if (state != STATE_JISX0208) abort();
      r[0] = ESC;
      r[1] = '$';
      r[2] = 'B';
      r += 3;
    }
    r[0] = (lasttwo >> 8) & 0xff;
    r[1] = lasttwo & 0xff;
    r += 2;
  }

  /* Try ASCII. */
  ret = ascii_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] < 0x80) {
      count += (state == STATE_ASCII ? 1 : 4);
      if (n < count)
        return RET_TOOSMALL;
      if (state != STATE_ASCII) {
        r[0] = ESC;
        r[1] = '(';
        r[2] = 'B';
        r += 3;
        state = STATE_ASCII;
      }
      r[0] = buf[0];
      COMBINE_STATE_NO_LASTTWO;
      conv->ostate = state;
      return count;
    }
  }

  /* Try JIS X 0201-1976 Roman. */
  ret = jisx0201_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] < 0x80) {
      count += (state == STATE_JISX0201ROMAN ? 1 : 4);
      if (n < count)
        return RET_TOOSMALL;
      if (state != STATE_JISX0201ROMAN) {
        r[0] = ESC;
        r[1] = '(';
        r[2] = 'J';
        r += 3;
        state = STATE_JISX0201ROMAN;
      }
      r[0] = buf[0];
      COMBINE_STATE_NO_LASTTWO;
      conv->ostate = state;
      return count;
    }
  }

  jch = ucs4_to_jisx0213(wc);

  /* Try JIS X 0208-1990 in place of JIS X 0208-1978 and JIS X 0208-1983. */
  ret = jisx0208_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (buf[0] < 0x80 && buf[1] < 0x80) {
      if (jch & 0x0080) {
        /* A possible match in comp_table_data. Buffer it. */
        prevstate = state;
        lasttwo = jch & 0x7f7f;
        state = STATE_JISX0208;
        COMBINE_STATE;
        conv->ostate = state;
        return count;
      } else {
        count += (state == STATE_JISX0208 ? 2 : 5);
        if (n < count)
          return RET_TOOSMALL;
        if (state != STATE_JISX0208) {
          r[0] = ESC;
          r[1] = '$';
          r[2] = 'B';
          r += 3;
          state = STATE_JISX0208;
        }
        r[0] = buf[0];
        r[1] = buf[1];
        COMBINE_STATE_NO_LASTTWO;
        conv->ostate = state;
        return count;
      }
    }
  }

  /* Try JISX 0213 plane 1 and JISX 0213 plane 2. */
  if (jch != 0) {
    if (jch & 0x8000) {
      /* JISX 0213 plane 2. */
      if (state != STATE_JISX02132) {
        count += 4;
        if (n < count)
          return RET_TOOSMALL;
        r[0] = ESC;
        r[1] = '$';
        r[2] = '(';
        r[3] = 'P';
        r += 4;
        state = STATE_JISX02132;
      }
    } else {
      /* JISX 0213 plane 1. */
      if (state != STATE_JISX02131) {
        count += 4;
        if (n < count)
          return RET_TOOSMALL;
        r[0] = ESC;
        r[1] = '$';
        r[2] = '(';
        r[3] = 'Q';
        r += 4;
        state = STATE_JISX02131;
      }
    }
    if (jch & 0x0080) {
      /* A possible match in comp_table_data. We have to buffer it. */
      /* We know it's a JISX 0213 plane 1 character. */
      if (jch & 0x8000) abort();
      prevstate = state;
      lasttwo = jch & 0x7f7f;
      COMBINE_STATE;
      conv->ostate = state;
      return count;
    }
    count += 2;
    if (n < count)
      return RET_TOOSMALL;
    r[0] = (jch >> 8) & 0x7f;
    r[1] = jch & 0x7f;
    COMBINE_STATE_NO_LASTTWO;
    conv->ostate = state;
    return count;
  }

  /* Try JIS X 0201-1976 Katakana. This is not officially part of
     ISO-2022-JP-3. Therefore we try it after all other attempts. */
  ret = jisx0201_wctomb(conv,buf,wc,1);
  if (ret != RET_ILUNI) {
    if (ret != 1) abort();
    if (buf[0] >= 0x80) {
      count += (state == STATE_JISX0201KATAKANA ? 1 : 4);
      if (n < count)
        return RET_TOOSMALL;
      if (state != STATE_JISX0201KATAKANA) {
        r[0] = ESC;
        r[1] = '(';
        r[2] = 'I';
        r += 3;
        state = STATE_JISX0201KATAKANA;
      }
      r[0] = buf[0]-0x80;
      COMBINE_STATE_NO_LASTTWO;
      conv->ostate = state;
      return count;
    }
  }

  return RET_ILUNI;
}

static int
iso2022_jp3_reset (conv_t conv, unsigned char *r, int n)
{
  state_t state = conv->ostate;
  SPLIT_STATE;
  {
    int count =
      (lasttwo ? (prevstate != state ? 3 : 0) + 2 : 0)
      + (state != STATE_ASCII ? 3 : 0);
    if (n < count)
      return RET_TOOSMALL;
    if (lasttwo) {
      if (prevstate != state) {
        if (state != STATE_JISX0208) abort();
        r[0] = ESC;
        r[1] = '$';
        r[2] = 'B';
        r += 3;
      }
      r[0] = (lasttwo >> 8) & 0xff;
      r[1] = lasttwo & 0xff;
      r += 2;
    }
    if (state != STATE_ASCII) {
      r[0] = ESC;
      r[1] = '(';
      r[2] = 'B';
    }
    /* conv->ostate = 0; will be done by the caller */
    return count;
  }
}

#undef COMBINE_STATE_NO_LASTTWO
#undef COMBINE_STATE
#undef SPLIT_STATE
#undef STATE_JISX02132
#undef STATE_JISX02131
#undef STATE_JISX0208
#undef STATE_JISX0201KATAKANA
#undef STATE_JISX0201ROMAN
#undef STATE_ASCII
