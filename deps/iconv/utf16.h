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
 * UTF-16
 */

/* Specification: RFC 2781 */

/* Here we accept FFFE/FEFF marks as endianness indicators everywhere
   in the stream, not just at the beginning. (This is contrary to what
   RFC 2781 section 3.2 specifies, but it allows concatenation of byte
   sequences to work flawlessly, while disagreeing with the RFC behaviour
   only for strings containing U+FEFF characters, which is quite rare.)
   The default is big-endian. */
/* The state is 0 if big-endian, 1 if little-endian. */
static int
utf16_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  state_t state = conv->istate;
  int count = 0;
  for (; n >= 2;) {
    ucs4_t wc = (state ? s[0] + (s[1] << 8) : (s[0] << 8) + s[1]);
    if (wc == 0xfeff) {
    } else if (wc == 0xfffe) {
      state ^= 1;
    } else if (wc >= 0xd800 && wc < 0xdc00) {
      if (n >= 4) {
        ucs4_t wc2 = (state ? s[2] + (s[3] << 8) : (s[2] << 8) + s[3]);
        if (!(wc2 >= 0xdc00 && wc2 < 0xe000))
          goto ilseq;
        *pwc = 0x10000 + ((wc - 0xd800) << 10) + (wc2 - 0xdc00);
        conv->istate = state;
        return count+4;
      } else
        break;
    } else if (wc >= 0xdc00 && wc < 0xe000) {
      goto ilseq;
    } else {
      *pwc = wc;
      conv->istate = state;
      return count+2;
    }
    s += 2; n -= 2; count += 2;
  }
  conv->istate = state;
  return RET_TOOFEW(count);

ilseq:
  conv->istate = state;
  return RET_SHIFT_ILSEQ(count);
}

/* We output UTF-16 in big-endian order, with byte-order mark.
   See RFC 2781 section 3.3 for a rationale: Some document formats
   mandate a BOM; the file concatenation issue is not so severe as
   long as the above utf16_mbtowc function is used. */
/* The state is 0 at the beginning, 1 after the BOM has been written. */
static int
utf16_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (wc != 0xfffe && !(wc >= 0xd800 && wc < 0xe000)) {
    int count = 0;
    if (!conv->ostate) {
      if (n >= 2) {
        r[0] = 0xFE;
        r[1] = 0xFF;
        r += 2; n -= 2; count += 2;
      } else
        return RET_TOOSMALL;
    }
    if (wc < 0x10000) {
      if (n >= 2) {
        r[0] = (unsigned char) (wc >> 8);
        r[1] = (unsigned char) wc;
        conv->ostate = 1;
        return count+2;
      } else
        return RET_TOOSMALL;
    }
    else if (wc < 0x110000) {
      if (n >= 4) {
        ucs4_t wc1 = 0xd800 + ((wc - 0x10000) >> 10);
        ucs4_t wc2 = 0xdc00 + ((wc - 0x10000) & 0x3ff);
        r[0] = (unsigned char) (wc1 >> 8);
        r[1] = (unsigned char) wc1;
        r[2] = (unsigned char) (wc2 >> 8);
        r[3] = (unsigned char) wc2;
        conv->ostate = 1;
        return count+4;
      } else
        return RET_TOOSMALL;
    }
  }
  return RET_ILUNI;
}
