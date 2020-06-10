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
 * UTF-7
 */

/* Specification: RFC 2152 (and old RFC 1641, RFC 1642) */
/* The original Base64 encoding is defined in RFC 2045. */

/* Set of direct characters:
 *   A-Z a-z 0-9 ' ( ) , - . / : ? space tab lf cr
 */
static const unsigned char direct_tab[128/8] = {
  0x00, 0x26, 0x00, 0x00, 0x81, 0xf3, 0xff, 0x87,
  0xfe, 0xff, 0xff, 0x07, 0xfe, 0xff, 0xff, 0x07,
};
#define isdirect(ch) ((ch) < 128 && ((direct_tab[(ch)>>3] >> (ch & 7)) & 1))

/* Set of direct and optional direct characters:
 *   A-Z a-z 0-9 ' ( ) , - . / : ? space tab lf cr
 *   ! " # $ % & * ; < = > @ [ ] ^ _ ` { | }
 */
static const unsigned char xdirect_tab[128/8] = {
  0x00, 0x26, 0x00, 0x00, 0xff, 0xf7, 0xff, 0xff,
  0xff, 0xff, 0xff, 0xef, 0xff, 0xff, 0xff, 0x3f,
};
#define isxdirect(ch) ((ch) < 128 && ((xdirect_tab[(ch)>>3] >> (ch & 7)) & 1))

/* Set of base64 characters, extended:
 *   A-Z a-z 0-9 + / -
 */
static const unsigned char xbase64_tab[128/8] = {
  0x00, 0x00, 0x00, 0x00, 0x00, 0xa8, 0xff, 0x03,
  0xfe, 0xff, 0xff, 0x07, 0xfe, 0xff, 0xff, 0x07,
};
#define isxbase64(ch) ((ch) < 128 && ((xbase64_tab[(ch)>>3] >> (ch & 7)) & 1))

/*
 * The state is structured as follows:
 * bit 1..0: shift
 * bit 7..2: data
 * Precise meaning:
 *   shift      data
 *     0         0           not inside base64 encoding
 *     1         0           inside base64, no pending bits
 *     2      XXXX00         inside base64, 4 bits remain from 2nd byte
 *     3      XX0000         inside base64, 2 bits remain from 3rd byte
 */

static int
utf7_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  state_t state = conv->istate;
  int count = 0; /* number of input bytes already read */
  if (state & 3)
    goto active;
  else
    goto inactive;

inactive:
  {
    /* Here (state & 3) == 0 */
    if (n < count+1)
      goto none;
    {
      unsigned char c = *s;
      if (isxdirect(c)) {
        *pwc = (ucs4_t) c;
        conv->istate = state;
        return count+1;
      }
      if (c == '+') {
        if (n < count+2)
          goto none;
        if (s[1] == '-') {
          *pwc = (ucs4_t) '+';
          conv->istate = state;
          return count+2;
        }
        s++; count++;
        state = 1;
        goto active;
      }
      goto ilseq;
    }
  }

active:
  {
    /* base64 encoding active */
    unsigned int wc = 0;
    state_t base64state = state;
    unsigned int kmax = 2; /* number of payload bytes to read */
    unsigned int k = 0; /* number of payload bytes already read */
    unsigned int base64count = 0; /* number of base64 bytes already read */
    for (;;) {
      unsigned char c = *s;
      unsigned int i;
      if (c >= 'A' && c <= 'Z')
        i = c-'A';
      else if (c >= 'a' && c <= 'z')
        i = c-'a'+26;
      else if (c >= '0' && c <= '9')
        i = c-'0'+52;
      else if (c == '+')
        i = 62;
      else if (c == '/')
        i = 63;
      else {
        /* c terminates base64 encoding */
        if (base64state & -4)
          goto ilseq; /* data must be 0, otherwise illegal */
        if (base64count)
          goto ilseq; /* partial UTF-16 characters are invalid */
        if (c == '-') {
          s++; count++;
        }
        state = 0;
        goto inactive;
      }
      s++; base64count++;
      /* read 6 bits: 0 <= i < 64 */
      switch (base64state & 3) {
        case 1: /* inside base64, no pending bits */
          base64state = (i << 2) | 0; break;
        case 0: /* inside base64, 6 bits remain from 1st byte */
          wc = (wc << 8) | (base64state & -4) | (i >> 4); k++;
          base64state = ((i & 15) << 4) | 2; break;
        case 2: /* inside base64, 4 bits remain from 2nd byte */
          wc = (wc << 8) | (base64state & -4) | (i >> 2); k++;
          base64state = ((i & 3) << 6) | 3; break;
        case 3: /* inside base64, 2 bits remain from 3rd byte */
          wc = (wc << 8) | (base64state & -4) | i; k++;
          base64state = 1; break;
      }
      if (k == kmax) {
        /* UTF-16: When we see a High Surrogate, we must also decode
           the following Low Surrogate. */
        if (kmax == 2 && (wc >= 0xd800 && wc < 0xdc00))
          kmax = 4;
        else
          break;
      }
      if (n < count+base64count+1)
        goto none;
    }
    /* Here k = kmax > 0, hence base64count > 0. */
    if ((base64state & 3) == 0) abort();
    if (kmax == 4) {
      ucs4_t wc1 = wc >> 16;
      ucs4_t wc2 = wc & 0xffff;
      if (!(wc1 >= 0xd800 && wc1 < 0xdc00)) abort();
      if (!(wc2 >= 0xdc00 && wc2 < 0xe000)) goto ilseq;
      *pwc = 0x10000 + ((wc1 - 0xd800) << 10) + (wc2 - 0xdc00);
    } else {
      *pwc = wc;
    }
    conv->istate = base64state;
    return count+base64count;
  }

none:
  conv->istate = state;
  return RET_TOOFEW(count);

ilseq:
  conv->istate = state;
  return RET_SHIFT_ILSEQ(count);
}

/*
 * The state is structured as follows:
 * bit 1..0: shift
 * bit 7..2: data
 * Precise meaning:
 *   shift      data
 *     0         0           not inside base64 encoding
 *     1         0           inside base64, no pending bits
 *     2       XX00          inside base64, 2 bits known for 2nd byte
 *     3       XXXX          inside base64, 4 bits known for 3rd byte
 */

/* Define this to 1 if you want the so-called "optional direct" characters
      ! " # $ % & * ; < = > @ [ ] ^ _ ` { | }
   to be encoded. Define to 0 if you want them to be passed straight through,
   like the so-called "direct" characters.
   We set this to 1 because it's safer.
 */
#define UTF7_ENCODE_OPTIONAL_CHARS 1

static int
utf7_wctomb (conv_t conv, unsigned char *r, ucs4_t iwc, int n)
{
  state_t state = conv->ostate;
  unsigned int wc = iwc;
  int count = 0;
  if (state & 3)
    goto active;

/*inactive:*/
  {
    if (UTF7_ENCODE_OPTIONAL_CHARS ? isdirect(wc) : isxdirect(wc)) {
      r[0] = (unsigned char) wc;
      /*conv->ostate = state;*/
      return 1;
    } else {
      *r++ = '+';
      if (wc == '+') {
        if (n < 2)
          return RET_TOOSMALL;
        *r = '-';
        /*conv->ostate = state;*/
        return 2;
      }
      count = 1;
      state = 1;
      goto active;
    }
  }

active:
  {
    /* base64 encoding active */
    if (UTF7_ENCODE_OPTIONAL_CHARS ? isdirect(wc) : isxdirect(wc)) {
      /* deactivate base64 encoding */
      count += ((state & 3) >= 2 ? 1 : 0) + (isxbase64(wc) ? 1 : 0) + 1;
      if (n < count)
        return RET_TOOSMALL;
      if ((state & 3) >= 2) {
        unsigned int i = state & -4;
        unsigned char c;
        if (i < 26)
          c = i+'A';
        else if (i < 52)
          c = i-26+'a';
        else if (i < 62)
          c = i-52+'0';
        else if (i == 62)
          c = '+';
        else if (i == 63)
          c = '/';
        else
          abort();
        *r++ = c;
      }
      if (isxbase64(wc))
        *r++ = '-';
      state = 0;
      *r++ = (unsigned char) wc;
      conv->ostate = state;
      return count;
    } else {
      unsigned int k; /* number of payload bytes to write */
      if (wc < 0x10000) {
        k = 2;
        count += ((state & 3) >= 2 ? 3 : 2);
      } else if (wc < 0x110000) {
        unsigned int wc1 = 0xd800 + ((wc - 0x10000) >> 10);
        unsigned int wc2 = 0xdc00 + ((wc - 0x10000) & 0x3ff);
        wc = (wc1 << 16) | wc2;
        k = 4;
        count += ((state & 3) >= 3 ? 6 : 5);
      } else
        return RET_ILUNI;
      if (n < count)
        return RET_TOOSMALL;
      for (;;) {
        unsigned int i;
        unsigned char c;
        switch (state & 3) {
          case 0: /* inside base64, 6 bits known for 4th byte */
            c = (state & -4) >> 2; state = 1; break;
          case 1: /* inside base64, no pending bits */
            i = (wc >> (8 * --k)) & 0xff;
            c = i >> 2; state = ((i & 3) << 4) | 2; break;
          case 2: /* inside base64, 2 bits known for 2nd byte */
            i = (wc >> (8 * --k)) & 0xff;
            c = (state & -4) | (i >> 4); state = ((i & 15) << 2) | 3; break;
          case 3: /* inside base64, 4 bits known for 3rd byte */
            i = (wc >> (8 * --k)) & 0xff;
            c = (state & -4) | (i >> 6); state = ((i & 63) << 2) | 0; break;
          default: abort(); /* stupid gcc */
        }
        if (c < 26)
          c = c+'A';
        else if (c < 52)
          c = c-26+'a';
        else if (c < 62)
          c = c-52+'0';
        else if (c == 62)
          c = '+';
        else if (c == 63)
          c = '/';
        else
          abort();
        *r++ = c;
        if ((state & 3) && (k == 0))
          break;
      }
      conv->ostate = state;
      return count;
    }
  }
}

static int
utf7_reset (conv_t conv, unsigned char *r, int n)
{
  state_t state = conv->ostate;
  if (state & 3) {
    /* deactivate base64 encoding */
    unsigned int count = ((state & 3) >= 2 ? 1 : 0) + 1;
    if (n < count)
      return RET_TOOSMALL;
    if ((state & 3) >= 2) {
      unsigned int i = state & -4;
      unsigned char c;
      if (i < 26)
        c = i+'A';
      else if (i < 52)
        c = i-26+'a';
      else if (i < 62)
        c = i-52+'0';
      else if (i == 62)
        c = '+';
      else if (i == 63)
        c = '/';
      else
        abort();
      *r++ = c;
    }
    *r++ = '-';
    /* conv->ostate = 0; will be done by the caller */
    return count;
  } else
    return 0;
}
