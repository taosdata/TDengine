/*
 * Copyright (C) 1999-2001, 2005 Free Software Foundation, Inc.
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
 * CP950
 */

/*
 * Microsoft CP950 is a slightly extended and slightly modified version of
 * BIG5. The differences between the EASTASIA/OTHER/BIG5.TXT and
 * VENDORS/MICSFT/WINDOWS/CP950.TXT tables found on ftp.unicode.org are
 * as follows:
 *
 * 1. Some characters in the BIG5 range are defined differently:
 *
 *     code   BIG5.TXT                       CP950.TXT
 *    0xA145  0x2022 # BULLET                0x2027 # HYPHENATION POINT
 *    0xA14E  0xFF64 # HALFWIDTH IDEOGRAPHIC COMMA
 *                                           0xFE51 # SMALL IDEOGRAPHIC COMMA
 *    0xA15A    ---                          0x2574 # BOX DRAWINGS LIGHT LEFT
 *    0xA1C2  0x203E # OVERLINE              0x00AF # MACRON
 *    0xA1C3    ---                          0xFFE3 # FULLWIDTH MACRON
 *    0xA1C5    ---                          0x02CD # MODIFIER LETTER LOW MACRON
 *    0xA1E3  0x223C # TILDE OPERATOR        0xFF5E # FULLWIDTH TILDE
 *    0xA1F2  0x2641 # EARTH                 0x2295 # CIRCLED PLUS
 *    0xA1F3  0x2609 # SUN                   0x2299 # CIRCLED DOT OPERATOR
 *    0xA1FE    ---                          0xFF0F # FULLWIDTH SOLIDUS
 *    0xA240    ---                          0xFF3C # FULLWIDTH REVERSE SOLIDUS
 *    0xA241  0xFF0F # FULLWIDTH SOLIDUS     0x2215 # DIVISION SLASH
 *    0xA242  0xFF3C # FULLWIDTH REVERSE SOLIDUS
 *                                           0xFE68 # SMALL REVERSE SOLIDUS
 *    0xA244  0x00A5 # YEN SIGN              0xFFE5 # FULLWIDTH YEN SIGN
 *    0xA246  0x00A2 # CENT SIGN             0xFFE0 # FULLWIDTH CENT SIGN
 *    0xA247  0x00A3 # POUND SIGN            0xFFE1 # FULLWIDTH POUND SIGN
 *    0xA2CC    ---                          0x5341
 *    0xA2CE    ---                          0x5345
 *
 * 2. A small new row. See cp950ext.h.
 *
 * 3. CP950.TXT is lacking the range 0xC6A1..0xC7FC (Hiragana, Katakana,
 *    Cyrillic, circled digits, parenthesized digits).
 *
 *    We implement this omission, because said range is marked "uncertain"
 *    in the unicode.org BIG5 table.
 *
 * The table found on Microsoft's website furthermore adds:
 *
 * 4. A single character:
 *
 *     code   CP950.TXT
 *    0xA3E1  0x20AC # EURO SIGN
 *
 * Many variants of BIG5 or CP950 (in JDK, Solaris, OSF/1, Windows-2000, ICU,
 * as well as our BIG5-2003 converter) also add:
 *
 * 5. Private area mappings:
 *
 *              code                 Unicode
 *    0x{81..8D}{40..7E,A1..FE}  U+EEB8..U+F6B0
 *    0x{8E..A0}{40..7E,A1..FE}  U+E311..U+EEB7
 *    0x{FA..FE}{40..7E,A1..FE}  U+E000..U+E310
 *
 * We add them too because, although there are backward compatibility problems
 * when a character from a private area is moved to an official Unicode code
 * point, they are useful for some people in practice.
 */

static const unsigned short cp950_2uni_pagea1[314] = {
  /* 0xa1 */
  0x3000, 0xff0c, 0x3001, 0x3002, 0xff0e, 0x2027, 0xff1b, 0xff1a,
  0xff1f, 0xff01, 0xfe30, 0x2026, 0x2025, 0xfe50, 0xfe51, 0xfe52,
  0x00b7, 0xfe54, 0xfe55, 0xfe56, 0xfe57, 0xff5c, 0x2013, 0xfe31,
  0x2014, 0xfe33, 0x2574, 0xfe34, 0xfe4f, 0xff08, 0xff09, 0xfe35,
  0xfe36, 0xff5b, 0xff5d, 0xfe37, 0xfe38, 0x3014, 0x3015, 0xfe39,
  0xfe3a, 0x3010, 0x3011, 0xfe3b, 0xfe3c, 0x300a, 0x300b, 0xfe3d,
  0xfe3e, 0x3008, 0x3009, 0xfe3f, 0xfe40, 0x300c, 0x300d, 0xfe41,
  0xfe42, 0x300e, 0x300f, 0xfe43, 0xfe44, 0xfe59, 0xfe5a, 0xfe5b,
  0xfe5c, 0xfe5d, 0xfe5e, 0x2018, 0x2019, 0x201c, 0x201d, 0x301d,
  0x301e, 0x2035, 0x2032, 0xff03, 0xff06, 0xff0a, 0x203b, 0x00a7,
  0x3003, 0x25cb, 0x25cf, 0x25b3, 0x25b2, 0x25ce, 0x2606, 0x2605,
  0x25c7, 0x25c6, 0x25a1, 0x25a0, 0x25bd, 0x25bc, 0x32a3, 0x2105,
  0x00af, 0xffe3, 0xff3f, 0x02cd, 0xfe49, 0xfe4a, 0xfe4d, 0xfe4e,
  0xfe4b, 0xfe4c, 0xfe5f, 0xfe60, 0xfe61, 0xff0b, 0xff0d, 0x00d7,
  0x00f7, 0x00b1, 0x221a, 0xff1c, 0xff1e, 0xff1d, 0x2266, 0x2267,
  0x2260, 0x221e, 0x2252, 0x2261, 0xfe62, 0xfe63, 0xfe64, 0xfe65,
  0xfe66, 0xff5e, 0x2229, 0x222a, 0x22a5, 0x2220, 0x221f, 0x22bf,
  0x33d2, 0x33d1, 0x222b, 0x222e, 0x2235, 0x2234, 0x2640, 0x2642,
  0x2295, 0x2299, 0x2191, 0x2193, 0x2190, 0x2192, 0x2196, 0x2197,
  0x2199, 0x2198, 0x2225, 0x2223, 0xff0f,
  /* 0xa2 */
  0xff3c, 0x2215, 0xfe68, 0xff04, 0xffe5, 0x3012, 0xffe0, 0xffe1,
  0xff05, 0xff20, 0x2103, 0x2109, 0xfe69, 0xfe6a, 0xfe6b, 0x33d5,
  0x339c, 0x339d, 0x339e, 0x33ce, 0x33a1, 0x338e, 0x338f, 0x33c4,
  0x00b0, 0x5159, 0x515b, 0x515e, 0x515d, 0x5161, 0x5163, 0x55e7,
  0x74e9, 0x7cce, 0x2581, 0x2582, 0x2583, 0x2584, 0x2585, 0x2586,
  0x2587, 0x2588, 0x258f, 0x258e, 0x258d, 0x258c, 0x258b, 0x258a,
  0x2589, 0x253c, 0x2534, 0x252c, 0x2524, 0x251c, 0x2594, 0x2500,
  0x2502, 0x2595, 0x250c, 0x2510, 0x2514, 0x2518, 0x256d, 0x256e,
  0x2570, 0x256f, 0x2550, 0x255e, 0x256a, 0x2561, 0x25e2, 0x25e3,
  0x25e5, 0x25e4, 0x2571, 0x2572, 0x2573, 0xff10, 0xff11, 0xff12,
  0xff13, 0xff14, 0xff15, 0xff16, 0xff17, 0xff18, 0xff19, 0x2160,
  0x2161, 0x2162, 0x2163, 0x2164, 0x2165, 0x2166, 0x2167, 0x2168,
  0x2169, 0x3021, 0x3022, 0x3023, 0x3024, 0x3025, 0x3026, 0x3027,
  0x3028, 0x3029, 0x5341, 0x5344, 0x5345, 0xff21, 0xff22, 0xff23,
  0xff24, 0xff25, 0xff26, 0xff27, 0xff28, 0xff29, 0xff2a, 0xff2b,
  0xff2c, 0xff2d, 0xff2e, 0xff2f, 0xff30, 0xff31, 0xff32, 0xff33,
  0xff34, 0xff35, 0xff36, 0xff37, 0xff38, 0xff39, 0xff3a, 0xff41,
  0xff42, 0xff43, 0xff44, 0xff45, 0xff46, 0xff47, 0xff48, 0xff49,
  0xff4a, 0xff4b, 0xff4c, 0xff4d, 0xff4e, 0xff4f, 0xff50, 0xff51,
  0xff52, 0xff53, 0xff54, 0xff55, 0xff56,
};

#include "cp950ext.h"

static int
cp950_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c = *s;
  /* Code set 0 (ASCII) */
  if (c < 0x80)
    return ascii_mbtowc(conv,pwc,s,n);
  /* Code set 1 (BIG5 extended) */
  if (c >= 0x81 && c < 0xff) {
    if (n < 2)
      return RET_TOOFEW(0);
    {
      unsigned char c2 = s[1];
      if ((c2 >= 0x40 && c2 < 0x7f) || (c2 >= 0xa1 && c2 < 0xff)) {
        if (c >= 0xa1) {
          if (c < 0xa3) {
            unsigned int i = 157 * (c - 0xa1) + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
            unsigned short wc = cp950_2uni_pagea1[i];
            if (wc != 0xfffd) {
              *pwc = (ucs4_t) wc;
              return 2;
            }
          }
          if (!((c == 0xc6 && c2 >= 0xa1) || c == 0xc7)) {
            int ret = big5_mbtowc(conv,pwc,s,2);
            if (ret != RET_ILSEQ)
              return ret;
          }
          if (c == 0xa3 && c2 == 0xe1) {
            *pwc = 0x20ac;
            return 2;
          }
          if (c >= 0xfa) {
            /* User-defined characters */
            *pwc = 0xe000 + 157 * (c - 0xfa) + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
            return 2;
          }
        } else {
          /* 0x81 <= c < 0xa1. */
          /* User-defined characters */
          *pwc = (c >= 0x8e ? 0xdb18 : 0xeeb8) + 157 * (c - 0x81)
                 + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
          return 2;
        }
      }
    }
    if (c == 0xf9) {
      int ret = cp950ext_mbtowc(conv,pwc,s,2);
      if (ret != RET_ILSEQ)
        return ret;
    }
  }
  return RET_ILSEQ;
}

static int
cp950_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  unsigned char buf[2];
  int ret;

  /* Code set 0 (ASCII) */
  ret = ascii_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  /* Code set 1 (BIG5 extended) */
  switch (wc >> 8) {
    case 0x00:
      if (wc == 0x00af) { buf[0] = 0xa1; buf[1] = 0xc2; ret = 2; break; }
      if (wc == 0x00a2 || wc == 0x00a3 || wc == 0x00a4)
        return RET_ILUNI;
      break;
    case 0x02:
      if (wc == 0x02cd) { buf[0] = 0xa1; buf[1] = 0xc5; ret = 2; break; }
      break;
    case 0x20:
      if (wc == 0x2027) { buf[0] = 0xa1; buf[1] = 0x45; ret = 2; break; }
      if (wc == 0x20ac) { buf[0] = 0xa3; buf[1] = 0xe1; ret = 2; break; }
      if (wc == 0x2022 || wc == 0x203e)
        return RET_ILUNI;
      break;
    case 0x22:
      if (wc == 0x2215) { buf[0] = 0xa2; buf[1] = 0x41; ret = 2; break; }
      if (wc == 0x2295) { buf[0] = 0xa1; buf[1] = 0xf2; ret = 2; break; }
      if (wc == 0x2299) { buf[0] = 0xa1; buf[1] = 0xf3; ret = 2; break; }
      if (wc == 0x223c)
        return RET_ILUNI;
      break;
    case 0x25:
      if (wc == 0x2574) { buf[0] = 0xa1; buf[1] = 0x5a; ret = 2; break; }
      break;
    case 0x26:
      if (wc == 0x2609 || wc == 0x2641)
        return RET_ILUNI;
      break;
    case 0xe0: case 0xe1: case 0xe2: case 0xe3: case 0xe4: case 0xe5:
    case 0xe6: case 0xe7: case 0xe8: case 0xe9: case 0xea: case 0xeb:
    case 0xec: case 0xed: case 0xee: case 0xef: case 0xf0: case 0xf1:
    case 0xf2: case 0xf3: case 0xf4: case 0xf5: case 0xf6:
      {
        /* User-defined characters */
        unsigned int i = wc - 0xe000;
        if (i < 5809) {
          unsigned int c1 = i / 157;
          unsigned int c2 = i % 157;
          buf[0] = c1 + (c1 < 5 ? 0xfa : c1 < 24 ? 0x89 : 0x69);
          buf[1] = c2 + (c2 < 0x3f ? 0x40 : 0x62);
          ret = 2;
          break;
        }
      }
      break;
    case 0xfe:
      if (wc == 0xfe51) { buf[0] = 0xa1; buf[1] = 0x4e; ret = 2; break; }
      if (wc == 0xfe68) { buf[0] = 0xa2; buf[1] = 0x42; ret = 2; break; }
      break;
    case 0xff:
      if (wc == 0xff0f) { buf[0] = 0xa1; buf[1] = 0xfe; ret = 2; break; }
      if (wc == 0xff3c) { buf[0] = 0xa2; buf[1] = 0x40; ret = 2; break; }
      if (wc == 0xff5e) { buf[0] = 0xa1; buf[1] = 0xe3; ret = 2; break; }
      if (wc == 0xffe0) { buf[0] = 0xa2; buf[1] = 0x46; ret = 2; break; }
      if (wc == 0xffe1) { buf[0] = 0xa2; buf[1] = 0x47; ret = 2; break; }
      if (wc == 0xffe3) { buf[0] = 0xa1; buf[1] = 0xc3; ret = 2; break; }
      if (wc == 0xffe5) { buf[0] = 0xa2; buf[1] = 0x44; ret = 2; break; }
      if (wc == 0xff64)
        return RET_ILUNI;
      break;
  }
  if (ret == RET_ILUNI)
    ret = big5_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (!((buf[0] == 0xc6 && buf[1] >= 0xa1) || buf[0] == 0xc7)) {
      if (n < 2)
        return RET_TOOSMALL;
      r[0] = buf[0];
      r[1] = buf[1];
      return 2;
    }
  }
  ret = cp950ext_wctomb(conv,buf,wc,2);
  if (ret != RET_ILUNI) {
    if (ret != 2) abort();
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = buf[0];
    r[1] = buf[1];
    return 2;
  }

  return RET_ILUNI;
}
