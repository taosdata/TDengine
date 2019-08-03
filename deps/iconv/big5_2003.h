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
 * BIG5-2003
 */

/*
 * BIG5-2003 is a slightly extended and slightly modified version of BIG5.
 * It is actually nearer to Microsoft CP950 than to BIG5. The differences
 * between EASTASIA/OTHER/BIG5.TXT found on ftp.unicode.org and BIG5-2003.TXT
 * are as follows:
 *
 * 1. Some characters in the symbols area (0xA140..0xA2CE) are defined
 *    differently:
 *
 *     code   BIG5.TXT                       BIG5-2003.TXT
 *    0xA145  0x2022 # BULLET                0x2027 # HYPHENATION POINT
 *    0xA14E  0xFF64 # HALFWIDTH IDEOGRAPHIC COMMA
 *                                           0xFE51 # SMALL IDEOGRAPHIC COMMA
 *    0xA156  0x2013 # EN DASH               0x2015 # HORIZONTAL BAR
 *    0xA15A    ---                          0x2574 # BOX DRAWINGS LIGHT LEFT
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
 *    0xA2A4  0x2550 # BOX DRAWINGS DOUBLE HORIZONTAL
 *                                           0x2501 # BOX DRAWINGS HEAVY HORIZONTAL
 *    0xA2A5  0x255E # BOX DRAWINGS VERTICAL SINGLE AND RIGHT DOUBLE
 *                                           0x251D # BOX DRAWINGS VERTICAL LIGHT AND RIGHT HEAVY
 *    0xA2A6  0x256A # BOX DRAWINGS VERTICAL SINGLE AND HORIZONTAL DOUBLE
 *                                           0x253F # BOX DRAWINGS VERTICAL LIGHT AND HORIZONTAL HEAVY
 *    0xA2A7  0x2561 # BOX DRAWINGS VERTICAL SINGLE AND LEFT DOUBLE
 *                                           0x2525 # BOX DRAWINGS VERTICAL LIGHT AND LEFT HEAVY
 *    0xA2CC    ---                          0x3038 # HANGZHOU NUMERAL TEN
 *    0xA2CD  0x5344                         0x3039 # HANGZHOU NUMERAL TWENTY
 *    0xA2CE    ---                          0x303A # HANGZHOU NUMERAL THIRTY
 *
 * 2. A control symbols area is added:
 *
 *         code
 *    0xA3C0..0xA3E0  U+2400..U+2421
 *
 * 3. The Euro sign is added:
 *
 *     code
 *    0xA3E1  0x20AC # EURO SIGN
 *
 * 4. Some characters in the main area are defined differently:
 *
 *     code   BIG5.TXT                       BIG5-2003.TXT
 *    0xC255  0x5F5D                         0x5F5E
 *
 * 5. The area 0xC6A1..0xC7FE is organized differently:
 *
 *         code
 *    0xC6A1..0xC6BE  numerals (was in BIG5.TXT at 0xC7E9..0xC7FC)
 *    0xC6BF..0xC6D7  radicals
 *    0xC6D8..0xC6E6  rarely used symbols
 *    0xC6E7..0xC77A  hiragana (U+3041..U+3093, was in BIG5.TXT at 0xC6A5..0xC6F7)
 *    0xC77B..0xC7F2  katakana (U+30A1..U+30F6, was in BIG5.TXT at 0xC6F8..0xC7B0)
 *
 * 6. Some characters are added at 0xF9D6..0xF9DC.
 *
 * 7. Box drawing characters are added at 0xF9DD..0xF9FE.
 *
 *    Note: 4 of these characters are mapped in a non-inversible way, because
 *    Unicode does not yet include the corresponding characters:
 *
 *     code                                           Unicode approximation
 *    0xF9FA  BOX DRAWINGS DOUBLE ARC DOWN AND RIGHT  0x2554
 *    0xF9FB  BOX DRAWINGS DOUBLE ARC DOWN AND LEFT   0x2557
 *    0xF9FC  BOX DRAWINGS DOUBLE ARC UP AND RIGHT    0x255A
 *    0xF9FD  BOX DRAWINGS DOUBLE ARC UP AND LEFT     0x255D
 *
 * 8. Private area mappings are added:
 *
 *              code                 Unicode
 *    0x{81..8D}{40..7E,A1..FE}  U+EEB8..U+F6B0
 *    0x{8E..A0}{40..7E,A1..FE}  U+E311..U+EEB7
 *    0x{FA..FE}{40..7E,A1..FE}  U+E000..U+E310
 *
 *    These mappings are not contained in the BSMI Big5-2003 standard. However,
 *    they were contained in a draft of it.
 */

static const unsigned short big5_2003_2uni_pagea1[314] = {
  /* 0xa1 */
  0x3000, 0xff0c, 0x3001, 0x3002, 0xff0e, 0x2027, 0xff1b, 0xff1a,
  0xff1f, 0xff01, 0xfe30, 0x2026, 0x2025, 0xfe50, 0xfe51, 0xfe52,
  0x00b7, 0xfe54, 0xfe55, 0xfe56, 0xfe57, 0xff5c, 0x2015, 0xfe31,
  0x2014, 0xfe33, 0x2574, 0xfe34, 0xfe4f, 0xff08, 0xff09, 0xfe35,
  0xfe36, 0xff5b, 0xff5d, 0xfe37, 0xfe38, 0x3014, 0x3015, 0xfe39,
  0xfe3a, 0x3010, 0x3011, 0xfe3b, 0xfe3c, 0x300a, 0x300b, 0xfe3d,
  0xfe3e, 0x3008, 0x3009, 0xfe3f, 0xfe40, 0x300c, 0x300d, 0xfe41,
  0xfe42, 0x300e, 0x300f, 0xfe43, 0xfe44, 0xfe59, 0xfe5a, 0xfe5b,
  0xfe5c, 0xfe5d, 0xfe5e, 0x2018, 0x2019, 0x201c, 0x201d, 0x301d,
  0x301e, 0x2035, 0x2032, 0xff03, 0xff06, 0xff0a, 0x203b, 0x00a7,
  0x3003, 0x25cb, 0x25cf, 0x25b3, 0x25b2, 0x25ce, 0x2606, 0x2605,
  0x25c7, 0x25c6, 0x25a1, 0x25a0, 0x25bd, 0x25bc, 0x32a3, 0x2105,
  0x203e, 0xffe3, 0xff3f, 0x02cd, 0xfe49, 0xfe4a, 0xfe4d, 0xfe4e,
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
  0x2570, 0x256f, 0x2501, 0x251d, 0x253f, 0x2525, 0x25e2, 0x25e3,
  0x25e5, 0x25e4, 0x2571, 0x2572, 0x2573, 0xff10, 0xff11, 0xff12,
  0xff13, 0xff14, 0xff15, 0xff16, 0xff17, 0xff18, 0xff19, 0x2160,
  0x2161, 0x2162, 0x2163, 0x2164, 0x2165, 0x2166, 0x2167, 0x2168,
  0x2169, 0x3021, 0x3022, 0x3023, 0x3024, 0x3025, 0x3026, 0x3027,
  0x3028, 0x3029, 0x3038, 0x3039, 0x303a, 0xff21, 0xff22, 0xff23,
  0xff24, 0xff25, 0xff26, 0xff27, 0xff28, 0xff29, 0xff2a, 0xff2b,
  0xff2c, 0xff2d, 0xff2e, 0xff2f, 0xff30, 0xff31, 0xff32, 0xff33,
  0xff34, 0xff35, 0xff36, 0xff37, 0xff38, 0xff39, 0xff3a, 0xff41,
  0xff42, 0xff43, 0xff44, 0xff45, 0xff46, 0xff47, 0xff48, 0xff49,
  0xff4a, 0xff4b, 0xff4c, 0xff4d, 0xff4e, 0xff4f, 0xff50, 0xff51,
  0xff52, 0xff53, 0xff54, 0xff55, 0xff56,
};

static const unsigned short big5_2003_2uni_pagec6[70] = {
  /* 0xc6a1 */
  0x2460, 0x2461, 0x2462, 0x2463, 0x2464, 0x2465, 0x2466, 0x2467,
  0x2468, 0x2469, 0x2474, 0x2475, 0x2476, 0x2477, 0x2478, 0x2479,
  0x247a, 0x247b, 0x247c, 0x247d, 0x2170, 0x2171, 0x2172, 0x2173,
  0x2174, 0x2175, 0x2176, 0x2177, 0x2178, 0x2179, 0x2f02, 0x2f03,
  0x2f05, 0x2f07, 0x2f0c, 0x2f0d, 0x2f0e, 0x2f13, 0x2f16, 0x2f19,
  0x2f1b, 0x2f22, 0x2f27, 0x2f2e, 0x2f33, 0x2f34, 0x2f35, 0x2f39,
  0x2f3a, 0x2f41, 0x2f46, 0x2f67, 0x2f68, 0x2fa1, 0x2faa, 0x00a8,
  0xff3e, 0x30fd, 0x30fe, 0x309d, 0x309e, 0xfffd, 0xfffd, 0x3005,
  0x3006, 0x3007, 0x30fc, 0xff3b, 0xff3d, 0x273d,
};

static const unsigned short big5_2003_2uni_pagef9[41] = {
  /* 0xf9d6 */
  0x7881, 0x92b9, 0x88cf, 0x58bb, 0x6052, 0x7ca7, 0x5afa,
  /* 0xf9dd */
  0x2554, 0x2566, 0x2557, 0x2560, 0x256c, 0x2563, 0x255a, 0x2569,
  0x255d, 0x2552, 0x2564, 0x2555, 0x255e, 0x256a, 0x2561, 0x2558,
  0x2567, 0x255b, 0x2553, 0x2565, 0x2556, 0x255f, 0x256b, 0x2562,
  0x2559, 0x2568, 0x255c, 0x2551, 0x2550,
  0x2554, 0x2557, 0x255a, 0x255d, /* not invertible */
  0x2593,
};

static int
big5_2003_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
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
            unsigned short wc = big5_2003_2uni_pagea1[i];
            if (wc != 0xfffd) {
              *pwc = (ucs4_t) wc;
              return 2;
            }
          }
          if (!((c == 0xc6 && c2 >= 0xa1) || c == 0xc7)) {
            if (!(c == 0xc2 && c2 == 0x55)) {
              int ret = big5_mbtowc(conv,pwc,s,2);
              if (ret != RET_ILSEQ)
                return ret;
              if (c == 0xa3) {
                if (c2 >= 0xc0 && c2 <= 0xe1) {
                  *pwc = (c2 == 0xe1 ? 0x20ac : c2 == 0xe0 ? 0x2421 : 0x2340 + c2);
                  return 2;
                }
              } else if (c == 0xf9) {
                if (c2 >= 0xd6) {
                  *pwc = big5_2003_2uni_pagef9[c2-0xd6];
                  return 2;
                }
              } else if (c >= 0xfa) {
                *pwc = 0xe000 + 157 * (c - 0xfa) + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
                return 2;
              }
            } else {
              /* c == 0xc2 && c2 == 0x55. */
              *pwc = 0x5f5e;
              return 2;
            }
          } else {
            /* (c == 0xc6 && c2 >= 0xa1) || c == 0xc7. */
            unsigned int i = 157 * (c - 0xc6) + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
            if (i < 133) {
              /* 63 <= i < 133. */
              unsigned short wc = big5_2003_2uni_pagec6[i-63];
              if (wc != 0xfffd) {
                *pwc = (ucs4_t) wc;
                return 2;
              }
            } else if (i < 216) {
              /* 133 <= i < 216. Hiragana. */
              *pwc = 0x3041 - 133 + i;
              return 2;
            } else if (i < 302) {
              /* 216 <= i < 302. Katakana. */
              *pwc = 0x30a1 - 216 + i;
              return 2;
            }
          }
        } else {
          /* 0x81 <= c < 0xa1. */
          *pwc = (c >= 0x8e ? 0xdb18 : 0xeeb8) + 157 * (c - 0x81)
                 + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
          return 2;
        }
      }
    }
  }
  return RET_ILSEQ;
}

static const unsigned char big5_2003_2charset_page25[29] = {
  /* 0x2550 */
  0xf9, 0xf8, 0xe6, 0xef, 0xdd, 0xe8, 0xf1, 0xdf,
  0xec, 0xf5, 0xe3, 0xee, 0xf7, 0xe5, 0xe9, 0xf2,
  0xe0, 0xeb, 0xf4, 0xe2, 0xe7, 0xf0, 0xde, 0xed,
  0xf6, 0xe4, 0xea, 0xf3, 0xe1,
};

static int
big5_2003_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
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
      if (wc == 0x00a8) { buf[0] = 0xc6; buf[1] = 0xd8; ret = 2; break; }
      if (wc == 0x00a2 || wc == 0x00a3 || wc == 0x00a5)
        return RET_ILUNI;
      break;
    case 0x02:
      if (wc == 0x02cd) { buf[0] = 0xa1; buf[1] = 0xc5; ret = 2; break; }
      break;
    case 0x04:
      return RET_ILUNI;
    case 0x20:
      if (wc == 0x2015) { buf[0] = 0xa1; buf[1] = 0x56; ret = 2; break; }
      if (wc == 0x2027) { buf[0] = 0xa1; buf[1] = 0x45; ret = 2; break; }
      if (wc == 0x20ac) { buf[0] = 0xa3; buf[1] = 0xe1; ret = 2; break; }
      if (wc == 0x2013 || wc == 0x2022)
        return RET_ILUNI;
      break;
    case 0x21:
      if (wc >= 0x2170 && wc <= 0x2179) {
        buf[0] = 0xc6; buf[1] = wc - 0x20bb; ret = 2;
        break;
      }
      break;
    case 0x22:
      if (wc == 0x2215) { buf[0] = 0xa2; buf[1] = 0x41; ret = 2; break; }
      if (wc == 0x2295) { buf[0] = 0xa1; buf[1] = 0xf2; ret = 2; break; }
      if (wc == 0x2299) { buf[0] = 0xa1; buf[1] = 0xf3; ret = 2; break; }
      if (wc == 0x223c)
        return RET_ILUNI;
      break;
    case 0x24:
      if (wc <= 0x241f) { buf[0] = 0xa3; buf[1] = wc - 0x2340; ret = 2; break; }
      if (wc == 0x2421) { buf[0] = 0xa3; buf[1] = 0xe0; ret = 2; break; }
      if (wc >= 0x2460 && wc <= 0x2469) {
        buf[0] = 0xc6; buf[1] = wc - 0x23bf; ret = 2;
        break;
      }
      if (wc >= 0x2474 && wc <= 0x247d) {
        buf[0] = 0xc6; buf[1] = wc - 0x23c9; ret = 2;
        break;
      }
      break;
    case 0x25:
      if (wc == 0x2501) { buf[0] = 0xa2; buf[1] = 0xa4; ret = 2; break; }
      if (wc == 0x251d) { buf[0] = 0xa2; buf[1] = 0xa5; ret = 2; break; }
      if (wc == 0x2525) { buf[0] = 0xa2; buf[1] = 0xa7; ret = 2; break; }
      if (wc == 0x253f) { buf[0] = 0xa2; buf[1] = 0xa6; ret = 2; break; }
      if (wc >= 0x2550 && wc <= 0x256c) {
        buf[0] = 0xf9; buf[1] = big5_2003_2charset_page25[wc-0x2550]; ret = 2;
        break;
      }
      if (wc == 0x2574) { buf[0] = 0xa1; buf[1] = 0x5a; ret = 2; break; }
      if (wc == 0x2593) { buf[0] = 0xf9; buf[1] = 0xfe; ret = 2; break; }
      break;
    case 0x26:
      if (wc == 0x2609 || wc == 0x2641)
        return RET_ILUNI;
      break;
    case 0x27:
      if (wc == 0x273d) { buf[0] = 0xc6; buf[1] = 0xe6; ret = 2; break; }
      break;
    case 0x2f:
      if (wc == 0x2f02) { buf[0] = 0xc6; buf[1] = 0xbf; ret = 2; break; }
      if (wc == 0x2f03) { buf[0] = 0xc6; buf[1] = 0xc0; ret = 2; break; }
      if (wc == 0x2f05) { buf[0] = 0xc6; buf[1] = 0xc1; ret = 2; break; }
      if (wc == 0x2f07) { buf[0] = 0xc6; buf[1] = 0xc2; ret = 2; break; }
      if (wc == 0x2f0c) { buf[0] = 0xc6; buf[1] = 0xc3; ret = 2; break; }
      if (wc == 0x2f0d) { buf[0] = 0xc6; buf[1] = 0xc4; ret = 2; break; }
      if (wc == 0x2f0e) { buf[0] = 0xc6; buf[1] = 0xc5; ret = 2; break; }
      if (wc == 0x2f13) { buf[0] = 0xc6; buf[1] = 0xc6; ret = 2; break; }
      if (wc == 0x2f16) { buf[0] = 0xc6; buf[1] = 0xc7; ret = 2; break; }
      if (wc == 0x2f19) { buf[0] = 0xc6; buf[1] = 0xc8; ret = 2; break; }
      if (wc == 0x2f1b) { buf[0] = 0xc6; buf[1] = 0xc9; ret = 2; break; }
      if (wc == 0x2f22) { buf[0] = 0xc6; buf[1] = 0xca; ret = 2; break; }
      if (wc == 0x2f27) { buf[0] = 0xc6; buf[1] = 0xcb; ret = 2; break; }
      if (wc == 0x2f2e) { buf[0] = 0xc6; buf[1] = 0xcc; ret = 2; break; }
      if (wc == 0x2f33) { buf[0] = 0xc6; buf[1] = 0xcd; ret = 2; break; }
      if (wc == 0x2f34) { buf[0] = 0xc6; buf[1] = 0xce; ret = 2; break; }
      if (wc == 0x2f35) { buf[0] = 0xc6; buf[1] = 0xcf; ret = 2; break; }
      if (wc == 0x2f39) { buf[0] = 0xc6; buf[1] = 0xd0; ret = 2; break; }
      if (wc == 0x2f3a) { buf[0] = 0xc6; buf[1] = 0xd1; ret = 2; break; }
      if (wc == 0x2f41) { buf[0] = 0xc6; buf[1] = 0xd2; ret = 2; break; }
      if (wc == 0x2f46) { buf[0] = 0xc6; buf[1] = 0xd3; ret = 2; break; }
      if (wc == 0x2f67) { buf[0] = 0xc6; buf[1] = 0xd4; ret = 2; break; }
      if (wc == 0x2f68) { buf[0] = 0xc6; buf[1] = 0xd5; ret = 2; break; }
      if (wc == 0x2fa1) { buf[0] = 0xc6; buf[1] = 0xd6; ret = 2; break; }
      if (wc == 0x2faa) { buf[0] = 0xc6; buf[1] = 0xd7; ret = 2; break; }
      break;
    case 0x30:
      if (wc >= 0x3005 && wc <= 0x3007) {
        buf[0] = 0xc6; buf[1] = wc - 0x2f25; ret = 2;
        break;
      }
      if (wc >= 0x3038 && wc <= 0x303a) {
        buf[0] = 0xa2; buf[1] = wc - 0x2f6c; ret = 2;
        break;
      }
      if (wc >= 0x3041 && wc <= 0x3093) {
        if (wc < 0x3059) {
          buf[0] = 0xc6; buf[1] = wc - 0x2f5a;
        } else {
          buf[0] = 0xc7; buf[1] = wc - 0x3019;
        }
        ret = 2;
        break;
      }
      if (wc == 0x309d) { buf[0] = 0xc6; buf[1] = 0xdc; ret = 2; break; }
      if (wc == 0x309e) { buf[0] = 0xc6; buf[1] = 0xdd; ret = 2; break; }
      if (wc >= 0x30a1 && wc <= 0x30f6) {
        buf[0] = 0xc7; buf[1] = wc - (wc < 0x30a5 ? 0x3026 : 0x3004); ret = 2;
        break;
      }
      if (wc == 0x30fc) { buf[0] = 0xc6; buf[1] = 0xe3; ret = 2; break; }
      if (wc == 0x30fd) { buf[0] = 0xc6; buf[1] = 0xda; ret = 2; break; }
      if (wc == 0x30fe) { buf[0] = 0xc6; buf[1] = 0xdb; ret = 2; break; }
      break;
    case 0x53:
      if (wc == 0x5344)
        return RET_ILUNI;
      break;
    case 0x58:
      if (wc == 0x58bb) { buf[0] = 0xf9; buf[1] = 0xd9; ret = 2; break; }
      break;
    case 0x5a:
      if (wc == 0x5afa) { buf[0] = 0xf9; buf[1] = 0xdc; ret = 2; break; }
      break;
    case 0x5f:
      if (wc == 0x5f5e) { buf[0] = 0xc2; buf[1] = 0x55; ret = 2; break; }
      if (wc == 0x5f5d)
        return RET_ILUNI;
      break;
    case 0x60:
      if (wc == 0x6052) { buf[0] = 0xf9; buf[1] = 0xda; ret = 2; break; }
      break;
    case 0x78:
      if (wc == 0x7881) { buf[0] = 0xf9; buf[1] = 0xd6; ret = 2; break; }
      break;
    case 0x7c:
      if (wc == 0x7ca7) { buf[0] = 0xf9; buf[1] = 0xdb; ret = 2; break; }
      break;
    case 0x88:
      if (wc == 0x88cf) { buf[0] = 0xf9; buf[1] = 0xd8; ret = 2; break; }
      break;
    case 0x92:
      if (wc == 0x92b9) { buf[0] = 0xf9; buf[1] = 0xd7; ret = 2; break; }
      break;
    case 0xe0: case 0xe1: case 0xe2: case 0xe3: case 0xe4: case 0xe5:
    case 0xe6: case 0xe7: case 0xe8: case 0xe9: case 0xea: case 0xeb:
    case 0xec: case 0xed: case 0xee: case 0xef: case 0xf0: case 0xf1:
    case 0xf2: case 0xf3: case 0xf4: case 0xf5: case 0xf6:
      {
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
      if (wc == 0xff3b) { buf[0] = 0xc6; buf[1] = 0xe4; ret = 2; break; }
      if (wc == 0xff3c) { buf[0] = 0xa2; buf[1] = 0x40; ret = 2; break; }
      if (wc == 0xff3d) { buf[0] = 0xc6; buf[1] = 0xe5; ret = 2; break; }
      if (wc == 0xff3e) { buf[0] = 0xc6; buf[1] = 0xd9; ret = 2; break; }
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
    if (n < 2)
      return RET_TOOSMALL;
    r[0] = buf[0];
    r[1] = buf[1];
    return 2;
  }

  return RET_ILUNI;
}
