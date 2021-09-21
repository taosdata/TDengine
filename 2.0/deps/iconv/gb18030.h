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
 * GB18030
 */

/*
 * GB18030, as specified in the GB18030 standard, is an extension of GBK.
 *
 * In what follows, page numbers refer to the GB18030 standard (second
 * printing).
 *
 *
 * It consists of the following parts:
 *
 * One-byte range:
 *   ASCII        p. 2         0x{00..7F}
 *
 * Two-byte range:
 *   GBK part 1   p. 10..12    0x{A1..A9}{A1..FE}
 *   GBK part 2   p. 13..36    0x{B0..F7}{A1..FE}
 *   GBK part 3   p. 37..52    0x{81..A0}{40..7E,80..FE}
 *   GBK part 4   p. 53..81    0x{AA..FE}{40..7E,80..A0}
 *   GBK part 5   p. 82        0x{A8..A9}{40..7E,80..A0}
 *   UDA part 1   p. 83..84    0x{AA..AF}{A1..FE}              U+E000..U+E233
 *   UDA part 2   p. 85..87    0x{F8..FE}{A1..FE}              U+E234..U+E4C5
 *   UDA part 3   p. 88..90    0x{A1..A7}{40..7E,80..A0}       U+E4C6..U+E765
 *
 * Four-byte range:
 *   BMP rest     p. 94..283   0x{81..84}{30..39}{81..FE}{30..39}
 *                                                     rest of U+0080..U+FFFF
 *   Planes 1-16  p. 5         0x{90..FE}{30..39}{81..FE}{30..39}
 *                                                            U+10000..U+10FFFF
 *
 * To GBK part 1 were added:
 * 1. 0xA2E3, 0xA8BF.
 * 2. Characters mapped to the Unicode PUA
 *      0xA2AB..0xA2B0                      U+E766..U+E76B
 *      0xA2E4                              U+E76D
 *      0xA2EF..0xA2F0                      U+E76E..U+E76F
 *      0xA2FD..0xA2FE                      U+E770..U+E771
 *      0xA4F4..0xA4FE                      U+E772..U+E77C
 *      0xA5F7..0xA5FE                      U+E77D..U+E784
 *      0xA6B9..0xA6C0                      U+E785..U+E78C
 *      0xA6D9..0xA6DF [glyphs here!!]      U+E78D..U+E793
 *      0xA6EC..0xA6ED [glyphs here!!]      U+E794..U+E795
 *      0xA6F3         [glyphs here!!]      U+E796
 *      0xA6F6..0xA6FE                      U+E797..U+E79F
 *      0xA7C2..0xA7D0                      U+E7A0..U+E7AE
 *      0xA7F2..0xA7FE                      U+E7AF..U+E7BB
 *      0xA8BC         [glyphs here!!]      U+E7C7
 *      0xA8C1..0xA8C4                      U+E7C9..U+E7CC
 *      0xA8EA..0xA8FE                      U+E7CD..U+E7E1
 *      0xA9A1..0xA9A3                      U+E7FE..U+E800
 *      0xA9F0..0xA9FE                      U+E801..U+E80F
 *
 * To GBK part 2 were added:
 * 3. Characters mapped to the Unicode PUA
 *      0xD7FA..0xD7FE                      U+E810..0xE814
 *
 * To GBK part 3 nothing was added.
 *
 * To GBK part 4 were added:
 * 4. 0xFE{50,54..58,5A..60,62..65,68..6B,6E..75,77..7D,80..8F,92..9F}.
 * 5. Characters mapped to the Unicode PUA
 *      0xFE51..0xFE53 [glyphs here!!]      U+E816..U+E818
 *      0xFE59         [glyphs here!!]      U+E81E
 *      0xFE61         [glyphs here!!]      U+E826
 *      0xFE66..0xFE67 [glyphs here!!]      U+E82B..U+E82C
 *      0xFE6C..0xFE6D [glyphs here!!]      U+E831..U+E832
 *      0xFE76         [glyphs here!!]      U+E83B
 *      0xFE7E         [glyphs here!!]      U+E843
 *      0xFE90..0xFE91 [glyphs here!!]      U+E854..U+E855
 *      0xFEA0         [glyphs here!!]      U+E864
 *
 * To GBK part 5 were added:
 * 6. 0xA98A..0xA995.
 * 7. Characters mapped to the Unicode PUA
 *      0xA896..0xA8A0                      U+E7BC..U+E7C6
 *      0xA958                              U+E7E2
 *      0xA95B                              U+E7E3
 *      0xA95D..0xA95F                      U+E7E4..U+E7E6
 *      0xA997..0xA9A0                      U+E7F4..U+E7FD
 *
 * UDA part 1 contains the user-defined characters, mapped to the Unicode PUA
 * U+E000..U+E233 in ascending order.
 *
 * UDA part 2 contains the user-defined characters, mapped to the Unicode PUA
 * U+E234..U+E4C5 in ascending order.
 *
 * UDA part 3 contains the user-defined characters, mapped to the Unicode PUA
 * U+E4C6..U+E765 in ascending order.
 *
 * The four-byte range 0x{81..84}{30..39}{81..FE}{30..39}
 * contains the rest of the Unicode BMP in ascending order.
 *    Start: 0x81308130 = 0x0080
 *    End:   0x8431A439 = 0xFFFF
 *
 * The four-byte range 0x{90..E3}{30..39}{81..FE}{30..39}
 * contains the remaining 16 Unicode planes in Unicode order.
 *    Start: 0x90308130 = 0x010000
 *    End:   0xE3329A35 = 0x10FFFF
 *
 *
 * Unassigned Unicode characters are mapped. For example,
 *   U+173F = 0x8134BF35   (p. 120)
 *   U+2EFF = 0x81398B31   (p. 148)
 *   U+FFFE = 0x8431A438   (p. 283)
 *
 *
 * The Unicode PUA (U+E000..U+F8FF) is mapped as follows:
 *   p. 83..84    0x{AA..AF}{A1..FE}                  U+E000..U+E233
 *   p. 85..87    0x{F8..FE}{A1..FE}                  U+E234..U+E4C5
 *   p. 88..90    0x{A1..A7}{40..7E,80..A0}           U+E4C6..U+E765
 *   p. 10        0xA2AB..0xA2B0                      U+E766..U+E76B
 *   p. 255       0x8336C739                          U+E76C
 *   p. 10        0xA2E4                              U+E76D
 *   p. 10        0xA2EF..0xA2F0                      U+E76E..U+E76F
 *   p. 10        0xA2FD..0xA2FE                      U+E770..U+E771
 *   p. 11        0xA4F4..0xA4FE                      U+E772..U+E77C
 *   p. 11        0xA5F7..0xA5FE                      U+E77D..U+E784
 *   p. 11        0xA6B9..0xA6C0                      U+E785..U+E78C
 *   p. 11        0xA6D9..0xA6DF [glyphs here!!]      U+E78D..U+E793
 *   p. 11        0xA6EC..0xA6ED [glyphs here!!]      U+E794..U+E795
 *   p. 11        0xA6F3 [glyphs here!!]              U+E796
 *   p. 11        0xA6F6..0xA6FE                      U+E797..U+E79F
 *   p. 12        0xA7C2..0xA7D0                      U+E7A0..U+E7AE
 *   p. 12        0xA7F2..0xA7FE                      U+E7AF..U+E7BB
 *   p. 82        0xA896..0xA8A0                      U+E7BC..U+E7C6
 *   p. 12        0xA8BC [glyphs here!!]              U+E7C7
 *   p. 255       0x8336C830                          U+E7C8
 *   p. 12        0xA8C1..0xA8C4                      U+E7C9..U+E7CC
 *   p. 12        0xA8EA..0xA8FE                      U+E7CD..U+E7E1
 *   p. 82        0xA958                              U+E7E2
 *   p. 82        0xA95B                              U+E7E3
 *   p. 82        0xA95D..0xA95F                      U+E7E4..U+E7E6
 *   p. 255       0x8336C831..0x8336C933              U+E7E7..U+E7F3
 *   p. 82        0xA997..0xA9A0                      U+E7F4..U+E7FD
 *   p. 12        0xA9A1..0xA9A3                      U+E7FE..U+E800
 *   p. 12        0xA9F0..0xA9FE                      U+E801..U+E80F
 *   p. 26        0xD7FA..0xD7FE                      U+E810..0xE814
 *   p. 255       0x8336C934                          U+E815
 *   p. 81        0xFE51..0xFE53 [glyphs here!!]      U+E816..U+E818
 *   p. 255       0x8336C935..0x8336C939              U+E819..U+E81D
 *   p. 81        0xFE59 [glyphs here!!]              U+E81E
 *   p. 255       0x8336CA30..0x8336CA36              U+E81F..U+E825
 *   p. 81        0xFE61 [glyphs here!!]              U+E826
 *   p. 255       0x8336CA37..0x8336CB30              U+E827..U+E82A
 *   p. 81        0xFE66..0xFE67 [glyphs here!!]      U+E82B..U+E82C
 *   p. 255       0x8336CB31..0x8336CB34              U+E82D..U+E830
 *   p. 81        0xFE6C..0xFE6D [glyphs here!!]      U+E831..U+E832
 *   p. 255       0x8336CB35..0x8336CC32              U+E833..U+E83A
 *   p. 81        0xFE76 [glyphs here!!]              U+E83B
 *   p. 255       0x8336CC33..0x8336CC39              U+E83C..U+E842
 *   p. 81        0xFE7E [glyphs here!!]              U+E843
 *   p. 255       0x8336CD30..0x8336CE35              U+E844..U+E853
 *   p. 81        0xFE90..0xFE91 [glyphs here!!]      U+E854..U+E855
 *   p. 255       0x8336CE36..0x8336CF39              U+E856..U+E863
 *   p. 81        0xFEA0 [glyphs here!!]              U+E864
 *   p. 255..276  0x8336D030..0x84308130              U+E865..U+F8FF
 *
 *
 * The Unicode surrogate area (U+D800..U+DFFF) is not mapped. (p. 255)
 *
 */

#include "gb18030ext.h"
#include "gb18030uni.h"

static int
gb18030_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  int ret;

  /* Code set 0 (ASCII) */
  if (*s < 0x80)
    return ascii_mbtowc(conv,pwc,s,n);

  /* Code set 1 (GBK extended) */
  ret = gbk_mbtowc(conv,pwc,s,n);
  if (ret != RET_ILSEQ)
    return ret;

  ret = gb18030ext_mbtowc(conv,pwc,s,n);
  if (ret != RET_ILSEQ)
    return ret;

  /* Code set 2 (remainder of Unicode U+0000..U+FFFF), including
     User-defined characters, two-byte part of range U+E766..U+E864 */
  ret = gb18030uni_mbtowc(conv,pwc,s,n);
  if (ret != RET_ILSEQ)
    return ret;
  /* User-defined characters range U+E000..U+E765 */
  {
    unsigned char c1 = s[0];
    if ((c1 >= 0xaa && c1 <= 0xaf) || (c1 >= 0xf8 && c1 <= 0xfe)) {
      if (n >= 2) {
        unsigned char c2 = s[1];
        if (c2 >= 0xa1 && c2 <= 0xfe) {
          *pwc = 0xe000 + 94 * (c1 >= 0xf8 ? c1 - 0xf2 : c1 - 0xaa) + (c2 - 0xa1);
          return 2;
        }
      } else
        return RET_TOOFEW(0);
    } else if (c1 >= 0xa1 && c1 <= 0xa7) {
      if (n >= 2) {
        unsigned char c2 = s[1];
        if (c2 >= 0x40 && c2 <= 0xa1 && c2 != 0x7f) {
          *pwc = 0xe4c6 + 96 * (c1 - 0xa1) + c2 - (c2 >= 0x80 ? 0x41 : 0x40);
          return 2;
        }
      } else
        return RET_TOOFEW(0);
    }
  }

  /* Code set 3 (Unicode U+10000..U+10FFFF) */
  {
    unsigned char c1 = s[0];
    if (c1 >= 0x90 && c1 <= 0xe3) {
      if (n >= 2) {
        unsigned char c2 = s[1];
        if (c2 >= 0x30 && c2 <= 0x39) {
          if (n >= 3) {
            unsigned char c3 = s[2];
            if (c3 >= 0x81 && c3 <= 0xfe) {
              if (n >= 4) {
                unsigned char c4 = s[3];
                if (c4 >= 0x30 && c4 <= 0x39) {
                  unsigned int i = (((c1 - 0x90) * 10 + (c2 - 0x30)) * 126 + (c3 - 0x81)) * 10 + (c4 - 0x30);
                  if (i >= 0 && i < 0x100000) {
                    *pwc = (ucs4_t) (0x10000 + i);
                    return 4;
                  }
                }
                return RET_ILSEQ;
              }
              return RET_TOOFEW(0);
            }
            return RET_ILSEQ;
          }
          return RET_TOOFEW(0);
        }
        return RET_ILSEQ;
      }
      return RET_TOOFEW(0);
    }
    return RET_ILSEQ;
  }
}

static const unsigned short gb18030_pua2charset[32*3] = {
/* Unicode range   GB18030 range */
  0xe766, 0xe76b,  0xa2ab, /*.. 0xa2b0, */
  0xe76d, 0xe76d,  0xa2e4,
  0xe76e, 0xe76f,  0xa2ef, /*.. 0xa2f0, */
  0xe770, 0xe771,  0xa2fd, /*.. 0xa2fe, */
  0xe772, 0xe77c,  0xa4f4, /*.. 0xa4fe, */
  0xe77d, 0xe784,  0xa5f7, /*.. 0xa5fe, */
  0xe785, 0xe78c,  0xa6b9, /*.. 0xa6c0, */
  0xe78d, 0xe793,  0xa6d9, /*.. 0xa6df, */
  0xe794, 0xe795,  0xa6ec, /*.. 0xa6ed, */
  0xe796, 0xe796,  0xa6f3,
  0xe797, 0xe79f,  0xa6f6, /*.. 0xa6fe, */
  0xe7a0, 0xe7ae,  0xa7c2, /*.. 0xa7d0, */
  0xe7af, 0xe7bb,  0xa7f2, /*.. 0xa7fe, */
  0xe7bc, 0xe7c6,  0xa896, /*.. 0xa8a0, */
  0xe7c7, 0xe7c7,  0xa8bc,
  0xe7c9, 0xe7cc,  0xa8c1, /*.. 0xa8c4, */
  0xe7cd, 0xe7e1,  0xa8ea, /*.. 0xa8fe, */
  0xe7e2, 0xe7e2,  0xa958,
  0xe7e3, 0xe7e3,  0xa95b,
  0xe7e4, 0xe7e6,  0xa95d, /*.. 0xa95f, */
  0xe7f4, 0xe800,  0xa997, /*.. 0xa9a3, */
  0xe801, 0xe80f,  0xa9f0, /*.. 0xa9fe, */
  0xe810, 0xe814,  0xd7fa, /*.. 0xd7fe, */
  0xe816, 0xe818,  0xfe51, /*.. 0xfe53, */
  0xe81e, 0xe81e,  0xfe59,
  0xe826, 0xe826,  0xfe61,
  0xe82b, 0xe82c,  0xfe66, /*.. 0xfe67, */
  0xe831, 0xe832,  0xfe6c, /*.. 0xfe6d, */
  0xe83b, 0xe83b,  0xfe76,
  0xe843, 0xe843,  0xfe7e,
  0xe854, 0xe855,  0xfe90, /*.. 0xfe91, */
  0xe864, 0xe864,  0xfea0,
};

static int
gb18030_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  int ret;

  /* Code set 0 (ASCII) */
  ret = ascii_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  /* Code set 1 (GBK extended) */
  ret = gbk_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  ret = gb18030ext_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  /* Code set 2 (remainder of Unicode U+0000..U+FFFF) */
  if (wc >= 0xe000 && wc <= 0xe864) {
    if (n >= 2) {
      if (wc < 0xe766) {
        /* User-defined characters range U+E000..U+E765 */
        if (wc < 0xe4c6) {
          unsigned int i = wc - 0xe000;
          r[1] = (i % 94) + 0xa1; i = i / 94;
          r[0] = (i < 6 ? i + 0xaa : i + 0xf2);
          return 2;
        } else {
          unsigned int i = wc - 0xe4c6;
          r[0] = (i / 96) + 0xa1; i = i % 96;
          r[1] = i + (i >= 0x3f ? 0x41 : 0x40);
          return 2;
        }
      } else {
        /* User-defined characters, two-byte part of range U+E766..U+E864 */
        unsigned int k1 = 0;
        unsigned int k2 = 32;
        /* Invariant: We know that if wc occurs in Unicode interval in
           gb18030_pua2charset, it does so at a k with  k1 <= k < k2. */
        while (k1 < k2) {
          unsigned int k = (k1 + k2) / 2;
          if (wc < gb18030_pua2charset[k*3+0])
            k2 = k;
          else if (wc > gb18030_pua2charset[k*3+1])
            k1 = k + 1;
          else {
            unsigned short c =
              gb18030_pua2charset[k*3+2] + (wc - gb18030_pua2charset[k*3+0]);
            r[0] = (c >> 8);
            r[1] = (c & 0xff);
            return 2;
          }
        }
      }
    } else
      return RET_TOOSMALL;
  }
  ret = gb18030uni_wctomb(conv,r,wc,n);
  if (ret != RET_ILUNI)
    return ret;

  /* Code set 3 (Unicode U+10000..U+10FFFF) */
  if (n >= 4) {
    if (wc >= 0x10000 && wc < 0x110000) {
      unsigned int i = wc - 0x10000;
      r[3] = (i % 10) + 0x30; i = i / 10;
      r[2] = (i % 126) + 0x81; i = i / 126;
      r[1] = (i % 10) + 0x30; i = i / 10;
      r[0] = i + 0x90;
      return 4;
    }
    return RET_ILUNI;
  }
  return RET_TOOSMALL;
}
