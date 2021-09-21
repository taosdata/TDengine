/*
 * Copyright (C) 1999-2006 Free Software Foundation, Inc.
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
 * HKSCS:2004
 */

static const unsigned short hkscs2004_2uni_page87[58] = {
  /* 0x87 */
  0x0af0, 0x1032, 0x0d03, 0x0ca6, 0x0c78, 0x4167, 0x1177, 0x0cb3,
  0x44b1, 0x10e2, 0x44c5, 0x0595, 0x0e36, 0x0e44, 0x1047, 0x1040,
  0x39bf, 0x3417, 0x4252, 0x3f8b, 0x40d2, 0x1057, 0x4d51, 0x0e4f,
  0x0cda, 0x1085, 0x446c, 0x1107, 0x0fa4, 0x0da1, 0x3d23, 0x1e25,
  0x3c54, 0x2d63, 0x3606, 0x3761, 0x1a4d, 0x13fb, 0x28fd, 0x2195,
  0x141d, 0x47b9, 0x06f4, 0x2534, 0x43ef, 0x16db, 0x2e5e, 0x15a4,
  0x0125, 0x4bb0, 0x15d1, 0x16b7, 0x17fc, 0x1b6e, 0x2393, 0x4a45,
  0x1f61, 0x1f9d,
};
static const unsigned short hkscs2004_2uni_page8c[189] = {
  /* 0x8c */
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x2b6f, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd, 0x28fd,
  0x28fd, 0x1ae7, 0x28fd, 0x1c57, 0x20ca, 0x0688, 0x0bc3, 0x3256,
  0x3196, 0x0a9a, 0x0c36, 0x28fd, 0x17d5, 0x351a, 0x24f9, 0x1778,
  0x0612, 0x3351, 0x1878, 0x27b2, 0x1d57, 0x0c58, 0x38ec, 0x2f23,
  0x1077, 0x0478, 0x004a, 0x29a4, 0x3e41, 0x24cc, 0x12b4, 0x2a39,
  0x14bf, 0x226c, 0x2656, 0x49fa, 0x193b,
  /* 0x8d */
  0x2c9f, 0x28fd, 0x30c1, 0x466d, 0x0902, 0x0dbb, 0x4879, 0x0707,
  0x27b3, 0x4cb5, 0x08f8, 0x02d6, 0x0df7, 0x3e46, 0x097c, 0x45b2,
  0x42ff, 0x0c6d, 0x03d4, 0x3b9a, 0x0c61, 0x0c1b, 0x1189, 0x107b,
  0x1176, 0x0cea, 0x07c8, 0x3a0f, 0x0161, 0x0bde, 0x0bbd, 0x09ed,
};

static const ucs4_t hkscs2004_2uni_upages[78] = {
  0x03400, 0x03600, 0x03700, 0x03800, 0x03900, 0x03b00, 0x03d00, 0x03f00,
  0x04000, 0x04100, 0x04300, 0x04400, 0x04500, 0x04600, 0x04700, 0x04a00,
  0x04c00, 0x04d00, 0x04f00, 0x05600, 0x05900, 0x05a00, 0x05b00, 0x05c00,
  0x05d00, 0x05f00, 0x06600, 0x06700, 0x06e00, 0x07100, 0x07200, 0x07400,
  0x07900, 0x07d00, 0x08100, 0x08500, 0x08a00, 0x09700, 0x09800, 0x09f00,
  0x0ff00, 0x20100, 0x20200, 0x20a00, 0x20b00, 0x21a00, 0x21d00, 0x21e00,
  0x22100, 0x22700, 0x23200, 0x23500, 0x23600, 0x23b00, 0x23e00, 0x23f00,
  0x24000, 0x24200, 0x24b00, 0x25400, 0x25a00, 0x26b00, 0x26c00, 0x26e00,
  0x27000, 0x27200, 0x27300, 0x27b00, 0x27c00, 0x28600, 0x28900, 0x28b00,
  0x29000, 0x29800, 0x29900, 0x29e00, 0x2a100, 0x2a300,
};

static int
hkscs2004_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c1 = s[0];
  if ((c1 == 0x87) || (c1 >= 0x8c && c1 <= 0x8d)) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if ((c2 >= 0x40 && c2 < 0x7f) || (c2 >= 0xa1 && c2 < 0xff)) {
        unsigned int i = 157 * (c1 - 0x80) + (c2 - (c2 >= 0xa1 ? 0x62 : 0x40));
        ucs4_t wc = 0xfffd;
        unsigned short swc;
        if (i < 1884) {
          if (i < 1157)
            swc = hkscs2004_2uni_page87[i-1099],
            wc = hkscs2004_2uni_upages[swc>>8] | (swc & 0xff);
        } else {
          if (i < 2073)
            swc = hkscs2004_2uni_page8c[i-1884],
            wc = hkscs2004_2uni_upages[swc>>8] | (swc & 0xff);
        }
        if (wc != 0xfffd) {
          *pwc = wc;
          return 2;
        }
      }
      return RET_ILSEQ;
    }
    return RET_TOOFEW(0);
  }
  return RET_ILSEQ;
}

static const unsigned short hkscs2004_2charset[123] = {
  0x8cf4, 0x8770, 0x8d5c, 0x8d4b, 0x8d52, 0x8cf3, 0x874b, 0x8cea,
  0x8cdf, 0x876a, 0x8d47, 0x8d5a, 0x8d4a, 0x8d44, 0x8d4e, 0x8d5f,
  0x8ce3, 0x8740, 0x8d5e, 0x8ce0, 0x8d5d, 0x8d55, 0x8ce4, 0x8cef,
  0x8d54, 0x8d51, 0x8744, 0x8743, 0x8747, 0x8758, 0x8d59, 0x8742,
  0x875d, 0x8d45, 0x8d4c, 0x874c, 0x874d, 0x8757, 0x875c, 0x8741,
  0x874f, 0x874e, 0x8755, 0x8cf2, 0x8d57, 0x8759, 0x8749, 0x875b,
  0x8d58, 0x8746, 0x8d56, 0x8cf8, 0x8765, 0x8768, 0x8cfa, 0x876f,
  0x8772, 0x8773, 0x876d, 0x8ce9, 0x8ce6, 0x8774, 0x8cec, 0x8cfe,
  0x8764, 0x8cdb, 0x8775, 0x8cdd, 0x8cee, 0x875f, 0x8778, 0x8779,
  0x8cde, 0x8767, 0x8cfb, 0x8776, 0x8cf7, 0x8ce8, 0x876b, 0x8cfc,
  0x8ced, 0x8d48, 0x8cf5, 0x8cf9, 0x8c62, 0x8d40, 0x8761, 0x876e,
  0x8cf1, 0x8d42, 0x8ce2, 0x8ce1, 0x8ceb, 0x8751, 0x8ce7, 0x8762,
  0x8763, 0x8cf0, 0x8750, 0x8d5b, 0x8d53, 0x8760, 0x875e, 0x8cf6,
  0x8d4d, 0x8753, 0x8754, 0x8745, 0x8752, 0x8d50, 0x876c, 0x875a,
  0x8748, 0x874a, 0x8d4f, 0x8d43, 0x8769, 0x8d46, 0x8cfd, 0x8777,
  0x8771, 0x8d49, 0x8756,
};

static const Summary16 hkscs2004_uni2indx_page34[5] = {
  /* 0x3400 */
  {    0, 0x0000 }, {    0, 0x0000 }, {    0, 0x0000 }, {    0, 0x0000 },
  {    0, 0x0400 },
};
static const Summary16 hkscs2004_uni2indx_page36[56] = {
  /* 0x3600 */
  {    1, 0x0000 }, {    1, 0x0000 }, {    1, 0x0020 }, {    2, 0x0000 },
  {    2, 0x0000 }, {    2, 0x0000 }, {    2, 0x0002 }, {    3, 0x0000 },
  {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 },
  {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 },
  /* 0x3700 */
  {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 },
  {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 },
  {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 }, {    3, 0x0000 },
  {    3, 0x0000 }, {    3, 0x0040 }, {    4, 0x0000 }, {    4, 0x0000 },
  /* 0x3800 */
  {    4, 0x0000 }, {    4, 0x0000 }, {    4, 0x0000 }, {    4, 0x0000 },
  {    4, 0x0000 }, {    4, 0x0000 }, {    4, 0x0000 }, {    4, 0x0000 },
  {    4, 0x0000 }, {    4, 0x0000 }, {    4, 0x0000 }, {    4, 0x0000 },
  {    4, 0x0000 }, {    4, 0x0010 }, {    5, 0x0000 }, {    5, 0x0000 },
  /* 0x3900 */
  {    5, 0x0000 }, {    5, 0x0000 }, {    5, 0x0000 }, {    5, 0x0000 },
  {    5, 0x0000 }, {    5, 0x0000 }, {    5, 0x0000 }, {    5, 0x0100 },
};
static const Summary16 hkscs2004_uni2indx_page3b[10] = {
  /* 0x3b00 */
  {    6, 0x0000 }, {    6, 0x0000 }, {    6, 0x0000 }, {    6, 0x0000 },
  {    6, 0x0000 }, {    6, 0x0000 }, {    6, 0x0000 }, {    6, 0x0000 },
  {    6, 0x0000 }, {    6, 0x0020 },
};
static const Summary16 hkscs2004_uni2indx_page3d[16] = {
  /* 0x3d00 */
  {    7, 0x0000 }, {    7, 0x0004 }, {    8, 0x0000 }, {    8, 0x0000 },
  {    8, 0x0000 }, {    8, 0x0000 }, {    8, 0x0000 }, {    8, 0x0000 },
  {    8, 0x0100 }, {    9, 0x0000 }, {    9, 0x0000 }, {    9, 0x0000 },
  {    9, 0x0000 }, {    9, 0x0000 }, {    9, 0x0000 }, {    9, 0x0010 },
};
static const Summary16 hkscs2004_uni2indx_page3f[47] = {
  /* 0x3f00 */
  {   10, 0x0080 }, {   11, 0x0000 }, {   11, 0x0000 }, {   11, 0x0000 },
  {   11, 0x0000 }, {   11, 0x0000 }, {   11, 0x0000 }, {   11, 0x0000 },
  {   11, 0x0000 }, {   11, 0x0000 }, {   11, 0x0000 }, {   11, 0x0000 },
  {   11, 0x0100 }, {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 },
  /* 0x4000 */
  {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 },
  {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 },
  {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 },
  {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0000 }, {   12, 0x0100 },
  /* 0x4100 */
  {   13, 0x0004 }, {   14, 0x0000 }, {   14, 0x0000 }, {   14, 0x0000 },
  {   14, 0x0000 }, {   14, 0x0000 }, {   14, 0x0000 }, {   14, 0x1000 },
  {   15, 0x0000 }, {   15, 0x0000 }, {   15, 0x0000 }, {   15, 0x0000 },
  {   15, 0x0000 }, {   15, 0x0000 }, {   15, 0x2000 },
};
static const Summary16 hkscs2004_uni2indx_page43[69] = {
  /* 0x4300 */
  {   16, 0x0000 }, {   16, 0x0000 }, {   16, 0x0000 }, {   16, 0x0000 },
  {   16, 0x0000 }, {   16, 0x0000 }, {   16, 0x0000 }, {   16, 0x0000 },
  {   16, 0x0000 }, {   16, 0x0400 }, {   17, 0x0000 }, {   17, 0x0000 },
  {   17, 0x0000 }, {   17, 0x0000 }, {   17, 0x0000 }, {   17, 0x0001 },
  /* 0x4400 */
  {   18, 0x0000 }, {   18, 0x0000 }, {   18, 0x0000 }, {   18, 0x0000 },
  {   18, 0x0000 }, {   18, 0x0000 }, {   18, 0x0000 }, {   18, 0x0000 },
  {   18, 0x0000 }, {   18, 0x0000 }, {   18, 0x0000 }, {   18, 0x2000 },
  {   19, 0x0008 }, {   20, 0x4000 }, {   21, 0x0000 }, {   21, 0x0000 },
  /* 0x4500 */
  {   21, 0x0000 }, {   21, 0x0800 }, {   22, 0x0000 }, {   22, 0x0040 },
  {   23, 0x0000 }, {   23, 0x0100 }, {   24, 0x2002 }, {   26, 0x0100 },
  {   27, 0x0000 }, {   27, 0x0000 }, {   27, 0x0040 }, {   28, 0x0008 },
  {   29, 0x0000 }, {   29, 0x0400 }, {   30, 0x0400 }, {   31, 0x0000 },
  /* 0x4600 */
  {   31, 0x0008 }, {   32, 0x0000 }, {   32, 0x0000 }, {   32, 0x0000 },
  {   32, 0x0000 }, {   32, 0x0000 }, {   32, 0x0000 }, {   32, 0x0000 },
  {   32, 0x0000 }, {   32, 0x0000 }, {   32, 0x0002 }, {   33, 0x0800 },
  {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0000 }, {   34, 0x0080 },
  /* 0x4700 */
  {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0000 }, {   35, 0x0040 },
  {   36, 0x8010 },
};
static const Summary16 hkscs2004_uni2indx_page4a[11] = {
  /* 0x4a00 */
  {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 },
  {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0000 },
  {   38, 0x0000 }, {   38, 0x0000 }, {   38, 0x0010 },
};
static const Summary16 hkscs2004_uni2indx_page4c[25] = {
  /* 0x4c00 */
  {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0000 }, {   39, 0x0004 },
  {   40, 0x0081 }, {   42, 0x0080 }, {   43, 0x0000 }, {   43, 0x0880 },
  {   45, 0x0020 }, {   46, 0x0000 }, {   46, 0x0000 }, {   46, 0x0000 },
  {   46, 0x0000 }, {   46, 0x0000 }, {   46, 0x0004 }, {   47, 0x0000 },
  /* 0x4d00 */
  {   47, 0x0080 }, {   48, 0x0000 }, {   48, 0x0000 }, {   48, 0x0000 },
  {   48, 0x0000 }, {   48, 0x0000 }, {   48, 0x0000 }, {   48, 0x00c0 },
  {   50, 0x0200 },
};
static const Summary16 hkscs2004_uni2indx_page4f[12] = {
  /* 0x4f00 */
  {   51, 0x0000 }, {   51, 0x0000 }, {   51, 0x0000 }, {   51, 0x0000 },
  {   51, 0x0000 }, {   51, 0x0000 }, {   51, 0x0000 }, {   51, 0x0000 },
  {   51, 0x0000 }, {   51, 0x0000 }, {   51, 0x0000 }, {   51, 0x0010 },
};
static const Summary16 hkscs2004_uni2indx_page56[16] = {
  /* 0x5600 */
  {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 },
  {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 },
  {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 },
  {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0000 }, {   52, 0x0800 },
};
static const Summary16 hkscs2004_uni2indx_page59[72] = {
  /* 0x5900 */
  {   53, 0x0000 }, {   53, 0x2000 }, {   54, 0x0000 }, {   54, 0x0000 },
  {   54, 0x0000 }, {   54, 0x0000 }, {   54, 0x0000 }, {   54, 0x0000 },
  {   54, 0x0000 }, {   54, 0x0000 }, {   54, 0x0000 }, {   54, 0x8000 },
  {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0000 },
  /* 0x5a00 */
  {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0000 },
  {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0000 },
  {   55, 0x0000 }, {   55, 0x0000 }, {   55, 0x0010 }, {   56, 0x0000 },
  {   56, 0x0000 }, {   56, 0x0002 }, {   57, 0x0000 }, {   57, 0x0000 },
  /* 0x5b00 */
  {   57, 0x0000 }, {   57, 0x0000 }, {   57, 0x0000 }, {   57, 0x0000 },
  {   57, 0x0000 }, {   57, 0x0000 }, {   57, 0x0000 }, {   57, 0x0000 },
  {   57, 0x0000 }, {   57, 0x0000 }, {   57, 0x0000 }, {   57, 0x0080 },
  {   58, 0x0000 }, {   58, 0x0800 }, {   59, 0x0000 }, {   59, 0x0000 },
  /* 0x5c00 */
  {   59, 0x0000 }, {   59, 0x0000 }, {   59, 0x0000 }, {   59, 0x0000 },
  {   59, 0x0000 }, {   59, 0x0000 }, {   59, 0x0000 }, {   59, 0x0100 },
  {   60, 0x0000 }, {   60, 0x0000 }, {   60, 0x0000 }, {   60, 0x0000 },
  {   60, 0x0000 }, {   60, 0x0020 }, {   61, 0x0000 }, {   61, 0x1000 },
  /* 0x5d00 */
  {   62, 0x0000 }, {   62, 0x0000 }, {   62, 0x0000 }, {   62, 0x0000 },
  {   62, 0x0000 }, {   62, 0x0000 }, {   62, 0x0000 }, {   62, 0x0100 },
};
static const Summary16 hkscs2004_uni2indx_page5f[4] = {
  /* 0x5f00 */
  {   63, 0x0000 }, {   63, 0x0000 }, {   63, 0x0000 }, {   63, 0x0800 },
};
static const Summary16 hkscs2004_uni2indx_page66[23] = {
  /* 0x6600 */
  {   64, 0x0000 }, {   64, 0x0000 }, {   64, 0x0000 }, {   64, 0x0000 },
  {   64, 0x2000 }, {   65, 0x0000 }, {   65, 0x0000 }, {   65, 0x0000 },
  {   65, 0x0000 }, {   65, 0x0000 }, {   65, 0x0000 }, {   65, 0x0000 },
  {   65, 0x0000 }, {   65, 0x0000 }, {   65, 0x0080 }, {   66, 0x0000 },
  /* 0x6700 */
  {   66, 0x0000 }, {   66, 0x0000 }, {   66, 0x0000 }, {   66, 0x0000 },
  {   66, 0x0000 }, {   66, 0x0000 }, {   66, 0x4000 },
};
static const Summary16 hkscs2004_uni2indx_page6e[6] = {
  /* 0x6e00 */
  {   67, 0x0000 }, {   67, 0x0000 }, {   67, 0x0000 }, {   67, 0x0000 },
  {   67, 0x0000 }, {   67, 0x0080 },
};
static const Summary16 hkscs2004_uni2indx_page71[19] = {
  /* 0x7100 */
  {   68, 0x0000 }, {   68, 0x0000 }, {   68, 0x0000 }, {   68, 0x0000 },
  {   68, 0x0000 }, {   68, 0x0080 }, {   69, 0x0000 }, {   69, 0x0000 },
  {   69, 0x0000 }, {   69, 0x0000 }, {   69, 0x0000 }, {   69, 0x0000 },
  {   69, 0x0000 }, {   69, 0x0000 }, {   69, 0x0000 }, {   69, 0x0000 },
  /* 0x7200 */
  {   69, 0x0000 }, {   69, 0x0000 }, {   69, 0x0020 },
};
static const Summary16 hkscs2004_uni2indx_page74[10] = {
  /* 0x7400 */
  {   70, 0x0000 }, {   70, 0x0000 }, {   70, 0x0000 }, {   70, 0x0000 },
  {   70, 0x0000 }, {   70, 0x0000 }, {   70, 0x0002 }, {   71, 0x0000 },
  {   71, 0x0000 }, {   71, 0x2000 },
};
static const Summary16 hkscs2004_uni2indx_page79[13] = {
  /* 0x7900 */
  {   72, 0x0000 }, {   72, 0x0000 }, {   72, 0x0000 }, {   72, 0x0000 },
  {   72, 0x0000 }, {   72, 0x0000 }, {   72, 0x0000 }, {   72, 0x0000 },
  {   72, 0x0000 }, {   72, 0x0000 }, {   72, 0x0000 }, {   72, 0x0000 },
  {   72, 0x0400 },
};
static const Summary16 hkscs2004_uni2indx_page7d[10] = {
  /* 0x7d00 */
  {   73, 0x0000 }, {   73, 0x0000 }, {   73, 0x0000 }, {   73, 0x0000 },
  {   73, 0x0000 }, {   73, 0x0000 }, {   73, 0x0000 }, {   73, 0x0000 },
  {   73, 0x0000 }, {   73, 0x0020 },
};
static const Summary16 hkscs2004_uni2indx_page81[7] = {
  /* 0x8100 */
  {   74, 0x0000 }, {   74, 0x0000 }, {   74, 0x0000 }, {   74, 0x0000 },
  {   74, 0x0000 }, {   74, 0x0000 }, {   74, 0x1000 },
};
static const Summary16 hkscs2004_uni2indx_page85[10] = {
  /* 0x8500 */
  {   75, 0x0000 }, {   75, 0x0000 }, {   75, 0x0000 }, {   75, 0x0000 },
  {   75, 0x0000 }, {   75, 0x0000 }, {   75, 0x0000 }, {   75, 0x0000 },
  {   75, 0x0000 }, {   75, 0x0008 },
};
static const Summary16 hkscs2004_uni2indx_page8a[16] = {
  /* 0x8a00 */
  {   76, 0x0000 }, {   76, 0x0000 }, {   76, 0x0000 }, {   76, 0x0000 },
  {   76, 0x0000 }, {   76, 0x0000 }, {   76, 0x0000 }, {   76, 0x0000 },
  {   76, 0x0000 }, {   76, 0x0000 }, {   76, 0x0000 }, {   76, 0x0000 },
  {   76, 0x1000 }, {   77, 0x0000 }, {   77, 0x0000 }, {   77, 0x0200 },
};
static const Summary16 hkscs2004_uni2indx_page97[22] = {
  /* 0x9700 */
  {   78, 0x0000 }, {   78, 0x0000 }, {   78, 0x0000 }, {   78, 0x0010 },
  {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 },
  {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 },
  {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 },
  /* 0x9800 */
  {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 }, {   79, 0x0000 },
  {   79, 0x0000 }, {   79, 0x0040 },
};
static const Summary16 hkscs2004_uni2indx_page9f[12] = {
  /* 0x9f00 */
  {   80, 0x0000 }, {   80, 0x0000 }, {   80, 0x0000 }, {   80, 0x0000 },
  {   80, 0x0000 }, {   80, 0x0000 }, {   80, 0x0000 }, {   80, 0x0000 },
  {   80, 0x0000 }, {   80, 0x0000 }, {   80, 0x0000 }, {   80, 0x000c },
};
static const Summary16 hkscs2004_uni2indx_page201[20] = {
  /* 0x20100 */
  {   82, 0x0000 }, {   82, 0x0000 }, {   82, 0x0000 }, {   82, 0x0000 },
  {   82, 0x0000 }, {   82, 0x0000 }, {   82, 0x0000 }, {   82, 0x0000 },
  {   82, 0x0000 }, {   82, 0x0000 }, {   82, 0x0010 }, {   83, 0x0000 },
  {   83, 0x0000 }, {   83, 0x0000 }, {   83, 0x0000 }, {   83, 0x0000 },
  /* 0x20200 */
  {   83, 0x0000 }, {   83, 0x0000 }, {   83, 0x0000 }, {   83, 0x0200 },
};
static const Summary16 hkscs2004_uni2indx_page20a[26] = {
  /* 0x20a00 */
  {   84, 0x0000 }, {   84, 0x0000 }, {   84, 0x0000 }, {   84, 0x0000 },
  {   84, 0x0000 }, {   84, 0x0000 }, {   84, 0x8000 }, {   85, 0x0000 },
  {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 },
  {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 },
  /* 0x20b00 */
  {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 },
  {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 }, {   85, 0x0000 },
  {   85, 0x0000 }, {   85, 0x8000 },
};
static const Summary16 hkscs2004_uni2indx_page21a[7] = {
  /* 0x21a00 */
  {   86, 0x0000 }, {   86, 0x0000 }, {   86, 0x0000 }, {   86, 0x0000 },
  {   86, 0x0000 }, {   86, 0x0000 }, {   86, 0x0008 },
};
static const Summary16 hkscs2004_uni2indx_page21d[19] = {
  /* 0x21d00 */
  {   87, 0x0000 }, {   87, 0x0000 }, {   87, 0x0000 }, {   87, 0x0000 },
  {   87, 0x0000 }, {   87, 0x4000 }, {   88, 0x0000 }, {   88, 0x0000 },
  {   88, 0x0000 }, {   88, 0x0000 }, {   88, 0x0000 }, {   88, 0x0000 },
  {   88, 0x0000 }, {   88, 0x0000 }, {   88, 0x0000 }, {   88, 0x0000 },
  /* 0x21e00 */
  {   88, 0x0000 }, {   88, 0x0000 }, {   88, 0x0008 },
};
static const Summary16 hkscs2004_uni2indx_page221[13] = {
  /* 0x22100 */
  {   89, 0x0000 }, {   89, 0x0000 }, {   89, 0x0000 }, {   89, 0x0000 },
  {   89, 0x0000 }, {   89, 0x0000 }, {   89, 0x0000 }, {   89, 0x0000 },
  {   89, 0x0000 }, {   89, 0x0000 }, {   89, 0x0000 }, {   89, 0x0000 },
  {   89, 0x0002 },
};
static const Summary16 hkscs2004_uni2indx_page227[10] = {
  /* 0x22700 */
  {   90, 0x0000 }, {   90, 0x0000 }, {   90, 0x0000 }, {   90, 0x0000 },
  {   90, 0x0000 }, {   90, 0x0000 }, {   90, 0x0000 }, {   90, 0x0000 },
  {   90, 0x0000 }, {   90, 0x0040 },
};
static const Summary16 hkscs2004_uni2indx_page232[6] = {
  /* 0x23200 */
  {   91, 0x0000 }, {   91, 0x0000 }, {   91, 0x0000 }, {   91, 0x0000 },
  {   91, 0x0000 }, {   91, 0x0040 },
};
static const Summary16 hkscs2004_uni2indx_page235[18] = {
  /* 0x23500 */
  {   92, 0x0000 }, {   92, 0x0000 }, {   92, 0x0000 }, {   92, 0x0000 },
  {   92, 0x0000 }, {   92, 0x0002 }, {   93, 0x0000 }, {   93, 0x0000 },
  {   93, 0x0000 }, {   93, 0x0000 }, {   93, 0x0000 }, {   93, 0x0000 },
  {   93, 0x0000 }, {   93, 0x0000 }, {   93, 0x0000 }, {   93, 0x0000 },
  /* 0x23600 */
  {   93, 0x0000 }, {   93, 0x0080 },
};
static const Summary16 hkscs2004_uni2indx_page23b[2] = {
  /* 0x23b00 */
  {   94, 0x0000 }, {   94, 0x0400 },
};
static const Summary16 hkscs2004_uni2indx_page23e[47] = {
  /* 0x23e00 */
  {   95, 0x0040 }, {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 },
  {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 },
  {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 },
  {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 },
  /* 0x23f00 */
  {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0000 },
  {   96, 0x0000 }, {   96, 0x0000 }, {   96, 0x0002 }, {   97, 0x0000 },
  {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 },
  {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 },
  /* 0x24000 */
  {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 },
  {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 },
  {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x0000 },
  {   97, 0x0000 }, {   97, 0x0000 }, {   97, 0x1000 },
};
static const Summary16 hkscs2004_uni2indx_page242[12] = {
  /* 0x24200 */
  {   98, 0x0000 }, {   98, 0x0000 }, {   98, 0x0000 }, {   98, 0x0000 },
  {   98, 0x0000 }, {   98, 0x0000 }, {   98, 0x0000 }, {   98, 0x0000 },
  {   98, 0x0000 }, {   98, 0x0000 }, {   98, 0x0000 }, {   98, 0x8000 },
};
static const Summary16 hkscs2004_uni2indx_page24b[1] = {
  /* 0x24b00 */
  {   99, 0x8000 },
};
static const Summary16 hkscs2004_uni2indx_page254[10] = {
  /* 0x25400 */
  {  100, 0x0000 }, {  100, 0x0000 }, {  100, 0x0000 }, {  100, 0x0000 },
  {  100, 0x0000 }, {  100, 0x0000 }, {  100, 0x0000 }, {  100, 0x0000 },
  {  100, 0x0000 }, {  100, 0x0400 },
};
static const Summary16 hkscs2004_uni2indx_page25a[6] = {
  /* 0x25a00 */
  {  101, 0x0000 }, {  101, 0x0000 }, {  101, 0x0000 }, {  101, 0x0000 },
  {  101, 0x0000 }, {  101, 0x0010 },
};
static const Summary16 hkscs2004_uni2indx_page26b[21] = {
  /* 0x26b00 */
  {  102, 0x0000 }, {  102, 0x0000 }, {  102, 0x0008 }, {  103, 0x0000 },
  {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 },
  {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 },
  {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 },
  /* 0x26c00 */
  {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 }, {  103, 0x0000 },
  {  103, 0x0042 },
};
static const Summary16 hkscs2004_uni2indx_page26e[9] = {
  /* 0x26e00 */
  {  105, 0x0000 }, {  105, 0x0000 }, {  105, 0x0000 }, {  105, 0x0000 },
  {  105, 0x0000 }, {  105, 0x0000 }, {  105, 0x0000 }, {  105, 0x0000 },
  {  105, 0x0800 },
};
static const Summary16 hkscs2004_uni2indx_page270[14] = {
  /* 0x27000 */
  {  106, 0x0000 }, {  106, 0x0000 }, {  106, 0x0000 }, {  106, 0x0000 },
  {  106, 0x0000 }, {  106, 0x0000 }, {  106, 0x0000 }, {  106, 0x0000 },
  {  106, 0x0000 }, {  106, 0x0000 }, {  106, 0x0000 }, {  106, 0x0000 },
  {  106, 0x0000 }, {  106, 0x0004 },
};
static const Summary16 hkscs2004_uni2indx_page272[32] = {
  /* 0x27200 */
  {  107, 0x0000 }, {  107, 0x0000 }, {  107, 0x0000 }, {  107, 0x0000 },
  {  107, 0x0000 }, {  107, 0x0000 }, {  107, 0x0080 }, {  108, 0x0000 },
  {  108, 0x0000 }, {  108, 0x0000 }, {  108, 0x0000 }, {  108, 0x0000 },
  {  108, 0x0000 }, {  108, 0x0000 }, {  108, 0x0000 }, {  108, 0x0000 },
  /* 0x27300 */
  {  108, 0x0000 }, {  108, 0x0000 }, {  108, 0x0000 }, {  108, 0x0000 },
  {  108, 0x0000 }, {  108, 0x0004 }, {  109, 0x0000 }, {  109, 0x0000 },
  {  109, 0x0000 }, {  109, 0x0000 }, {  109, 0x0000 }, {  109, 0x0000 },
  {  109, 0x0000 }, {  109, 0x0000 }, {  109, 0x0000 }, {  109, 0x8000 },
};
static const Summary16 hkscs2004_uni2indx_page27b[29] = {
  /* 0x27b00 */
  {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x0000 },
  {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x0000 },
  {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x0000 },
  {  110, 0x0000 }, {  110, 0x0000 }, {  110, 0x8000 }, {  111, 0x0000 },
  /* 0x27c00 */
  {  111, 0x0000 }, {  111, 0x0000 }, {  111, 0x0000 }, {  111, 0x0000 },
  {  111, 0x0000 }, {  111, 0x0000 }, {  111, 0x1000 }, {  112, 0x0000 },
  {  112, 0x0000 }, {  112, 0x0000 }, {  112, 0x0000 }, {  112, 0x0002 },
  {  113, 0x0020 },
};
static const Summary16 hkscs2004_uni2indx_page286[12] = {
  /* 0x28600 */
  {  114, 0x0000 }, {  114, 0x0000 }, {  114, 0x0000 }, {  114, 0x0000 },
  {  114, 0x0000 }, {  114, 0x0000 }, {  114, 0x0000 }, {  114, 0x0000 },
  {  114, 0x0000 }, {  114, 0x0000 }, {  114, 0x0000 }, {  114, 0x0004 },
};
static const Summary16 hkscs2004_uni2indx_page289[7] = {
  /* 0x28900 */
  {  115, 0x0000 }, {  115, 0x0000 }, {  115, 0x0000 }, {  115, 0x0000 },
  {  115, 0x0000 }, {  115, 0x0000 }, {  115, 0x2000 },
};
static const Summary16 hkscs2004_uni2indx_page28b[12] = {
  /* 0x28b00 */
  {  116, 0x0000 }, {  116, 0x0000 }, {  116, 0x0000 }, {  116, 0x0000 },
  {  116, 0x0000 }, {  116, 0x0000 }, {  116, 0x0000 }, {  116, 0x0000 },
  {  116, 0x0000 }, {  116, 0x0000 }, {  116, 0x0000 }, {  116, 0x0200 },
};
static const Summary16 hkscs2004_uni2indx_page290[8] = {
  /* 0x29000 */
  {  117, 0x0000 }, {  117, 0x0000 }, {  117, 0x0000 }, {  117, 0x0000 },
  {  117, 0x0000 }, {  117, 0x0000 }, {  117, 0x0000 }, {  117, 0x0200 },
};
static const Summary16 hkscs2004_uni2indx_page298[21] = {
  /* 0x29800 */
  {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 },
  {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 },
  {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 },
  {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0000 }, {  118, 0x0400 },
  /* 0x29900 */
  {  119, 0x0000 }, {  119, 0x0000 }, {  119, 0x0000 }, {  119, 0x0000 },
  {  119, 0x0020 },
};
static const Summary16 hkscs2004_uni2indx_page29e[12] = {
  /* 0x29e00 */
  {  120, 0x0000 }, {  120, 0x0000 }, {  120, 0x0000 }, {  120, 0x0000 },
  {  120, 0x0000 }, {  120, 0x0000 }, {  120, 0x0000 }, {  120, 0x0000 },
  {  120, 0x0000 }, {  120, 0x0000 }, {  120, 0x0000 }, {  120, 0x0001 },
};
static const Summary16 hkscs2004_uni2indx_page2a1[12] = {
  /* 0x2a100 */
  {  121, 0x0000 }, {  121, 0x0000 }, {  121, 0x0000 }, {  121, 0x0000 },
  {  121, 0x0000 }, {  121, 0x0000 }, {  121, 0x0000 }, {  121, 0x0000 },
  {  121, 0x0000 }, {  121, 0x0000 }, {  121, 0x0000 }, {  121, 0x0020 },
};
static const Summary16 hkscs2004_uni2indx_page2a3[6] = {
  /* 0x2a300 */
  {  122, 0x0000 }, {  122, 0x0000 }, {  122, 0x0000 }, {  122, 0x0000 },
  {  122, 0x0000 }, {  122, 0x0002 },
};

static int
hkscs2004_wctomb (conv_t conv, unsigned char *r, ucs4_t wc, int n)
{
  if (n >= 2) {
    const Summary16 *summary = NULL;
    if (wc < 0x21a00) {
      if (wc < 0x6e00) {
        if (wc >= 0x3400 && wc < 0x3450)
          summary = &hkscs2004_uni2indx_page34[(wc>>4)-0x340];
        else if (wc >= 0x3600 && wc < 0x3980)
          summary = &hkscs2004_uni2indx_page36[(wc>>4)-0x360];
        else if (wc >= 0x3b00 && wc < 0x3ba0)
          summary = &hkscs2004_uni2indx_page3b[(wc>>4)-0x3b0];
        else if (wc >= 0x3d00 && wc < 0x3e00)
          summary = &hkscs2004_uni2indx_page3d[(wc>>4)-0x3d0];
        else if (wc >= 0x3f00 && wc < 0x41f0)
          summary = &hkscs2004_uni2indx_page3f[(wc>>4)-0x3f0];
        else if (wc >= 0x4300 && wc < 0x4750)
          summary = &hkscs2004_uni2indx_page43[(wc>>4)-0x430];
        else if (wc >= 0x4a00 && wc < 0x4ab0)
          summary = &hkscs2004_uni2indx_page4a[(wc>>4)-0x4a0];
        else if (wc >= 0x4c00 && wc < 0x4d90)
          summary = &hkscs2004_uni2indx_page4c[(wc>>4)-0x4c0];
        else if (wc >= 0x4f00 && wc < 0x4fc0)
          summary = &hkscs2004_uni2indx_page4f[(wc>>4)-0x4f0];
        else if (wc >= 0x5600 && wc < 0x5700)
          summary = &hkscs2004_uni2indx_page56[(wc>>4)-0x560];
        else if (wc >= 0x5900 && wc < 0x5d80)
          summary = &hkscs2004_uni2indx_page59[(wc>>4)-0x590];
        else if (wc >= 0x5f00 && wc < 0x5f40)
          summary = &hkscs2004_uni2indx_page5f[(wc>>4)-0x5f0];
        else if (wc >= 0x6600 && wc < 0x6770)
          summary = &hkscs2004_uni2indx_page66[(wc>>4)-0x660];
      } else {
        if (wc >= 0x6e00 && wc < 0x6e60)
          summary = &hkscs2004_uni2indx_page6e[(wc>>4)-0x6e0];
        else if (wc >= 0x7100 && wc < 0x7230)
          summary = &hkscs2004_uni2indx_page71[(wc>>4)-0x710];
        else if (wc >= 0x7400 && wc < 0x74a0)
          summary = &hkscs2004_uni2indx_page74[(wc>>4)-0x740];
        else if (wc >= 0x7900 && wc < 0x79d0)
          summary = &hkscs2004_uni2indx_page79[(wc>>4)-0x790];
        else if (wc >= 0x7d00 && wc < 0x7da0)
          summary = &hkscs2004_uni2indx_page7d[(wc>>4)-0x7d0];
        else if (wc >= 0x8100 && wc < 0x8170)
          summary = &hkscs2004_uni2indx_page81[(wc>>4)-0x810];
        else if (wc >= 0x8500 && wc < 0x85a0)
          summary = &hkscs2004_uni2indx_page85[(wc>>4)-0x850];
        else if (wc >= 0x8a00 && wc < 0x8b00)
          summary = &hkscs2004_uni2indx_page8a[(wc>>4)-0x8a0];
        else if (wc >= 0x9700 && wc < 0x9860)
          summary = &hkscs2004_uni2indx_page97[(wc>>4)-0x970];
        else if (wc >= 0x9f00 && wc < 0x9fc0)
          summary = &hkscs2004_uni2indx_page9f[(wc>>4)-0x9f0];
        else if (wc >= 0x20100 && wc < 0x20240)
          summary = &hkscs2004_uni2indx_page201[(wc>>4)-0x2010];
        else if (wc >= 0x20a00 && wc < 0x20ba0)
          summary = &hkscs2004_uni2indx_page20a[(wc>>4)-0x20a0];
      }
    } else {
      if (wc < 0x26b00) {
        if (wc >= 0x21a00 && wc < 0x21a70)
          summary = &hkscs2004_uni2indx_page21a[(wc>>4)-0x21a0];
        else if (wc >= 0x21d00 && wc < 0x21e30)
          summary = &hkscs2004_uni2indx_page21d[(wc>>4)-0x21d0];
        else if (wc >= 0x22100 && wc < 0x221d0)
          summary = &hkscs2004_uni2indx_page221[(wc>>4)-0x2210];
        else if (wc >= 0x22700 && wc < 0x227a0)
          summary = &hkscs2004_uni2indx_page227[(wc>>4)-0x2270];
        else if (wc >= 0x23200 && wc < 0x23260)
          summary = &hkscs2004_uni2indx_page232[(wc>>4)-0x2320];
        else if (wc >= 0x23500 && wc < 0x23620)
          summary = &hkscs2004_uni2indx_page235[(wc>>4)-0x2350];
        else if (wc >= 0x23b00 && wc < 0x23b20)
          summary = &hkscs2004_uni2indx_page23b[(wc>>4)-0x23b0];
        else if (wc >= 0x23e00 && wc < 0x240f0)
          summary = &hkscs2004_uni2indx_page23e[(wc>>4)-0x23e0];
        else if (wc >= 0x24200 && wc < 0x242c0)
          summary = &hkscs2004_uni2indx_page242[(wc>>4)-0x2420];
        else if (wc >= 0x24b00 && wc < 0x24b10)
          summary = &hkscs2004_uni2indx_page24b[(wc>>4)-0x24b0];
        else if (wc >= 0x25400 && wc < 0x254a0)
          summary = &hkscs2004_uni2indx_page254[(wc>>4)-0x2540];
        else if (wc >= 0x25a00 && wc < 0x25a60)
          summary = &hkscs2004_uni2indx_page25a[(wc>>4)-0x25a0];
      } else {
        if (wc >= 0x26b00 && wc < 0x26c50)
          summary = &hkscs2004_uni2indx_page26b[(wc>>4)-0x26b0];
        else if (wc >= 0x26e00 && wc < 0x26e90)
          summary = &hkscs2004_uni2indx_page26e[(wc>>4)-0x26e0];
        else if (wc >= 0x27000 && wc < 0x270e0)
          summary = &hkscs2004_uni2indx_page270[(wc>>4)-0x2700];
        else if (wc >= 0x27200 && wc < 0x27400)
          summary = &hkscs2004_uni2indx_page272[(wc>>4)-0x2720];
        else if (wc >= 0x27b00 && wc < 0x27cd0)
          summary = &hkscs2004_uni2indx_page27b[(wc>>4)-0x27b0];
        else if (wc >= 0x28600 && wc < 0x286c0)
          summary = &hkscs2004_uni2indx_page286[(wc>>4)-0x2860];
        else if (wc >= 0x28900 && wc < 0x28970)
          summary = &hkscs2004_uni2indx_page289[(wc>>4)-0x2890];
        else if (wc >= 0x28b00 && wc < 0x28bc0)
          summary = &hkscs2004_uni2indx_page28b[(wc>>4)-0x28b0];
        else if (wc >= 0x29000 && wc < 0x29080)
          summary = &hkscs2004_uni2indx_page290[(wc>>4)-0x2900];
        else if (wc >= 0x29800 && wc < 0x29950)
          summary = &hkscs2004_uni2indx_page298[(wc>>4)-0x2980];
        else if (wc >= 0x29e00 && wc < 0x29ec0)
          summary = &hkscs2004_uni2indx_page29e[(wc>>4)-0x29e0];
        else if (wc >= 0x2a100 && wc < 0x2a1c0)
          summary = &hkscs2004_uni2indx_page2a1[(wc>>4)-0x2a10];
        else if (wc >= 0x2a300 && wc < 0x2a360)
          summary = &hkscs2004_uni2indx_page2a3[(wc>>4)-0x2a30];
      }
    }
    if (summary) {
      unsigned short used = summary->used;
      unsigned int i = wc & 0x0f;
      if (used & ((unsigned short) 1 << i)) {
        unsigned short c;
        /* Keep in `used' only the bits 0..i-1. */
        used &= ((unsigned short) 1 << i) - 1;
        /* Add `summary->indx' and the number of bits set in `used'. */
        used = (used & 0x5555) + ((used & 0xaaaa) >> 1);
        used = (used & 0x3333) + ((used & 0xcccc) >> 2);
        used = (used & 0x0f0f) + ((used & 0xf0f0) >> 4);
        used = (used & 0x00ff) + (used >> 8);
        c = hkscs2004_2charset[summary->indx + used];
        r[0] = (c >> 8); r[1] = (c & 0xff);
        return 2;
      }
    }
    return RET_ILUNI;
  }
  return RET_TOOSMALL;
}
