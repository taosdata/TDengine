/*
 * Copyright (C) 1999-2001 Free Software Foundation, Inc.
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
 * CNS 11643-1992 plane 4
 */

/*
 * The table has been split into two parts. Each part's entries fit it 16 bits.
 * But the combined table would need 17 bits per entry.
 */
#include "cns11643_4a.h"
#include "cns11643_4b.h"

static int
cns11643_4_mbtowc (conv_t conv, ucs4_t *pwc, const unsigned char *s, int n)
{
  unsigned char c1 = s[0];
  if ((c1 >= 0x21 && c1 <= 0x6e)) {
    if (n >= 2) {
      unsigned char c2 = s[1];
      if (c2 >= 0x21 && c2 < 0x7f) {
        unsigned int i = 94 * (c1 - 0x21) + (c2 - 0x21);
        ucs4_t wc = 0xfffd;
        unsigned short swc;
        {
          if (i < 2914)
            swc = cns11643_4a_2uni_page21[i],
            wc = cns11643_4a_2uni_upages[swc>>8] | (swc & 0xff);
          else if (i < 7298)
            swc = cns11643_4b_2uni_page40[i-2914],
            wc = cns11643_4b_2uni_upages[swc>>8] | (swc & 0xff);
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
