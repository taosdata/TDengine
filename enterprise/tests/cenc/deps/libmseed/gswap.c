/***************************************************************************
 * Functions for generalized, in-place byte swapping between LSBF and
 * MSBF byte orders.
 *
 * Some standard C99 integer types are needed, namely uint8_t and
 * uint32_t.  Each function expects its input to be a void pointer to
 * a quantity of the appropriate size.
 *
 * There are two versions of most routines, one that works on
 * quantities regardless of alignment (gswapX) and one that works on
 * memory aligned quantities (gswapXa).  The memory aligned versions
 * (gswapXa) are much faster than the other versions (gswapX), but the
 * memory *must* be aligned.
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

#include "libmseed.h"

/* Swap routines that work on any (aligned or not) quantities */

void
ms_gswap2 (void *data2)
{
  uint8_t temp;

  union {
    uint8_t c[2];
  } dat;

  memcpy (&dat, data2, 2);
  temp     = dat.c[0];
  dat.c[0] = dat.c[1];
  dat.c[1] = temp;
  memcpy (data2, &dat, 2);
}

void
ms_gswap4 (void *data4)
{
  uint8_t temp;

  union {
    uint8_t c[4];
  } dat;

  memcpy (&dat, data4, 4);
  temp     = dat.c[0];
  dat.c[0] = dat.c[3];
  dat.c[3] = temp;
  temp     = dat.c[1];
  dat.c[1] = dat.c[2];
  dat.c[2] = temp;
  memcpy (data4, &dat, 4);
}

void
ms_gswap8 (void *data8)
{
  uint8_t temp;

  union {
    uint8_t c[8];
  } dat;

  memcpy (&dat, data8, 8);
  temp     = dat.c[0];
  dat.c[0] = dat.c[7];
  dat.c[7] = temp;

  temp     = dat.c[1];
  dat.c[1] = dat.c[6];
  dat.c[6] = temp;

  temp     = dat.c[2];
  dat.c[2] = dat.c[5];
  dat.c[5] = temp;

  temp     = dat.c[3];
  dat.c[3] = dat.c[4];
  dat.c[4] = temp;
  memcpy (data8, &dat, 8);
}

/* Swap routines that work on memory aligned quantities */

void
ms_gswap2a (void *data2)
{
  uint16_t *data = (uint16_t *) data2;

  *data = (((*data >> 8) & 0xff) | ((*data & 0xff) << 8));
}

void
ms_gswap4a (void *data4)
{
  uint32_t *data = (uint32_t *) data4;

  *data = (((*data >> 24) & 0xff) | ((*data & 0xff) << 24) |
           ((*data >> 8) & 0xff00) | ((*data & 0xff00) << 8));
}

void
ms_gswap8a (void *data8)
{
  uint32_t *data4 = (uint32_t *) data8;
  uint32_t h0, h1;

  h0 = data4[0];
  h0 = (((h0 >> 24) & 0xff) | ((h0 & 0xff) << 24) |
        ((h0 >> 8) & 0xff00) | ((h0 & 0xff00) << 8));

  h1 = data4[1];
  h1 = (((h1 >> 24) & 0xff) | ((h1 & 0xff) << 24) |
        ((h1 >> 8) & 0xff00) | ((h1 & 0xff00) << 8));

  data4[0] = h1;
  data4[1] = h0;
}
