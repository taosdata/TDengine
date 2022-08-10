/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"

#ifdef WINDOWS

/*
 * windows implementation
 */

unsigned char _MyBitScanForward64(unsigned long *ret, uint64_t x) {
  unsigned long x0 = (unsigned long)x, top, bottom;
  _BitScanForward(&top, (unsigned long)(x >> 32));
  _BitScanForward(&bottom, x0);
  *ret = x0 ? bottom : 32 + top;
  return x != 0;
}

unsigned char _MyBitScanReverse64(unsigned long *ret, uint64_t x) {
  unsigned long x1 = (unsigned long)(x >> 32), top, bottom;
  _BitScanReverse(&top, x1);
  _BitScanReverse(&bottom, (unsigned long)x);
  *ret = x1 ? top + 32 : bottom;
  return x != 0;
}

int32_t BUILDIN_CLZL(uint64_t val) {
  unsigned long r = 0;
#ifdef _WIN64
  _BitScanReverse64(&r, val);
#else
  _MyBitScanReverse64(&r, val);
#endif
  return (int)(63 - r);
}

int32_t BUILDIN_CLZ(uint32_t val) {
  unsigned long r = 0;
  _BitScanReverse(&r, val);
  return (int)(31 - r);
}

int32_t BUILDIN_CTZL(uint64_t val) {
  unsigned long r = 0;
#ifdef _WIN64
  _BitScanForward64(&r, val);
#else
  _MyBitScanForward64(&r, val);
#endif
  return (int)(r);
}

int32_t BUILDIN_CTZ(uint32_t val) {
  unsigned long r = 0;
  _BitScanForward(&r, val);
  return (int)(r);
}

#endif
