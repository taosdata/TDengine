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

#ifndef _TD_WIDE_INTEGER_H_
#define _TD_WIDE_INTEGER_H_

#include <stdint.h>
#include "tdef.h"
#ifdef __cplusplus
extern "C" {
#endif

typedef struct _UInt128 {
  uint64_t low;
  uint64_t high;
} _UInt128;

// TODO wjm use cmake to check if the compiler supports __int128_t
#if defined(__GNUC__) || defined(__clang__)
// #if 0
typedef __uint128_t UInt128;
#else
typedef _UInt128 UInt128;
#define Int128 UInt128
#endif

#define SAFE_SIGNED_OP(a, b, SIGNED_TYPE, UNSIGNED_TYPE, OP) (SIGNED_TYPE)((UNSIGNED_TYPE)(a)OP(UNSIGNED_TYPE)(b))
#define SAFE_INT64_ADD(a, b)                                 SAFE_SIGNED_OP(a, b, int64_t, uint64_t, +)
#define SAFE_INT64_SUBTRACT(a, b)                            SAFE_SIGNED_OP(a, b, int64_t, uint64_t, -)

void     makeUInt128(UInt128* pInt, DecimalWord hi, DecimalWord lo);
uint64_t uInt128Hi(const UInt128* pInt);
uint64_t uInt128Lo(const UInt128* pInt);

void uInt128Abs(UInt128* pInt);
void uInt128Add(UInt128* pLeft, const UInt128* pRight);
void uInt128Subtract(UInt128* pLeft, const UInt128* pRight);
void uInt128Multiply(UInt128* pLeft, const UInt128* pRight);
void uInt128Divide(UInt128* pLeft, const UInt128* pRight);
void uInt128Mod(UInt128* pLeft, const UInt128* pRight);
bool uInt128Lt(const UInt128* pLeft, const UInt128* pRight);
bool uInt128Gt(const UInt128* pLeft, const UInt128* pRight);
bool uInt128Eq(const UInt128* pLeft, const UInt128* pRight);

extern const UInt128  uInt128_1e18;
extern const UInt128  uInt128Zero;
extern const uint64_t k1e18;

static inline int32_t countLeadingZeros(uint64_t v) {
#if defined(__clang__) || defined(__GUNC__)
  if (v == 0) return 64;
  return __builtin_clzll(v);
#else
  int32_t bitpos = 0;
  while (v != 0) {
    v >>= 1;
    ++bitpos;
  }
  return 64 - bitpos;
#endif
}

#ifdef __cplusplus
}
#endif

#endif /* _TD_WIDE_INTEGER_H_ */
