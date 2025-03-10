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

#include <stdbool.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

struct uint128 {
  uint64_t low;
  uint64_t high;
};

struct int128 {
  uint64_t low;
  int64_t  high;
};

struct uint256 {
  struct uint128 low;
  struct uint128 high;
};

struct int256 {
  struct uint128 low;
  struct int128 high;
};

#define UInt128 struct uint128
#define UInt256 struct uint256
#define Int128  struct int128
#define Int256  struct int256

#define SAFE_SIGNED_OP(a, b, SIGNED_TYPE, UNSIGNED_TYPE, OP) (SIGNED_TYPE)((UNSIGNED_TYPE)(a)OP(UNSIGNED_TYPE)(b))
#define SAFE_INT64_ADD(a, b)                                 SAFE_SIGNED_OP(a, b, int64_t, uint64_t, +)
#define SAFE_INT64_SUBTRACT(a, b)                            SAFE_SIGNED_OP(a, b, int64_t, uint64_t, -)

void     makeUInt128(UInt128* pInt, uint64_t hi, uint64_t lo);
uint64_t uInt128Hi(const UInt128* pInt);
uint64_t uInt128Lo(const UInt128* pInt);
void     uInt128Add(UInt128* pLeft, const UInt128* pRight);
void     uInt128Subtract(UInt128* pLeft, const UInt128* pRight);
void     uInt128Multiply(UInt128* pLeft, const UInt128* pRight);
void     uInt128Divide(UInt128* pLeft, const UInt128* pRight);
void     uInt128Mod(UInt128* pLeft, const UInt128* pRight);
bool     uInt128Lt(const UInt128* pLeft, const UInt128* pRight);
bool     uInt128Gt(const UInt128* pLeft, const UInt128* pRight);
bool     uInt128Eq(const UInt128* pLeft, const UInt128* pRight);

extern const UInt128  uInt128_1e18;
extern const UInt128  uInt128Zero;
extern const uint64_t k1e18;
extern const UInt128 uInt128One;
extern const UInt128 uInt128Two;

Int128  makeInt128(int64_t high, uint64_t low);
int64_t int128Hi(const Int128* pUint128);
uint64_t int128Lo(const Int128* pUint128);
Int128  int128Abs(const Int128* pInt128);
Int128  int128Negate(const Int128* pInt128);
Int128  int128Add(const Int128* pLeft, const Int128* pRight);
Int128  int128Subtract(const Int128* pLeft, const Int128* pRight);
Int128  int128Multiply(const Int128* pLeft, const Int128* pRight);
Int128  int128Divide(const Int128* pLeft, const Int128* pRight);
Int128  int128Mod(const Int128* pLeft, const Int128* pRight);
bool    int128Lt(const Int128* pLeft, const Int128* pRight);
bool    int128Gt(const Int128* pLeft, const Int128* pRight);
bool    int128Eq(const Int128* pLeft, const Int128* pRight);
Int128  int128RightShift(const Int128* pLeft, int32_t shift);

extern const Int128 int128Zero;
extern const Int128 int128One;

UInt256 makeUint256(UInt128 high, UInt128 low);
UInt128 uInt256Hi(const UInt256* pUint256);
UInt128 uInt256Lo(const UInt256* pUint256);
UInt256 uInt256Add(const UInt256* pLeft, const UInt256* pRight);
UInt256 uInt256Subtract(const UInt256* pLeft, const UInt256* pRight);
UInt256 uInt256Multiply(const UInt256* pLeft, const UInt256* pRight);
UInt256 uInt256Divide(const UInt256* pLeft, const UInt256* pRight);
UInt256 uInt256Mod(const UInt256* pLeft, const UInt256* pRight);
bool    uInt256Lt(const UInt256* pLeft, const UInt256* pRight);
bool    uInt256Gt(const UInt256* pLeft, const UInt256* pRight);
bool    uInt256Eq(const UInt256* pLeft, const UInt256* pRight);
UInt256 uInt256RightShift(const UInt256* pLeft, int32_t shift);

extern const UInt256 uInt256Zero;
extern const UInt256 uInt256One;

Int256  makeInt256(Int128 high, UInt128 low);
Int128 int256Hi(const Int256* pUint256);
UInt128 int256Lo(const Int256* pUint256);
Int256  int256Abs(const Int256* pInt256);
Int256  int256Negate(const Int256* pInt256);
Int256  int256Add(const Int256* pLeft, const Int256* pRight);
Int256  int256Subtract(const Int256* pLeft, const Int256* pRight);
Int256  int256Multiply(const Int256* pLeft, const Int256* pRight);
Int256  int256Divide(const Int256* pLeft, const Int256* pRight);
Int256  int256Mod(const Int256* pLeft, const Int256* pRight);
bool    int256Lt(const Int256* pLeft, const Int256* pRight);
bool    int256Gt(const Int256* pLeft, const Int256* pRight);
bool    int256Eq(const Int256* pLeft, const Int256* pRight);
Int256  int256RightShift(const Int256* pLeft, int32_t shift);

extern const Int256 int256Zero;
extern const Int256 int256One;
extern const Int256 int256Two;

#ifdef __cplusplus
}
#endif

static inline int32_t countLeadingZeros(uint64_t v) {
#if defined(__clang__) || defined(__GNUC__)
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


#endif /* _TD_WIDE_INTEGER_H_ */
