#include "wideInteger.h"
#include "intx/int128.hpp"
#include "intx/intx.hpp"


const UInt128  uInt128Zero = {0, 0};
const uint64_t k1e18 = 1000000000000000000LL;
const UInt128  uInt128_1e18 = {k1e18, 0};
const UInt128 uInt128One = {1, 0};
const UInt128 uInt128Two = {2, 0};

void makeUInt128(uint128* pUint128, uint64_t high, uint64_t low) {
  intx::uint128* pIntxUint = (intx::uint128*)pUint128;
  pIntxUint->hi = high;
  pIntxUint->lo = low;
}

uint64_t uInt128Hi(const UInt128* pInt) {
  intx::uint128 *pIntUint = (intx::uint128*)pInt;
  return pIntUint->hi;
}

uint64_t uInt128Lo(const UInt128* pInt) {
  intx::uint128 *pIntUint = (intx::uint128*)pInt;
  return pIntUint->lo;
}

void uInt128Add(UInt128* pLeft, const UInt128* pRight) {
  intx::uint128 *pX = (intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  *pX += *pY;
}
void uInt128Subtract(UInt128* pLeft, const UInt128* pRight) {
  intx::uint128 *pX = (intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  *pX -= *pY;
}
void uInt128Multiply(UInt128* pLeft, const UInt128* pRight) {
  intx::uint128 *pX = (intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  *pX *= *pY;
  /* __uint128_t *px = (__uint128_t*)pLeft;
  const __uint128_t *py = (__uint128_t*)pRight;
  *px = *px * *py; */
}
void uInt128Divide(UInt128* pLeft, const UInt128* pRight) {
  intx::uint128 *pX = (intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  *pX /= *pY;
  /* __uint128_t *px = (__uint128_t*)pLeft;
  const __uint128_t *py = (__uint128_t*)pRight;
  *px = *px / *py; */
}
void uInt128Mod(UInt128* pLeft, const UInt128* pRight) {
  intx::uint128 *pX = (intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  *pX %= *pY;
  /* __uint128_t *px = (__uint128_t*)pLeft;
  const __uint128_t *py = (__uint128_t*)pRight;
  *px = *px % *py; */
}
bool uInt128Lt(const UInt128* pLeft, const UInt128* pRight) {
  const intx::uint128 *pX = (const intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  return *pX < *pY;
}
bool uInt128Gt(const UInt128* pLeft, const UInt128* pRight) {
  const intx::uint128 *pX = (const intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  return *pX > *pY;
}
bool uInt128Eq(const UInt128* pLeft, const UInt128* pRight) {
  const intx::uint128 *pX = (const intx::uint128*)pLeft;
  const intx::uint128 *pY = (const intx::uint128*)pRight;
  return *pX == *pY;
}

Int128 makeInt128(int64_t high, uint64_t low) {
  Int128 int128 = {low, high};
  return int128;
}
int64_t int128Hi(const Int128* pUint128) {
  return pUint128->high;
}
uint64_t int128Lo(const Int128* pUint128) {
  return pUint128->low;
}
Int128 int128Abs(const Int128* pInt128) {
  if (int128Lt(pInt128, &int128Zero)) {
    return int128Negate(pInt128);
  }
  return *pInt128;
}
Int128 int128Negate(const Int128* pInt128) {
  uint64_t low = ~pInt128->low + 1;
  int64_t  high = ~pInt128->high;
  if (low == 0) high += 1;
  return makeInt128(high, low);
}
Int128 int128Add(const Int128* pLeft, const Int128* pRight) {
  intx::uint128 result = *(intx::uint128*)pLeft + *(intx::uint128*)pRight;
  return *(Int128*)&result;
}
Int128 int128Subtract(const Int128* pLeft, const Int128* pRight) {
  intx::uint128 result = *(intx::uint128*)pLeft - *(intx::uint128*)pRight;
  return *(Int128*)&result;
}
Int128 int128Multiply(const Int128* pLeft, const Int128* pRight) {
  intx::uint128 result = *(intx::uint128*)pLeft * *(intx::uint128*)pRight;
  return *(Int128*)&result;
}
Int128 int128Divide(const Int128* pLeft, const Int128* pRight) {
  intx::uint128 result = *(intx::uint128*)pLeft / *(intx::uint128*)pRight;
  return *(Int128*)&result;
}
Int128 int128Mod(const Int128* pLeft, const Int128* pRight) {
  intx::uint128 result = *(intx::uint128*)pLeft % *(intx::uint128*)pRight;
  return *(Int128*)&result;
}
bool int128Lt(const Int128* pLeft, const Int128* pRight) {
  return pLeft->high < pRight->high || (pLeft->high == pRight->high && pLeft->low < pRight->low);
}
bool int128Gt(const Int128* pLeft, const Int128* pRight) {
  return int128Lt(pRight, pLeft);
}
bool int128Eq(const Int128* pLeft, const Int128* pRight) {
  return pLeft->high == pRight->high && pLeft->low == pRight->low;
}
Int128 int128RightShift(const Int128* pLeft, int32_t shift) {
  intx::uint128 result = *(intx::uint128*)pLeft >> shift;
  return *(Int128*)&result;
}

const Int128 int128Zero = {0, 0};
const Int128 int128One = {1, 0};

UInt256 makeUint256(UInt128 high, UInt128 low) {
  UInt256 uint256 = {high, low};
  return uint256;
}
uint128 uInt256Hi(const UInt256* pUint256) {
  return pUint256->high;
}
uint128 uInt256Lo(const UInt256* pUint256) {
  return pUint256->low;
}
UInt256 uInt256Add(const UInt256* pLeft, const UInt256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft + *(intx::uint256*)pRight;
  return *(UInt256*)&result;
}
UInt256 uInt256Subtract(const UInt256* pLeft, const UInt256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft - *(intx::uint256*)pRight;
  return *(UInt256*)&result;
}
UInt256 uInt256Multiply(const UInt256* pLeft, const UInt256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft * *(intx::uint256*)pRight;
  return *(UInt256*)&result;
}
UInt256 uInt256Divide(const UInt256* pLeft, const UInt256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft / *(intx::uint256*)pRight;
  return *(UInt256*)&result;
}
UInt256 uInt256Mod(const UInt256* pLeft, const UInt256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft % *(intx::uint256*)pRight;
  return *(UInt256*)&result;
}
bool uInt256Lt(const UInt256* pLeft, const UInt256* pRight) {
  return *(intx::uint256*)pLeft < *(intx::uint256*)pRight;
}
bool uInt256Gt(const UInt256* pLeft, const UInt256* pRight) {
  return *(intx::uint256*)pLeft > *(intx::uint256*)pRight;
}
bool uInt256Eq(const UInt256* pLeft, const UInt256* pRight) {
  return *(intx::uint256*)pLeft == *(intx::uint256*)pRight;
}
UInt256 uInt256RightShift(const UInt256* pLeft, int32_t shift) {
  intx::uint256 result = *(intx::uint256*)pLeft >> shift;
  return *(UInt256*)&result;
}

Int256 makeInt256(Int128 high, UInt128 low) {
  Int256 int256 = {low, high};
  return int256;
}
Int128 int256Hi(const Int256* pUint256) {
  return pUint256->high;
}
UInt128 int256Lo(const Int256* pUint256) {
  return pUint256->low;
}
Int256 int256Abs(const Int256* pInt256) {
  if (int256Lt(pInt256, &int256Zero)) {
    return int256Negate(pInt256);
  }
  return *pInt256;
}

Int256 int256Negate(const Int256* pInt256) {
  return int256Subtract(&int256Zero, pInt256);
}
Int256 int256Add(const Int256* pLeft, const Int256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft + *(intx::uint256*)pRight;
  return *(Int256*)&result;
}
Int256 int256Subtract(const Int256* pLeft, const Int256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft - *(intx::uint256*)pRight;
  return *(Int256*)&result;
}
Int256 int256Multiply(const Int256* pLeft, const Int256* pRight) {
  intx::uint256 result = *(intx::uint256*)pLeft * *(intx::uint256*)pRight;
  return *(Int256*)&result;
}
Int256 int256Divide(const Int256* pLeft, const Int256* pRight) {
  Int256 l = *pLeft, r = *pRight;
  bool   leftNegative = int256Lt(pLeft, &int256Zero), rightNegative = int256Lt(pRight, &int256Zero);
  if (leftNegative) {
    l = int256Abs(pLeft);
  }
  if (rightNegative) {
    r = int256Abs(pRight);
  }
  intx::uint256 result = *(intx::uint256*)&l / *(intx::uint256*)&r;
  Int256 res =  *(Int256*)&result;
  if (leftNegative != rightNegative)
    res = int256Negate(&res);
  return res;
}

Int256 int256Mod(const Int256* pLeft, const Int256* pRight) {
  Int256 left = *pLeft, right = *pRight;
  bool leftNegative = int256Lt(pLeft, &int256Zero);
  if (leftNegative) {
    left = int256Abs(&left);
  }
  bool rightNegate = int256Lt(pRight, &int256Zero);
  if (rightNegate) right = int256Abs(pRight);
  intx::uint256 result = *(intx::uint256*)&left % *(intx::uint256*)&right;
  Int256 res =  *(Int256*)&result;
  if (leftNegative) res = int256Negate(&res);
  return res;
}
bool int256Lt(const Int256* pLeft, const Int256* pRight) {
  Int128  hiLeft = int256Hi(pLeft), hiRight = int256Hi(pRight);
  UInt128 lowLeft = int256Lo(pLeft), lowRight = int256Lo(pRight);
  return int128Lt(&hiLeft, &hiRight) || (int128Eq(&hiLeft, &hiRight) && uInt128Lt(&lowLeft, &lowRight));
}
bool int256Gt(const Int256* pLeft, const Int256* pRight) {
  return int256Lt(pRight, pLeft);
}
bool int256Eq(const Int256* pLeft, const Int256* pRight) {
  Int128  hiLeft = int256Hi(pLeft), hiRight = int256Hi(pRight);
  UInt128 lowLeft = int256Lo(pLeft), lowRight = int256Lo(pRight);
  return int128Eq(&hiLeft, &hiRight) && uInt128Eq(&lowLeft, &lowRight);
}
Int256 int256RightShift(const Int256* pLeft, int32_t shift) {
  intx::uint256 result = *(intx::uint256*)pLeft >> shift;
  return *(Int256*)&result;
}

const Int256 int256One = {uInt128One, int128Zero};
const Int256 int256Zero = {uInt128Zero, int128Zero};
const Int256 int256Two = {uInt128Two, int128Zero};

