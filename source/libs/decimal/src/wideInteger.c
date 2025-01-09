#include "wideInteger.h"

#if defined(__GNUC__) || defined(__clang__)
// #if 0
void     makeUInt128(UInt128* pInt, DecimalWord hi, DecimalWord lo) { *pInt = ((UInt128)hi) << 64 | lo; }
uint64_t uInt128Hi(const UInt128* pInt) { return *pInt >> 64; }
uint64_t uInt128Lo(const UInt128* pInt) { return *pInt & 0xFFFFFFFFFFFFFFFF; }

void uInt128Abs(UInt128* pInt);
void uInt128Add(UInt128* pLeft, const UInt128* pRight) { *pLeft += *pRight; }
void uInt128Subtract(UInt128* pLeft, const UInt128* pRight);
void uInt128Multiply(UInt128* pLeft, const UInt128* pRight) { *pLeft *= *pRight; }
void uInt128Divide(UInt128* pLeft, const UInt128* pRight) { *pLeft /= *pRight; }
void uInt128Mod(UInt128* pLeft, const UInt128* pRight) { *pLeft %= *pRight; }
bool uInt128Lt(const UInt128* pLeft, const UInt128* pRight);
bool uInt128Gt(const UInt128* pLeft, const UInt128* pRight);
bool uInt128Eq(const UInt128* pLeft, const UInt128* pRight) { return *pLeft == *pRight; }

const UInt128  uInt128Zero = 0;
const uint64_t k1e18 = 1000000000000000000LL;
const UInt128  uInt128_1e18 = k1e18;
#else

void uInt128Multiply(UInt128* pLeft, const UInt128* pRight) {}

#endif
