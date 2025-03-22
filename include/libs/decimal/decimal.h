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

#ifndef _TD_DECIMAL_H_
#define _TD_DECIMAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tdef.h"
#include "ttypes.h"
typedef struct SValue SValue;
typedef void          DecimalType;

typedef struct Decimal64 {
  DecimalWord words[1];  // do not touch it directly, use DECIMAL64_GET_VALUE MACRO
} Decimal64;

#define DECIMAL64_GET_VALUE(pDec)      (int64_t)((pDec)->words[0])
#define DECIMAL64_SET_VALUE(pDec, val) (*(int64_t*)((pDec)->words)) = (int64_t)(val)
#define DECIMAL64_CLONE(pDst, pFrom)   ((Decimal64*)(pDst))->words[0] = ((Decimal64*)(pFrom))->words[0]

static const Decimal64 decimal64Zero = {{0}};
static const Decimal64 decimal64Two = {{2}};
static const Decimal64 decimal64Min = {{(uint64_t)-999999999999999999LL}};
static const Decimal64 decimal64Max = {{(uint64_t)999999999999999999LL}};
#define DECIMAL64_ZERO decimal64Zero
#define DECIMAL64_MAX decimal64Max
#define DECIMAL64_MIN decimal64Min

typedef struct Decimal128 {
  DecimalWord words[2];  // do not touch it directly, use DECIMAL128_HIGH_WORD/DECIMAL128_LOW_WORD
} Decimal128;

#define Decimal        Decimal128
#define decimalFromStr decimal128FromStr
#define makeDecimal    makeDecimal128

#define DEFINE_DECIMAL128(lo, hi) {{lo, hi}}
static const Decimal128 decimal128Zero = DEFINE_DECIMAL128(0, 0);
static const Decimal128 decimal128Two = DEFINE_DECIMAL128(2, 0);
static const Decimal128 decimal128Max = DEFINE_DECIMAL128(687399551400673280ULL - 1, 5421010862427522170LL);
static const Decimal128 decimal128Min = DEFINE_DECIMAL128(17759344522308878337ULL, 13025733211282029445ULL);
#define DECIMAL128_LOW_WORD(pDec)           (uint64_t)((pDec)->words[0])
#define DECIMAL128_SET_LOW_WORD(pDec, val)  (pDec)->words[0] = val
#define DECIMAL128_HIGH_WORD(pDec)          (int64_t)((pDec)->words[1])
#define DECIMAL128_SET_HIGH_WORD(pDec, val) *(int64_t*)((pDec)->words + 1) = val

#define DECIMAL128_ZERO decimal128Zero
#define DECIMAL128_MAX  decimal128Max
#define DECIMAL128_MIN  decimal128Min
#define DECIMAL128_CLONE(pDst, pFrom) makeDecimal128(pDst, DECIMAL128_HIGH_WORD(pFrom), DECIMAL128_LOW_WORD(pFrom))

typedef struct SDecimalCompareCtx {
  const void* pData;
  int8_t      type;
  STypeMod    typeMod;
} SDecimalCompareCtx;

void makeDecimal64(Decimal64* pDec64, int64_t w);
void makeDecimal128(Decimal128* pDec128, int64_t hi, uint64_t low);

void decimalFromTypeMod(STypeMod typeMod, uint8_t* precision, uint8_t* scale);

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale, Decimal64* result);
int32_t decimal128FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale,
                          Decimal128* result);

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal);
int32_t decimal128ToDataVal(Decimal128* dec, SValue* pVal);

int32_t decimalToStr(const DecimalType* pDec, int8_t type, int8_t precision, int8_t scale, char* pBuf, int32_t bufLen);

int32_t decimalGetRetType(const SDataType* pLeftT, const SDataType* pRightT, EOperatorType opType, SDataType* pOutType);
bool    decimal64Compare(EOperatorType op, const SDecimalCompareCtx* pLeft, const SDecimalCompareCtx* pRight);
bool    decimalCompare(EOperatorType op, const SDecimalCompareCtx* pLeft, const SDecimalCompareCtx* pRight);
int32_t decimalOp(EOperatorType op, const SDataType* pLeftT, const SDataType* pRightT, const SDataType* pOutT,
                  const void* pLeftData, const void* pRightData, void* pOutputData);
int32_t convertToDecimal(const void* pData, const SDataType* pInputType, void* pOut, const SDataType* pOutType);
bool    decimal128AddCheckOverflow(const Decimal128* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
void    encodeDecimal(const DecimalType* pDec, int8_t type, void* pBuf);
void    decodeDecimal(const void* pBuf, int8_t type, DecimalType* pDec);

DEFINE_TYPE_FROM_DECIMAL_FUNCS(, Decimal64);
DEFINE_TYPE_FROM_DECIMAL_FUNCS(, Decimal128);

// word num use DECIMAL_WORD_NUM(Decimal64) or DECIMAL_WORD_NUM(Decimal128)
typedef struct SDecimalOps {
  void (*negate)(DecimalType* pWord);
  void (*abs)(DecimalType* pWord);
  void (*add)(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  void (*subtract)(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  void (*multiply)(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  void (*divide)(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum, DecimalType* pRemainder);
  void (*mod)(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  bool (*lt)(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  bool (*gt)(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  bool (*eq)(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
  int32_t (*toStr)(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen);
} SDecimalOps;

// all these ops only used for operations on decimal types with same scale
const SDecimalOps* getDecimalOps(int8_t dataType);

#if 0
__int128 decimal128ToInt128(const Decimal128* pDec);
#endif
int32_t  TEST_decimal64From_int64_t(Decimal64* pDec, uint8_t prec, uint8_t scale, int64_t v);
int32_t  TEST_decimal64From_uint64_t(Decimal64* pDec, uint8_t prec, uint8_t scale, uint64_t v);
int32_t  TEST_decimal64From_double(Decimal64* pDec, uint8_t prec, uint8_t scale, double v);
double   TEST_decimal64ToDouble(Decimal64* pDec, uint8_t prec, uint8_t scale);
int32_t  TEST_decimal64FromDecimal64(const Decimal64* pInput, uint8_t inputPrec, uint8_t inputScale, Decimal64* pOutput,
                                     uint8_t outputPrec, uint8_t outputScale);
int32_t  TEST_decimal64FromDecimal128(const Decimal128* pInput, uint8_t prec, uint8_t scale, Decimal64* pOutput,
                                      uint8_t outputPrec, uint8_t outputScale);

int32_t TEST_decimal128From_int64_t(Decimal128* pDec, uint8_t prec, uint8_t scale, int64_t v);
int32_t TEST_decimal128From_uint64_t(Decimal128* pDec, uint8_t prec, uint8_t scale, uint64_t v);
int32_t TEST_decimal128From_double(Decimal128* pDec, uint8_t prec, uint8_t scale, double v);
double  TEST_decimal128ToDouble(Decimal128* pDec, uint8_t prec, uint8_t scale);
int32_t TEST_decimal128FromDecimal64(const Decimal64* pInput, uint8_t inputPrec, uint8_t inputScale,
                                     Decimal128* pOutput, uint8_t outputPrec, uint8_t outputScale);
int32_t TEST_decimal128FromDecimal128(const Decimal128* pDec, uint8_t prec, uint8_t scale, Decimal128* pOutput,
                                      uint8_t outputPrec, uint8_t outputScale);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DECIMAL_H_*/
