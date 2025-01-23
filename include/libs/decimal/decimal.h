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
#define DECIMAL64_MAX                  999999999999999999LL
#define DECIMAL64_MIN                  -999999999999999999LL

typedef struct Decimal128 {
  DecimalWord words[2];  // do not touch it directly, use DECIMAL128_HIGH_WORD/DECIMAL128_LOW_WORD
} Decimal128;

#define Decimal        Decimal128
#define decimalFromStr decimal128FromStr
#define makeDecimal    makeDecimal128

typedef struct SDecimalCompareCtx {
  void*    pData;
  int8_t   type;
  STypeMod typeMod;
} SDecimalCompareCtx;

// TODO wjm check if we need to expose these functions in decimal.h
void makeDecimal64(Decimal64* pDec64, int64_t w);
void makeDecimal128(Decimal128* pDec128, int64_t hi, uint64_t low);

#define DECIMAL_WORD_NUM(TYPE) sizeof(TYPE) / sizeof(DecimalWord)

void decimalFromTypeMod(STypeMod typeMod, uint8_t* precision, uint8_t* scale);

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale, Decimal64* result);
int32_t decimal128FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale,
                          Decimal128* result);

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal);
int32_t decimal128ToDataVal(Decimal128* dec, SValue* pVal);

int32_t decimalToStr(const DecimalType* pDec, int8_t type, int8_t precision, int8_t scale, char* pBuf, int32_t bufLen);

int32_t decimalGetRetType(const SDataType* pLeftT, const SDataType* pRightT, EOperatorType opType, SDataType* pOutType);
bool    decimalCompare(EOperatorType op, const SDecimalCompareCtx* pLeft, const SDecimalCompareCtx* pRight);
int32_t decimalOp(EOperatorType op, const SDataType* pLeftT, const SDataType* pRightT, const SDataType* pOutT,
                  const void* pLeftData, const void* pRightData, void* pOutputData);
int32_t convertToDecimal(const void* pData, const SDataType* pInputType, void* pOut, const SDataType* pOutType);

DEFINE_TYPE_FROM_DECIMAL_FUNCS(, Decimal64);
DEFINE_TYPE_FROM_DECIMAL_FUNCS(, Decimal128);

// TODO wjm change rightWordNum to DecimalType??
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

// all these ops only used for comparing decimal types with same scale
SDecimalOps* getDecimalOps(int8_t dataType);

__int128 decimal128ToInt128(const Decimal128* pDec);
int32_t  TEST_decimal64From_int64_t(Decimal64* pDec, uint8_t prec, uint8_t scale, int64_t v);
int32_t  TEST_decimal64From_uint64_t(Decimal64* pDec, uint8_t prec, uint8_t scale, uint64_t v);
int32_t  TEST_decimal64From_double(Decimal64* pDec, uint8_t prec, uint8_t scale, double v);
double  TEST_decimal64ToDouble(Decimal64* pDec, uint8_t prec, uint8_t scale);

int32_t TEST_decimal128From_int64_t(Decimal128* pDec, uint8_t prec, uint8_t scale, int64_t v);
int32_t TEST_decimal128From_uint64_t(Decimal128* pDec, uint8_t prec, uint8_t scale, uint64_t v);
int32_t TEST_decimal128From_double(Decimal128* pDec, uint8_t prec, uint8_t scale, double v);
double  TEST_decimal128ToDouble(Decimal128* pDec, uint8_t prec, uint8_t scale);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DECIMAL_H_*/
