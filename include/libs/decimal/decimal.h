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
typedef struct SDataType SDataType;
typedef struct SValue    SValue;

typedef struct Decimal64 {
  DecimalWord words[1];
} Decimal64;

typedef struct Decimal128 {
  DecimalWord words[2];
} Decimal128;

void makeDecimal64(Decimal64* pDec64, DecimalWord w);
void makeDecimal128(Decimal128* pDec128, int64_t hi, uint64_t low);

#define DECIMAL_WORD_NUM(TYPE) sizeof(TYPE) / sizeof(DecimalWord)

int32_t decimalCalcTypeMod(const SDataType* pType);
void    decimalFromTypeMod(STypeMod typeMod, uint8_t* precision, uint8_t* scale);

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal64* result);
int32_t decimal128FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal128* result);

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal);
int32_t decimal128ToDataVal(Decimal128* dec, SValue* pVal);

int32_t decimalToStr(const DecimalWord* pDec, int8_t type, int8_t precision, int8_t scale, char* pBuf, int32_t bufLen);

typedef struct SDecimalOps {
  void (*negate)(DecimalWord* pWord);
  void (*abs)(DecimalWord* pWord);
  void (*add)(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  void (*subtract)(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  void (*multiply)(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  void (*divide)(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum, DecimalWord* pRemainder);
  void (*mod)(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  bool (*lt)(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  bool (*gt)(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  bool (*eq)(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
  int32_t (*toStr)(const DecimalWord* pInt, uint8_t scale, char* pBuf, int32_t bufLen);
} SDecimalOps;

SDecimalOps* getDecimalOps(int8_t dataType);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DECIMAL_H_*/
