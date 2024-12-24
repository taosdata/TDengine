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

#include "ttypes.h"
#include "tdef.h"
typedef struct SDataType SDataType;
typedef struct SValue SValue;

typedef struct Decimal64 {
  DecimalWord words[1];
} Decimal64;

typedef struct Decimal128 {
  DecimalWord words[2];
} Decimal128;

#define DECIMAL_WORD_NUM(TYPE) sizeof(TYPE) / sizeof(DecimalWord)

int32_t decimalCalcTypeMod(const SDataType* pType);
void    decimalFromTypeMod(STypeMod typeMod, uint8_t* precision, uint8_t* scale);

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal64* result);
int32_t decimal128FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal128* result);

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal);
int32_t decimal128ToDataVal(const Decimal128* dec, SValue* pVal);

int32_t decimalToStr(DecimalWord* pDec, int8_t precision, int8_t scale, char* pBuf, int32_t bufLen);

typedef struct DecimalVar DecimalVar;

typedef struct SDecimalVarOps {
    uint8_t wordNum;
    int32_t (*add)(DecimalVar* pLeft, const DecimalVar* pRight);
    int32_t (*multiply)(DecimalVar* pLeft, const DecimalVar* pRight);
} SDecimalVarOps;

typedef struct SWideInteger {
  int32_t wordNum;
  DecimalWord *words;
} SWideInteger;

// TODO wjm rename it
typedef struct SWideIntegerOps {
  uint8_t wordNum;
  int32_t (*abs)(DecimalWord* pInt);
  int32_t (*add)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  int32_t (*subtract)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  int32_t (*multiply)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  int32_t (*divide)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  int32_t (*mod)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  bool (*lt)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  bool (*gt)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  bool (*eq)(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
  int32_t (*toStr)(DecimalWord* pInt, char* pBuf, int32_t bufLen);
} SWideIntegerOps;

#ifdef __cplusplus
}
#endif

#endif /*_TD_DECIMAL_H_*/
