/*
 *
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

#include "decimal.h"
#include "querynodes.h"

typedef enum DecimalInternalType {
  DECIMAL_64 = 0,
  DECIMAL_128 = 1,
} DecimalInternalType;

#define DECIMAL_GET_INTERNAL_TYPE(precision) ((precision) > TSDB_DECIMAL64_MAX_SCALE ? DECIMAL_128 : DECIMAL_64)
#define DECIMAL_GET_WORD_NUM(decimalInternalType) ((decimalInternalType) == DECIMAL_64 ? DECIMAL_WORD_NUM(Decimal64) : DECIMAL_WORD_NUM(Decimal128))
#define DecimalMax Decimal128

static int32_t int64Abs(DecimalWord* pInt);
static int32_t int64Add(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static int32_t int64Subtract(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static int32_t int64Multiply(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static int32_t int64divide(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static int32_t int64Mod(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static bool    int64Lt(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static bool    int64Gt(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static bool    int64Eq(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum);
static int32_t int64ToStr(DecimalWord* pInt, char* pBuf, int32_t bufLen);

typedef __int128_t Int128;

SWideIntegerOps wideIntegerOps [2] = {
  {DECIMAL_WORD_NUM(Decimal64), int64Abs, int64Add, int64Subtract, int64Multiply, int64divide, int64Mod, int64Lt, int64Gt, int64Eq, int64ToStr},
  {DECIMAL_WORD_NUM(Decimal128), 0, 0, 0}};

SDecimalVarOps decimalVarOps[2] = {
    {DECIMAL_WORD_NUM(Decimal64), 0, 0},
    {DECIMAL_WORD_NUM(Decimal128), 0, 0}
};

struct DecimalVar {
  DecimalInternalType type;
  uint8_t             precision;
  uint8_t             scale;
  int32_t             exponent;
  int8_t              sign;
  DecimalWord*        words;
};

static uint8_t maxPrecision(DecimalInternalType type) {
  switch (type) {
    case DECIMAL_64:
      return TSDB_DECIMAL64_MAX_PRECISION;
    case DECIMAL_128:
      return TSDB_DECIMAL128_MAX_PRECISION;
    default:
      return 0;
  }
}

static int32_t decimalVarFromStr(const char* str, int32_t len, DecimalVar* result);

int32_t decimalCalcTypeMod(const SDataType* pType) {
  if (IS_DECIMAL_TYPE(pType->type)) {
    return (pType->precision << 8) + pType->scale;
  }
  return 0;
}

void decimalFromTypeMod(STypeMod typeMod, uint8_t* precision, uint8_t* scale) {
  *precision = (uint8_t)((typeMod >> 8) & 0xFF);
  *scale = (uint8_t)(typeMod & 0xFF);
}

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal64* result) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_64, .words = result->words};
  code = decimalVarFromStr(str, len, &var);
  *precision = var.precision;
  *scale = var.scale;
  return code;
}

int32_t decimal128FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal128* result) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_128, .words = result->words};
  *precision = var.precision;
  *scale = var.scale;
  return code;
}

static int32_t decimalVarFromStr(const char* str, int32_t len, DecimalVar* result) {
  int32_t code = 0, pos = 0;
  result->precision = 0;
  result->scale = 0;
  bool     leadingZeroes = true, afterPoint = false;
  uint32_t places = 0;
  result->sign = 1;

  if (len == 0) return TSDB_CODE_INVALID_DATA_FMT;
  SWideIntegerOps ops =
      wideIntegerOps[result->type];  // TODO wjm currently, the meaning from type to index is not clear
  SDecimalVarOps decOps = decimalVarOps[result->type];

  // sign
  switch (str[pos]) {
    case '-':
      result->sign = -1;
    case '+':
      pos++;
    default:
      break;
  }

  for (; pos < len; ++pos) {
    switch (str[pos]) {
      case '.':
        afterPoint = true;
        leadingZeroes = false;
        break;
      case '0':
        if (leadingZeroes) break;
        if (afterPoint) {
          places++;
          break;
        }
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9': {
        leadingZeroes = false;
        ++places;
        if (result->precision + places > maxPrecision(result->type)) {
          if (afterPoint) {
            break;
          } else {
            // TODO wjm value too large
            return TSDB_CODE_INVALID_DATA_FMT;
          }
        } else {
          result->precision += places;
          if (afterPoint) {
            result->scale++;
          }
          DecimalWord ten = 10, digit = str[pos] - '0';
          ops.multiply(result->words, &ten, 1);
          ops.add(result->words, &digit, 1);
          places = 0;
          break;
        }
      }
      case 'e':
      case 'E':
        break;
      default:
        break;
    }
  }
  if (result->sign < 0) {
    DecimalWord sign = (DecimalWord)result->sign;
    ops.multiply(result->words, &sign, 1);
  }
  return code;
}

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal) {
  VALUE_SET_TRIVIAL_DATUM(pVal, dec->words[0]);
  return TSDB_CODE_SUCCESS;
}

int32_t decimal128ToDataVal(const Decimal128* dec, SValue* pVal) {
  pVal->pData = taosMemCalloc(2, sizeof(DecimalWord));
  if (!pVal->pData) return terrno;
  valueSetDatum(pVal, TSDB_DATA_TYPE_DECIMAL, dec->words, DECIMAL_WORD_NUM(Decimal128) * sizeof(DecimalWord));
  return TSDB_CODE_SUCCESS;
}

static int32_t int64Abs(DecimalWord* pInt) {
  *pInt = TABS(*pInt);
  return 0;
}

static int32_t int64Add(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  int32_t code = 0;
  Int128 sum = *pLeft;
  sum += *pRight;
  *pLeft = (DecimalWord)sum;
  return code;
}

static int32_t int64Subtract(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  int32_t code = 0;
  Int128 res = *pLeft;
  res -= *pRight;
  *pLeft = (DecimalWord)res;
  return code;
}

static int32_t int64Multiply(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  Int128 res = *pLeft;
  res *= *pRight;
  *pLeft = (DecimalWord)res;
  return 0;
}

static int32_t int64divide(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  Int128 res = *pLeft;
  res /= *pRight;
  *pLeft = (DecimalWord)res;
  return 0;
}

static int32_t int64Mod(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  Int128 res = *pLeft;
  res %= *pRight;
  *pLeft = (DecimalWord)res;
  return 0;
}

static bool int64Lt(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  return *pLeft < *pRight;
}
static bool int64Gt(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  return *pLeft > *pRight;
}
static bool int64Eq(DecimalWord* pLeft, DecimalWord* pRight, uint8_t rightWordNum) {
  return *pLeft == *pRight;
}
static int32_t int64ToStr(DecimalWord* pInt, char *pBuf, int32_t bufLen) {
  return snprintf(pBuf, bufLen, "%"PRId64, *pInt);
}

static int64_t SCALE_MULTIPLIER_64[19] = {1LL,
                                          10LL,
                                          100LL,
                                          1000LL,
                                          10000LL,
                                          100000LL,
                                          1000000LL,
                                          10000000LL,
                                          100000000LL,
                                          1000000000LL,
                                          10000000000LL,
                                          100000000000LL,
                                          1000000000000LL,
                                          10000000000000LL,
                                          100000000000000LL,
                                          1000000000000000LL,
                                          10000000000000000LL,
                                          100000000000000000LL,
                                          1000000000000000000LL};

static int32_t decimalGetWhole(const DecimalWord* pDec, DecimalInternalType type, int8_t scale, DecimalWord* pWhole) {
  SWideIntegerOps ops = wideIntegerOps[type];
  if (type == DECIMAL_64) {
    pWhole[0] = *pDec;
    DecimalWord scaleMul = SCALE_MULTIPLIER_64[scale];
    int32_t code = ops.divide(pWhole, &scaleMul, 1);
    if (TSDB_CODE_SUCCESS != 0) {
      // TODO wjm
    }
    ops.abs(pWhole);
  } else {
    memcpy(pWhole, pDec, DECIMAL_GET_WORD_NUM(type) * sizeof(DecimalWord));
    // TODO wjm
    //ops.divide(pWhole->words, )
  }
  return 0;
}

static int32_t decimalGetFrac(const DecimalWord* pDec, DecimalInternalType type, int8_t scale, DecimalWord* pFrac) {
  SWideIntegerOps ops = wideIntegerOps[type];
  if (type == DECIMAL_64) {
    pFrac[0] = *pDec;
    DecimalWord scaleMul = SCALE_MULTIPLIER_64[scale];
    int32_t code = ops.mod(pFrac, &scaleMul, 1);
    ops.abs(pFrac);
  } else {

  }
  return 0;
}

int32_t decimalToStr(DecimalWord* pDec, int8_t precision, int8_t scale, char* pBuf, int32_t bufLen) {
  DecimalInternalType iType = DECIMAL_GET_INTERNAL_TYPE(precision);
  int32_t             wordNum = DECIMAL_GET_WORD_NUM(iType);
  SWideIntegerOps     ops = wideIntegerOps[iType];
  DecimalMax          whole = {0}, frac = {0};
  DecimalWord         zero = 0;
  int32_t             pos = 0;

  if (ops.lt(pDec, &zero, 1)) {
    pos = sprintf(pBuf, "-");
  }
  int32_t code = decimalGetWhole(pDec, iType, scale, whole.words);
  if (!ops.eq(whole.words, &zero, 1)) {
    pos += ops.toStr(whole.words, pBuf + pos, bufLen - pos);
  }
  pos += snprintf(pBuf + pos, bufLen - pos, ".");
  code = decimalGetFrac(pDec, iType, scale, frac.words);
  ops.toStr(frac.words, pBuf + pos, bufLen - pos);
  return 0;
}
