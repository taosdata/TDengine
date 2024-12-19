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

SWideIntegerOps wideIntegerOps [2] = {
  {DECIMAL_WORD_NUM(Decimal64), 0, 0, 0},
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

uint8_t maxPrecision(DecimalInternalType type) {
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

#define DECIMAL_INTERNAL_TYPE(TYPE) DECIMAL_WORD_NUM(TYPE) - 1

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal64* result) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_INTERNAL_TYPE(Decimal64), .words = result->words};
  code = decimalVarFromStr(str, len, &var);
  return code;
}

int32_t decimal128FromStr(const char* str, int32_t len, uint8_t* precision, uint8_t* scale, Decimal128* result) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_INTERNAL_TYPE(Decimal128), .words = result->words};
  return code;
}

static int32_t decimalVarFromStr(const char* str, int32_t len, DecimalVar* result) {
  int32_t code = 0, pos = 0;
  result->precision = 0;
  result->scale = 0;
  bool     leadingZeroes = true, afterPoint = false;
  uint32_t places = 0;

  if (len == 0) return TSDB_CODE_INVALID_DATA_FMT;
  SWideIntegerOps ops =
      wideIntegerOps[result->type];  // TODO wjm currently, the meaning from type to index is not clear
  SDecimalVarOps decOps = decimalVarOps[result->type];

  // sign
  switch (str[pos]) {
    case '-':
      result->sign = -1;
    case '+':
      result->sign = 1;
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
    ++pos;
  }
  DecimalWord sign = result->sign;
  ops.multiply(result->words, &sign, 1);
  return code;
}

int32_t varMultiply(DecimalVar* pLeft, const DecimalVar* pRight) {
  int32_t code = 0;

  return code;
}

int32_t varAdd(DecimalVar* pLeft, const DecimalVar* pRight) {
  int32_t code = 0;

  return code;
}

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal) {
  pVal->words = taosMemoryCalloc(1, sizeof(DecimalWord));
  if (!pVal->words) return terrno;
  pVal->wordNum = DECIMAL_WORD_NUM(Decimal64);
  pVal->words[0] = *dec->words;
  return TSDB_CODE_SUCCESS;
}

int32_t decimal128ToDataVal(const Decimal128* dec, SValue* pVal) {
  pVal->words = taosMemCalloc(2, sizeof(DecimalWord));
  if (!pVal->words) return terrno;
  pVal->wordNum = DECIMAL_WORD_NUM(Decimal128);
  memcpy(pVal->words, dec->words, sizeof(dec->words));
  return TSDB_CODE_SUCCESS;
}
