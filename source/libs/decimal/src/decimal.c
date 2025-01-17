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
#include "tdataformat.h"
#include "wideInteger.h"

typedef enum DecimalInternalType {
  DECIMAL_64 = 0,
  DECIMAL_128 = 1,
} DecimalInternalType;

#define DECIMAL_GET_INTERNAL_TYPE(dataType) ((dataType) == TSDB_DATA_TYPE_DECIMAL ? DECIMAL_128 : DECIMAL_64)
#define DECIMAL_GET_WORD_NUM(decimalInternalType) \
  ((decimalInternalType) == DECIMAL_64 ? DECIMAL_WORD_NUM(Decimal64) : DECIMAL_WORD_NUM(Decimal128))
static SDecimalOps* getDecimalOpsImp(DecimalInternalType t);

#define DECIMAL_MIN_ADJUSTED_SCALE 6

typedef struct DecimalVar {
  DecimalInternalType type;
  uint8_t             precision;
  uint8_t             scale;
  int32_t             exponent;
  int8_t              sign;
  DecimalType*        pDec;
} DecimalVar;

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

static const uint8_t typeConvertDecimalPrec[] = {
    0, 1, 3, 5, 10, 19, TSDB_DECIMAL128_MAX_PRECISION, TSDB_DECIMAL_MAX_PRECISION, 0, 19, 10, 3, 5, 10, 20, 0,
    0, 0, 0, 0, 0,  0};

int32_t decimalGetRetType(const SDataType* pLeftT, const SDataType* pRightT, EOperatorType opType,
                          SDataType* pOutType) {
  if (IS_FLOAT_TYPE(pLeftT->type) || IS_FLOAT_TYPE(pRightT->type) || IS_VAR_DATA_TYPE(pLeftT->type) ||
      IS_VAR_DATA_TYPE(pRightT->type)) {
    pOutType->type = TSDB_DATA_TYPE_DOUBLE;
    pOutType->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
    return 0;
  }

  if (IS_NULL_TYPE(pLeftT->type) || IS_NULL_TYPE(pRightT->type)) {
    pOutType->type = TSDB_DATA_TYPE_NULL;
    pOutType->bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return 0;
  }

  // TODO wjm check not supported types
  uint8_t p1 = pLeftT->precision, s1 = pLeftT->scale, p2 = pRightT->precision, s2 = pRightT->scale;

  if (!IS_DECIMAL_TYPE(pLeftT->type)) {
    p1 = typeConvertDecimalPrec[pLeftT->type];
    s1 = 0;
  }
  if (!IS_DECIMAL_TYPE(pRightT->type)) {
    p2 = typeConvertDecimalPrec[pRightT->type];
    s2 = 0;
  }

  switch (opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
      pOutType->scale = TMAX(s1, s2);
      pOutType->precision = TMAX(p1 - s1, p2 - s2) + pOutType->scale + 1;
      break;
    case OP_TYPE_MULTI:
      pOutType->scale = s1 + s2;
      pOutType->precision = p1 + p2 + 1;
      break;
    case OP_TYPE_DIV:
      pOutType->scale = TMAX(s1 + p2 + 1, DECIMAL_MIN_ADJUSTED_SCALE);
      pOutType->precision = p1 - s1 + s2 + pOutType->scale;
      break;
    case OP_TYPE_REM:
      pOutType->scale = TMAX(s1, s2);
      pOutType->precision = TMIN(p1 - s1, p2 - s2) + pOutType->scale;
      break;
    default:
      return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  if (pOutType->precision > TSDB_DECIMAL_MAX_PRECISION) {
    int8_t minScale = TMIN(DECIMAL_MIN_ADJUSTED_SCALE, pOutType->scale);
    int8_t delta = pOutType->precision - TSDB_DECIMAL_MAX_PRECISION;
    pOutType->precision = TSDB_DECIMAL_MAX_PRECISION;
    pOutType->scale = TMAX(minScale, (int8_t)(pOutType->scale) - delta);
  }
  pOutType->type = TSDB_DATA_TYPE_DECIMAL;
  pOutType->bytes = tDataTypes[pOutType->type].bytes;
  return 0;
}

static int32_t decimalVarFromStr(const char* str, int32_t len, DecimalVar* result);

static int32_t decimalVarFromStr(const char* str, int32_t len, DecimalVar* result) {
  int32_t code = 0, pos = 0;
  result->precision = 0;
  result->scale = 0;
  bool     leadingZeroes = true, afterPoint = false;
  uint32_t places = 0;
  result->sign = 1;

  if (len == 0) return TSDB_CODE_INVALID_DATA_FMT;
  SDecimalOps* pOps = getDecimalOpsImp(result->type);

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
            result->scale += places;
          }
          DecimalWord ten = 10, digit = str[pos] - '0';
          while (places-- > 0) {
            pOps->multiply(result->pDec, &ten, 1);
          }
          pOps->add(result->pDec, &digit, 1);
          places = 0;
          break;
        }
      }
      case 'e':
      case 'E':
        // TODO wjm handle E
        break;
      default:
        break;
    }
  }
  if (result->sign < 0) {
    pOps->negate(result->pDec);
  }
  return code;
}

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal) {
  VALUE_SET_TRIVIAL_DATUM(pVal, DECIMAL64_GET_VALUE(dec));
  return TSDB_CODE_SUCCESS;
}

int32_t decimal128ToDataVal(Decimal128* dec, SValue* pVal) {
  void* pV = taosMemCalloc(1, sizeof(Decimal128));
  if (!pV) return terrno;
  memcpy(pV, dec, DECIMAL_WORD_NUM(Decimal128) * sizeof(DecimalWord));
  valueSetDatum(pVal, TSDB_DATA_TYPE_DECIMAL, pV, DECIMAL_WORD_NUM(Decimal128) * sizeof(DecimalWord));
  return TSDB_CODE_SUCCESS;
}

// TODO wjm use uint64_t ???
static Decimal64 SCALE_MULTIPLIER_64[19] = {1LL,
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

static const Decimal64 decimal64Zero = {0};
#define DECIMAL64_ONE  SCALE_MULTIPLIER_64[0]
#define DECIMAL64_ZERO decimal64Zero

#define DECIMAL64_GET_MAX(precision, pMax)                                \
  do {                                                                    \
    *(pMax) = SCALE_MULTIPLIER_64[precision];                             \
    decimal64Subtract(pMax, &DECIMAL64_ONE, DECIMAL_WORD_NUM(Decimal64)); \
  } while (0)

#define DECIMAL64_SIGN(pDec) (1 | (DECIMAL64_GET_VALUE(pDec) >> 63))

static int32_t decimalGetWhole(const DecimalType* pDec, DecimalInternalType type, int8_t scale, DecimalType* pWhole) {
  SDecimalOps* pOps = getDecimalOpsImp(type);
  if (type == DECIMAL_64) {
    DECIMAL64_CLONE(pWhole, pDec);
    Decimal64 scaleMul = SCALE_MULTIPLIER_64[scale];
    pOps->divide(pWhole, &scaleMul, 1, NULL);
    if (TSDB_CODE_SUCCESS != 0) {
      // TODO wjm
    }
    pOps->abs(pWhole);
  } else {
    memcpy(pWhole, pDec, DECIMAL_GET_WORD_NUM(type) * sizeof(DecimalWord));
    // TODO wjm
    // pOps.divide(pWhole, )
  }
  return 0;
}

static int32_t decimalGetFrac(const DecimalType* pDec, DecimalInternalType type, int8_t scale, DecimalType* pFrac) {
  SDecimalOps* pOps = getDecimalOpsImp(type);
  if (type == DECIMAL_64) {
    DECIMAL64_CLONE(pFrac, pDec);
    Decimal64 scaleMul = SCALE_MULTIPLIER_64[scale];
    pOps->mod(pFrac, &scaleMul, 1);
    pOps->abs(pFrac);
  } else {
  }
  return 0;
}

static void    decimal64Negate(DecimalType* pInt);
static void    decimal64Abs(DecimalType* pInt);
static void    decimal64Add(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static void    decimal64Subtract(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static void    decimal64Multiply(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static void    decimal64divide(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum,
                               DecimalType* pRemainder);
static void    decimal64Mod(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static bool    decimal64Lt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static bool    decimal64Gt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static bool    decimal64Eq(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static int32_t decimal64ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen);
static void    decimal64ScaleDown(Decimal64* pDec, uint8_t scaleDown);
static void    decimal64ScaleUp(Decimal64* pDec, uint8_t scaleUp);
void           decimal64ScaleTo(Decimal64* pDec, uint8_t oldScale, uint8_t newScale);

static void    decimal128Negate(DecimalType* pInt);
static void    decimal128Abs(DecimalType* pWord);
static void    decimal128Add(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static void    decimal128Subtract(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static void    decimal128Multiply(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static void    decimal128Divide(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum,
                                DecimalType* pRemainder);
static void    decimal128Mod(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static bool    decimal128Lt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static bool    decimal128Gt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static bool    decimal128Eq(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum);
static int32_t decimal128ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen);
void           decimal128ScaleTo(Decimal128* pDec, uint8_t oldScale, uint8_t newScale);
void           decimal128ScaleDown(Decimal128* pDec, uint8_t scaleDown);
void           decimal128ScaleUp(Decimal128* pDec, uint8_t scaleUp);
int32_t        decimal128CountLeadingBinaryZeros(const Decimal128* pDec);

SDecimalOps decimal64Ops = {decimal64Negate,   decimal64Abs,    decimal64Add,  decimal64Subtract,
                            decimal64Multiply, decimal64divide, decimal64Mod,  decimal64Lt,
                            decimal64Gt,       decimal64Eq,     decimal64ToStr};
SDecimalOps decimal128Ops = {decimal128Negate,   decimal128Abs,    decimal128Add,  decimal128Subtract,
                             decimal128Multiply, decimal128Divide, decimal128Mod,  decimal128Lt,
                             decimal128Gt,       decimal128Eq,     decimal128ToStr};

static SDecimalOps* getDecimalOpsImp(DecimalInternalType t) {
  switch (t) {
    case DECIMAL_128:
      return &decimal128Ops;
    case DECIMAL_64:
      return &decimal64Ops;
    default:
      return NULL;
  }
}
SDecimalOps* getDecimalOps(int8_t dataType) { return getDecimalOpsImp(DECIMAL_GET_INTERNAL_TYPE(dataType)); }

void makeDecimal64(Decimal64* pDec64, int64_t w) { DECIMAL64_SET_VALUE(pDec64, w); }

// TODO wjm handle overflow problem of DecimalWord
// it's impossible that pDec == INT64_MIN, only 18 digits can be accepted.
void decimal64Negate(DecimalType* pInt) {
  Decimal64* pDec = pInt;
  DECIMAL64_SET_VALUE(pDec, -DECIMAL64_GET_VALUE(pDec));
}
void decimal64Abs(DecimalType* pInt) {
  Decimal64* pDec = pInt;
  DECIMAL64_SET_VALUE(pDec, TABS(DECIMAL64_GET_VALUE(pDec)));
}
void decimal64Add(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal64*       pDecL = pLeft;
  const Decimal64* pDecR = pRight;
  DECIMAL64_SET_VALUE(pDecL, SAFE_INT64_ADD(DECIMAL64_GET_VALUE(pDecL), DECIMAL64_GET_VALUE(pDecR)));
}
void decimal64Subtract(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal64*       pDecL = pLeft;
  const Decimal64* pDecR = pRight;
  DECIMAL64_SET_VALUE(pDecL, SAFE_INT64_SUBTRACT(DECIMAL64_GET_VALUE(pDecL), DECIMAL64_GET_VALUE(pDecR)));
}
void decimal64Multiply(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal64* pDecL = pLeft;
  Decimal64  decR = *((Decimal64*)pRight);
  bool       sign = DECIMAL64_SIGN(pDecL) != DECIMAL64_SIGN(&decR);
  decimal64Abs(pLeft);
  decimal64Abs(&decR);
  uint64_t x = DECIMAL64_GET_VALUE(pDecL), y = DECIMAL64_GET_VALUE(&decR);
  x *= y;
  DECIMAL64_SET_VALUE(pDecL, x);
  if (sign) decimal64Negate(pDecL);
}
void decimal64divide(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum, DecimalType* pRemainder) {
  Decimal64* pDecL = pLeft;
  Decimal64  decR = *((Decimal64*)pRight);
  Decimal64* pDecRemainder = pRemainder;
  bool       sign = DECIMAL64_SIGN(pDecL) != DECIMAL64_SIGN(&decR);
  decimal64Abs(pDecL);
  decimal64Abs(&decR);
  uint64_t x = DECIMAL64_GET_VALUE(pDecL), y = DECIMAL64_GET_VALUE(&decR);
  uint64_t z = x;
  x /= y;
  DECIMAL64_SET_VALUE(pDecL, x);
  if (sign) decimal64Negate(pDecL);
  if (pDecRemainder) {
    z %= y;
    DECIMAL64_SET_VALUE(pDecRemainder, z);
  }
}
void decimal64Mod(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal64  remainder = {0};
  Decimal64* pDec = pLeft;
  decimal64divide(pLeft, pRight, rightWordNum, &remainder);
  DECIMAL64_SET_VALUE(pDec, DECIMAL64_GET_VALUE(&remainder));
}

bool decimal64Lt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  const Decimal64 *pDecL = pLeft, *pDecR = pRight;
  return DECIMAL64_GET_VALUE(pDecL) < DECIMAL64_GET_VALUE(pDecR);
}
bool decimal64Gt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  return DECIMAL64_GET_VALUE((Decimal64*)pLeft) > DECIMAL64_GET_VALUE((Decimal64*)pRight);
}
bool decimal64Eq(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  return DECIMAL64_GET_VALUE((Decimal64*)pLeft) == DECIMAL64_GET_VALUE(((Decimal64*)pRight));
}
int32_t decimal64ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen) {
  Decimal     whole = {0}, frac = {0};
  DecimalWord zero = 0;  // TODO wjm remove zero, use SIGN
  int32_t     pos = 0;

  if (DECIMAL64_SIGN((Decimal64*)pInt) == -1) {
    pos = sprintf(pBuf, "-");
  }
  int32_t code = decimalGetWhole(pInt, DECIMAL_64, scale, &whole);
  pos += snprintf(pBuf + pos, bufLen - pos, "%" PRId64, DECIMAL64_GET_VALUE(&whole));
  if (scale > 0) {
    decimalGetFrac(pInt, DECIMAL_64, scale, &frac);
    if (DECIMAL64_GET_VALUE(&frac) != 0 || DECIMAL64_GET_VALUE(&whole) != 0) {
      TAOS_STRCAT(pBuf + pos, ".");
      pos += 1;
      char format[16] = "\%0";
      snprintf(format + 2, 14, "%" PRIu8 PRIu64, scale);
      snprintf(pBuf + pos, bufLen - pos, format, DECIMAL64_GET_VALUE(&frac));
    }
  }
  return 0;
}

// TODO wjm handle endian problem
#define DECIMAL128_LOW_WORD(pDec)           (uint64_t)((pDec)->words[0])
#define DECIMAL128_SET_LOW_WORD(pDec, val)  (pDec)->words[0] = val
#define DECIMAL128_HIGH_WORD(pDec)          (int64_t)((pDec)->words[1])
#define DECIMAL128_SET_HIGH_WORD(pDec, val) *(int64_t*)((pDec)->words + 1) = val
// return 1 if positive or zero, else return -1
#define DECIMAL128_SIGN(pDec) (1 | (DECIMAL128_HIGH_WORD(pDec) >> 63))

// TODO wjm handle endian problem
#define DEFINE_DECIMAL128(lo, hi) {lo, hi}

static const Decimal128 SCALE_MULTIPLIER_128[38 + 1] = {
    DEFINE_DECIMAL128(1LL, 0),
    DEFINE_DECIMAL128(10LL, 0),
    DEFINE_DECIMAL128(100LL, 0),
    DEFINE_DECIMAL128(1000LL, 0),
    DEFINE_DECIMAL128(10000LL, 0),
    DEFINE_DECIMAL128(100000LL, 0),
    DEFINE_DECIMAL128(1000000LL, 0),
    DEFINE_DECIMAL128(10000000LL, 0),
    DEFINE_DECIMAL128(100000000LL, 0),
    DEFINE_DECIMAL128(1000000000LL, 0),
    DEFINE_DECIMAL128(10000000000LL, 0),
    DEFINE_DECIMAL128(100000000000LL, 0),
    DEFINE_DECIMAL128(1000000000000LL, 0),
    DEFINE_DECIMAL128(10000000000000LL, 0),
    DEFINE_DECIMAL128(100000000000000LL, 0),
    DEFINE_DECIMAL128(1000000000000000LL, 0),
    DEFINE_DECIMAL128(10000000000000000LL, 0),
    DEFINE_DECIMAL128(100000000000000000LL, 0),
    DEFINE_DECIMAL128(1000000000000000000LL, 0),
    DEFINE_DECIMAL128(10000000000000000000ULL, 0LL),
    DEFINE_DECIMAL128(7766279631452241920ULL, 5LL),
    DEFINE_DECIMAL128(3875820019684212736ULL, 54LL),
    DEFINE_DECIMAL128(1864712049423024128ULL, 542LL),
    DEFINE_DECIMAL128(200376420520689664ULL, 5421LL),
    DEFINE_DECIMAL128(2003764205206896640ULL, 54210LL),
    DEFINE_DECIMAL128(1590897978359414784ULL, 542101LL),
    DEFINE_DECIMAL128(15908979783594147840ULL, 5421010LL),
    DEFINE_DECIMAL128(11515845246265065472ULL, 54210108LL),
    DEFINE_DECIMAL128(4477988020393345024ULL, 542101086LL),
    DEFINE_DECIMAL128(7886392056514347008ULL, 5421010862LL),
    DEFINE_DECIMAL128(5076944270305263616ULL, 54210108624LL),
    DEFINE_DECIMAL128(13875954555633532928ULL, 542101086242LL),
    DEFINE_DECIMAL128(9632337040368467968ULL, 5421010862427LL),
    DEFINE_DECIMAL128(4089650035136921600ULL, 54210108624275LL),
    DEFINE_DECIMAL128(4003012203950112768ULL, 542101086242752LL),
    DEFINE_DECIMAL128(3136633892082024448ULL, 5421010862427522LL),
    DEFINE_DECIMAL128(12919594847110692864ULL, 54210108624275221LL),
    DEFINE_DECIMAL128(68739955140067328ULL, 542101086242752217LL),
    DEFINE_DECIMAL128(687399551400673280ULL, 5421010862427522170LL),
};

static const Decimal128 decimal128Zero = DEFINE_DECIMAL128(0, 0);
static const Decimal128 decimal128Max = DEFINE_DECIMAL128(687399551400673280ULL - 1, 5421010862427522170LL);

#define DECIMAL128_ZERO decimal128Zero
#define DECIMAL128_MAX  decimal128Max
#define DECIMAL128_ONE  SCALE_MULTIPLIER_128[0]
#define DECIMAL128_TEN  SCALE_MULTIPLIER_128[1]

// To calculate how many bits for integer X.
// eg. 999(3 digits) -> 1111100111(10 bits) -> bitsForNumDigits[3] = 10
static const int32_t bitsForNumDigits[] = {0,  4,  7,  10, 14,  17,  20,  24,  27,  30,  34,  37,  40,
                                           44, 47, 50, 54, 57,  60,  64,  67,  70,  74,  77,  80,  84,
                                           87, 90, 94, 97, 100, 103, 107, 110, 113, 117, 120, 123, 127};

// TODO wjm pre define it??  actually, its MAX_INTEGER, not MAX
#define DECIMAL128_GET_MAX(precision, pMax)                                  \
  do {                                                                       \
    *(pMax) = SCALE_MULTIPLIER_128[precision];                               \
    decimal128Subtract(pMax, &DECIMAL128_ONE, DECIMAL_WORD_NUM(Decimal128)); \
  } while (0)

void makeDecimal128(Decimal128* pDec128, int64_t hi, uint64_t low) {
  DECIMAL128_SET_HIGH_WORD(pDec128, hi);
  DECIMAL128_SET_LOW_WORD(pDec128, low);
}

static void makeDecimal128FromDecimal64(Decimal128* pTarget, Decimal64 decimal64) {
  bool negative = false;
  if (DECIMAL64_SIGN(&decimal64) == -1) {
    decimal64Negate(&decimal64);
    negative = true;
  }
  makeDecimal128(pTarget, 0, DECIMAL64_GET_VALUE(&decimal64));
  if (negative) decimal128Negate(pTarget);
}

static void decimal128Negate(DecimalType* pWord) {
  Decimal128* pDec = (Decimal128*)pWord;
  uint64_t    lo = ~DECIMAL128_LOW_WORD(pDec) + 1;
  int64_t     hi = ~DECIMAL128_HIGH_WORD(pDec);
  if (lo == 0) hi = SAFE_INT64_ADD(hi, 1);
  makeDecimal128(pDec, hi, lo);
}

static void decimal128Abs(DecimalType* pWord) {
  if (DECIMAL128_SIGN((Decimal128*)pWord) == -1) {
    decimal128Negate(pWord);
  }
}

// TODO wjm put it out of decimal128 functions
#define DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pTarget, rightDec, pWord) \
  if (rightWordNum != WORD_NUM(Decimal128)) {                                   \
    Decimal64 d64 = {0};                                                        \
    makeDecimal64(&d64, *(int64_t*)pWord);                                      \
    makeDecimal128FromDecimal64(&rightDec, d64);                                \
    pTarget = &rightDec;                                                        \
  }

static void decimal128Add(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  int64_t  hi = SAFE_INT64_ADD(DECIMAL128_HIGH_WORD(pLeftDec), DECIMAL128_HIGH_WORD(pRightDec));
  uint64_t lo = DECIMAL128_LOW_WORD(pLeftDec) + DECIMAL128_LOW_WORD(pRightDec);
  hi = SAFE_INT64_ADD(hi, lo < DECIMAL128_LOW_WORD(pLeftDec));
  makeDecimal128(pLeftDec, hi, lo);
}

static void decimal128Subtract(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  int64_t  hi = SAFE_INT64_SUBTRACT(DECIMAL128_HIGH_WORD(pLeftDec), DECIMAL128_HIGH_WORD(pRightDec));
  uint64_t lo = DECIMAL128_LOW_WORD(pLeftDec) - DECIMAL128_LOW_WORD(pRightDec);
  hi = SAFE_INT64_SUBTRACT(hi, lo > DECIMAL128_LOW_WORD(pLeftDec));
  makeDecimal128(pLeftDec, hi, lo);
}

static void decimal128Multiply(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  bool       negate = DECIMAL128_SIGN(pLeftDec) != DECIMAL128_SIGN(pRightDec);
  Decimal128 x = *pLeftDec, y = *pRightDec;
  decimal128Abs(&x);
  decimal128Abs(&y);

  UInt128 res = {0}, tmp = {0};
  makeUInt128(&res, DECIMAL128_HIGH_WORD(&x), DECIMAL128_LOW_WORD(&x));
  makeUInt128(&tmp, DECIMAL128_HIGH_WORD(&y), DECIMAL128_LOW_WORD(&y));
  uInt128Multiply(&res, &tmp);
  makeDecimal128(pLeftDec, uInt128Hi(&res), uInt128Lo(&res));
  if (negate) decimal128Negate(pLeftDec);
}

static bool decimal128Lt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  // TODO wjm pRightDec use const
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return DECIMAL128_HIGH_WORD(pLeftDec) < DECIMAL128_HIGH_WORD(pRightDec) ||
         (DECIMAL128_HIGH_WORD(pLeftDec) == DECIMAL128_HIGH_WORD(pRightDec) &&
          DECIMAL128_LOW_WORD(pLeftDec) < DECIMAL128_LOW_WORD(pRightDec));
}

static void decimal128Divide(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum,
                             DecimalType* pRemainder) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight, *pRemainderDec = (Decimal128*)pRemainder;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  bool       negate = DECIMAL128_SIGN(pLeftDec) != DECIMAL128_SIGN(pRightDec);
  UInt128    a = {0}, b = {0}, c = {0}, d = {0};
  Decimal128 x = *pLeftDec, y = *pRightDec;
  decimal128Abs(&x);
  decimal128Abs(&y);
  makeUInt128(&a, DECIMAL128_HIGH_WORD(&x), DECIMAL128_LOW_WORD(&x));
  makeUInt128(&d, DECIMAL128_HIGH_WORD(&x), DECIMAL128_LOW_WORD(&x));
  makeUInt128(&b, DECIMAL128_HIGH_WORD(&y), DECIMAL128_LOW_WORD(&y));
  // TODO wjm refine the interface, so that here do not need to copy a
  uInt128Divide(&a, &b);
  uInt128Mod(&d, &b);
  makeDecimal128(pLeftDec, uInt128Hi(&a), uInt128Lo(&a));
  if (pRemainder) makeDecimal128(pRemainderDec, uInt128Hi(&d), uInt128Lo(&d));
  if (negate) decimal128Negate(pLeftDec);
  if (DECIMAL128_SIGN(pLeftDec) == -1 && pRemainder) decimal128Negate(pRemainderDec);
}

static void decimal128Mod(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {}

static bool decimal128Gt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return decimal128Lt(pRightDec, pLeftDec, WORD_NUM(Decimal128));
}

static bool decimal128Eq(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return DECIMAL128_HIGH_WORD(pLeftDec) == DECIMAL128_HIGH_WORD(pRightDec) &&
         DECIMAL128_LOW_WORD(pLeftDec) == DECIMAL128_LOW_WORD(pRightDec);
}

static void extractDecimal128Digits(const Decimal128* pDec, uint64_t* digits, int32_t* digitNum) {
#define DIGIT_NUM_ONCE 18
  UInt128 a = {0};
  UInt128 b = {0};
  *digitNum = 0;
  makeUInt128(&a, DECIMAL128_HIGH_WORD(pDec), DECIMAL128_LOW_WORD(pDec));
  while (!uInt128Eq(&a, &uInt128Zero)) {
    uint64_t hi = a >> 64;  // TODO wjm ???
    uint64_t lo = a;

    uint64_t hiQuotient = hi / k1e18;
    uint64_t hiRemainder = hi % k1e18;
    makeUInt128(&b, hiRemainder, lo);
    uInt128Divide(&b, &uInt128_1e18);
    uint64_t loQuotient = uInt128Lo(&b);
    makeUInt128(&b, hiRemainder, lo);
    uInt128Mod(&b, &uInt128_1e18);
    uint64_t loRemainder = uInt128Lo(&b);
    makeUInt128(&a, hiQuotient, loQuotient);
    digits[(*digitNum)++] = loRemainder;
  }
}

// TODO wjm checkBuflen
static int32_t decimal128ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen) {
  const Decimal128* pDec = (const Decimal128*)pInt;
  bool              negative = DECIMAL128_SIGN(pDec) == -1;
  uint64_t          segments[3] = {0};
  int32_t           digitNum = 0;
  char              buf[64] = {0};
  int32_t           len = 0;
  if (negative) {
    Decimal128 copy = {0};
    makeDecimal128(&copy, DECIMAL128_HIGH_WORD(pDec), DECIMAL128_LOW_WORD(pDec));
    decimal128Abs(&copy);
    extractDecimal128Digits(&copy, segments, &digitNum);
    buf[0] = '-';
    len = 1;
  } else {
    extractDecimal128Digits(pDec, segments, &digitNum);
  }
  if (digitNum == 0) {
    TAOS_STRNCAT(pBuf, "0", 2);
    return 0;
  }
  for (int32_t i = digitNum - 1; i >= 0; --i) {
    len += snprintf(buf + len, 64 - len, i == digitNum - 1 ? "%" PRIu64 : "%018" PRIu64, segments[i]);
  }
  int32_t wholeLen = len - scale;
  if (wholeLen > 0) {
    TAOS_STRNCAT(pBuf, buf, wholeLen);
  } else {
    TAOS_STRNCAT(pBuf, "0", 2);
  }
  if (scale > 0) {
    TAOS_STRNCAT(pBuf, ".", 2);
    TAOS_STRNCAT(pBuf, buf + TMAX(0, wholeLen), scale);
  }
  return 0;
}
// TODO wjm refine this interface
int32_t decimalToStr(const DecimalType* pDec, int8_t dataType, int8_t precision, int8_t scale, char* pBuf,
                     int32_t bufLen) {
  pBuf[0] = '\0';
  DecimalInternalType iType = DECIMAL_GET_INTERNAL_TYPE(dataType);
  switch (iType) {
    case DECIMAL_64:
      return decimal64ToStr(pDec, scale, pBuf, bufLen);
    case DECIMAL_128:
      return decimal128ToStr(pDec, scale, pBuf, bufLen);
    default:
      break;
  }
  return 0;
}

static void decimalAddLargePositive(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                                    const SDataType* pOT) {
  Decimal wholeX = *pX, wholeY = *pY, fracX = {0}, fracY = {0};
  decimal128Divide(&wholeX, &SCALE_MULTIPLIER_128[pXT->scale], WORD_NUM(Decimal), &fracX);
  decimal128Divide(&wholeY, &SCALE_MULTIPLIER_128[pYT->scale], WORD_NUM(Decimal), &fracY);

  uint8_t maxScale = TMAX(pXT->scale, pYT->scale);
  decimal128ScaleUp(&fracX, maxScale - pXT->scale);
  decimal128ScaleUp(&fracY, maxScale - pYT->scale);

  Decimal pMultiplier = SCALE_MULTIPLIER_128[maxScale];
  Decimal right = fracX;
  Decimal carry = {0};
  decimal128Subtract(&pMultiplier, &fracY, WORD_NUM(Decimal));
  if (!decimal128Gt(&pMultiplier, &fracX, WORD_NUM(Decimal))) {
    decimal128Subtract(&right, &pMultiplier, WORD_NUM(Decimal));
    makeDecimal128(&carry, 0, 1);
  } else {
    decimal128Add(&right, &fracY, WORD_NUM(Decimal));
  }

  decimal128ScaleDown(&right, maxScale - pOT->scale);
  decimal128Add(&wholeX, &wholeY, WORD_NUM(Decimal));
  decimal128Add(&wholeX, &carry, WORD_NUM(Decimal));
  decimal128Multiply(&wholeX, &SCALE_MULTIPLIER_128[pOT->scale], WORD_NUM(Decimal));
  decimal128Add(&wholeX, &right, WORD_NUM(Decimal));
  *pX = wholeX;
}

static void decimalAddLargeNegative(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                                    const SDataType* pOT) {
  Decimal wholeX = *pX, wholeY = *pY, fracX = {0}, fracY = {0};
  decimal128Divide(&wholeX, &SCALE_MULTIPLIER_128[pXT->scale], WORD_NUM(Decimal), &fracX);
  decimal128Divide(&wholeY, &SCALE_MULTIPLIER_128[pYT->scale], WORD_NUM(Decimal), &fracY);

  uint8_t maxScale = TMAX(pXT->scale, pYT->scale);
  decimal128ScaleUp(&fracX, maxScale - pXT->scale);
  decimal128ScaleUp(&fracY, maxScale - pYT->scale);

  decimal128Add(&wholeX, &wholeY, WORD_NUM(Decimal));
  decimal128Add(&fracX, &fracY, WORD_NUM(Decimal));

  if (DECIMAL128_SIGN(&wholeX) == -1 && decimal128Gt(&fracX, &DECIMAL128_ZERO, WORD_NUM(Decimal128))) {
    decimal128Add(&wholeX, &DECIMAL128_ONE, WORD_NUM(Decimal));
    decimal128Subtract(&fracX, &SCALE_MULTIPLIER_128[maxScale], WORD_NUM(Decimal));
  } else if (decimal128Gt(&wholeX, &DECIMAL128_ZERO, WORD_NUM(Decimal128)) && DECIMAL128_SIGN(&fracX) == -1) {
    decimal128Subtract(&wholeX, &DECIMAL128_ONE, WORD_NUM(Decimal));
    decimal128Add(&fracX, &SCALE_MULTIPLIER_128[maxScale], WORD_NUM(Decimal));
  }

  decimal128ScaleDown(&fracX, maxScale - pOT->scale);
  decimal128Multiply(&wholeX, &SCALE_MULTIPLIER_128[pOT->scale], WORD_NUM(Decimal));
  decimal128Add(&wholeX, &fracX, WORD_NUM(Decimal));
  *pX = wholeX;
}

static void decimalAdd(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                       const SDataType* pOT) {
  if (pOT->precision < TSDB_DECIMAL_MAX_PRECISION) {
    uint8_t maxScale = TMAX(pXT->scale, pYT->scale);
    Decimal tmpY = *pY;
    decimal128ScaleTo(pX, pXT->scale, maxScale);
    decimal128ScaleTo(&tmpY, pYT->scale, maxScale);
    decimal128Add(pX, &tmpY, WORD_NUM(Decimal));
  } else {
    int8_t signX = DECIMAL128_SIGN(pX), signY = DECIMAL128_SIGN(pY);
    if (signX == 1 && signY == 1) {
      decimalAddLargePositive(pX, pXT, pY, pYT, pOT);
    } else if (signX == -1 && signY == -1) {
      decimal128Negate(pX);
      Decimal y = *pY;
      decimal128Negate(&y);
      decimalAddLargePositive(pX, pXT, &y, pYT, pOT);
      decimal128Negate(pX);
    } else {
      decimalAddLargeNegative(pX, pXT, pY, pYT, pOT);
    }
  }
}

static int32_t decimalMultiply(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                               const SDataType* pOT) {
  if (pOT->precision < TSDB_DECIMAL_MAX_PRECISION) {
    decimal128Multiply(pX, pY, WORD_NUM(Decimal));
  } else if (decimal128Eq(pX, &DECIMAL128_ZERO, WORD_NUM(Decimal)) ||
             decimal128Eq(pY, &DECIMAL128_ZERO, WORD_NUM(Decimal))) {
    makeDecimal128(pX, 0, 0);
  } else {
    int8_t  deltaScale = pXT->scale + pYT->scale - pOT->scale;
    Decimal xAbs = *pX, yAbs = *pY;
    decimal128Abs(&xAbs);
    decimal128Abs(&yAbs);
    if (deltaScale == 0) {
      // no need to trim scale
      Decimal max = DECIMAL128_MAX;

      decimal128Divide(&max, &yAbs, WORD_NUM(Decimal), NULL);
      if (decimal128Gt(&xAbs, &max, WORD_NUM(Decimal))) {
        return TSDB_CODE_DECIMAL_OVERFLOW;
      } else {
        decimal128Multiply(pX, pY, WORD_NUM(Decimal));
      }
    } else {
      int32_t leadingZeros = decimal128CountLeadingBinaryZeros(&xAbs) + decimal128CountLeadingBinaryZeros(&yAbs);
      if (leadingZeros <= 128) {
        // need to trim scale
        return TSDB_CODE_DECIMAL_OVERFLOW;
      } else {
        // no need to trim scale
        if (deltaScale <= 38) {
          decimal128Multiply(pX, pY, WORD_NUM(Decimal));
          decimal128ScaleDown(pX, deltaScale);
        } else {
          makeDecimal128(pX, 0, 0);
        }
      }
    }
  }
  return 0;
}

int32_t decimalDivide(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                      const SDataType* pOT) {
  if (decimal128Eq(pY, &DECIMAL128_ZERO, WORD_NUM(Decimal))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;  // TODO wjm divide zero error
  }

  int8_t deltaScale = pOT->scale + pYT->scale - pXT->scale;
  assert(deltaScale >= 0);

  Decimal xTmp = *pX;
  decimal128Abs(&xTmp);
  int32_t bitsOccupied = 128 - decimal128CountLeadingBinaryZeros(&xTmp);
  if (bitsOccupied + bitsForNumDigits[deltaScale] <= 127) {
    xTmp = *pX;
    decimal128ScaleUp(&xTmp, deltaScale);
    Decimal remainder = {0};
    decimal128Divide(&xTmp, pY, WORD_NUM(Decimal), &remainder);

    Decimal tmpY = *pY, two = DEFINE_DECIMAL128(2, 0);
    decimal64Abs(&tmpY);
    decimal128Multiply(&remainder, &two, WORD_NUM(Decimal));
    decimal128Abs(&remainder);
    if (!decimal128Lt(&remainder, &tmpY, WORD_NUM(Decimal))) {
      int64_t extra = (DECIMAL128_SIGN(pX) ^ DECIMAL128_SIGN(pY)) + 1;
      decimal128Add(&xTmp, &extra, WORD_NUM(Decimal64));
    }
  } else {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  *pX = xTmp;
  return 0;
}

int32_t decimalOp(EOperatorType op, const SDataType* pLeftT, const SDataType* pRightT, const SDataType* pOutT,
                  const void* pLeftData, const void* pRightData, void* pOutputData) {
  int32_t code = 0;
  // TODO wjm if output precision <= 18, no need to convert to decimal128

  Decimal   left = {0}, right = {0};
  SDataType lt = {.type = TSDB_DATA_TYPE_DECIMAL,
                  .precision = TSDB_DECIMAL_MAX_PRECISION,
                  .bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes,
                  .scale = pLeftT->scale};
  SDataType rt = {.type = TSDB_DATA_TYPE_DECIMAL,
                  .precision = TSDB_DECIMAL_MAX_PRECISION,
                  .bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes,
                  .scale = pRightT->scale};
  if (TSDB_DATA_TYPE_DECIMAL != pLeftT->type) {
    code = convertToDecimal(pLeftData, pLeftT, &left, &lt);
    if (TSDB_CODE_SUCCESS != code) return code;
  } else {
    left = *(Decimal*)pLeftData;
  }
  if (TSDB_DATA_TYPE_DECIMAL != pRightT->type) {
    code = convertToDecimal(pRightData, pRightT, &right, &rt);
    if (TSDB_CODE_SUCCESS != code) return code;
    pRightData = &right;
  } else {
    right = *(Decimal*)pRightData;
  }

  SDecimalOps* pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  switch (op) {
    case OP_TYPE_ADD:
      decimalAdd(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_SUB:
      decimal128Negate(&right);
      decimalAdd(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_MULTI:
      code = decimalMultiply(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_DIV:
      code = decimalDivide(&left, &lt, &right, &rt, pOutT);
      break;
    default:
      break;
  }
  if (0 == code && pOutT->type != TSDB_DATA_TYPE_DECIMAL) {
    lt = *pOutT;
    lt.type = TSDB_DATA_TYPE_DECIMAL;
    code = convertToDecimal(&left, &lt, pOutputData, pOutT);
  } else {
    *(Decimal*)pOutputData = left;
  }
  return code;
}

#define ABS_INT64(v)  (v) == INT64_MIN ? (uint64_t)INT64_MAX + 1 : (uint64_t)llabs(v)
#define ABS_UINT64(v) (v)

#define DECIMAL64_IS_OVERFLOW(v, max)  decimal64Gt(&(v), &max, DECIMAL_WORD_NUM(Decimal64))
#define DECIMAL128_IS_OVERFLOW(v, max) decimal128Gt(&(v), &max, DECIMAL_WORD_NUM(Decimal128))

#define CHECK_OVERFLOW_AND_MAKE_DECIMAL64(pDec, v, max, ABS) \
  ({                                                         \
    int32_t   code = 0;                                      \
    Decimal64 dv = {ABS(v)};                                 \
    if (DECIMAL64_IS_OVERFLOW(dv, max)) {                    \
      code = TSDB_CODE_DECIMAL_OVERFLOW;                     \
    } else {                                                 \
      makeDecimal64(pDec, (int64_t)(v));                     \
    }                                                        \
    code;                                                    \
  })

#define MAKE_DECIMAL64_SIGNED(pDec, v, max)   CHECK_OVERFLOW_AND_MAKE_DECIMAL64(pDec, v, max, ABS_INT64)
#define MAKE_DECIMAL64_UNSIGNED(pDec, v, max) CHECK_OVERFLOW_AND_MAKE_DECIMAL64(pDec, v, max, ABS_UINT64);

static int64_t int64FromDecimal64(const DecimalType* pDec, uint8_t prec, uint8_t scale) { return 0; }

static uint64_t uint64FromDecimal64(const DecimalType* pDec, uint8_t prec, uint8_t scale) { return 0; }

static int32_t decimal64FromInt64(DecimalType* pDec, uint8_t prec, uint8_t scale, int64_t val) {
  Decimal64 max = {0};
  DECIMAL64_GET_MAX(prec - scale, &max);
  if (DECIMAL64_GET_VALUE(&max) < val || -DECIMAL64_GET_VALUE(&max) > val) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  DECIMAL64_SET_VALUE((Decimal64*)pDec, val);
  decimal64ScaleUp(pDec, scale);
  return 0;
}

static int32_t decimal64FromUint64(DecimalType* pDec, uint8_t prec, uint8_t scale, uint64_t val) {
  Decimal64 max = {0};
  DECIMAL64_GET_MAX(prec - scale, &max);
  if ((uint64_t)DECIMAL64_GET_VALUE(&max) < val) return TSDB_CODE_DECIMAL_OVERFLOW;
  DECIMAL64_SET_VALUE((Decimal64*)pDec, val);
  decimal64ScaleUp(pDec, scale);
  return 0;
}

static int32_t decimal64FromDouble(DecimalType* pDec, uint8_t prec, uint8_t scale, double val) { return 0; }

static int32_t decimal64FromDecimal128(DecimalType* pDec, uint8_t prec, uint8_t scale, const DecimalType* pVal,
                                       uint8_t valPrec, uint8_t valScale) {
  Decimal128 dec128 = *(Decimal128*)pVal, tmpDec128 = {0};
  bool       negative = DECIMAL128_SIGN(&dec128) == -1;
  if (negative) decimal128Negate(&dec128);
  tmpDec128 = dec128;

  Decimal64 max = {0};
  DECIMAL64_GET_MAX(prec - scale, &max);
  decimal128ScaleTo(&dec128, valScale, 0);
  if (decimal128Gt(&dec128, &max, WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128ScaleTo(&tmpDec128, valScale, scale);
  DECIMAL64_SET_VALUE((Decimal64*)pDec, DECIMAL128_LOW_WORD(&tmpDec128));
  if (negative) decimal64Negate(pDec);
  return 0;
}

static int32_t decimal64FromDecimal64(DecimalType* pDec, uint8_t prec, uint8_t scale, const DecimalType* pVal,
                                      uint8_t valPrec, uint8_t valScale) {
  Decimal64 dec64 = *(Decimal64*)pVal, max = {0};
  bool      negative = DECIMAL64_SIGN(&dec64) == -1;
  if (negative) decimal64Negate(&dec64);
  *(Decimal64*)pDec = dec64;

  DECIMAL64_GET_MAX(prec - scale, &max);
  decimal64ScaleTo(&dec64, valScale, 0);
  if (decimal64Lt(&max, &dec64, WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal64ScaleTo(pDec, valScale, scale);
  if (negative) decimal64Negate(pDec);
  return 0;
}

static int64_t int64FromDecimal128(const DecimalType* pDec, uint8_t prec, uint8_t scale) { return 0; }

static uint64_t uint64FromDecimal128(const DecimalType* pDec, uint8_t prec, uint8_t scale) { return 0; }

static int32_t decimal128FromInt64(DecimalType* pDec, uint8_t prec, uint8_t scale, int64_t val) {
  if (prec - scale <= 18) {  // TODO wjm test int64 with 19 digits.
    Decimal64 max = {0};
    DECIMAL64_GET_MAX(prec - scale, &max);
    if (DECIMAL64_GET_VALUE(&max) < val || -DECIMAL64_GET_VALUE(&max) > val) return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  uint64_t valAbs = ABS_INT64(val);
  makeDecimal128(pDec, 0, valAbs);
  if (val < 0) decimal128Negate(pDec);
  decimal128ScaleUp(pDec, scale);
  return 0;
}

static int32_t decimal128FromUint64(DecimalType* pDec, uint8_t prec, uint8_t scale, uint64_t val) {
  if (prec - scale <= 19) {
    Decimal128 max = {0}, decVal = {0};
    DECIMAL128_GET_MAX(prec - scale, &max);
    makeDecimal128(&decVal, 0, val);
    if (decimal128Gt(&decVal, &max, DECIMAL_WORD_NUM(Decimal128))) {
      return TSDB_CODE_DECIMAL_OVERFLOW;
    }
  }
  makeDecimal128(pDec, 0, val);
  decimal128ScaleUp(pDec, scale);
  return 0;
}

static int32_t decimal128FromDouble(DecimalType* pDec, uint8_t prec, uint8_t scale, double val) { return 0; }

static int32_t decimal128FromDecimal64(DecimalType* pDec, uint8_t prec, uint8_t scale, const DecimalType* pVal,
                                       uint8_t valPrec, uint8_t valScale) {
  Decimal64 dec64 = *(Decimal64*)pVal;
  bool      negative = DECIMAL64_SIGN(&dec64) == -1;
  if (negative) decimal64Negate(&dec64);

  makeDecimal128(pDec, 0, DECIMAL64_GET_VALUE(&dec64));
  Decimal128 max = {0};
  DECIMAL128_GET_MAX(prec - scale, &max);
  decimal64ScaleTo(&dec64, valScale, 0);
  if (decimal128Lt(&max, &dec64, WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128ScaleTo(pDec, valScale, scale);
  if (negative) decimal128Negate(pDec);
  return 0;
}

static int32_t decimal128FromDecimal128(DecimalType* pDec, uint8_t prec, uint8_t scale, const DecimalType* pVal,
                                        uint8_t valPrec, uint8_t valScale) {
  bool       negative = DECIMAL128_SIGN((Decimal128*)pVal) == -1;
  Decimal128 tmpDec = *(Decimal128*)pVal;
  if (negative) decimal128Negate(&tmpDec);
  *(Decimal128*)pDec = tmpDec;

  Decimal128 max = {0};
  DECIMAL128_GET_MAX(prec - scale, &max);
  decimal128ScaleTo(&tmpDec, valScale, 0);
  if (decimal128Lt(&max, &tmpDec, WORD_NUM(Decimal128))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128ScaleTo(pDec, valScale, scale);
  if (negative) decimal128Negate(pDec);
  return 0;
}

#define CONVERT_TO_DECIMAL(pData, pInputType, pOut, pOutType, decimal)                                           \
  ({                                                                                                             \
    int32_t  code = 0;                                                                                           \
    int64_t  val = 0;                                                                                            \
    uint64_t uval = 0;                                                                                           \
    double   dval = 0;                                                                                           \
    switch (pInputType->type) {                                                                                  \
      case TSDB_DATA_TYPE_NULL:                                                                                  \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_BOOL:                                                                                  \
        uval = *(const bool*)pData;                                                                              \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                            \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_TINYINT:                                                                               \
        val = *(const int8_t*)pData;                                                                             \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                              \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_SMALLINT:                                                                              \
        val = *(const int16_t*)pData;                                                                            \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                              \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_INT:                                                                                   \
        val = *(const int32_t*)pData;                                                                            \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                              \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_TIMESTAMP:                                                                             \
      case TSDB_DATA_TYPE_BIGINT:                                                                                \
        val = *(const int64_t*)pData;                                                                            \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                              \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_UTINYINT:                                                                              \
        uval = *(const uint8_t*)pData;                                                                           \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                            \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_USMALLINT:                                                                             \
        uval = *(const uint16_t*)pData;                                                                          \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                            \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_UINT:                                                                                  \
        uval = *(const uint32_t*)pData;                                                                          \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                            \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_UBIGINT:                                                                               \
        uval = *(const uint64_t*)pData;                                                                          \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                            \
        break;                                                                                                   \
      case TSDB_DATA_TYPE_FLOAT: {                                                                               \
        dval = *(const float*)pData;                                                                             \
        code = decimal##FromDouble(pOut, pOutType->precision, pOutType->scale, dval);                            \
      } break;                                                                                                   \
      case TSDB_DATA_TYPE_DOUBLE: {                                                                              \
        dval = *(const double*)pData;                                                                            \
        code = decimal##FromDouble(pOut, pOutType->precision, pOutType->scale, dval);                            \
      } break;                                                                                                   \
      case TSDB_DATA_TYPE_DECIMAL64: {                                                                           \
        code = decimal##FromDecimal64(pOut, pOutType->precision, pOutType->scale, pData, pInputType->precision,  \
                                      pInputType->scale);                                                        \
      } break;                                                                                                   \
      case TSDB_DATA_TYPE_DECIMAL: {                                                                             \
        code = decimal##FromDecimal128(pOut, pOutType->precision, pOutType->scale, pData, pInputType->precision, \
                                       pInputType->scale);                                                       \
      } break;                                                                                                   \
      case TSDB_DATA_TYPE_VARCHAR:                                                                               \
      case TSDB_DATA_TYPE_VARBINARY:                                                                             \
      case TSDB_DATA_TYPE_NCHAR:                                                                                 \
      default:                                                                                                   \
        code = TSDB_CODE_OPS_NOT_SUPPORT;                                                                        \
        break;                                                                                                   \
    }                                                                                                            \
    code;                                                                                                        \
  })

int32_t convertToDecimal(const void* pData, const SDataType* pInputType, void* pOut, const SDataType* pOutType) {
  // if (pInputType->type == pOutType->type) return 0;
  int32_t code = 0;

  switch (pOutType->type) {
    case TSDB_DATA_TYPE_DECIMAL64: {
      code = CONVERT_TO_DECIMAL(pData, pInputType, pOut, pOutType, decimal64);
    } break;
    case TSDB_DATA_TYPE_DECIMAL: {
      code = CONVERT_TO_DECIMAL(pData, pInputType, pOut, pOutType, decimal128);
    } break;
    default:
      code = TSDB_CODE_INTERNAL_ERROR;
      break;
  }
  return code;
}

void decimal64ScaleDown(Decimal64* pDec, uint8_t scaleDown) {
  if (scaleDown > 0) {
    Decimal64 divisor = SCALE_MULTIPLIER_64[scaleDown];
    decimal64divide(pDec, &divisor, WORD_NUM(Decimal64), NULL);
  }
}

void decimal64ScaleUp(Decimal64* pDec, uint8_t scaleUp) {
  if (scaleUp > 0) {
    Decimal64 multiplier = SCALE_MULTIPLIER_64[scaleUp];
    decimal64Multiply(pDec, &multiplier, WORD_NUM(Decimal64));
  }
}

void decimal64ScaleTo(Decimal64* pDec, uint8_t oldScale, uint8_t newScale) {
  if (newScale > oldScale)
    decimal64ScaleUp(pDec, newScale - oldScale);
  else if (newScale < oldScale)
    decimal64ScaleDown(pDec, oldScale - newScale);
}

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale, Decimal64* pRes) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_64, .pDec = pRes->words};
  DECIMAL64_SET_VALUE(pRes, 0);
  code = decimalVarFromStr(str, len, &var);
  if (TSDB_CODE_SUCCESS != code) return code;
  Decimal64 max = {0};
  DECIMAL64_GET_MAX(expectPrecision, &max);
  decimal64ScaleTo(pRes, var.scale, expectScale);
  if (decimal64Gt(pRes, &max, 1)) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  return code;
}

void decimal128ScaleDown(Decimal128* pDec, uint8_t scaleDown) {
  if (scaleDown > 0) {
    Decimal128 divisor = SCALE_MULTIPLIER_128[scaleDown];
    decimal128Divide(pDec, &divisor, 2, NULL);
  }
}

void decimal128ScaleUp(Decimal128* pDec, uint8_t scaleUp) {
  if (scaleUp > 0) {
    Decimal128 multiplier = SCALE_MULTIPLIER_128[scaleUp];
    decimal128Multiply(pDec, &multiplier, WORD_NUM(Decimal128));
  }
}

void decimal128ScaleTo(Decimal128* pDec, uint8_t oldScale, uint8_t newScale) {
  if (newScale > oldScale)
    decimal128ScaleUp(pDec, newScale - oldScale);
  else if (newScale < oldScale)
    decimal128ScaleDown(pDec, oldScale - newScale);
}

int32_t decimal128FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale,
                          Decimal128* pRes) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_128, .pDec = pRes->words};
  DECIMAL128_SET_HIGH_WORD(pRes, 0);
  DECIMAL128_SET_LOW_WORD(pRes, 0);
  code = decimalVarFromStr(str, len, &var);
  if (TSDB_CODE_SUCCESS != code) return code;
  Decimal128 max = {0};
  DECIMAL128_GET_MAX(expectPrecision, &max);
  decimal128ScaleTo(pRes, var.scale, expectScale);
  if (decimal128Gt(pRes, &max, 2)) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  return code;
}

__int128 decimal128ToInt128(const Decimal128* pDec) {
  __int128 ret = 0;
  ret = DECIMAL128_HIGH_WORD(pDec);
  ret <<= 64;
  ret |= DECIMAL128_LOW_WORD(pDec);
  return ret;
}

int32_t decimal128CountLeadingBinaryZeros(const Decimal128* pDec) {
  if (DECIMAL128_HIGH_WORD(pDec) == 0) {
    return 64 + countLeadingZeros(DECIMAL128_LOW_WORD(pDec));
  } else {
    return countLeadingZeros((uint64_t)DECIMAL128_HIGH_WORD(pDec));
  }
}

#define IMPL_INTEGER_TYPE_FROM_DECIMAL_TYPE(oType, decimalType, sign)                    \
  oType oType##From##decimalType(const DecimalType* pDec, uint8_t prec, uint8_t scale) { \
    return (oType)sign##int64##From##decimalType(pDec, prec, scale);                     \
  }
#define IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(oType, decimalType) \
  IMPL_INTEGER_TYPE_FROM_DECIMAL_TYPE(oType, decimalType, )
#define IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(oType, decimalType) \
  IMPL_INTEGER_TYPE_FROM_DECIMAL_TYPE(oType, decimalType, u)

IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int8_t, Decimal64)
IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int16_t, Decimal64)
IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int32_t, Decimal64)
IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int64_t, Decimal64)

IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint8_t, Decimal64)
IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint16_t, Decimal64)
IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint32_t, Decimal64)
IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint64_t, Decimal64)

double doubleFromDecimal64(const void* pDec, uint8_t prec, uint8_t scale) { return 0; }

bool boolFromDecimal64(const void* pDec, uint8_t prec, uint8_t scale) {
  return !decimal64Eq(pDec, &decimal64Zero, WORD_NUM(Decimal64));
}

#define IMPL_REAL_TYPE_FROM_DECIMAL_TYPE(oType, decimalType)                             \
  oType oType##From##decimalType(const DecimalType* pDec, uint8_t prec, uint8_t scale) { \
    return (oType) double##From##decimalType(pDec, prec, scale);                         \
  }

IMPL_REAL_TYPE_FROM_DECIMAL_TYPE(float, Decimal64);

IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int8_t, Decimal128)
IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int16_t, Decimal128)
IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int32_t, Decimal128)
IMP_SIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(int64_t, Decimal128)

IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint8_t, Decimal128)
IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint16_t, Decimal128)
IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint32_t, Decimal128)
IMP_UNSIGNED_INTEGER_TYPE_FROM_DECIMAL_TYPE(uint64_t, Decimal128)

bool   boolFromDecimal128(const void* pDec, uint8_t prec, uint8_t scale) { return true; }
double doubleFromDecimal128(const void* pDec, uint8_t prec, uint8_t scale) { return 0; }
IMPL_REAL_TYPE_FROM_DECIMAL_TYPE(float, Decimal128);
