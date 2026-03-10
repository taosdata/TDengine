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

typedef enum DecimalRoundType {
  ROUND_TYPE_CEIL,
  ROUND_TYPE_FLOOR,
  ROUND_TYPE_TRUNC,
  ROUND_TYPE_HALF_ROUND_UP,
} DecimalRoundType;

#define DECIMAL_GET_INTERNAL_TYPE(dataType) ((dataType) == TSDB_DATA_TYPE_DECIMAL ? DECIMAL_128 : DECIMAL_64)
#define DECIMAL_GET_WORD_NUM(decimalInternalType) \
  ((decimalInternalType) == DECIMAL_64 ? DECIMAL_WORD_NUM(Decimal64) : DECIMAL_WORD_NUM(Decimal128))
static SDecimalOps* getDecimalOpsImp(DecimalInternalType t);

#define DECIMAL_MIN_ADJUSTED_SCALE 6

static Decimal64 SCALE_MULTIPLIER_64[TSDB_DECIMAL64_MAX_PRECISION + 1] = {1LL,
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

typedef struct DecimalVar {
  DecimalInternalType type;
  int8_t              precision;
  int8_t              scale;
  int32_t             exponent;
  int8_t              sign;
  DecimalType*        pDec;
  int32_t             weight;
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
  if (pLeftT->type == TSDB_DATA_TYPE_JSON || pRightT->type == TSDB_DATA_TYPE_JSON ||
      pLeftT->type == TSDB_DATA_TYPE_VARBINARY || pRightT->type == TSDB_DATA_TYPE_VARBINARY)
    return TSDB_CODE_TSC_INVALID_OPERATION;
  if ((pLeftT->type >= TSDB_DATA_TYPE_BLOB && pLeftT->type <= TSDB_DATA_TYPE_GEOMETRY) ||
      (pRightT->type >= TSDB_DATA_TYPE_BLOB && pRightT->type <= TSDB_DATA_TYPE_GEOMETRY)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
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

int32_t calcCurPrec(int32_t prec, int32_t places, int32_t exp, int32_t weight, int32_t firstValidScale) {
  if (exp == 0) return prec + places;
  if (exp < 0) {
    if (weight + exp >= 0) return prec + places;
    return prec + places - exp - weight;
  }
  if (weight > 0) return prec + places;
  return prec + places - TMIN(firstValidScale - 1, exp);
}

int32_t calcActualWeight(int32_t prec, int32_t scale, int32_t exp, int32_t weight, int32_t firstValidScale) {
  if (exp == 0) return prec - scale;
  if (exp < 0) {
    if (weight + exp >= 0) return weight + exp;
    return 0;
  }
  if (weight > 0) return weight + exp;
  if (firstValidScale == 0) return 0;
  return TMAX(0, exp - firstValidScale);
}

static int32_t decimalVarFromStr(const char* str, int32_t len, DecimalVar* result) {
  int32_t code = 0, pos = 0;
  int32_t expectPrecision = result->precision;
  int32_t expectScale = result->scale;
  result->precision = 0;
  result->scale = 0;
  result->exponent = 0;
  bool     leadingZeroes = true, afterPoint = false, rounded = false, stop = false;
  uint32_t places = 0;
  result->sign = 1;
  int32_t weight = 0;
  int32_t firstValidScale = 0;

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
  int32_t pos2 = pos;
  while(pos2 < len) {
    if (isdigit(str[pos2] || str[pos] == '.')) continue;
    if (str[pos2] == 'e' || str[pos2] == 'E') {
      result->exponent = atoi(str + pos2 + 1);
      break;
    }
    pos2++;
  }

  for (; pos < len && !stop; ++pos) {
    switch (str[pos]) {
      case '.':
        weight = result->precision;
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
        if (firstValidScale == 0 && afterPoint) firstValidScale = places;

        int32_t   curPrec = calcCurPrec(result->precision, places, result->exponent, weight, firstValidScale);
        int32_t   scaleUp = 0;
        Decimal64 delta = {0};
        if (curPrec > maxPrecision(result->type)) {
          if (!afterPoint) return TSDB_CODE_DECIMAL_OVERFLOW;
          int32_t curScale = result->scale - result->exponent + places;
          if (rounded || curScale > expectScale + 1 /*scale already overflowed, no need do rounding*/ ||
              curPrec - 1 != maxPrecision(result->type) /* not the maxPrecision + 1 digit, no need do rounding*/ ||
              str[pos] < '5')
            break;

          // Do rounding for the maxPrecision + 1 digit.
          // Here we cannot directly add this digit into the results, because it may cause overflow.
          DECIMAL64_SET_VALUE(&delta, 1);
          scaleUp = places - 1;
          rounded = true;
        } else {
          scaleUp = places;
          DECIMAL64_SET_VALUE(&delta, str[pos] - '0');
        }

        result->precision += scaleUp;
        if (afterPoint) result->scale += scaleUp;
        while (scaleUp != 0) {
          int32_t curScale = TMIN(17, scaleUp);
          pOps->multiply(result->pDec, &SCALE_MULTIPLIER_64[curScale], DECIMAL_WORD_NUM(Decimal64));
          scaleUp -= curScale;
        }
        pOps->add(result->pDec, &delta, DECIMAL_WORD_NUM(Decimal64));
        places = 0;
      } break;
      case 'e':
      case 'E': {
          stop = true;
        } break;
      default:
        stop = true;
        break;
    }
  }
  result->weight = calcActualWeight(result->precision, result->scale, result->exponent, weight, firstValidScale);
  if (result->precision + result->scale > 0) result->scale -= result->exponent;
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

#define DECIMAL64_ONE  SCALE_MULTIPLIER_64[0]

#define DECIMAL64_GET_MAX(precision, pMax)                                \
  do {                                                                    \
    *(pMax) = SCALE_MULTIPLIER_64[precision];                             \
    decimal64Subtract(pMax, &DECIMAL64_ONE, DECIMAL_WORD_NUM(Decimal64)); \
  } while (0)

#define DECIMAL64_SIGN(pDec) (1 | (DECIMAL64_GET_VALUE(pDec) >> 63))

static void decimal64GetWhole(const DecimalType* pDec, int8_t scale, DecimalType* pWhole) {
  const SDecimalOps* pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL64);
  DECIMAL64_CLONE(pWhole, pDec);
  Decimal64 scaleMul = SCALE_MULTIPLIER_64[scale];
  pOps->divide(pWhole, &scaleMul, 1, NULL);
  pOps->abs(pWhole);
}

static void decimal64GetFrac(const DecimalType* pDec, int8_t scale, DecimalType* pFrac) {
  const SDecimalOps* pOps = getDecimalOpsImp(DECIMAL_64);
  DECIMAL64_CLONE(pFrac, pDec);
  Decimal64 scaleMul = SCALE_MULTIPLIER_64[scale];
  pOps->mod(pFrac, &scaleMul, 1);
  pOps->abs(pFrac);
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
static void    decimal64ScaleDown(Decimal64* pDec, uint8_t scaleDown, bool round);
static void    decimal64ScaleUp(Decimal64* pDec, uint8_t scaleUp);
static void    decimal64ScaleTo(Decimal64* pDec, uint8_t oldScale, uint8_t newScale);
int32_t        decimal64ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen);

static void decimal64RoundWithPositiveScale(Decimal64* pDec, uint8_t prec, int8_t scale, uint8_t toPrec,
                                            uint8_t toScale, DecimalRoundType roundType, bool* overflow);

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
static void    decimal128ScaleTo(Decimal128* pDec, uint8_t oldScale, uint8_t newScale);
static void    decimal128ScaleDown(Decimal128* pDec, uint8_t scaleDown, bool round);
static void    decimal128ScaleUp(Decimal128* pDec, uint8_t scaleUp);
static int32_t decimal128CountLeadingBinaryZeros(const Decimal128* pDec);
static int32_t decimal128FromInt64(DecimalType* pDec, uint8_t prec, uint8_t scale, int64_t val);
static int32_t decimal128FromUint64(DecimalType* pDec, uint8_t prec, uint8_t scale, uint64_t val);
int32_t        decimal128ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen);
//
// rounding functions
static void    decimal128RoundWithPositiveScale(Decimal128* pDec, uint8_t prec, uint8_t scale, uint8_t toPrec,
                                                uint8_t toScale, DecimalRoundType roundType, bool* overflow);
static void    decimal128RoundWithNegativeScale(Decimal128* pDec, uint8_t prec, uint8_t scale, int8_t toScale,
                                                DecimalRoundType roundType, bool* overflow);
static void    decimal128ModifyScaleAndPrecision(Decimal128* pDec, uint8_t scale, uint8_t toPrec, int8_t toScale,
                                                 bool* overflow);
static int32_t decimal128CountRoundingDelta(const Decimal128* pDec, int8_t scale, int8_t toScale,
                                            DecimalRoundType roundType);

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
const SDecimalOps* getDecimalOps(int8_t dataType) { return getDecimalOpsImp(DECIMAL_GET_INTERNAL_TYPE(dataType)); }

void makeDecimal64(Decimal64* pDec64, int64_t w) { DECIMAL64_SET_VALUE(pDec64, w); }

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
  if (!pBuf) return TSDB_CODE_INVALID_PARA;
  Decimal whole = {0}, frac = {0};
  int32_t pos = 0;
  char    buf[64] = {0};

  if (DECIMAL64_SIGN((Decimal64*)pInt) == -1) {
    pos = sprintf(buf, "-");
  }
  decimal64GetWhole(pInt, scale, &whole);
  pos += snprintf(buf + pos, bufLen - pos, "%" PRId64, DECIMAL64_GET_VALUE(&whole));
  if (scale > 0) {
    decimal64GetFrac(pInt, scale, &frac);
    if (DECIMAL64_GET_VALUE(&frac) != 0 || DECIMAL64_GET_VALUE(&whole) != 0) {
      TAOS_STRCAT(buf + pos, ".");
      pos += 1;
      // NOTE: never generate format string dynamically
      //       decimalTest has been passed.
      snprintf(buf + pos, bufLen - pos, "%0*" PRIu64, scale, DECIMAL64_GET_VALUE(&frac));
    }
  }
  TAOS_STRNCPY(pBuf, buf, bufLen);
  return 0;
}

// return 1 if positive or zero, else return -1
#define DECIMAL128_SIGN(pDec) (1 | (DECIMAL128_HIGH_WORD(pDec) >> 63))

static const Decimal128 SCALE_MULTIPLIER_128[TSDB_DECIMAL128_MAX_PRECISION + 1] = {
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

static double getDoubleScaleMultiplier(uint8_t scale) {
  static double SCALE_MULTIPLIER_DOUBLE[TSDB_DECIMAL_MAX_PRECISION + 1] = {0};
  static bool   initialized = false;
  if (!initialized) {
    SCALE_MULTIPLIER_DOUBLE[0] = 1.0;
    for (int32_t idx = 1; idx <= TSDB_DECIMAL_MAX_PRECISION; ++idx) {
      SCALE_MULTIPLIER_DOUBLE[idx] = SCALE_MULTIPLIER_DOUBLE[idx - 1] * 10;
    }
    initialized = true;
  }
  return SCALE_MULTIPLIER_DOUBLE[scale];
};

#define DECIMAL128_ONE  SCALE_MULTIPLIER_128[0]
#define DECIMAL128_TEN  SCALE_MULTIPLIER_128[1]

// To calculate how many bits for integer X.
// eg. 999(3 digits) -> 1111100111(10 bits) -> bitsForNumDigits[3] = 10
static const int32_t bitsForNumDigits[] = {0,  4,  7,  10, 14,  17,  20,  24,  27,  30,  34,  37,  40,
                                           44, 47, 50, 54, 57,  60,  64,  67,  70,  74,  77,  80,  84,
                                           87, 90, 94, 97, 100, 103, 107, 110, 113, 117, 120, 123, 127};

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

#define DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pTarget, rightDec, pWord) \
  if (rightWordNum != DECIMAL_WORD_NUM(Decimal128)) {                                   \
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

  bool leftNegate = DECIMAL128_SIGN(pLeftDec) == -1, rightNegate = DECIMAL128_SIGN(pRightDec) == -1;
  UInt128    a = {0}, b = {0}, c = {0}, d = {0};
  Decimal128 x = *pLeftDec, y = *pRightDec;
  decimal128Abs(&x);
  decimal128Abs(&y);
  makeUInt128(&a, DECIMAL128_HIGH_WORD(&x), DECIMAL128_LOW_WORD(&x));
  makeUInt128(&d, DECIMAL128_HIGH_WORD(&x), DECIMAL128_LOW_WORD(&x));
  makeUInt128(&b, DECIMAL128_HIGH_WORD(&y), DECIMAL128_LOW_WORD(&y));
  uInt128Divide(&a, &b);
  uInt128Mod(&d, &b);
  makeDecimal128(pLeftDec, uInt128Hi(&a), uInt128Lo(&a));
  if (pRemainder) makeDecimal128(pRemainderDec, uInt128Hi(&d), uInt128Lo(&d));
  if (leftNegate != rightNegate) decimal128Negate(pLeftDec);
  if (leftNegate && pRemainder) decimal128Negate(pRemainderDec);
}

static void decimal128Mod(DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 pLeftDec = *(Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight, right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  decimal128Divide(&pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal128),
                   pLeft);
}

static bool decimal128Gt(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return decimal128Lt(pRightDec, pLeftDec, DECIMAL_WORD_NUM(Decimal128));
}

static bool decimal128Eq(const DecimalType* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return DECIMAL128_HIGH_WORD(pLeftDec) == DECIMAL128_HIGH_WORD(pRightDec) &&
         DECIMAL128_LOW_WORD(pLeftDec) == DECIMAL128_LOW_WORD(pRightDec);
}

#define DIGIT_NUM_ONCE 18
static void extractDecimal128Digits(const Decimal128* pDec, uint64_t* digits, int32_t* digitNum) {
  UInt128 a = {0};
  UInt128 b = {0};
  *digitNum = 0;
  makeUInt128(&a, DECIMAL128_HIGH_WORD(pDec), DECIMAL128_LOW_WORD(pDec));
  while (!uInt128Eq(&a, &uInt128Zero)) {
    uint64_t hi = uInt128Hi(&a);
    uint64_t lo = uInt128Lo(&a);

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

int32_t decimal128ToStr(const DecimalType* pInt, uint8_t scale, char* pBuf, int32_t bufLen) {
  if (!pBuf) return TSDB_CODE_INVALID_PARA;
  const Decimal128* pDec = (const Decimal128*)pInt;
  bool              negative = DECIMAL128_SIGN(pDec) == -1;
  uint64_t          segments[3] = {0};
  int32_t           digitNum = 0;
  char              buf[64] = {0}, buf2[64] = {0};
  int32_t           len = 0;
  if (negative) {
    Decimal128 copy = {0};
    makeDecimal128(&copy, DECIMAL128_HIGH_WORD(pDec), DECIMAL128_LOW_WORD(pDec));
    decimal128Abs(&copy);
    extractDecimal128Digits(&copy, segments, &digitNum);
    TAOS_STRNCAT(buf2, "-", 2);
  } else {
    extractDecimal128Digits(pDec, segments, &digitNum);
  }
  if (digitNum == 0) {
    TAOS_STRNCPY(pBuf, "0", bufLen);
    return 0;
  }
  for (int32_t i = digitNum - 1; i >= 0; --i) {
    len += snprintf(buf + len, 64 - len, i == digitNum - 1 ? "%" PRIu64 : "%018" PRIu64, segments[i]);
  }
  int32_t wholeLen = len - scale;
  if (wholeLen > 0) {
    TAOS_STRNCAT(buf2, buf, wholeLen);
  } else {
    TAOS_STRNCAT(buf2, "0", 2);
  }
  if (scale > 0) {
    static const char *format = "0000000000000000000000000000000000000000";
    TAOS_STRNCAT(buf2, ".", 2);
    if (wholeLen < 0) TAOS_STRNCAT(buf2, format, TABS(wholeLen));
    TAOS_STRNCAT(buf2, buf + TMAX(0, wholeLen), scale);
  }
  TAOS_STRNCPY(pBuf, buf2, bufLen);
  return 0;
}
int32_t decimalToStr(const DecimalType* pDec, int8_t dataType, int8_t precision, int8_t scale, char* pBuf,
                     int32_t bufLen) {
  DecimalInternalType iType = DECIMAL_GET_INTERNAL_TYPE(dataType);
  switch (iType) {
    case DECIMAL_64:
      return decimal64ToStr(pDec, scale, pBuf, bufLen);
    case DECIMAL_128:
      return decimal128ToStr(pDec, scale, pBuf, bufLen);
    default:
      break;
  }
  return TSDB_CODE_INVALID_PARA;
}

static int32_t decimalAddLargePositive(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                                    const SDataType* pOT) {
  Decimal wholeX = *pX, wholeY = *pY, fracX = {0}, fracY = {0};
  decimal128Divide(&wholeX, &SCALE_MULTIPLIER_128[pXT->scale], DECIMAL_WORD_NUM(Decimal), &fracX);
  decimal128Divide(&wholeY, &SCALE_MULTIPLIER_128[pYT->scale], DECIMAL_WORD_NUM(Decimal), &fracY);

  uint8_t maxScale = TMAX(pXT->scale, pYT->scale);
  decimal128ScaleUp(&fracX, maxScale - pXT->scale);
  decimal128ScaleUp(&fracY, maxScale - pYT->scale);

  Decimal pMultiplier = SCALE_MULTIPLIER_128[maxScale];
  Decimal right = fracX;
  Decimal carry = {0};
  decimal128Subtract(&pMultiplier, &fracY, DECIMAL_WORD_NUM(Decimal));
  if (!decimal128Gt(&pMultiplier, &fracX, DECIMAL_WORD_NUM(Decimal))) {
    decimal128Subtract(&right, &pMultiplier, DECIMAL_WORD_NUM(Decimal));
    makeDecimal128(&carry, 0, 1);
  } else {
    decimal128Add(&right, &fracY, DECIMAL_WORD_NUM(Decimal));
  }

  decimal128ScaleDown(&right, maxScale - pOT->scale, true);
  if (decimal128AddCheckOverflow(&wholeX, &wholeY, DECIMAL_WORD_NUM(Decimal))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128Add(&wholeX, &wholeY, DECIMAL_WORD_NUM(Decimal));
  if (decimal128AddCheckOverflow(&wholeX, &carry, DECIMAL_WORD_NUM(Decimal))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128Add(&wholeX, &carry, DECIMAL_WORD_NUM(Decimal));
  decimal128Multiply(&wholeX, &SCALE_MULTIPLIER_128[pOT->scale], DECIMAL_WORD_NUM(Decimal));
  decimal128Add(&wholeX, &right, DECIMAL_WORD_NUM(Decimal));
  *pX = wholeX;
  return 0;
}

static void decimalAddLargeNegative(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                                    const SDataType* pOT) {
  Decimal wholeX = *pX, wholeY = *pY, fracX = {0}, fracY = {0};
  decimal128Divide(&wholeX, &SCALE_MULTIPLIER_128[pXT->scale], DECIMAL_WORD_NUM(Decimal), &fracX);
  decimal128Divide(&wholeY, &SCALE_MULTIPLIER_128[pYT->scale], DECIMAL_WORD_NUM(Decimal), &fracY);

  uint8_t maxScale = TMAX(pXT->scale, pYT->scale);
  decimal128ScaleUp(&fracX, maxScale - pXT->scale);
  decimal128ScaleUp(&fracY, maxScale - pYT->scale);

  decimal128Add(&wholeX, &wholeY, DECIMAL_WORD_NUM(Decimal));
  decimal128Add(&fracX, &fracY, DECIMAL_WORD_NUM(Decimal));

  if (DECIMAL128_SIGN(&wholeX) == -1 && decimal128Gt(&fracX, &DECIMAL128_ZERO, DECIMAL_WORD_NUM(Decimal128))) {
    decimal128Add(&wholeX, &DECIMAL128_ONE, DECIMAL_WORD_NUM(Decimal));
    decimal128Subtract(&fracX, &SCALE_MULTIPLIER_128[maxScale], DECIMAL_WORD_NUM(Decimal));
  } else if (decimal128Gt(&wholeX, &DECIMAL128_ZERO, DECIMAL_WORD_NUM(Decimal128)) && DECIMAL128_SIGN(&fracX) == -1) {
    decimal128Subtract(&wholeX, &DECIMAL128_ONE, DECIMAL_WORD_NUM(Decimal));
    decimal128Add(&fracX, &SCALE_MULTIPLIER_128[maxScale], DECIMAL_WORD_NUM(Decimal));
  }

  decimal128ScaleDown(&fracX, maxScale - pOT->scale, true);
  decimal128Multiply(&wholeX, &SCALE_MULTIPLIER_128[pOT->scale], DECIMAL_WORD_NUM(Decimal));
  decimal128Add(&wholeX, &fracX, DECIMAL_WORD_NUM(Decimal));
  *pX = wholeX;
}

static int32_t decimalAdd(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                       const SDataType* pOT) {
  int32_t code = 0;
  if (pOT->precision < TSDB_DECIMAL_MAX_PRECISION) {
    uint8_t maxScale = TMAX(pXT->scale, pYT->scale);
    Decimal tmpY = *pY;
    decimal128ScaleTo(pX, pXT->scale, maxScale);
    decimal128ScaleTo(&tmpY, pYT->scale, maxScale);
    decimal128Add(pX, &tmpY, DECIMAL_WORD_NUM(Decimal));
  } else {
    int8_t signX = DECIMAL128_SIGN(pX), signY = DECIMAL128_SIGN(pY);
    if (signX == 1 && signY == 1) {
      code = decimalAddLargePositive(pX, pXT, pY, pYT, pOT);
    } else if (signX == -1 && signY == -1) {
      decimal128Negate(pX);
      Decimal y = *pY;
      decimal128Negate(&y);
      code = decimalAddLargePositive(pX, pXT, &y, pYT, pOT);
      decimal128Negate(pX);
    } else {
      decimalAddLargeNegative(pX, pXT, pY, pYT, pOT);
    }
  }
  return code;
}

static void makeInt256FromDecimal128(Int256* pTarget, const Decimal128* pDec) {
  bool negative = DECIMAL128_SIGN(pDec) == -1;
  Decimal128 abs = *pDec;
  decimal128Abs(&abs);
  UInt128 tmp = {DECIMAL128_LOW_WORD(&abs), DECIMAL128_HIGH_WORD(&abs)};
  *pTarget = makeInt256(int128Zero, tmp);
  if (negative) {
    *pTarget = int256Negate(pTarget);
  }
}

static Int256 int256ScaleBy(const Int256* pX, int32_t scale) {
  Int256 result = *pX;
  if (scale > 0) {
    Int256 multiplier = {0};
    makeInt256FromDecimal128(&multiplier, &SCALE_MULTIPLIER_128[scale]);
    result = int256Multiply(pX, &multiplier);
  } else if (scale < 0) {
    Int256 divisor = {0};
    makeInt256FromDecimal128(&divisor, &SCALE_MULTIPLIER_128[-scale]);
    result = int256Divide(pX, &divisor);
    Int256 remainder = int256Mod(pX, &divisor);
    Int256 afterShift = int256RightShift(&divisor, 1);
    remainder = int256Abs(&remainder);
    if (!int256Gt(&afterShift, &remainder)) {
      if (int256Gt(pX, &int256Zero)) {
        result = int256Add(&result, &int256One);
      } else {
        result = int256Subtract(&result, &int256One);
      }
    }
  }
  return result;
}

static bool convertInt256ToDecimal128(const Int256* pX, Decimal128* pDec) {
  bool overflow = false;
  Int256 abs = int256Abs(pX);
  bool isNegative = int256Lt(pX, &int256Zero);
  UInt128 low = int256Lo(&abs);
  uint64_t lowLow= uInt128Lo(&low);
  uint64_t lowHigh = uInt128Hi(&low);
  Int256 afterShift = int256RightShift(&abs, 128);

  if (int256Gt(&afterShift, &int256Zero)) {
    overflow = true;
  } else if (lowHigh > INT64_MAX) {
    overflow = true;
  } else {
    makeDecimal128(pDec, lowHigh, lowLow);
    if (decimal128Gt(pDec, &decimal128Max, DECIMAL_WORD_NUM(Decimal128))) {
      overflow = true;
    }
  }
  if (isNegative) {
    decimal128Negate(pDec);
  }
  return overflow;
}

static int32_t decimalMultiply(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                               const SDataType* pOT) {
  if (pOT->precision < TSDB_DECIMAL_MAX_PRECISION) {
    decimal128Multiply(pX, pY, DECIMAL_WORD_NUM(Decimal));
  } else if (decimal128Eq(pX, &DECIMAL128_ZERO, DECIMAL_WORD_NUM(Decimal)) ||
             decimal128Eq(pY, &DECIMAL128_ZERO, DECIMAL_WORD_NUM(Decimal))) {
    makeDecimal128(pX, 0, 0);
  } else {
    int8_t  deltaScale = pXT->scale + pYT->scale - pOT->scale;
    Decimal xAbs = *pX, yAbs = *pY;
    decimal128Abs(&xAbs);
    decimal128Abs(&yAbs);
    if (deltaScale == 0) {
      // no need to trim scale
      Decimal max = DECIMAL128_MAX;

      decimal128Divide(&max, &yAbs, DECIMAL_WORD_NUM(Decimal), NULL);
      if (decimal128Gt(&xAbs, &max, DECIMAL_WORD_NUM(Decimal))) {
        return TSDB_CODE_DECIMAL_OVERFLOW;
      } else {
        decimal128Multiply(pX, pY, DECIMAL_WORD_NUM(Decimal));
      }
    } else {
      int32_t leadingZeros = decimal128CountLeadingBinaryZeros(&xAbs) + decimal128CountLeadingBinaryZeros(&yAbs);
      if (leadingZeros <= 128) {
        // need to trim scale
        Int256 x256 = {0}, y256 = {0};
        makeInt256FromDecimal128(&x256, pX);
        makeInt256FromDecimal128(&y256, pY);
        Int256 res = int256Multiply(&x256, &y256);
        if (deltaScale != 0) {
          res = int256ScaleBy(&res, -deltaScale);
        }
        bool overflow = convertInt256ToDecimal128(&res, pX);
        if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
      } else {
        // no need to trim scale
        if (deltaScale <= 38) {
          decimal128Multiply(pX, pY, DECIMAL_WORD_NUM(Decimal));
          decimal128ScaleDown(pX, deltaScale, true);
        } else {
          makeDecimal128(pX, 0, 0);
        }
      }
    }
  }
  return 0;
}

static int32_t decimalDivide(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                      const SDataType* pOT) {
  if (decimal128Eq(pY, &DECIMAL128_ZERO, DECIMAL_WORD_NUM(Decimal))) {
    return TSDB_CODE_DIVISION_BY_ZERO;
  }

  int8_t deltaScale = pOT->scale + pYT->scale - pXT->scale;

  Decimal xTmp = *pX;
  decimal128Abs(&xTmp);
  int32_t bitsOccupied = 128 - decimal128CountLeadingBinaryZeros(&xTmp);
  if (bitsOccupied + bitsForNumDigits[deltaScale] <= 127) {
    xTmp = *pX;
    decimal128ScaleUp(&xTmp, deltaScale);
    Decimal remainder = {0};
    decimal128Divide(&xTmp, pY, DECIMAL_WORD_NUM(Decimal), &remainder);

    Decimal tmpY = *pY;
    decimal128Abs(&tmpY);
    decimal128Multiply(&remainder, &decimal128Two, DECIMAL_WORD_NUM(Decimal));
    decimal128Abs(&remainder);
    if (!decimal128Lt(&remainder, &tmpY, DECIMAL_WORD_NUM(Decimal))) {
      Decimal64 extra = {(DECIMAL128_SIGN(pX) ^ DECIMAL128_SIGN(pY)) + 1};
      decimal128Add(&xTmp, &extra, DECIMAL_WORD_NUM(Decimal64));
    }
    *pX = xTmp;
  } else {
    Int256 x256 = {0}, y256 = {0};
    makeInt256FromDecimal128(&x256, pX);
    Int256 xScaledUp = int256ScaleBy(&x256, deltaScale);
    makeInt256FromDecimal128(&y256, pY);
    Int256 res = int256Divide(&xScaledUp, &y256);
    Int256 remainder = int256Mod(&xScaledUp, &y256);

    remainder = int256Multiply(&remainder, &int256Two);
    remainder = int256Abs(&remainder);
    y256 = int256Abs(&y256);
    if (!int256Lt(&remainder, &y256)) {
      if ((DECIMAL128_SIGN(pX) ^ DECIMAL128_SIGN(pY)) == 0) {
        res = int256Add(&res, &int256One);
      } else {
        res = int256Subtract(&res, &int256One);
      }
    }
    bool overflow = convertInt256ToDecimal128(&res, pX);
    if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  return 0;
}

static int32_t decimalMod(Decimal* pX, const SDataType* pXT, const Decimal* pY, const SDataType* pYT,
                          const SDataType* pOT) {
  if (decimal128Eq(pY, &DECIMAL128_ZERO, DECIMAL_WORD_NUM(Decimal))) {
    return TSDB_CODE_DIVISION_BY_ZERO;
  }
  Decimal xAbs = *pX, yAbs = *pY;
  decimal128Abs(&xAbs);
  decimal128Abs(&yAbs);
  int32_t xlz = decimal128CountLeadingBinaryZeros(&xAbs), ylz = decimal128CountLeadingBinaryZeros(&yAbs);
  if (pXT->scale < pYT->scale) {
    // x scale up
    xlz = xlz - bitsForNumDigits[pYT->scale - pXT->scale];
  } else if (pXT->scale > pYT->scale) {
    // y scale up
    ylz = ylz - bitsForNumDigits[pXT->scale - pYT->scale];
  }
  int32_t lz = TMIN(xlz, ylz);
  if (lz >= 2) {
    // it's safe to scale up
    yAbs = *pY;
    decimal128ScaleTo(pX, pXT->scale, TMAX(pXT->scale, pYT->scale));
    decimal128ScaleTo(&yAbs, pYT->scale, TMAX(pXT->scale, pYT->scale));
    decimal128Mod(pX, &yAbs, DECIMAL_WORD_NUM(Decimal));
  } else {
    Int256 x256 = {0}, y256 = {0};
    makeInt256FromDecimal128(&x256, pX);
    makeInt256FromDecimal128(&y256, pY);
    if (pXT->scale < pYT->scale) {
      x256 = int256ScaleBy(&x256, pYT->scale - pXT->scale);
    } else if (pXT->scale > pYT->scale) {
      y256 = int256ScaleBy(&y256, pXT->scale - pYT->scale);
    }
    Int256 res = int256Mod(&x256, &y256);
    if (convertInt256ToDecimal128(&res, pX)) {
      return TSDB_CODE_DECIMAL_OVERFLOW;
    }
  }
  return 0;
}

int32_t decimalOp(EOperatorType op, const SDataType* pLeftT, const SDataType* pRightT, const SDataType* pOutT,
                  const void* pLeftData, const void* pRightData, void* pOutputData) {
  int32_t code = 0;

  Decimal   left = {0}, right = {0};
  SDataType lt = {.type = TSDB_DATA_TYPE_DECIMAL,
                  .precision = TSDB_DECIMAL_MAX_PRECISION,
                  .bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes,
                  .scale = pLeftT->scale};
  SDataType rt = {.type = TSDB_DATA_TYPE_DECIMAL,
                  .precision = TSDB_DECIMAL_MAX_PRECISION,
                  .bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes,
                  .scale = 0};
  if (pRightT) rt.scale = pRightT->scale;
  if (TSDB_DATA_TYPE_DECIMAL != pLeftT->type) {
    code = convertToDecimal(pLeftData, pLeftT, &left, &lt);
    if (TSDB_CODE_SUCCESS != code) return code;
  } else {
    left = *(Decimal*)pLeftData;
  }
  if (pRightT && TSDB_DATA_TYPE_DECIMAL != pRightT->type) {
    code = convertToDecimal(pRightData, pRightT, &right, &rt);
    if (TSDB_CODE_SUCCESS != code) return code;
    pRightData = &right;
  } else if (pRightData){
    right = *(Decimal*)pRightData;
  }
#ifdef DEBUG
  char left_var[64] = {0}, right_var[64] = {0};
  decimal128ToStr(&left, lt.scale, left_var, 64);
  decimal128ToStr(&right, rt.scale, right_var, 64);
#endif

  switch (op) {
    case OP_TYPE_ADD:
      code = decimalAdd(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_SUB:
      decimal128Negate(&right);
      code = decimalAdd(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_MULTI:
      code = decimalMultiply(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_DIV:
      code = decimalDivide(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_REM:
      code = decimalMod(&left, &lt, &right, &rt, pOutT);
      break;
    case OP_TYPE_MINUS:
      decimal128Negate(&left);
      break;
    default:
      code = TSDB_CODE_TSC_INVALID_OPERATION;
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

bool doCompareDecimal128(EOperatorType op, const Decimal128* pLeftDec, const Decimal128* pRightDec) {
  switch (op) {
    case OP_TYPE_GREATER_THAN:
      return decimal128Gt(pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal));
    case OP_TYPE_GREATER_EQUAL:
      return !decimal128Lt(pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal));
    case OP_TYPE_LOWER_THAN:
      return decimal128Lt(pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal));
    case OP_TYPE_LOWER_EQUAL:
      return !decimal128Gt(pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal));
    case OP_TYPE_EQUAL:
      return decimal128Eq(pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal));
    case OP_TYPE_NOT_EQUAL:
      return !decimal128Eq(pLeftDec, pRightDec, DECIMAL_WORD_NUM(Decimal));
    default:
      break;
  }
  return false;
}

bool decimal64Compare(EOperatorType op, const SDecimalCompareCtx* pLeft, const SDecimalCompareCtx* pRight) {
  bool ret = false;
  uint8_t leftPrec = 0, leftScale = 0, rightPrec = 0, rightScale = 0;
  decimalFromTypeMod(pLeft->typeMod, &leftPrec, &leftScale);
  decimalFromTypeMod(pRight->typeMod, &rightPrec, &rightScale);
  int32_t deltaScale = leftScale - rightScale;

  Decimal64 leftDec = *(Decimal64*)pLeft->pData, rightDec = *(Decimal64*)pRight->pData;

  if (deltaScale != 0) {
    bool needInt128 = (deltaScale < 0 && leftPrec - deltaScale > TSDB_DECIMAL64_MAX_PRECISION) ||
                      (rightPrec + deltaScale > TSDB_DECIMAL64_MAX_PRECISION);
    if (needInt128) {
      Decimal128 dec128L = {0}, dec128R = {0};
      makeDecimal128FromDecimal64(&dec128L, leftDec);
      makeDecimal128FromDecimal64(&dec128R, rightDec);
      return doCompareDecimal128(op, &dec128L, &dec128R);
    } else {
      if (deltaScale < 0) {
        decimal64ScaleUp(&leftDec, -deltaScale);
      } else {
        decimal64ScaleUp(&rightDec, deltaScale);
      }
    }
  }

  switch (op) {
    case OP_TYPE_GREATER_THAN:
      return decimal64Gt(&leftDec, &rightDec, DECIMAL_WORD_NUM(Decimal64));
    case OP_TYPE_GREATER_EQUAL:
      return !decimal64Lt(&leftDec, &rightDec, DECIMAL_WORD_NUM(Decimal64));
    case OP_TYPE_LOWER_THAN:
      return decimal64Lt(&leftDec, &rightDec, DECIMAL_WORD_NUM(Decimal64));
    case OP_TYPE_LOWER_EQUAL:
      return !decimal64Gt(&leftDec, &rightDec, DECIMAL_WORD_NUM(Decimal64));
    case OP_TYPE_EQUAL:
      return decimal64Eq(&leftDec, &rightDec, DECIMAL_WORD_NUM(Decimal64));
    case OP_TYPE_NOT_EQUAL:
      return !decimal64Eq(&leftDec, &rightDec, DECIMAL_WORD_NUM(Decimal64));
    default:
      break;
  }
  return ret;
}

bool decimalCompare(EOperatorType op, const SDecimalCompareCtx* pLeft, const SDecimalCompareCtx* pRight) {
  if (pLeft->type == TSDB_DATA_TYPE_DECIMAL64 && pRight->type == TSDB_DATA_TYPE_DECIMAL64) {
    return decimal64Compare(op, pLeft, pRight);
  }
  bool    ret = false;
  uint8_t leftPrec = 0, leftScale = 0, rightPrec = 0, rightScale = 0;
  decimalFromTypeMod(pLeft->typeMod, &leftPrec, &leftScale);
  decimalFromTypeMod(pRight->typeMod, &rightPrec, &rightScale);

  if (pLeft->type == TSDB_DATA_TYPE_DECIMAL64) {
    Decimal128 dec128 = {0};
    makeDecimal128FromDecimal64(&dec128, *(Decimal64*)pLeft->pData);
    SDecimalCompareCtx leftCtx = {.pData = &dec128,
                                  .type = TSDB_DATA_TYPE_DECIMAL,
                                  .typeMod = decimalCalcTypeMod(TSDB_DECIMAL128_MAX_PRECISION, leftScale)};
    return decimalCompare(op, &leftCtx, pRight);
  } else if (pRight->type == TSDB_DATA_TYPE_DECIMAL64) {
    Decimal128 dec128 = {0};
    makeDecimal128FromDecimal64(&dec128, *(Decimal64*)pRight->pData);
    SDecimalCompareCtx rightCtx = {.pData = &dec128,
                                  .type = TSDB_DATA_TYPE_DECIMAL,
                                  .typeMod = decimalCalcTypeMod(TSDB_DECIMAL128_MAX_PRECISION, rightScale)};
    return decimalCompare(op, pLeft, &rightCtx);
  }
  int32_t deltaScale = leftScale - rightScale;
  Decimal pLeftDec = *(Decimal*)pLeft->pData, pRightDec = *(Decimal*)pRight->pData;

  if (deltaScale != 0) {
    bool needInt256 = (deltaScale < 0 && leftPrec - deltaScale > TSDB_DECIMAL_MAX_PRECISION) ||
                      (rightPrec + deltaScale > TSDB_DECIMAL_MAX_PRECISION);
    if (needInt256) {
      Int256 x = {0}, y = {0};
      makeInt256FromDecimal128(&x, &pLeftDec);
      makeInt256FromDecimal128(&y, &pRightDec);
      if (leftScale < rightScale) {
        x = int256ScaleBy(&x, rightScale - leftScale);
      } else {
        y = int256ScaleBy(&y, leftScale - rightScale);
      }
      switch (op) {
      case OP_TYPE_GREATER_THAN:
        return int256Gt(&x, &y);
      case OP_TYPE_GREATER_EQUAL:
        return !int256Lt(&x, &y);
      case OP_TYPE_LOWER_THAN:
        return int256Lt(&x, &y);
      case OP_TYPE_LOWER_EQUAL:
        return !int256Gt(&x, &y);
      case OP_TYPE_EQUAL:
        return int256Eq(&x, &y);
      case OP_TYPE_NOT_EQUAL:
        return !int256Eq(&x, &y);
      default:
        break;
      }
      return false;
    } else {
      if (deltaScale < 0) {
        decimal128ScaleUp(&pLeftDec, -deltaScale);
      } else {
        decimal128ScaleUp(&pRightDec, deltaScale);
      }
    }
  }
  return doCompareDecimal128(op, &pLeftDec, &pRightDec);
}

#define ABS_INT64(v)  (v) == INT64_MIN ? (uint64_t)INT64_MAX + 1 : (uint64_t)llabs(v)
#define ABS_UINT64(v) (v)

static int64_t int64FromDecimal64(const DecimalType* pDec, uint8_t prec, uint8_t scale) {
  Decimal64 rounded = *(Decimal64*)pDec;
  bool      overflow = false;
  decimal64RoundWithPositiveScale(&rounded, prec, scale, prec, 0, ROUND_TYPE_HALF_ROUND_UP, &overflow);
  if (overflow) return 0;

  return DECIMAL64_GET_VALUE(&rounded);
}

static uint64_t uint64FromDecimal64(const DecimalType* pDec, uint8_t prec, uint8_t scale) {
  Decimal64 rounded = *(Decimal64*)pDec;
  bool      overflow = false;
  decimal64RoundWithPositiveScale(&rounded, prec, scale, prec, 0, ROUND_TYPE_HALF_ROUND_UP, &overflow);
  if (overflow) return 0;

  return DECIMAL64_GET_VALUE(&rounded);
}

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

static int32_t decimal64FromDouble(DecimalType* pDec, uint8_t prec, uint8_t scale, double val) {
  double unscaled = val * getDoubleScaleMultiplier(scale);
  if (isnan(unscaled)) {
    goto __OVERFLOW__;
  }
  unscaled = round(unscaled);

  bool negative = unscaled < 0 ? true : false;
  double abs = TABS(unscaled);
  if (abs > ldexp(1.0, 63) - 1) {
    goto __OVERFLOW__;
  }

  uint64_t result = (uint64_t)abs;
  makeDecimal64(pDec, result);
  Decimal64 max = {0};
  DECIMAL64_GET_MAX(prec, &max);
  if (decimal64Gt(pDec, &max, DECIMAL_WORD_NUM(Decimal64))) goto __OVERFLOW__;
  if (negative) decimal64Negate(pDec);
  return 0;

__OVERFLOW__:
  makeDecimal64(pDec, 0);
  return TSDB_CODE_DECIMAL_OVERFLOW;
}

static int32_t decimal64FromDecimal128(DecimalType* pDec, uint8_t prec, uint8_t scale, const DecimalType* pVal,
                                       uint8_t valPrec, uint8_t valScale) {
  Decimal128 dec128 = *(Decimal128*)pVal, tmpDec128 = {0};
  bool       negative = DECIMAL128_SIGN(&dec128) == -1;
  if (negative) decimal128Negate(&dec128);
  tmpDec128 = dec128;

  Decimal64 max = {0};
  DECIMAL64_GET_MAX(prec - scale, &max);
  decimal128ScaleDown(&dec128, valScale, false);
  if (decimal128Gt(&dec128, &max, DECIMAL_WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128ScaleTo(&tmpDec128, valScale, scale);
  DECIMAL64_SET_VALUE((Decimal64*)pDec, DECIMAL128_LOW_WORD(&tmpDec128));
  DECIMAL64_GET_MAX(prec, &max);
  if (decimal64Lt(&max, pDec, DECIMAL_WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
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
  decimal64ScaleDown(&dec64, valScale, false);
  if (decimal64Lt(&max, &dec64, DECIMAL_WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal64ScaleTo(pDec, valScale, scale);
  DECIMAL64_GET_MAX(prec, &max);
  if (decimal64Lt(&max, pDec, DECIMAL_WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  if (negative) decimal64Negate(pDec);
  return 0;
}

static int64_t int64FromDecimal128(const DecimalType* pDec, uint8_t prec, uint8_t scale) {
  Decimal128 rounded = *(Decimal128*)pDec;
  bool       overflow = false;
  decimal128RoundWithPositiveScale(&rounded, prec, scale, prec, 0, ROUND_TYPE_HALF_ROUND_UP, &overflow);
  if (overflow) {
    return 0;
  }
  Decimal128 max = {0}, min = {0};
  (void)decimal128FromInt64(&max, TSDB_DECIMAL128_MAX_PRECISION, 0, INT64_MAX);
  (void)decimal128FromInt64(&min, TSDB_DECIMAL128_MAX_PRECISION, 0, INT64_MIN);
  if (decimal128Gt(&rounded, &max, DECIMAL_WORD_NUM(Decimal128)) || decimal128Lt(&rounded, &min, DECIMAL_WORD_NUM(Decimal128))) {
    overflow = true;
    return (int64_t)DECIMAL128_LOW_WORD(&rounded);
  }

  return (int64_t)DECIMAL128_LOW_WORD(&rounded);
}

static uint64_t uint64FromDecimal128(const DecimalType* pDec, uint8_t prec, uint8_t scale) {
  Decimal128 rounded = *(Decimal128*)pDec;
  bool       overflow = false;
  decimal128RoundWithPositiveScale(&rounded, prec, scale, prec, 0, ROUND_TYPE_HALF_ROUND_UP, &overflow);
  if (overflow) return 0;

  Decimal128 max = {0};
  (void)decimal128FromUint64(&max, TSDB_DECIMAL128_MAX_PRECISION, 0, UINT64_MAX);
  if (decimal128Gt(&rounded, &max, DECIMAL_WORD_NUM(Decimal128)) ||
      decimal128Lt(&rounded, &decimal128Zero, DECIMAL_WORD_NUM(Decimal128))) {
    overflow = true;
    return DECIMAL128_LOW_WORD(&rounded);
  }
  return DECIMAL128_LOW_WORD(&rounded);
}

static int32_t decimal128FromInt64(DecimalType* pDec, uint8_t prec, uint8_t scale, int64_t val) {
  if (prec - scale <= 18) {
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

static int32_t decimal128FromDouble(DecimalType* pDec, uint8_t prec, uint8_t scale, double val) {
  double unscaled = val * getDoubleScaleMultiplier(scale);
  if (isnan(unscaled)) {
    goto __OVERFLOW__;
  }
  unscaled = round(unscaled);

  bool   negative = unscaled < 0 ? true : false;
  double abs = TABS(unscaled);
  if (abs > ldexp(1.0, 127) - 1) {
    goto __OVERFLOW__;
  }

  uint64_t hi = (uint64_t)ldexp(abs, -64), lo = (uint64_t)(abs - ldexp((double)hi, 64));
  makeDecimal128(pDec, hi, lo);
  Decimal128 max = {0};
  DECIMAL128_GET_MAX(prec, &max);
  if (decimal128Gt(pDec, &max, DECIMAL_WORD_NUM(Decimal128))) goto __OVERFLOW__;
  if (negative) decimal128Negate(pDec);
  return 0;

__OVERFLOW__:
  *(Decimal128*)pDec = decimal128Zero;
  return TSDB_CODE_DECIMAL_OVERFLOW;
}

static int32_t decimal128FromDecimal64(DecimalType* pDec, uint8_t prec, uint8_t scale, const DecimalType* pVal,
                                       uint8_t valPrec, uint8_t valScale) {
  Decimal64 dec64 = *(Decimal64*)pVal;
  bool      negative = DECIMAL64_SIGN(&dec64) == -1;
  if (negative) decimal64Negate(&dec64);

  makeDecimal128(pDec, 0, DECIMAL64_GET_VALUE(&dec64));
  Decimal128 max = {0};
  DECIMAL128_GET_MAX(prec - scale, &max);
  decimal64ScaleDown(&dec64, valScale, false);
  if (decimal128Lt(&max, &dec64, DECIMAL_WORD_NUM(Decimal64))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128ScaleTo(pDec, valScale, scale);
  DECIMAL128_GET_MAX(prec, &max);
  if (decimal128Lt(&max, pDec, DECIMAL_WORD_NUM(Decimal128))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
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
  decimal128ScaleDown(&tmpDec, valScale, false);
  if (decimal128Lt(&max, &tmpDec, DECIMAL_WORD_NUM(Decimal128))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  decimal128ScaleTo(pDec, valScale, scale);
  DECIMAL128_GET_MAX(prec, &max);
  if (decimal128Lt(&max, pDec, DECIMAL_WORD_NUM(Decimal128))) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  if (negative) decimal128Negate(pDec);
  return 0;
}

#define CONVERT_TO_DECIMAL(pData, pInputType, pOut, pOutType, decimal)                                               \
  do {                                                                                                               \
    int64_t  val = 0;                                                                                                \
    uint64_t uval = 0;                                                                                               \
    double   dval = 0;                                                                                               \
    switch (pInputType->type) {                                                                                      \
      case TSDB_DATA_TYPE_NULL:                                                                                      \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_BOOL:                                                                                      \
        uval = *(const bool*)pData;                                                                                  \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                                \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_TINYINT:                                                                                   \
        val = *(const int8_t*)pData;                                                                                 \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                                  \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_SMALLINT:                                                                                  \
        val = *(const int16_t*)pData;                                                                                \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                                  \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_INT:                                                                                       \
        val = *(const int32_t*)pData;                                                                                \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                                  \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_TIMESTAMP:                                                                                 \
      case TSDB_DATA_TYPE_BIGINT:                                                                                    \
        val = *(const int64_t*)pData;                                                                                \
        code = decimal##FromInt64(pOut, pOutType->precision, pOutType->scale, val);                                  \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_UTINYINT:                                                                                  \
        uval = *(const uint8_t*)pData;                                                                               \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                                \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_USMALLINT:                                                                                 \
        uval = *(const uint16_t*)pData;                                                                              \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                                \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_UINT:                                                                                      \
        uval = *(const uint32_t*)pData;                                                                              \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                                \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_UBIGINT:                                                                                   \
        uval = *(const uint64_t*)pData;                                                                              \
        code = decimal##FromUint64(pOut, pOutType->precision, pOutType->scale, uval);                                \
        break;                                                                                                       \
      case TSDB_DATA_TYPE_FLOAT: {                                                                                   \
        dval = *(const float*)pData;                                                                                 \
        code = decimal##FromDouble(pOut, pOutType->precision, pOutType->scale, dval);                                \
      } break;                                                                                                       \
      case TSDB_DATA_TYPE_DOUBLE: {                                                                                  \
        dval = *(const double*)pData;                                                                                \
        code = decimal##FromDouble(pOut, pOutType->precision, pOutType->scale, dval);                                \
      } break;                                                                                                       \
      case TSDB_DATA_TYPE_DECIMAL64: {                                                                               \
        code = decimal##FromDecimal64(pOut, pOutType->precision, pOutType->scale, pData, pInputType->precision,      \
                                      pInputType->scale);                                                            \
      } break;                                                                                                       \
      case TSDB_DATA_TYPE_DECIMAL: {                                                                                 \
        code = decimal##FromDecimal128(pOut, pOutType->precision, pOutType->scale, pData, pInputType->precision,     \
                                       pInputType->scale);                                                           \
      } break;                                                                                                       \
      case TSDB_DATA_TYPE_VARCHAR:                                                                                   \
      case TSDB_DATA_TYPE_VARBINARY:                                                                                 \
      case TSDB_DATA_TYPE_NCHAR: {                                                                                   \
        code = decimal##FromStr(pData, pInputType->bytes, pOutType->precision, pOutType->scale,                      \
                                pOut);                                                                               \
      } break;                                                                                                       \
      default:                                                                                                       \
        code = TSDB_CODE_OPS_NOT_SUPPORT;                                                                            \
        break;                                                                                                       \
    }                                                                                                                \
  } while (0)

int32_t convertToDecimal(const void* pData, const SDataType* pInputType, void* pOut, const SDataType* pOutType) {
  int32_t code = 0;

  switch (pOutType->type) {
    case TSDB_DATA_TYPE_DECIMAL64: {
      CONVERT_TO_DECIMAL(pData, pInputType, pOut, pOutType, decimal64);
    } break;
    case TSDB_DATA_TYPE_DECIMAL: {
      CONVERT_TO_DECIMAL(pData, pInputType, pOut, pOutType, decimal128);
    } break;
    default:
      code = TSDB_CODE_INTERNAL_ERROR;
      break;
  }
  return code;
}

void decimal64ScaleDown(Decimal64* pDec, uint8_t scaleDown, bool round) {
  if (scaleDown > 0) {
    Decimal64 divisor = SCALE_MULTIPLIER_64[scaleDown], remainder = {0};
    decimal64divide(pDec, &divisor, DECIMAL_WORD_NUM(Decimal64), &remainder);
    if (round) {
      decimal64Abs(&remainder);
      Decimal64 half = SCALE_MULTIPLIER_64[scaleDown];
      decimal64divide(&half, &decimal64Two, DECIMAL_WORD_NUM(Decimal64), NULL);
      if (!decimal64Lt(&remainder, &half, DECIMAL_WORD_NUM(Decimal64))) {
        Decimal64 delta = {DECIMAL64_SIGN(pDec)};
        decimal64Add(pDec, &delta, DECIMAL_WORD_NUM(Decimal64));
      }
    }
  }
}

void decimal64ScaleUp(Decimal64* pDec, uint8_t scaleUp) {
  if (scaleUp > 0) {
    Decimal64 multiplier = SCALE_MULTIPLIER_64[scaleUp];
    decimal64Multiply(pDec, &multiplier, DECIMAL_WORD_NUM(Decimal64));
  }
}

static void decimal64ScaleTo(Decimal64* pDec, uint8_t oldScale, uint8_t newScale) {
  if (newScale > oldScale)
    decimal64ScaleUp(pDec, newScale - oldScale);
  else if (newScale < oldScale)
    decimal64ScaleDown(pDec, oldScale - newScale, true);
}

static void decimal64ScaleAndCheckOverflow(Decimal64* pDec, int8_t scale, uint8_t toPrec, uint8_t toScale,
                                           bool* overflow) {
  int8_t deltaScale = toScale - scale;
  if (deltaScale >= 0) {
    Decimal64 max = {0};
    DECIMAL64_GET_MAX(toPrec - deltaScale, &max);
    Decimal64 abs = *pDec;
    decimal64Abs(&abs);
    if (decimal64Gt(&abs, &max, DECIMAL_WORD_NUM(Decimal64))) {
      if (overflow) *overflow = true;
    } else {
      decimal64ScaleUp(pDec, deltaScale);
    }
  } else if (deltaScale < 0) {
    Decimal64 res = *pDec, max = {0};
    decimal64ScaleDown(&res, -deltaScale, false);
    DECIMAL64_GET_MAX(toPrec, &max);
    Decimal64 abs = res;
    decimal64Abs(&abs);
    if (decimal64Gt(&abs, &max, DECIMAL_WORD_NUM(Decimal64))) {
      if (overflow) *overflow = true;
    } else {
      *pDec = res;
    }
  }
}

static int32_t decimal64CountRoundingDelta(const Decimal64* pDec, int8_t scale, int8_t toScale,
                                           DecimalRoundType roundType) {
  if (roundType == ROUND_TYPE_TRUNC || toScale >= scale) return 0;

  Decimal64 dec = *pDec;
  int32_t   res = 0;
  switch (roundType) {
    case ROUND_TYPE_HALF_ROUND_UP: {
      Decimal64 trailing = dec;
      decimal64Mod(&trailing, &SCALE_MULTIPLIER_64[scale - toScale], DECIMAL_WORD_NUM(Decimal64));
      if (decimal64Eq(&trailing, &decimal64Zero, DECIMAL_WORD_NUM(Decimal64))) {
        res = 0;
        break;
      }
      Decimal64 trailingAbs = trailing, baseDiv2 = SCALE_MULTIPLIER_64[scale - toScale];
      decimal64Abs(&trailingAbs);
      decimal64divide(&baseDiv2, &decimal64Two, DECIMAL_WORD_NUM(Decimal64), NULL);
      if (decimal64Lt(&trailingAbs, &baseDiv2, DECIMAL_WORD_NUM(Decimal64))) {
        res = 0;
        break;
      }
      res = DECIMAL64_SIGN(pDec) == 1 ? 1 : -1;
    } break;
    default:
      break;
  }
  return res;
}

static void decimal64RoundWithPositiveScale(Decimal64* pDec, uint8_t prec, int8_t scale, uint8_t toPrec,
                                            uint8_t toScale, DecimalRoundType roundType, bool* overflow) {
  Decimal64 scaled = *pDec;
  bool      overflowLocal = false;
  // scale up or down to toScale
  decimal64ScaleAndCheckOverflow(&scaled, scale, toPrec, toScale, &overflowLocal);
  if (overflowLocal) {
    if (overflow) *overflow = true;
    *pDec = decimal64Zero;
    return;
  }

  // calc rounding delta
  int32_t delta = decimal64CountRoundingDelta(pDec, scale, toScale, roundType);
  if (delta == 0) {
    *pDec = scaled;
    return;
  }

  Decimal64 deltaDec = {delta};
  // add the delta
  decimal64Add(&scaled, &deltaDec, DECIMAL_WORD_NUM(Decimal64));

  // check overflow again
  if (toPrec < prec) {
    Decimal64 max = {0};
    DECIMAL64_GET_MAX(toPrec, &max);
    Decimal64 scaledAbs = scaled;
    decimal64Abs(&scaledAbs);
    if (decimal64Gt(&scaledAbs, &max, DECIMAL_WORD_NUM(Decimal64))) {
      if (overflow) *overflow = true;
      *pDec = decimal64Zero;
      return;
    }
  }
  *pDec = scaled;
}

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale, Decimal64* pRes) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_64, .pDec = pRes->words, .precision = expectPrecision, .scale = expectScale};
  DECIMAL64_SET_VALUE(pRes, 0);
  code = decimalVarFromStr(str, len, &var);
  if (TSDB_CODE_SUCCESS != code) return code;
  if (var.weight > (int32_t)expectPrecision - expectScale) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  bool overflow = false;
  decimal64RoundWithPositiveScale(pRes, var.precision, var.scale, expectPrecision, expectScale,
                                  ROUND_TYPE_HALF_ROUND_UP, &overflow);
  if (overflow) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  return code;
}

static void decimal128ScaleDown(Decimal128* pDec, uint8_t scaleDown, bool round) {
  if (scaleDown > 0) {
    Decimal128 divisor = SCALE_MULTIPLIER_128[scaleDown], remainder = {0};
    decimal128Divide(pDec, &divisor, 2, &remainder);
    if (round) {
      decimal128Abs(&remainder);
      Decimal128 half = SCALE_MULTIPLIER_128[scaleDown];
      decimal128Divide(&half, &decimal128Two, 2, NULL);
      if (!decimal128Lt(&remainder, &half, DECIMAL_WORD_NUM(Decimal128))) {
        Decimal64 delta = {DECIMAL128_SIGN(pDec)};
        decimal128Add(pDec, &delta, DECIMAL_WORD_NUM(Decimal64));
      }
    }
  }
}

static void decimal128ScaleUp(Decimal128* pDec, uint8_t scaleUp) {
  if (scaleUp > 0) {
    Decimal128 multiplier = SCALE_MULTIPLIER_128[scaleUp];
    decimal128Multiply(pDec, &multiplier, DECIMAL_WORD_NUM(Decimal128));
  }
}

static void decimal128ScaleTo(Decimal128* pDec, uint8_t oldScale, uint8_t newScale) {
  if (newScale > oldScale)
    decimal128ScaleUp(pDec, newScale - oldScale);
  else if (newScale < oldScale)
    decimal128ScaleDown(pDec, oldScale - newScale, true);
}

int32_t decimal128FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale,
                          Decimal128* pRes) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_128, .pDec = pRes->words, .precision = expectPrecision, .scale = expectScale};
  DECIMAL128_SET_HIGH_WORD(pRes, 0);
  DECIMAL128_SET_LOW_WORD(pRes, 0);
  code = decimalVarFromStr(str, len, &var);
  if (TSDB_CODE_SUCCESS != code) return code;
  if (var.weight > (int32_t)expectPrecision - expectScale) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  bool overflow = false;
  decimal128RoundWithPositiveScale(pRes, var.precision, var.scale, expectPrecision, expectScale,
                                   ROUND_TYPE_HALF_ROUND_UP, &overflow);
  if (overflow) {
    return TSDB_CODE_DECIMAL_OVERFLOW;
  }
  return code;
}

#if 0
__int128 decimal128ToInt128(const Decimal128* pDec) {
  __int128 ret = 0;
  ret = DECIMAL128_HIGH_WORD(pDec);
  ret <<= 64;
  ret |= DECIMAL128_LOW_WORD(pDec);
  return ret;
}
#endif

static int32_t decimal128CountLeadingBinaryZeros(const Decimal128* pDec) {
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

double doubleFromDecimal64(const void* pDec, uint8_t prec, uint8_t scale) {
  int32_t   sign = DECIMAL64_SIGN((Decimal64*)pDec);
  Decimal64 abs = *(Decimal64*)pDec;
  decimal64Abs(&abs);
  return (double)DECIMAL64_GET_VALUE(&abs) * sign / getDoubleScaleMultiplier(scale);
}

bool boolFromDecimal64(const void* pDec, uint8_t prec, uint8_t scale) {
  return !decimal64Eq(pDec, &decimal64Zero, DECIMAL_WORD_NUM(Decimal64));
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

bool boolFromDecimal128(const void* pDec, uint8_t prec, uint8_t scale) {
  return !decimal128Eq(pDec, &decimal128Zero, DECIMAL_WORD_NUM(Decimal128));
}

double doubleFromDecimal128(const void* pDec, uint8_t prec, uint8_t scale) {
  int32_t    sign = DECIMAL128_SIGN((Decimal128*)pDec);
  Decimal128 abs = *(Decimal128*)pDec;
  decimal128Abs(&abs);
  double unscaled = DECIMAL128_LOW_WORD(&abs);
  unscaled += ldexp((double)DECIMAL128_HIGH_WORD(&abs), 64);
  return (unscaled * sign) / getDoubleScaleMultiplier(scale);
}

IMPL_REAL_TYPE_FROM_DECIMAL_TYPE(float, Decimal128);

static void decimal128RoundWithPositiveScale(Decimal128* pDec, uint8_t prec, uint8_t scale, uint8_t toPrec,
                                             uint8_t toScale, DecimalRoundType roundType, bool* overflow) {
  Decimal128 scaled = *pDec;
  bool       overflowLocal = false;
  // scale up or down to toScale
  decimal128ModifyScaleAndPrecision(&scaled, scale, toPrec, toScale, &overflowLocal);
  if (overflowLocal) {
    if (overflow) *overflow = true;
    *pDec = decimal128Zero;
    return;
  }

  // calc rounding delta, 1 or -1
  int32_t delta = decimal128CountRoundingDelta(pDec, scale, toScale, roundType);
  if (delta == 0) {
    *pDec = scaled;
    return;
  }

  Decimal64 deltaDec = {delta};
  // add the delta
  decimal128Add(&scaled, &deltaDec, DECIMAL_WORD_NUM(Decimal64));

  // check overflow again
  if (toPrec < prec) {
    Decimal128 max = {0};
    DECIMAL128_GET_MAX(toPrec, &max);
    Decimal128 scaledAbs = scaled;
    decimal128Abs(&scaledAbs);
    if (decimal128Gt(&scaledAbs, &max, DECIMAL_WORD_NUM(Decimal128))) {
      if (overflow) *overflow = true;
      *(Decimal128*)pDec = decimal128Zero;
      return;
    }
  }
  *(Decimal128*)pDec = scaled;
}

static void decimal128ModifyScaleAndPrecision(Decimal128* pDec, uint8_t scale, uint8_t toPrec, int8_t toScale,
                                              bool* overflow) {
  int8_t deltaScale = toScale - scale;
  if (deltaScale >= 0) {
    Decimal128 max = {0};
    DECIMAL128_GET_MAX(toPrec - deltaScale, &max);
    Decimal128 abs = *pDec;
    decimal128Abs(&abs);
    if (decimal128Gt(&abs, &max, DECIMAL_WORD_NUM(Decimal128))) {
      if (overflow) *overflow = true;
    } else {
      decimal128ScaleUp(pDec, deltaScale);
    }
  } else {
    Decimal128 res = *pDec, max = {0};
    decimal128ScaleDown(&res, -deltaScale, false);
    DECIMAL128_GET_MAX(toPrec, &max);
    if (decimal128Gt(&res, &max, DECIMAL_WORD_NUM(Decimal128))) {
      if (overflow) *overflow = true;
    } else {
      *(Decimal128*)pDec = res;
    }
  }
}

static int32_t decimal128CountRoundingDelta(const Decimal128* pDec, int8_t scale, int8_t toScale,
                                            DecimalRoundType roundType) {
  if (roundType == ROUND_TYPE_TRUNC || toScale >= scale) return 0;
  Decimal128 dec128 = *pDec;
  int32_t    res = 0;
  switch (roundType) {
    case ROUND_TYPE_HALF_ROUND_UP: {
      Decimal128 trailing = dec128;
      decimal128Mod(&trailing, &SCALE_MULTIPLIER_128[scale - toScale], DECIMAL_WORD_NUM(Decimal128));
      if (decimal128Eq(&trailing, &decimal128Zero, DECIMAL_WORD_NUM(Decimal128))) {
        res = 0;
        break;
      }
      Decimal128 tailingAbs = trailing, baseDiv2 = SCALE_MULTIPLIER_128[scale - toScale];
      decimal128Abs(&tailingAbs);
      decimal128Divide(&baseDiv2, &decimal128Two, DECIMAL_WORD_NUM(Decimal128), NULL);
      if (decimal128Lt(&tailingAbs, &baseDiv2, DECIMAL_WORD_NUM(Decimal128))) {
        res = 0;
        break;
      }
      res = DECIMAL128_SIGN(pDec) == -1 ? -1 : 1;
    } break;
    case ROUND_TYPE_TRUNC:
    default:
      break;
  }
  return res;
}

bool decimal128AddCheckOverflow(const Decimal128* pLeft, const DecimalType* pRight, uint8_t rightWordNum) {
  if (DECIMAL128_SIGN(pLeft) == 0) {
    Decimal128 max = decimal128Max;
    decimal128Subtract(&max, pLeft, DECIMAL_WORD_NUM(Decimal128));
    return decimal128Lt(&max, pRight, rightWordNum);
  } else {
    Decimal128 min = decimal128Min;
    decimal128Subtract(&min, pLeft, DECIMAL_WORD_NUM(Decimal128));
    return decimal128Gt(&min, pRight, rightWordNum);
  }
}

int32_t TEST_decimal64From_int64_t(Decimal64* pDec, uint8_t prec, uint8_t scale, int64_t v) {
  return decimal64FromInt64(pDec, prec, scale, v);
}
int32_t TEST_decimal64From_uint64_t(Decimal64* pDec, uint8_t prec, uint8_t scale, uint64_t v) {
  return decimal64FromUint64(pDec, prec, scale, v);
}
int32_t TEST_decimal64From_double(Decimal64* pDec, uint8_t prec, uint8_t scale, double v) {
  return decimal64FromDouble(pDec, prec, scale, v);
}
double  TEST_decimal64ToDouble(Decimal64* pDec, uint8_t prec, uint8_t scale) {
  return doubleFromDecimal64(pDec, prec, scale);
}

int32_t TEST_decimal128From_int64_t(Decimal128* pDec, uint8_t prec, uint8_t scale, int64_t v) {
  return decimal128FromInt64(pDec, prec, scale, v);
}
int32_t TEST_decimal128From_uint64_t(Decimal128* pDec, uint8_t prec, uint8_t scale, uint64_t v) {
  return decimal128FromUint64(pDec, prec, scale, v);
}
int32_t TEST_decimal128From_double(Decimal128* pDec, uint8_t prec, uint8_t scale, double v) {
  return decimal128FromDouble(pDec, prec, scale, v);
}
double  TEST_decimal128ToDouble(Decimal128* pDec, uint8_t prec, uint8_t scale) {
  return doubleFromDecimal128(pDec, prec, scale);
}

int32_t TEST_decimal64FromDecimal64(const Decimal64* pInput, uint8_t inputPrec, uint8_t inputScale, Decimal64* pOutput,
                                    uint8_t outputPrec, uint8_t outputScale) {
  return decimal64FromDecimal64(pOutput, outputPrec, outputScale, pInput, inputPrec, inputScale);
}

int32_t TEST_decimal64FromDecimal128(const Decimal128* pInput, uint8_t prec, uint8_t inputScale, Decimal64* pOutput,
                                     uint8_t outputPrec, uint8_t outputScale) {
  return decimal64FromDecimal128(pOutput, outputPrec, outputScale, pInput, prec, inputScale);
}

int32_t TEST_decimal128FromDecimal64(const Decimal64* pInput, uint8_t inputPrec, uint8_t inputScale,
                                     Decimal128* pOutput, uint8_t outputPrec, uint8_t outputScale) {
  return decimal128FromDecimal64(pOutput, outputPrec, outputScale, pInput, inputPrec, inputScale);
}
int32_t TEST_decimal128FromDecimal128(const Decimal128* pDec, uint8_t prec, uint8_t scale, Decimal128* pOutput,
                                      uint8_t outputPrec, uint8_t outputScale) {
  return decimal128FromDecimal128(pOutput, outputPrec, outputScale, pDec, prec, scale);
}

void encodeDecimal(const DecimalType* pDec, int8_t type, void* pBuf) {
  switch (type) {
    case TSDB_DATA_TYPE_DECIMAL64:
      *(DecimalWord*)pBuf = htobe64((DecimalWord)DECIMAL64_GET_VALUE((Decimal64*)pDec));
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      ((Decimal128*)pBuf)->words[0] = htobe64(DECIMAL128_LOW_WORD((Decimal128*)pDec));
      ((Decimal128*)pBuf)->words[1] = htobe64(DECIMAL128_HIGH_WORD((Decimal128*)pDec));
      break;
    default:
      break;
  }
}

void decodeDecimal(const void* pBuf, int8_t type, DecimalType* pDec) {
  switch (type) {
    case TSDB_DATA_TYPE_DECIMAL64:
      DECIMAL64_SET_VALUE((Decimal64*)pDec, be64toh(*(DecimalWord*)pBuf));
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      DECIMAL128_SET_LOW_WORD((Decimal128*)pDec, be64toh(((Decimal128*)pBuf)->words[0]));
      DECIMAL128_SET_HIGH_WORD((Decimal128*)pDec, be64toh(((Decimal128*)pBuf)->words[1]));
      break;
    default:
      break;
  }
}
