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
  DecimalWord*        words;
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

int32_t decimalGetRetType(const SDataType* pLeftT, const SDataType* pRightT, EOperatorType opType,
                          SDataType* pOutType) {
  if (IS_FLOAT_TYPE(pLeftT->type) || IS_FLOAT_TYPE(pRightT->type)) {
    pOutType->type = TSDB_DATA_TYPE_DOUBLE;
    pOutType->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
    return 0;
  }

  // TODO wjm check not supported types
  uint8_t p1 = pLeftT->precision, s1 = pLeftT->scale, p2 = pRightT->precision, s2 = pRightT->scale;

  if (IS_DECIMAL_TYPE(pLeftT->type)) {
    p2 = TSDB_DECIMAL128_MAX_PRECISION;
    s1 = s2;  // TODO wjm take which scale? Maybe use default DecimalMax
  } else {
    p1 = TSDB_DECIMAL128_MAX_PRECISION;
    s2 = s1;
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
  pOutType->type = TSDB_DATA_TYPE_DECIMAL;
  pOutType->bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes;
  return 0;
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

int32_t decimal64FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale,
                         Decimal64* result) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_64, .words = result->words};
  code = decimalVarFromStr(str, len, &var);
  if (TSDB_CODE_SUCCESS != code) return code;
  // TODO wjm precision check
  // scale auto fit
  return code;
}

int32_t decimal128FromStr(const char* str, int32_t len, uint8_t expectPrecision, uint8_t expectScale,
                          Decimal128* result) {
  int32_t    code = 0;
  DecimalVar var = {.type = DECIMAL_128, .words = result->words};
  code = decimalVarFromStr(str, len, &var);
  if (TSDB_CODE_SUCCESS != code) return code;
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
            result->scale++;
          }
          DecimalWord ten = 10, digit = str[pos] - '0';
          pOps->multiply(result->words, &ten, 1);
          pOps->add(result->words, &digit, 1);
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
    pOps->multiply(result->words, &sign, 1);
  }
  return code;
}

int32_t decimal64ToDataVal(const Decimal64* dec, SValue* pVal) {
  VALUE_SET_TRIVIAL_DATUM(pVal, dec->words[0]);
  return TSDB_CODE_SUCCESS;
}

int32_t decimal128ToDataVal(Decimal128* dec, SValue* pVal) {
  pVal->pData = taosMemCalloc(2, sizeof(DecimalWord));
  if (!pVal->pData) return terrno;
  valueSetDatum(pVal, TSDB_DATA_TYPE_DECIMAL, dec->words, DECIMAL_WORD_NUM(Decimal128) * sizeof(DecimalWord));
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

#define DECIMAL64_ONE SCALE_MULTIPLIER_64[0]

#define DECIMAL64_GET_MAX(precision, pMax)                                              \
  do {                                                                                  \
    *(pMax) = SCALE_MULTIPLIER_64[precision];                                           \
    decimal64Subtract((pMax)->words, DECIMAL64_ONE.words, DECIMAL_WORD_NUM(Decimal64)); \
  } while (0)

static int32_t decimalGetWhole(const DecimalWord* pDec, DecimalInternalType type, int8_t scale, DecimalWord* pWhole) {
  SDecimalOps* pOps = getDecimalOpsImp(type);
  if (type == DECIMAL_64) {
    pWhole[0] = *pDec;
    Decimal64 scaleMul = SCALE_MULTIPLIER_64[scale];
    pOps->divide(pWhole, scaleMul.words, 1, NULL);
    if (TSDB_CODE_SUCCESS != 0) {
      // TODO wjm
    }
    pOps->abs(pWhole);
  } else {
    memcpy(pWhole, pDec, DECIMAL_GET_WORD_NUM(type) * sizeof(DecimalWord));
    // TODO wjm
    // pOps.divide(pWhole->words, )
  }
  return 0;
}

static int32_t decimalGetFrac(const DecimalWord* pDec, DecimalInternalType type, int8_t scale, DecimalWord* pFrac) {
  SDecimalOps* pOps = getDecimalOpsImp(type);
  if (type == DECIMAL_64) {
    pFrac[0] = *pDec;
    Decimal64 scaleMul = SCALE_MULTIPLIER_64[scale];
    pOps->mod(pFrac, scaleMul.words, 1);
    pOps->abs(pFrac);
  } else {
  }
  return 0;
}

static void    decimal64Negate(DecimalWord* pInt);
static void    decimal64Abs(DecimalWord* pInt);
static void    decimal64Add(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static void    decimal64Subtract(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static void    decimal64Multiply(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static void    decimal64divide(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum,
                               DecimalWord* pRemainder);
static void    decimal64Mod(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static bool    decimal64Lt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static bool    decimal64Gt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static bool    decimal64Eq(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static int32_t decimal64ToStr(const DecimalWord* pInt, uint8_t scale, char* pBuf, int32_t bufLen);

static void    decimal128Negate(DecimalWord* pInt);
static void    decimal128Abs(DecimalWord* pWord);
static void    decimal128Add(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static void    decimal128Subtract(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static void    decimal128Multiply(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static void    decimal128divide(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum,
                                DecimalWord* pRemainder);
static void    decimal128Mod(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static bool    decimal128Lt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static bool    decimal128Gt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static bool    decimal128Eq(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum);
static int32_t decimal128ToStr(const DecimalWord* pInt, uint8_t scale, char* pBuf, int32_t bufLen);

SDecimalOps decimal64Ops = {decimal64Negate,   decimal64Abs,    decimal64Add,  decimal64Subtract,
                            decimal64Multiply, decimal64divide, decimal64Mod,  decimal64Lt,
                            decimal64Gt,       decimal64Eq,     decimal64ToStr};
SDecimalOps decimal128Ops = {decimal128Negate,   decimal128Abs,    decimal128Add,  decimal128Subtract,
                             decimal128Multiply, decimal128divide, decimal128Mod,  decimal128Lt,
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

void makeDecimal64(Decimal64* pDec64, DecimalWord w) { pDec64->words[0] = w; }

// TODO wjm handle overflow problem of DecimalWord
void decimal64Negate(DecimalWord* pInt) { *pInt = -(int64_t)(*pInt); }
void decimal64Abs(DecimalWord* pInt) { *pInt = TABS(*pInt); }
void decimal64Add(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) { *pLeft += *pRight; }
void decimal64Subtract(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) { *pLeft -= *pRight; }
void decimal64Multiply(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) { *pLeft *= *pRight; }
void decimal64divide(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum, DecimalWord* pRemainder) {
  *pLeft /= *pRight;
  if (pRemainder) *pRemainder = *pLeft % *pRight;
}
void decimal64Mod(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) { *pLeft %= *pRight; }
bool decimal64Lt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) { return *pLeft < *pRight; }
bool decimal64Gt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) { return *pLeft > *pRight; }
bool decimal64Eq(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  return *pLeft == *pRight;
}
int32_t decimal64ToStr(const DecimalWord* pInt, uint8_t scale, char* pBuf, int32_t bufLen) {
  return snprintf(pBuf, bufLen, "%" PRId64, *pInt);
}

// TODO wjm handle endian problem
#define DECIMAL128_LOW_WORDS(pDec)  (uint64_t)((pDec)->words[0])
#define DECIMAL128_HIGH_WORDS(pDec) (int64_t)((pDec)->words[1])
// return 1 if positive or zero, else return -1
#define DECIMAL128_SIGN(pDec) (1 | (DECIMAL128_HIGH_WORDS(pDec) >> 63))

// TODO wjm handle endian problem
static Decimal128 SCALE_MULTIPLIER_128[38 + 1] = {
    {0, 1LL},
    {0, 10LL},
    {0, 100LL},
    {0, 1000LL},
    {0, 10000LL},
    {0, 100000LL},
    {0, 1000000LL},
    {0, 10000000LL},
    {0, 100000000LL},
    {0, 1000000000LL},
    {0, 10000000000LL},
    {0, 100000000000LL},
    {0, 1000000000000LL},
    {0, 10000000000000LL},
    {0, 100000000000000LL},
    {0, 1000000000000000LL},
    {0, 10000000000000000LL},
    {0, 100000000000000000LL},
    {0, 1000000000000000000LL},
    {0LL, 10000000000000000000ULL},
    {5LL, 7766279631452241920ULL},
    {54LL, 3875820019684212736ULL},
    {542LL, 1864712049423024128ULL},
    {5421LL, 200376420520689664ULL},
    {54210LL, 2003764205206896640ULL},
    {542101LL, 1590897978359414784ULL},
    {5421010LL, 15908979783594147840ULL},
    {54210108LL, 11515845246265065472ULL},
    {542101086LL, 4477988020393345024ULL},
    {5421010862LL, 7886392056514347008ULL},
    {54210108624LL, 5076944270305263616ULL},
    {542101086242LL, 13875954555633532928ULL},
    {5421010862427LL, 9632337040368467968ULL},
    {54210108624275LL, 4089650035136921600ULL},
    {542101086242752LL, 4003012203950112768ULL},
    {5421010862427522LL, 3136633892082024448ULL},
    {54210108624275221LL, 12919594847110692864ULL},
    {542101086242752217LL, 68739955140067328ULL},
    {5421010862427522170LL, 687399551400673280ULL},
};

#define DECIMAL128_ONE SCALE_MULTIPLIER_128[0]
#define DECIMAL128_TEN SCALE_MULTIPLIER_128[1]

#define DECIMAL128_GET_MAX(precision, pMax)                                                \
  do {                                                                                     \
    *(pMax) = SCALE_MULTIPLIER_128[precision];                                             \
    decimal128Subtract((pMax)->words, DECIMAL128_ONE.words, DECIMAL_WORD_NUM(Decimal128)); \
  } while (0)

void makeDecimal128(Decimal128* pDec128, int64_t hi, uint64_t low) {
  // TODO wjm handle endian problem
  pDec128->words[1] = hi;
  pDec128->words[0] = low;
}

static void makeDecimal128FromDecimal64(Decimal128* pTarget, Decimal64 decimal64) {
  DecimalWord zero = 0, sign = 0;
  if (decimal64Lt(decimal64.words, &zero, 1)) {
    decimal64Negate(decimal64.words);
    sign = -1;
  }
  makeDecimal128(pTarget, sign, (uint64_t)decimal64.words[0]);
}

static void decimal128Negate(DecimalWord* pWord) {
  Decimal128* pDec = (Decimal128*)pWord;
  uint64_t    lo = ~DECIMAL128_LOW_WORDS(pDec) + 1;
  int64_t     hi = ~DECIMAL128_HIGH_WORDS(pDec);
  if (lo == 0) hi = SAFE_INT64_ADD(hi, 1);
  makeDecimal128(pDec, hi, lo);
}

static void decimal128Abs(DecimalWord* pWord) {
  DecimalWord zero = 0;
  if (decimal128Lt(pWord, &zero, 1)) {
    decimal128Negate(pWord);
  }
}

// TODO wjm put it out of decimal128 functions
#define DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pTarget, rightDec, pWord) \
  if (rightWordNum != WORD_NUM(Decimal128)) {                                   \
    Decimal64 d64 = {0};                                                        \
    makeDecimal64(&d64, *pWord);                                                \
    makeDecimal128FromDecimal64(&rightDec, d64);                                \
    pTarget = &rightDec;                                                        \
  }

static void decimal128Add(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  int64_t  hi = SAFE_INT64_ADD(DECIMAL128_HIGH_WORDS(pLeftDec), DECIMAL128_HIGH_WORDS(pRightDec));
  uint64_t lo = DECIMAL128_LOW_WORDS(pLeftDec) + DECIMAL128_LOW_WORDS(pRightDec);
  hi = SAFE_INT64_ADD(hi, lo < DECIMAL128_LOW_WORDS(pLeftDec));
  makeDecimal128(pLeftDec, hi, lo);
}

static void decimal128Subtract(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  int64_t  hi = SAFE_INT64_SUBTRACT(DECIMAL128_HIGH_WORDS(pLeftDec), DECIMAL128_HIGH_WORDS(pRightDec));
  uint64_t lo = DECIMAL128_LOW_WORDS(pLeftDec) - DECIMAL128_LOW_WORDS(pRightDec);
  hi = SAFE_INT64_SUBTRACT(hi, lo > DECIMAL128_LOW_WORDS(pLeftDec));
  makeDecimal128(pLeftDec, hi, lo);
}

static void decimal128Multiply(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  bool       negate = DECIMAL128_SIGN(pLeftDec) != DECIMAL128_SIGN(pRightDec);
  Decimal128 x = *pLeftDec, y = *pRightDec;
  decimal128Abs(x.words);
  decimal128Abs(y.words);

  UInt128 res = {0}, tmp = {0};
  makeUInt128(&res, DECIMAL128_HIGH_WORDS(&x), DECIMAL128_LOW_WORDS(&x));
  makeUInt128(&tmp, DECIMAL128_HIGH_WORDS(&y), DECIMAL128_LOW_WORDS(&y));
  uInt128Multiply(&res, &tmp);
  makeDecimal128(pLeftDec, uInt128Hi(&res), uInt128Lo(&res));
  if (negate) decimal128Negate(pLeftDec->words);
}

static bool decimal128Lt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  // TODO wjm pRightDec use const
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return DECIMAL128_HIGH_WORDS(pLeftDec) < DECIMAL128_HIGH_WORDS(pRightDec) ||
         (DECIMAL128_HIGH_WORDS(pLeftDec) == DECIMAL128_HIGH_WORDS(pRightDec) &&
          DECIMAL128_LOW_WORDS(pLeftDec) < DECIMAL128_LOW_WORDS(pRightDec));
}

static void decimal128divide(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum,
                             DecimalWord* pRemainder) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight, *pRemainderDec = (Decimal128*)pRemainder;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  bool       negate = DECIMAL128_SIGN(pLeftDec) != DECIMAL128_SIGN(pRightDec);
  UInt128    a = {0}, b = {0}, c = {0}, d = {0};
  Decimal128 x = *pLeftDec, y = *pRightDec;
  decimal128Abs(x.words);
  decimal128Abs(y.words);
  makeUInt128(&a, DECIMAL128_HIGH_WORDS(&x), DECIMAL128_LOW_WORDS(&x));
  makeUInt128(&d, DECIMAL128_HIGH_WORDS(&x), DECIMAL128_LOW_WORDS(&x));
  makeUInt128(&b, DECIMAL128_HIGH_WORDS(&y), DECIMAL128_LOW_WORDS(&y));
  // TODO wjm refine the interface, so that here do not need to copy a
  uInt128Divide(&a, &b);
  uInt128Mod(&d, &b);
  makeDecimal128(pLeftDec, uInt128Hi(&a), uInt128Lo(&a));
  makeDecimal128(pRemainderDec, uInt128Hi(&d), uInt128Lo(&d));
  if (negate) decimal128Negate(pLeftDec->words);
  if (DECIMAL128_SIGN(pLeftDec) == -1) decimal128Negate(pRemainderDec->words);
}

static void decimal128Mod(DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {}

static bool decimal128Gt(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return decimal128Lt(pRightDec->words, pLeftDec->words, WORD_NUM(Decimal128));
}

static bool decimal128Eq(const DecimalWord* pLeft, const DecimalWord* pRight, uint8_t rightWordNum) {
  Decimal128 *pLeftDec = (Decimal128*)pLeft, *pRightDec = (Decimal128*)pRight;
  Decimal128  right = {0};
  DECIMAL128_CHECK_RIGHT_WORD_NUM(rightWordNum, pRightDec, right, pRight);

  return pLeftDec->words == pRightDec->words;
}

static void extractDecimal128Digits(const Decimal128* pDec, uint64_t* digits, int32_t* digitNum) {
#define DIGIT_NUM_ONCE 18
  const uint64_t k1e18 = 100000000000000000;
  UInt128        a = {0};
  UInt128        b = {0};
  *digitNum = 0;
  makeUInt128(&a, DECIMAL128_HIGH_WORDS(pDec), DECIMAL128_LOW_WORDS(pDec));
  while (!uInt128Eq(&a, &uInt128Zero)) {
    uint64_t hi = a >> 64;
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
static int32_t decimal128ToStr(const DecimalWord* pInt, uint8_t scale, char* pBuf, int32_t bufLen) {
  const Decimal128* pDec = (const Decimal128*)pInt;
  bool              negative = DECIMAL128_SIGN(pDec) < 0;
  uint64_t          segments[3] = {0};
  int32_t           digitNum = 0;
  char              buf[64] = {0};
  if (negative) {
    Decimal128 copy = {0};
    makeDecimal128(&copy, DECIMAL128_HIGH_WORDS(pDec), DECIMAL128_LOW_WORDS(pDec));
    decimal128Abs(copy.words);
    extractDecimal128Digits(&copy, segments, &digitNum);
  } else {
    extractDecimal128Digits(pDec, segments, &digitNum);
  }
  int32_t len = 0;
  for (int32_t i = digitNum - 1; i >= 0; --i) {
    len += snprintf(buf + len, 64 - len, i == digitNum - 1 ? "%" PRIu64 : "%018" PRIu64, segments[i]);
  }
  int32_t wholeLen = len - scale;
  if (wholeLen > 0) {
    TAOS_STRNCAT(pBuf, buf, wholeLen);
  }
  if (scale > 0) {
    TAOS_STRNCAT(pBuf, ".", 2);
    TAOS_STRNCAT(pBuf, buf + TMAX(0, wholeLen), scale);
  }
  return 0;
}

int32_t decimalToStr(const DecimalWord* pDec, int8_t dataType, int8_t precision, int8_t scale, char* pBuf,
                     int32_t bufLen) {
  pBuf[0] = '\0';
  DecimalInternalType iType = DECIMAL_GET_INTERNAL_TYPE(dataType);
  switch (iType) {
    case DECIMAL_64: {
      int32_t      wordNum = DECIMAL_GET_WORD_NUM(iType);
      SDecimalOps* pOps = getDecimalOpsImp(iType);
      Decimal      whole = {0}, frac = {0};
      DecimalWord  zero = 0;
      int32_t      pos = 0;

      if (pOps->lt(pDec, &zero, 1)) {
        pos = sprintf(pBuf, "-");
      }
      int32_t code = decimalGetWhole(pDec, iType, scale, whole.words);
      if (!pOps->eq(whole.words, &zero, 1)) {
        pos += pOps->toStr(whole.words, scale, pBuf + pos, bufLen - pos);
      }
      pos += snprintf(pBuf + pos, bufLen - pos, ".");
      code = decimalGetFrac(pDec, iType, scale, frac.words);
      pOps->toStr(frac.words, scale, pBuf + pos, bufLen - pos);
      return 0;
    }
    case DECIMAL_128:
      return decimal128ToStr(pDec, scale, pBuf, bufLen);
    default:
      break;
  }
  return 0;
}

int32_t decimalOp(EOperatorType op, const SDataType* pLeftT, const SDataType* pRightT, const SDataType* pOutT,
                  const void* pLeftData, const void* pRightData, void* pOutputData) {
  int32_t code = 0;
  if (pOutT->type != TSDB_DATA_TYPE_DECIMAL) return TSDB_CODE_INTERNAL_ERROR;

  Decimal pLeft = *(Decimal*)pLeftData, pRight = *(Decimal*)pRightData;
  if (TSDB_DATA_TYPE_DECIMAL != pLeftT->type || pLeftT->scale != pOutT->scale) {
    code = convertToDecimal(pLeftData, pLeftT, &pLeft, pOutT);
    if (TSDB_CODE_SUCCESS != code) return code;
  }
  if (pRightT && (TSDB_DATA_TYPE_DECIMAL != pRightT->type || pRightT->scale != pOutT->scale)) {
    code = convertToDecimal(pRightData, pRightT, &pRight, pOutT);
    if (TSDB_CODE_SUCCESS != code) return code;
  }

  SDecimalOps* pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  switch (op) {
    case OP_TYPE_ADD:
      pOps->add(pLeft.words, pRight.words, WORD_NUM(Decimal));
      break;
    default:
      break;
  }
  return code;
}

#define ABS_INT64(v)  (v) == INT64_MIN ? (uint64_t)INT64_MAX + 1 : (uint64_t)llabs(v)
#define ABS_UINT64(v) (v)

#define DECIMAL64_IS_OVERFLOW(v, max)  decimal64Gt((v).words, max.words, DECIMAL_WORD_NUM(Decimal64))
#define DECIMAL128_IS_OVERFLOW(v, max) decimal128Gt((v).words, max.words, DECIMAL_WORD_NUM(Decimal128))

#define CHECK_OVERFLOW_AND_MAKE_DECIMAL64(pDec, v, max, ABS) \
  ({                                                         \
    int32_t   code = 0;                                      \
    Decimal64 dv = {ABS(v)};                                 \
    if (DECIMAL64_IS_OVERFLOW(dv, max)) {                    \
      code = TSDB_CODE_INTERNAL_ERROR;                       \
    } else {                                                 \
      makeDecimal64(pDec, (int64_t)(v));                     \
    }                                                        \
    code;                                                    \
  })

#define MAKE_DECIMAL64_SIGNED(pDec, v, max)   CHECK_OVERFLOW_AND_MAKE_DECIMAL64(pDec, v, max, ABS_INT64)
#define MAKE_DECIMAL64_UNSIGNED(pDec, v, max) CHECK_OVERFLOW_AND_MAKE_DECIMAL64(pDec, v, max, ABS_UINT64);

#define CHECK_OVERFLOW_AND_MAKE_DECIMAL128(pDec, v, max, ABS) \
  ({                                                          \
    int32_t    code = 0;                                      \
    Decimal128 dv = {0};                                      \
    makeDecimal128(&dv, 0, ABS(v));                           \
    if (DECIMAL128_IS_OVERFLOW(dv, max)) {                    \
      code = TSDB_CODE_INTERNAL_ERROR;                        \
    } else {                                                  \
      makeDecimal128(pDec, v < 0 ? -1 : 0, ABS(v));           \
    }                                                         \
    code;                                                     \
  })

#define MAKE_DECIMAL128_SIGNED(pDec, v, max)   CHECK_OVERFLOW_AND_MAKE_DECIMAL128(pDec, v, max, ABS_INT64)
#define MAKE_DECIMAL128_UNSIGNED(pDec, v, max) CHECK_OVERFLOW_AND_MAKE_DECIMAL128(pDec, v, max, ABS_UINT64)

#define CONVERT_TO_DECIMAL(inputType, pInputData, pOut, CHECK_AND_MAKE_DECIMAL, max, code) \
  do {                                                                                     \
    int64_t  v = 0;                                                                        \
    uint64_t uv = 0;                                                                       \
    switch (inputType) {                                                                   \
      case TSDB_DATA_TYPE_NULL:                                                            \
        uv = 0;                                                                            \
        code = CHECK_AND_MAKE_DECIMAL##_UNSIGNED(pOut, uv, max);                           \
        break;                                                                             \
      case TSDB_DATA_TYPE_BOOL:                                                            \
        uv = *(bool*)pInputData;                                                           \
        CHECK_AND_MAKE_DECIMAL##_UNSIGNED(pOut, uv, max);                                  \
        break;                                                                             \
      case TSDB_DATA_TYPE_TINYINT:                                                         \
        v = *(int8_t*)pInputData;                                                          \
        CHECK_AND_MAKE_DECIMAL##_SIGNED(pOut, v, max);                                     \
        break;                                                                             \
      case TSDB_DATA_TYPE_SMALLINT:                                                        \
        v = *(int16_t*)pInputData;                                                         \
        CHECK_AND_MAKE_DECIMAL##_SIGNED(pOut, v, max);                                     \
        break;                                                                             \
      case TSDB_DATA_TYPE_INT:                                                             \
        v = *(int32_t*)pInputData;                                                         \
        CHECK_AND_MAKE_DECIMAL##_SIGNED(pOut, v, max);                                     \
        break;                                                                             \
      case TSDB_DATA_TYPE_TIMESTAMP:                                                       \
      case TSDB_DATA_TYPE_BIGINT:                                                          \
        v = *(int64_t*)pInputData;                                                         \
        CHECK_AND_MAKE_DECIMAL##_SIGNED(pOut, v, max);                                     \
        break;                                                                             \
      case TSDB_DATA_TYPE_UTINYINT: {                                                      \
        uv = *(uint8_t*)pInputData;                                                        \
        CHECK_AND_MAKE_DECIMAL##_UNSIGNED(pOut, uv, max);                                  \
      } break;                                                                             \
      case TSDB_DATA_TYPE_USMALLINT: {                                                     \
        uv = *(uint16_t*)pInputData;                                                       \
        CHECK_AND_MAKE_DECIMAL##_UNSIGNED(pOut, uv, max);                                  \
      } break;                                                                             \
      case TSDB_DATA_TYPE_UINT: {                                                          \
        uv = *(uint32_t*)pInputData;                                                       \
        CHECK_AND_MAKE_DECIMAL##_UNSIGNED(pOut, uv, max);                                  \
      } break;                                                                             \
      case TSDB_DATA_TYPE_UBIGINT: {                                                       \
        uv = *(uint64_t*)pInputData;                                                       \
        CHECK_AND_MAKE_DECIMAL##_UNSIGNED(pOut, uv, max);                                  \
      } break;                                                                             \
      case TSDB_DATA_TYPE_FLOAT: {                                                         \
      } break;                                                                             \
      case TSDB_DATA_TYPE_DOUBLE: {                                                        \
      } break;                                                                             \
      case TSDB_DATA_TYPE_VARCHAR:                                                         \
      case TSDB_DATA_TYPE_VARBINARY:                                                       \
      case TSDB_DATA_TYPE_NCHAR: {                                                         \
      } break;                                                                             \
      case TSDB_DATA_TYPE_DECIMAL64: {                                                     \
      } break;                                                                             \
      case TSDB_DATA_TYPE_DECIMAL: {                                                       \
      } break;                                                                             \
      default:                                                                             \
        code = TSDB_CODE_OPS_NOT_SUPPORT;                                                  \
        break;                                                                             \
    }                                                                                      \
  } while (0)

int32_t convertToDecimal(const void* pData, const SDataType* pInputType, void* pOut, const SDataType* pOutType) {
  if (pInputType->type == pOutType->type) return 0;
  int32_t code = 0;

  switch (pOutType->type) {
    case TSDB_DATA_TYPE_DECIMAL64: {
      Decimal64 max = {0};
      DECIMAL64_GET_MAX(pOutType->precision - pOutType->scale, &max);
      CONVERT_TO_DECIMAL(pInputType->type, pData, pOut, MAKE_DECIMAL64, max, code);
    } break;
    case TSDB_DATA_TYPE_DECIMAL: {
      Decimal128 max = {0};
      DECIMAL128_GET_MAX(pOutType->precision - pOutType->scale, &max);
      CONVERT_TO_DECIMAL(pInputType->type, pData, pOut, MAKE_DECIMAL128, max, code);
    } break;
    default:
      code = TSDB_CODE_INTERNAL_ERROR;
      break;
  }
  if (TSDB_CODE_SUCCESS == code) {
    // scale to output scale
  }
  return code;
}
