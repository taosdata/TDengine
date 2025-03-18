#include <gtest/gtest.h>
#include <array>
#include <fstream>
#include <iostream>
#include <random>
#define ALLOW_FORBID_FUNC

#include "decimal.h"
#include "tdatablock.h"
using namespace std;

template <int N>
void printArray(const std::array<uint64_t, N>& arr) {
  auto it = arr.rbegin();
  for (; it != arr.rend(); ++it) {
    cout << *it;
  }
  cout << endl;
}

#if 0
template <int DIGIT_NUM>
void extractWideInteger(__int128 a) {
  uint64_t                             k = 10;
  std::array<uint64_t, 38 / DIGIT_NUM> segments{};
  int                                  seg_num = 0;
  for (int i = 0; i < DIGIT_NUM; ++i) {
    k *= 10;
  }

  while (a != 0) {
    uint64_t hi = a >> 64;
    uint64_t lo = a;
    cout << "hi: " << hi << " lo: " << lo << endl;
    uint64_t hi_quotient = hi / k;
    uint64_t hi_remainder = hi % k;
    // cout << "hi % 1e9: " << hi_remainder << endl;
    __int128 tmp = ((__int128)hi_remainder << 64) | (__int128)lo;
    uint64_t lo_remainder = tmp % k;
    uint64_t lo_quotient = tmp / k;
    a = (__int128)hi_quotient << 64 | (__int128)lo_quotient;
    segments[seg_num++] = lo_remainder;
  }
  printArray<38 / DIGIT_NUM>(segments);
}

void printDecimal(const DecimalType* pDec, uint8_t type, uint8_t prec, uint8_t scale) {
  char    buf[64] = {0};
  int32_t code = decimalToStr(pDec, type, prec, scale, buf, 64);
  ASSERT_EQ(code, 0);
  cout << buf;
}

__int128 generate_big_int128(uint32_t digitNum) {
  __int128 a = 0;
  for (int i = 0; i < digitNum + 1; ++i) {
    a *= 10;
    a += (i % 10);
  }
  return a;
}
#endif

void checkDecimal(const DecimalType* pDec, uint8_t t, uint8_t prec, uint8_t scale, const char* valExpect) {
  ASSERT_TRUE(t == prec > 18 ? TSDB_DATA_TYPE_DECIMAL : TSDB_DATA_TYPE_DECIMAL64);
  ASSERT_TRUE(scale <= prec);
  char    buf[64] = {0};
  int32_t code = decimalToStr(pDec, t, prec, scale, buf, 64);
  ASSERT_EQ(code, 0);
  ASSERT_STREQ(buf, valExpect);
  cout << "decimal" << (prec > 18 ? 128 : 64) << " " << (int32_t)prec << ":" << (int32_t)scale << " -> " << buf << endl;
}

class Numeric128;
class Numeric64 {
  Decimal64                dec_;
  static constexpr uint8_t DECIMAL_WORD_NUM = DECIMAL_WORD_NUM(Decimal64);

 public:
  friend class Numeric128;
  Numeric64() { dec_ = {0}; }
  int32_t fromStr(const string& str, uint8_t prec, uint8_t scale) {
    return decimal64FromStr(str.c_str(), str.size(), prec, scale, &dec_);
  }
  Numeric64& operator+=(const Numeric64& r) {
    getOps()->add(&dec_, &r.dec_, DECIMAL_WORD_NUM);
    return *this;
  }
  // Numeric64& operator+=(const Numeric128& r);

  bool       operator==(const Numeric64& r) const { return getOps()->eq(&dec_, &r.dec_, DECIMAL_WORD_NUM); }
  Numeric64& operator=(const Numeric64& r);
  Numeric64& operator=(const Numeric128& r);

  static const SDecimalOps* getOps() { return getDecimalOps(TSDB_DATA_TYPE_DECIMAL64); }
};

class Numeric128 {
  Decimal128               dec_;
  static constexpr uint8_t DECIMAL_WORD_NUM = DECIMAL_WORD_NUM(Decimal128);

 public:
  friend Numeric64;
  Numeric128() { dec_ = {0}; }
  Numeric128(const Numeric128& r) = default;
  int32_t fromStr(const string& str, uint8_t prec, uint8_t scale) {
    return decimal128FromStr(str.c_str(), str.size(), prec, scale, &dec_);
  }
  Numeric128& operator+=(const Numeric128& r) { return *this; }
  Numeric128& operator+=(const Numeric64& r) {
    getOps()->add(&dec_, &r.dec_, Numeric64::DECIMAL_WORD_NUM);
    return *this;
  }

  static const SDecimalOps* getOps() { return getDecimalOps(TSDB_DATA_TYPE_DECIMAL); }
};

Numeric64& Numeric64::operator=(const Numeric64& r) {
  dec_ = r.dec_;
  return *this;
}

template <int BitNum>
struct NumericType {};

template <>
struct NumericType<64> {
  using Type = Numeric64;
  static constexpr int8_t dataType = TSDB_DATA_TYPE_DECIMAL64;
  static constexpr int8_t maxPrec = TSDB_DECIMAL64_MAX_PRECISION;
  static constexpr int8_t bytes = DECIMAL64_BYTES;
};

template <>
struct NumericType<128> {
  using Type = Numeric128;
  static constexpr int8_t dataType = TSDB_DATA_TYPE_DECIMAL;
  static constexpr int8_t maxPrec = TSDB_DECIMAL_MAX_PRECISION;
  static constexpr int8_t bytes = DECIMAL128_BYTES;
};

template <typename T>
struct TrivialTypeInfo {
  using TrivialType = T;
};

#define DEFINE_TRIVIAL_TYPE_HELPER(type, tsdb_type) \
  template <>                                       \
  struct TrivialTypeInfo<type> {                    \
    static constexpr int8_t  dataType = tsdb_type;  \
    static constexpr int32_t bytes = sizeof(type);  \
  }

DEFINE_TRIVIAL_TYPE_HELPER(int8_t, TSDB_DATA_TYPE_TINYINT);
DEFINE_TRIVIAL_TYPE_HELPER(uint8_t, TSDB_DATA_TYPE_UTINYINT);
DEFINE_TRIVIAL_TYPE_HELPER(int16_t, TSDB_DATA_TYPE_SMALLINT);
DEFINE_TRIVIAL_TYPE_HELPER(uint16_t, TSDB_DATA_TYPE_USMALLINT);
DEFINE_TRIVIAL_TYPE_HELPER(int32_t, TSDB_DATA_TYPE_INT);
DEFINE_TRIVIAL_TYPE_HELPER(uint32_t, TSDB_DATA_TYPE_UINT);
DEFINE_TRIVIAL_TYPE_HELPER(int64_t, TSDB_DATA_TYPE_BIGINT);
DEFINE_TRIVIAL_TYPE_HELPER(uint64_t, TSDB_DATA_TYPE_UBIGINT);
DEFINE_TRIVIAL_TYPE_HELPER(float, TSDB_DATA_TYPE_FLOAT);
DEFINE_TRIVIAL_TYPE_HELPER(double, TSDB_DATA_TYPE_DOUBLE);
DEFINE_TRIVIAL_TYPE_HELPER(bool, TSDB_DATA_TYPE_BOOL);

template <int BitNum>
class Numeric {
  static_assert(BitNum == 64 || BitNum == 128, "only support Numeric64 and Numeric128");
  using Type = typename NumericType<BitNum>::Type;
  Type    dec_;
  uint8_t prec_;
  uint8_t scale_;

 public:
  Numeric(uint8_t prec, uint8_t scale, const std::string& str) : prec_(prec), scale_(scale) {
    if (prec > NumericType<BitNum>::maxPrec) throw std::string("prec too big") + std::to_string(prec);
    int32_t code = dec_.fromStr(str, prec, scale) != 0;
    if (code != 0) {
      cout << "failed to init decimal(" << (int32_t)prec << "," << (int32_t)scale << ") from str: " << str << "\t";
      throw std::overflow_error(tstrerror(code));
    }
  }
  Numeric() = default;
  Numeric(const Numeric& o) = default;
  ~Numeric() = default;
  Numeric& operator=(const Numeric& o) = default;

  static SDataType getRetType(EOperatorType op, const SDataType& lt, const SDataType& rt) {
    SDataType ot = {0};
    int32_t   code = decimalGetRetType(&lt, &rt, op, &ot);
    if (code != 0) throw std::runtime_error(tstrerror(code));
    return ot;
  }
  SDataType   type() const { return {NumericType<BitNum>::dataType, prec(), scale(), NumericType<BitNum>::bytes}; }
  uint8_t     prec() const { return prec_; }
  uint8_t     scale() const { return scale_; }
  const Type& dec() const { return dec_; }
  STypeMod    get_type_mod() const { return decimalCalcTypeMod(prec(), scale()); }

  template <int BitNum2>
  Numeric& binaryOp(const Numeric<BitNum2>& r, EOperatorType op) {
    auto out = binaryOp<BitNum2, BitNum>(r, op);
    return *this = out;
  }

  template <int BitNum2, int BitNumO>
  Numeric<BitNumO> binaryOp(const Numeric<BitNum2>& r, EOperatorType op) {
    SDataType        lt{NumericType<BitNum>::dataType, prec_, scale_, NumericType<BitNum>::bytes};
    SDataType        rt{NumericType<BitNum2>::dataType, r.prec(), r.scale(), NumericType<BitNum2>::bytes};
    SDataType        ot = getRetType(op, lt, rt);
    Numeric<BitNumO> out{ot.precision, ot.scale, "0"};
    int32_t          code = decimalOp(op, &lt, &rt, &ot, &dec_, &r.dec(), &out);
    if (code != 0) throw std::overflow_error(tstrerror(code));
    return out;
  }

  template <int BitNumO, typename T>
  Numeric<BitNumO> binaryOp(const T& r, EOperatorType op) {
    using TypeInfo = TrivialTypeInfo<T>;
    SDataType        lt{NumericType<BitNum>::dataType, prec_, scale_, NumericType<BitNum>::bytes};
    SDataType        rt{TypeInfo::dataType, 0, 0, TypeInfo::bytes};
    SDataType        ot = getRetType(op, lt, rt);
    Numeric<BitNumO> out{ot.precision, ot.scale, "0"};
    int32_t          code = decimalOp(op, &lt, &rt, &ot, &dec_, &r, &out);
    if (code == TSDB_CODE_DECIMAL_OVERFLOW) throw std::overflow_error(tstrerror(code));
    if (code != 0) throw std::runtime_error(tstrerror(code));
    return out;
  }
#define DEFINE_OPERATOR(op, op_type)                        \
  template <int BitNum2, int BitNumO = 128>                 \
  Numeric<BitNumO> operator op(const Numeric<BitNum2>& r) { \
    cout << *this << " " #op " " << r << " = ";             \
    auto res = binaryOp<BitNum2, BitNumO>(r, op_type);      \
    cout << res << endl;                                    \
    return res;                                             \
  }

  DEFINE_OPERATOR(+, OP_TYPE_ADD);
  DEFINE_OPERATOR(-, OP_TYPE_SUB);
  DEFINE_OPERATOR(*, OP_TYPE_MULTI);
  DEFINE_OPERATOR(/, OP_TYPE_DIV);
  DEFINE_OPERATOR(%, OP_TYPE_REM);

#define DEFINE_TYPE_OP(op, op_type)                                               \
  template <typename T, int BitNumO = 128>                                        \
  Numeric<BitNumO> operator op(const T & r) {                                     \
    cout << *this << " " #op " " << r << "(" << typeid(T).name() << ")" << " = "; \
    Numeric<BitNumO> res = {};                                                    \
    try {                                                                         \
      res = binaryOp<BitNumO, T>(r, op_type);                                     \
    } catch (...) {                                                               \
      throw;                                                                      \
    }                                                                             \
    cout << res << endl;                                                          \
    return res;                                                                   \
  }
  DEFINE_TYPE_OP(+, OP_TYPE_ADD);
  DEFINE_TYPE_OP(-, OP_TYPE_SUB);
  DEFINE_TYPE_OP(*, OP_TYPE_MULTI);
  DEFINE_TYPE_OP(/, OP_TYPE_DIV);
  DEFINE_TYPE_OP(%, OP_TYPE_REM);

#define DEFINE_REAL_OP(op)                                                     \
  double operator op(double v) {                                               \
    if (BitNum == 128)                                                         \
      return TEST_decimal128ToDouble((Decimal128*)&dec_, prec(), scale()) / v; \
    else if (BitNum == 64)                                                     \
      return TEST_decimal64ToDouble((Decimal64*)&dec_, prec(), scale()) / v;   \
    return 0;                                                                  \
  }
  DEFINE_REAL_OP(+);
  DEFINE_REAL_OP(-);
  DEFINE_REAL_OP(*);
  DEFINE_REAL_OP(/);

  template <int BitNum2>
  Numeric& operator+=(const Numeric<BitNum2>& r) {
    return binaryOp(r, OP_TYPE_ADD);
  }

  std::string toString() const {
    char    buf[64] = {0};
    int32_t code = decimalToStr(&dec_, NumericType<BitNum>::dataType, prec(), scale(), buf, 64);
    if (code != 0) throw std::string(tstrerror(code));
    return {buf};
  }

  std::string toStringTrimTailingZeros() const {
    auto    ret = toString();
    int32_t sizeToRemove = 0;
    auto    it = ret.rbegin();
    for (; it != ret.rend(); ++it) {
      if (*it == '0') {
        ++sizeToRemove;
        continue;
      }
      break;
    }
    if (ret.size() - sizeToRemove > 0) ret.resize(ret.size() - sizeToRemove);
    return ret;
  }
#define DEFINE_OPERATOR_T(type)                            \
  operator type() {                                        \
    if (BitNum == 64) {                                    \
      return type##FromDecimal64(&dec_, prec(), scale());  \
    } else if (BitNum == 128) {                            \
      return type##FromDecimal128(&dec_, prec(), scale()); \
    }                                                      \
    return 0;                                              \
  }
  DEFINE_OPERATOR_T(bool);
  DEFINE_OPERATOR_T(int8_t);
  DEFINE_OPERATOR_T(uint8_t);
  DEFINE_OPERATOR_T(int16_t);
  DEFINE_OPERATOR_T(uint16_t);
  DEFINE_OPERATOR_T(int32_t);
  DEFINE_OPERATOR_T(uint32_t);
  DEFINE_OPERATOR_T(int64_t);
  DEFINE_OPERATOR_T(uint64_t);
  DEFINE_OPERATOR_T(float);
  DEFINE_OPERATOR_T(double);

  Numeric& operator=(const char* str) {
    std::string s = str;
    int32_t     code = 0;
    if (BitNum == 64) {
      code = decimal64FromStr(s.c_str(), s.size(), prec(), scale(), (Decimal64*)&dec_);
    } else if (BitNum == 128) {
      code = decimal128FromStr(s.c_str(), s.size(), prec(), scale(), (Decimal128*)&dec_);
    }
    if (TSDB_CODE_SUCCESS != code) {
      throw std::string("failed to convert str to decimal64: ") + s + " " + tstrerror(code);
    }
    return *this;
  }

#define DEFINE_OPERATOR_FROM_FOR_BITNUM(type, BitNum)                                        \
  if (std::is_floating_point<type>::value) {                                                 \
    code = TEST_decimal##BitNum##From_double((Decimal##BitNum*)&dec_, prec(), scale(), v);   \
  } else if (std::is_signed<type>::value) {                                                  \
    code = TEST_decimal##BitNum##From_int64_t((Decimal##BitNum*)&dec_, prec(), scale(), v);  \
  } else if (std::is_unsigned<type>::value) {                                                \
    code = TEST_decimal##BitNum##From_uint64_t((Decimal##BitNum*)&dec_, prec(), scale(), v); \
  }

#define DEFINE_OPERATOR_EQ_T(type)                \
  Numeric& operator=(type v) {                    \
    int32_t code = 0;                             \
    if (BitNum == 64) {                           \
      DEFINE_OPERATOR_FROM_FOR_BITNUM(type, 64);  \
    } else if (BitNum == 128) {                   \
      DEFINE_OPERATOR_FROM_FOR_BITNUM(type, 128); \
    }                                             \
    return *this;                                 \
  }
  DEFINE_OPERATOR_EQ_T(int64_t);
  DEFINE_OPERATOR_EQ_T(int32_t);
  DEFINE_OPERATOR_EQ_T(int16_t);
  DEFINE_OPERATOR_EQ_T(int8_t);

  DEFINE_OPERATOR_EQ_T(uint64_t);
  DEFINE_OPERATOR_EQ_T(uint32_t);
  DEFINE_OPERATOR_EQ_T(uint16_t);
  DEFINE_OPERATOR_EQ_T(uint8_t);

  DEFINE_OPERATOR_EQ_T(bool);
  DEFINE_OPERATOR_EQ_T(double);
  DEFINE_OPERATOR_EQ_T(float);

  Numeric& operator=(const Decimal128& d) {
    SDataType inputDt = {TSDB_DATA_TYPE_DECIMAL, prec(), scale(), DECIMAL128_BYTES};
    SDataType outputDt = {NumericType<BitNum>::dataType, prec(), scale(), NumericType<BitNum>::bytes};
    int32_t   code = convertToDecimal(&d, &inputDt, &dec_, &outputDt);
    if (code == TSDB_CODE_DECIMAL_OVERFLOW) throw std::overflow_error(tstrerror(code));
    if (code != 0) throw std::runtime_error(tstrerror(code));
    return *this;
  }
  Numeric& operator=(const Decimal64& d) {
    SDataType inputDt = {TSDB_DATA_TYPE_DECIMAL64, prec(), scale(), DECIMAL64_BYTES};
    SDataType outputDt = {NumericType<BitNum>::dataType, prec_, scale_, NumericType<BitNum>::bytes};
    int32_t   code = convertToDecimal(&d, &inputDt, &dec_, &outputDt);
    if (code == TSDB_CODE_DECIMAL_OVERFLOW) throw std::overflow_error(tstrerror(code));
    if (code != 0) throw std::runtime_error(tstrerror(code));
    return *this;
  }

  template <int BitNum2>
  Numeric(const Numeric<BitNum2>& num2) {
    Numeric();
    *this = num2;
  }

  template <int BitNum2>
  Numeric& operator=(const Numeric<BitNum2>& num2) {
    static_assert(BitNum2 == 64 || BitNum2 == 128, "Only support decimal128/decimal64");
    SDataType inputDt = {num2.type().type, num2.prec(), num2.scale(), num2.type().bytes};
    SDataType outputDt = {NumericType<BitNum>::dataType, NumericType<BitNum>::maxPrec, num2.scale(),
                          NumericType<BitNum>::bytes};
    int32_t   code = convertToDecimal(&num2.dec(), &inputDt, &dec_, &outputDt);
    if (code == TSDB_CODE_DECIMAL_OVERFLOW) throw std::overflow_error(tstrerror(code));
    if (code != 0) throw std::runtime_error(tstrerror(code));
    prec_ = outputDt.precision;
    scale_ = outputDt.scale;
    return *this;
  }
#define DEFINE_COMPARE_OP(op, op_type)                                                        \
  template <typename T>                                                                       \
  bool operator op(const T& t) {                                                              \
    Numeric<128> lDec = *this, rDec = *this;                                                  \
    rDec = t;                                                                                 \
    SDecimalCompareCtx l = {(void*)&lDec.dec(), TSDB_DATA_TYPE_DECIMAL, lDec.get_type_mod()}, \
                       r = {(void*)&rDec.dec(), TSDB_DATA_TYPE_DECIMAL, rDec.get_type_mod()}; \
    return decimalCompare(op_type, &l, &r);                                                   \
  }

  DEFINE_COMPARE_OP(>, OP_TYPE_GREATER_THAN);
  DEFINE_COMPARE_OP(>=, OP_TYPE_GREATER_EQUAL);
  DEFINE_COMPARE_OP(<, OP_TYPE_LOWER_THAN);
  DEFINE_COMPARE_OP(<=, OP_TYPE_LOWER_EQUAL);
  DEFINE_COMPARE_OP(==, OP_TYPE_EQUAL);
  DEFINE_COMPARE_OP(!=, OP_TYPE_NOT_EQUAL);
};

template <int BitNum>
ostream& operator<<(ostream& os, const Numeric<BitNum>& n) {
  os << n.toString() << "(" << (int32_t)n.prec() << ":" << (int32_t)n.scale() << ")";
  return os;
}

TEST(decimal, numeric) {
  Numeric<64> dec{10, 4, "123.456"};
  Numeric<64> dec2{18, 10, "123456.123123"};
  auto        o = dec + dec2;
  ASSERT_EQ(o.toString(), "123579.5791230000");

  Numeric<128> dec128{37, 10, "123456789012300.09876543"};
  o = dec + dec128;
  ASSERT_EQ(o.toStringTrimTailingZeros(), "123456789012423.55476543");
  ASSERT_EQ(o.toString(), "123456789012423.5547654300");

  auto os = o - dec;
  ASSERT_EQ(os.toStringTrimTailingZeros(), dec128.toStringTrimTailingZeros());

  auto os2 = o - dec128;
  ASSERT_EQ(os2.toStringTrimTailingZeros(), dec.toStringTrimTailingZeros());

  os = dec * dec2;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "15241399.136273088");
  ASSERT_EQ(os.toString(), "15241399.13627308800000");

  os = dec * dec128;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "15241481344302520.993185");
  ASSERT_EQ(os.toString(), "15241481344302520.993185");

  os2 = os / dec128;
  ASSERT_EQ(os2.toStringTrimTailingZeros(), "123.456");
  ASSERT_EQ(os2.toString(), "123.456000");

  os = dec2 / dec;
  ASSERT_EQ(os.toString(), "1000.000997302682737169518");

  int32_t a = 123;
  os = dec + a;
  ASSERT_EQ(os.toString(), "246.4560");

  os = dec * a;
  ASSERT_EQ(os.toString(), "15185.0880");

  os = dec / 2;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "61.728");

  os = dec2 / 2;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "61728.0615615");

  os = dec128 / 2;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "61728394506150.049382715");

  auto dec3 = Numeric<64>(10, 2, "171154.38");
  os = dec3 / 2;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "85577.19");

  auto dec4 = Numeric<64>(10, 5, "1.23456");
  os = dec4 / 2;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "0.61728");

  os = dec4 / 123123123;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "0.0000000100270361");

  os = dec4 / (int64_t)123123123;
  ASSERT_EQ(os.toStringTrimTailingZeros(), "0.0000000100270361075880117");

  double dv = dec4 / 123123.123;

  Numeric<128> max{38, 0, "99999999999999999999999999999999999999.000"};
  ASSERT_EQ(max.toString(), "99999999999999999999999999999999999999");
  Numeric<128> zero{38, 0, "0"};
  auto         min = zero - max;
  ASSERT_EQ(min.toString(), "-99999999999999999999999999999999999999");

  dec = 123.456;
  ASSERT_EQ(dec.toString(), "123.4560");

  dec = 47563.36;
  dec128 = 0;
  o = dec128 + dec;  // (37, 10) + (10, 4) = (38, 10)
  ASSERT_EQ(o.toString(), "47563.3600000000");
  dec = 3749.00;
  o = o + dec;  // (38, 10) + (10, 4) = (38, 9)
  ASSERT_EQ(o.toString(), "51312.360000000");
}

TEST(decimal, decimalFromType) {
  Numeric<128> dec1{20, 4, "0"};
  dec1 = 123.123;
  ASSERT_EQ(dec1.toString(), "123.1230");
  dec1 = (float)123.123;
  ASSERT_EQ(dec1.toString(), "123.1230");
  dec1 = (int64_t)-9999999;
  ASSERT_EQ(dec1.toString(), "-9999999.0000");
  dec1 = "99.99999";
  ASSERT_EQ(dec1.toString(), "100.0000");
}

TEST(decimal, typeFromDecimal) {
  Numeric<128> dec1{18, 4, "1234"};
  Numeric<64>  dec2{18, 4, "1234"};
  int64_t      intv = dec1;
  uint64_t     uintv = dec1;
  double       doublev = dec1;
  ASSERT_EQ(intv, 1234);
  ASSERT_EQ(uintv, 1234);
  ASSERT_EQ(doublev, 1234);
  doublev = dec2;
  ASSERT_EQ(doublev, 1234);
  intv = dec1 = "123.43";
  uintv = dec1;
  doublev = dec1;
  ASSERT_EQ(intv, 123);
  ASSERT_EQ(uintv, 123);
  ASSERT_EQ(doublev, 123.43);
  doublev = dec2 = "123.54";
  ASSERT_EQ(doublev, 123.54);
  intv = dec1 = "123.66";
  uintv = dec1;
  doublev = dec1;
  ASSERT_EQ(intv, 124);
  ASSERT_EQ(uintv, 124);
  ASSERT_EQ(doublev, 123.66);

  intv = dec1 = "-123.44";
  uintv = dec1;
  doublev = dec1;
  ASSERT_EQ(intv, -123);
  ASSERT_EQ(uintv, 18446744073709551493ULL);
  ASSERT_EQ(doublev, -123.44);
  intv = dec1 = "-123.99";
  uintv = dec1;
  doublev = dec1;
  ASSERT_EQ(intv, -124);
  ASSERT_EQ(uintv, 18446744073709551492ULL);
  ASSERT_EQ(doublev, -123.99);

  bool boolv = false;
  boolv = dec1;
  ASSERT_TRUE(boolv);
  boolv = dec1 = "0";
  ASSERT_FALSE(boolv);
}

#if 0
TEST(decimal128, to_string) {
  __int128   i = generate_big_int128(37);
  int64_t    hi = i >> 64;
  uint64_t   lo = i;
  Decimal128 d;
  makeDecimal128(&d, hi, lo);
  char buf[64] = {0};
  decimalToStr(d.words, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf, 64);
  ASSERT_STREQ(buf, "123456789012345678901234567.8901234567");

  buf[0] = '\0';
  decimalToStr(d.words, TSDB_DATA_TYPE_DECIMAL, 38, 9, buf, 64);
  ASSERT_STREQ(buf, "1234567890123456789012345678.901234567");
}

TEST(decimal, divide) {
  __int128   i = generate_big_int128(15);
  int64_t    hi = i >> 64;
  uint64_t   lo = i;
  Decimal128 d;
  makeDecimal128(&d, hi, lo);

  Decimal128 d2 = {0};
  makeDecimal128(&d2, 0, 12345678);

  auto       ops = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  Decimal128 remainder = {0};
  int8_t     precision1 = 38, scale1 = 5, precision2 = 10, scale2 = 2;
  int8_t     out_scale = 25;
  int8_t     out_precision = std::min(precision1 - scale1 + scale2 + out_scale, 38);
  int8_t     delta_scale = out_scale + scale2 - scale1;
  printDecimal(&d, TSDB_DATA_TYPE_DECIMAL, precision1, scale1);
  __int128 a = 1;
  while (delta_scale-- > 0) a *= 10;
  Decimal128 multiplier = {0};
  makeDecimal128(&multiplier, a >> 64, a);
  ops->multiply(d.words, multiplier.words, 2);
  cout << " / ";
  printDecimal(&d2, TSDB_DATA_TYPE_DECIMAL, precision2, scale2);
  cout << " = ";
  ops->divide(d.words, d2.words, 2, remainder.words);
  printDecimal(&d, TSDB_DATA_TYPE_DECIMAL, out_precision, out_scale);
  ASSERT_TRUE(1);
}
#endif

TEST(decimal, conversion) {
  // convert uint8 to decimal
  char      buf[64] = {0};
  int8_t    i8 = 22;
  SDataType inputType = {TSDB_DATA_TYPE_TINYINT, 1};
  uint8_t   prec = 10, scale = 2;
  SDataType decType = {TSDB_DATA_TYPE_DECIMAL64, prec, scale, 8};
  Decimal64 dec64 = {0};
  int32_t   code = convertToDecimal(&i8, &inputType, &dec64, &decType);
  ASSERT_TRUE(code == 0);
  cout << "convert uint8: " << (int32_t)i8 << " to ";
  checkDecimal(&dec64, TSDB_DATA_TYPE_DECIMAL64, prec, scale, "22.00");

  Decimal128 dec128 = {0};
  decType.type = TSDB_DATA_TYPE_DECIMAL;
  decType.precision = 38;
  decType.scale = 10;
  decType.bytes = 16;
  code = convertToDecimal(&i8, &inputType, &dec128, &decType);
  ASSERT_TRUE(code == 0);
  cout << "convert uint8: " << (int32_t)i8 << " to ";
  checkDecimal(&dec128, TSDB_DATA_TYPE_DECIMAL, decType.precision, decType.scale, "22.0000000000");

  char inputBuf[64] = "123.000000000000000000000000000000001";
  code = decimal128FromStr(inputBuf, strlen(inputBuf), 38, 35, &dec128);
  ASSERT_EQ(code, 0);
  checkDecimal(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 35, "123.00000000000000000000000000000000100");

  inputType.type = TSDB_DATA_TYPE_DECIMAL64;
  inputType.precision = prec;
  inputType.scale = scale;
  code = convertToDecimal(&dec64, &inputType, &dec128, &decType);
  ASSERT_EQ(code, 0);
  checkDecimal(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, "22.0000000000");
}

static constexpr uint64_t k1E16 = 10000000000000000LL;

#if 0
TEST(decimal, decimalFromStr) {
  char       inputBuf[64] = "123.000000000000000000000000000000001";
  Decimal128 dec128 = {0};
  int32_t    code = decimal128FromStr(inputBuf, strlen(inputBuf), 38, 35, &dec128);
  ASSERT_EQ(code, 0);
  __int128 res = decimal128ToInt128(&dec128);
  __int128 resExpect = 123;
  resExpect *= k1E16;
  resExpect *= k1E16;
  resExpect *= 10;
  resExpect += 1;
  resExpect *= 100;
  ASSERT_EQ(res, resExpect);

  char      buf[64] = "999.999";
  Decimal64 dec64 = {0};
  code = decimal64FromStr(buf, strlen(buf), 6, 3, &dec64);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(999999, DECIMAL64_GET_VALUE(&dec64));

}
#endif

TEST(decimal, toStr) {
  Decimal64 dec = {0};
  char      buf[64] = {0};
  int32_t   code = decimalToStr(&dec, TSDB_DATA_TYPE_DECIMAL64, 10, 2, buf, 64);
  ASSERT_EQ(code, 0);
  ASSERT_STREQ(buf, "0");

  Decimal128 dec128 = {0};
  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf, 64);
  ASSERT_EQ(code, 0);
  ASSERT_STREQ(buf, "0");

  char buf2[1];
  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf2, 1);
  ASSERT_TRUE(buf2[0] == '0');

  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, NULL, 100);
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);

  makeDecimal128(&dec128, 999999999999, 999999999999);
  ASSERT_EQ(TSDB_CODE_SUCCESS, decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf, 64));
  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf2, 1);
  ASSERT_EQ(buf2[0], buf[0]);
}

SDataType getDecimalType(uint8_t prec, uint8_t scale) {
  if (prec > TSDB_DECIMAL_MAX_PRECISION) throw std::string("invalid prec: ") + std::to_string(prec);
  uint8_t type = decimalTypeFromPrecision(prec);
  return {type, prec, scale, tDataTypes[type].bytes};
}

bool operator==(const SDataType& lt, const SDataType& rt) {
  return lt.type == rt.type && lt.precision == rt.precision && lt.scale == rt.scale && lt.bytes == rt.bytes;
}

TEST(decimal, decimalOpRetType) {
  EOperatorType op = OP_TYPE_ADD;
  auto          ta = getDecimalType(10, 2);
  auto          tb = getDecimalType(10, 2);
  SDataType     tc{}, tExpect = {TSDB_DATA_TYPE_DECIMAL, 11, 2, sizeof(Decimal)};
  int32_t       code = decimalGetRetType(&ta, &tb, op, &tc);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tExpect, tc);

  ta.bytes = 8;
  ta.type = TSDB_DATA_TYPE_TIMESTAMP;

  code = decimalGetRetType(&ta, &tb, op, &tc);
  ASSERT_EQ(code, 0);
  tExpect.type = TSDB_DATA_TYPE_DECIMAL;
  tExpect.precision = 22;
  tExpect.scale = 2;
  tExpect.bytes = sizeof(Decimal);
  ASSERT_EQ(tExpect, tc);

  ta.bytes = 8;
  ta.type = TSDB_DATA_TYPE_DOUBLE;
  tc = {0};
  code = decimalGetRetType(&ta, &tb, op, &tc);
  ASSERT_EQ(code, 0);
  tExpect.type = TSDB_DATA_TYPE_DOUBLE;
  tExpect.precision = 0;
  tExpect.scale = 0;
  tExpect.bytes = 8;
  ASSERT_EQ(tExpect, tc);

  op = OP_TYPE_DIV;
  ta = getDecimalType(10, 2);
  tb = getDecimalType(10, 2);
  tExpect.type = TSDB_DATA_TYPE_DECIMAL;
  tExpect.precision = 23;
  tExpect.scale = 13;
  tExpect.bytes = sizeof(Decimal);
  code = decimalGetRetType(&ta, &tb, op, &tc);

  Numeric<64> aNum = {10, 2, "123.99"};
  int64_t     bInt64 = 317759474393305778;
  auto        res = aNum / bInt64;
  ASSERT_EQ(res.scale(), 22);
}

TEST(decimal, op) {
  const char *  stra = "123.99", *strb = "456.12";
  EOperatorType op = OP_TYPE_ADD;
  auto          ta = getDecimalType(10, 2);
  Decimal64     a = {0};
  int32_t       code = decimal64FromStr(stra, strlen(stra), ta.precision, ta.scale, &a);
  ASSERT_EQ(code, 0);

  auto      tb = getDecimalType(10, 2);
  Decimal64 b{0};
  code = decimal64FromStr(strb, strlen(strb), tb.precision, tb.scale, &b);
  ASSERT_EQ(code, 0);

  SDataType tc{}, tExpect{TSDB_DATA_TYPE_DECIMAL, 11, 2, sizeof(Decimal)};
  code = decimalGetRetType(&ta, &tb, op, &tc);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(tc, tExpect);
  Decimal res{};
  code = decimalOp(op, &ta, &tb, &tc, &a, &b, &res);
  ASSERT_EQ(code, 0);

  checkDecimal(&res, TSDB_DATA_TYPE_DECIMAL, tc.precision, tc.scale, "580.11");

  a = {1234567890};
  b = {9876543210};
  ta = getDecimalType(18, 5);
  tb = getDecimalType(15, 3);
  code = decimalGetRetType(&ta, &tb, op, &tc);
  ASSERT_EQ(code, 0);
  tExpect.precision = 19;
  tExpect.scale = 5;
  tExpect.type = TSDB_DATA_TYPE_DECIMAL;
  tExpect.bytes = sizeof(Decimal128);
  ASSERT_EQ(tExpect, tc);
  Decimal128 res128 = {0};
  code = decimalOp(op, &ta, &tb, &tc, &a, &b, &res128);
  ASSERT_EQ(code, 0);
  checkDecimal(&res128, 0, tExpect.precision, tExpect.scale, "9888888.88890");
}

struct DecimalStringRandomGeneratorConfig {
  uint8_t prec = 38;
  uint8_t scale = 10;
  bool    enableWeightOverflow = false;
  float   weightOverflowRatio = 0.001;
  bool    enableScaleOverflow = true;
  float   scaleOverFlowRatio = 0.1;
  bool    enablePositiveSign = false;
  bool    withCornerCase = true;
  float   cornerCaseRatio = 0.1;
  float   positiveRatio = 0.7;
};

class DecimalStringRandomGenerator {
  std::random_device                 rd_;
  std::mt19937                       gen_;
  std::uniform_int_distribution<int> dis_;
  static const std::array<string, 5> cornerCases;
  static const unsigned int          ratio_base = 1000000;

 public:
  DecimalStringRandomGenerator() : gen_(rd_()), dis_(0, ratio_base) {}
  std::string generate(const DecimalStringRandomGeneratorConfig& config) {
    std::string ret;
    auto        sign = generateSign(config.positiveRatio);
    if (config.enablePositiveSign || sign != '+') ret.push_back(sign);
    if (config.withCornerCase && currentShouldGenerateCornerCase(config.cornerCaseRatio)) {
      ret += generateCornerCase(config);
    } else {
      uint8_t prec = randomInt(config.prec - config.scale), scale = randomInt(config.scale);
      for (int i = 0; i < prec; ++i) {
        ret.push_back(generateDigit());
      }
      if (config.enableWeightOverflow && possible(config.weightOverflowRatio)) {
        int extra_weight = config.prec - prec + 1 + randomInt(TSDB_DECIMAL_MAX_PRECISION);
        while (extra_weight--) {
          ret.push_back(generateDigit());
        }
      }
      ret.push_back('.');
      for (int i = 0; i < scale; ++i) {
        ret.push_back(generateDigit());
      }
      if (config.enableScaleOverflow && possible(config.scaleOverFlowRatio)) {
        int extra_scale = config.scale - scale + 1 + randomInt(TSDB_DECIMAL_MAX_SCALE);
        while (extra_scale--) {
          ret.push_back(generateDigit());
        }
      }
    }
    return ret;
  }

 private:
  int    randomInt(int modulus) { return dis_(gen_) % modulus; }
  char   generateSign(float positive_ratio) { return possible(positive_ratio) ? '+' : '-'; }
  char   generateDigit() { return randomInt(10) + '0'; }
  bool   currentShouldGenerateCornerCase(float corner_case_ratio) { return possible(corner_case_ratio); }
  string generateCornerCase(const DecimalStringRandomGeneratorConfig& config) {
    string res{};
    if (possible(0.8)) {
      res = cornerCases[randomInt(cornerCases.size())];
    } else {
      res = std::string(config.prec - config.scale, generateDigit());
      if (possible(0.8)) {
        res.push_back('.');
        if (possible(0.8)) {
          res += std::string(config.scale, generateDigit());
        }
      }
    }
    return res;
  }

  bool possible(float ratio) { return randomInt(ratio_base) <= ratio * ratio_base; }
};

const std::array<string, 5> DecimalStringRandomGenerator::cornerCases = {"0", "NULL", "0.", ".0", "00000.000000"};

TEST(decimal, randomGenerator) {
  GTEST_SKIP();
  DecimalStringRandomGeneratorConfig config;
  DecimalStringRandomGenerator       generator;
  for (int i = 0; i < 1000; ++i) {
    auto str = generator.generate(config);
    cout << str << endl;
  }
}

#define ASSERT_OVERFLOW(op)             \
  do {                                  \
    try {                               \
      auto res = op;                    \
    } catch (std::overflow_error & e) { \
      cout << " overflow" << endl;      \
      break;                            \
    } catch (std::exception & e) {      \
      FAIL();                           \
    }                                   \
    FAIL();                             \
  } while (0)

#define ASSERT_RUNTIME_ERROR(op)        \
  do {                                  \
    try {                               \
      auto res = op;                    \
    } catch (std::overflow_error & e) { \
      FAIL();                           \
    } catch (std::runtime_error & e) {  \
      cout << " runtime error" << endl; \
      break;                            \
    } catch (std::exception & e) {      \
      FAIL();                           \
    }                                   \
    FAIL();                             \
  } while (0)

template <int32_t BitNum>
struct DecimalFromStrTestUnit {
  uint8_t     precision;
  uint8_t     scale;
  std::string input;
  std::string expect;
  bool        overflow;
};

template <int32_t BitNum>
void testDecimalFromStr(std::vector<DecimalFromStrTestUnit<BitNum>>& units) {
  for (auto& unit : units) {
    if (unit.overflow) {
      auto ff = [&]() {
        Numeric<BitNum> dec = {unit.precision, unit.scale, unit.input};
        return dec;
      };
      ASSERT_OVERFLOW(ff());
      continue;
    }
    cout << unit.input << " convert to decimal: (" << (int32_t)unit.precision << "," << (int32_t)unit.scale
         << "): " << unit.expect << endl;
    Numeric<BitNum> dec = {unit.precision, unit.scale, unit.input};
    ASSERT_EQ(dec.toString(), unit.expect);
  }
}

TEST(decimal, decimalFromStr_all) {
  std::vector<DecimalFromStrTestUnit<64>> units = {
      {10, 5, "1e+2.1", "100.00000", false},
      {10, 5, "0e+1000", "0", false},
      {10, 5, "0e-1000", "0", false},
      {10, 5, "0e-100", "0", false},
      {10, 5, "0e100", "0", false},
      {10, 5, "0e10", "0", false},
      {10, 5, "1.634e-5", "0.00002", false},
      {18, 16, "-0.0009900000000000000009e5", "-99.0000000000000001", false},
      {18, 10, "1.23456e7", "12345600.0000000000", false},
      {10, 5, "0e-10", "0", false},
      {10, 8, "1.000000000000000009e2", "", true},
      {10, 8, "1.2345e2", "", true},
      {18, 18, "-0.0000000000000100000010000000900000009e10", "-0.000100000010000001", false},
      {18, 18, "-0.1999999999999999999e-1", "-0.020000000000000000", false},
      {18, 18, "-0.9999999999999999999e-1", "-0.100000000000000000", false},
      {18, 18, "-9.999999999999999999e-1", "", true},
      {10, 10, "-9.9999999e-2", "-0.0999999990", false},
      {10, 6, "9.99999e4", "", true},
      {18, 4, "9.999999e1", "100.0000", false},
      {18, 18, "0.0000000000000000000000000010000000000000000199999e26", "0.100000000000000002", false},
      {18, 18, "0.000000000000000000000000001000000000000000009e26", "0.100000000000000001", false},
      {10, 10, "0.000000000010000000009e10", "0.1000000001", false},
      {10, 10, "0.000000000000000000009e10", "0.0000000001", false},
      {10, 10, "0.00000000001e10", "0.1000000000", false},
      {10, 7, "-1234567890e-8", "-12.3456789", false},
      {10, 4, "1e5", "100000.0000", false},
      {10, 3, "123.000E4", "1230000.000", false},
      {10, 3, "123.456E2", "12345.600", false},
      {18, 2, "1.2345e8", "123450000.00", false},
      {10, 2, "1.2345e8", "123450000.00", true},
      {18, 4, "9.99999", "10.0000", false},
      {10, 2, "123.45", "123.45", false},
      {10, 2, "123.456", "123.46", false},
      {10, 2, "123.454", "123.45"},
      {18, 2, "1234567890123456.456", "1234567890123456.46", false},
      {18, 2, "9999999999999999.995", "", true},
      {18, 2, "9999999999999999.994", "9999999999999999.99", false},
      {18, 2, "-9999999999999999.995", "", true},
      {18, 2, "-9999999999999999.994", "-9999999999999999.99", false},
      {18, 2, "-9999999999999999.9999999", "", true},
      {10, 2, "12345678.456", "12345678.46", false},
      {10, 2, "12345678.454", "12345678.45", false},
      {10, 2, "99999999.999", "", true},
      {10, 2, "-99999999.992", "-99999999.99", false},
      {10, 2, "-99999999.999", "", true},
      {10, 2, "-99999989.998", "-99999990.00", false},
      {10, 2, "-99999998.997", "-99999999.00", false},
      {10, 2, "-99999999.009", "-99999999.01", false},
      {18, 17, "-9.99999999999999999999", "", true},
      {18, 16, "-99.999999999999999899999", "-99.9999999999999999", false},
      {18, 16, "-99.999999999999990099999", "-99.9999999999999901", false},
      {18, 18, "0.0000000000000000099", "0.000000000000000010", false},
      {18, 18, "0.0000000000000000001", "0", false},
      {18, 18, "0.0000000000000000005", "0.000000000000000001", false},
      {18, 18, "-0.0000000000000000001", "0", false},
      {18, 18, "-0.00000000000000000019999", "0", false},
      {18, 18, "-0.0000000000000000005", "-0.000000000000000001", false},
      {18, 18, "-0.00000000000000000000000000123123123", "0", false},
      {18, 18, "0.10000000000000000000000000123123123", "0.100000000000000000", false},
      {18, 18, "0.000000000000000000000000000000000000006", "0", false},
      {18, 17, "1.00000000000000000999", "1.00000000000000001", false},
      {18, 17, "1.00000000000000000199", "1.00000000000000000", false},
      {15, 1, "-00000.", "0", false},
      {14, 12, "-.000", "0", false},
      {14, 12, "-.000000000000", "0", false},
      {14, 12, "-.", "0", false},
      {14, 10, "12345.12345", "", true},
      {14, 0, "123456789012345.123123", "", true},
      {18, 0, "1234567890123456789.123123", "", true},
      {18, 0, "1.23456e18", "", true},
      {18, 18, "1.23456e0", "", true},
      {18, 18, "1.23456e-1", "0.123456000000000000", false},
  };
  testDecimalFromStr(units);

  std::vector<DecimalFromStrTestUnit<128>> dec128Units = {
      {38, 38, "1.23456789121312312312312312355e-10", "0.00000000012345678912131231231231231236", false},
      {38, 10, "610854818164978322886511028.733672028246706062283745797933332437780013",
       "610854818164978322886511028.7336720282", false},
      {38, 10, "0e-10", "0", false},
      {38, 10, "0e10", "0", false},
      {38, 10, "e-100000", "0", false},
      {38, 10, "e-10", "0", false},
      {38, 10, "e10", "0", false},
      {38, 10, "-1.23456789012300000000000000000099000009e20", "-123456789012300000000.0000000001", false},
      {38, 10, "-1.234567890123e20", "-123456789012300000000.0000000000", false},
      {20, 15, "1234567890.9999999999e-6", "1234.567891000000000", false},
      {20, 15, "1234567890.9999999999999999999e-5", "12345.678910000000000", false},
      {20, 15, "1234567890.99999999999e-5", "12345.678910000000000", false},
      {20, 10, "12345667788.12312e-10", "1.2345667788", false},
      {20, 20, "1.234567123123123e-20", "0.00000000000000000001", false},
      {38, 38, "1.23456e-10", "0.00000000012345600000000000000000000000", false},
      {38, 10, "123456789012345678901234567.89012345679", "123456789012345678901234567.8901234568", false},
      {38, 10, "123456789012345678901234567.89012345670", "123456789012345678901234567.8901234567", false},
      {38, 10, "-123456789012345678901234567.89012345671", "-123456789012345678901234567.8901234567", false},
      {38, 10, "-123456789012345678901234567.89012345679", "-123456789012345678901234567.8901234568", false},
      {38, 10, "-9999999999999999999999999999.99999999995", "", true},
      {38, 10, "-9999999999999999999999999999.99999999994", "-9999999999999999999999999999.9999999999", false},
      {38, 10, "9999999999999999999999999999.99999999996", "", true},
      {38, 10, "9999999999999999999999999999.99999999994", "9999999999999999999999999999.9999999999", false},
      {36, 35, "9.99999999999999999999999999999999999", "9.99999999999999999999999999999999999", false},
      {36, 35, "9.999999999999999999999999999999999999111231231", "", true},
      {38, 38, "0.000000000000000000000000000000000000001", "0", false},
      {38, 38, "0.000000000000000000000000000000000000006", "0.00000000000000000000000000000000000001", false},
      {38, 35, "123.000000000000000000000000000000001", "123.00000000000000000000000000000000100", false},
      {38, 5, "123.", "123.00000", false},
      {20, 4, "-.12345", "-0.1235", false},
      {20, 4, "-.", "0", false},
      {30, 10, "1.2345e+20", "", true},
      {38, 38, "1.23456e0", "", true},
      {38, 38, "1.23456e-1", "0.12345600000000000000000000000000000000", false},
  };
  testDecimalFromStr(dec128Units);
}

TEST(decimal, op_overflow) {
  // divide 0 error
  Numeric<128> dec{38, 2, string(36, '9') + ".99"};
  ASSERT_RUNTIME_ERROR(dec / 0);

  // test decimal128Max
  Numeric<128> max{38, 10, "0"};
  max = decimal128Max;
  ASSERT_EQ(max.toString(), "9999999999999999999999999999.9999999999");

  {
    // multiply no overflow, trim scale
    auto res = max * 10;  // scale will be trimed to 6, and round up
    ASSERT_EQ(res.scale(), 6);
    ASSERT_EQ(res.toString(), "100000000000000000000000000000.000000");

    // multiply not overflow, no trim scale
    Numeric<64>  dec64{18, 10, "99999999.9999999999"};
    Numeric<128> dec128{19, 10, "999999999.9999999999"};

    auto rett = Numeric<64>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128.type());
    ASSERT_EQ(rett.precision, 38);
    ASSERT_EQ(rett.type, TSDB_DATA_TYPE_DECIMAL);
    ASSERT_EQ(rett.scale, dec64.scale() + dec128.scale());

    res = dec64 * dec128;
    ASSERT_EQ(res.toString(), "99999999999999999.89000000000000000001");

    // multiply not overflow, trim scale from 20 - 19
    Numeric<128> dec128_2{20, 10, "9999999999.9999999999"};
    rett = Numeric<128>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128_2.type());
    ASSERT_EQ(rett.scale, 19);
    res = dec64 * dec128_2;
    ASSERT_EQ(res.toString(), "999999999999999998.9900000000000000000");

    // trim scale from 20 - 18
    dec128_2 = {21, 10, "99999999999.9999999999"};
    rett = Numeric<128>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128_2.type());
    ASSERT_EQ(rett.scale, 18);
    res = dec64 * dec128_2;
    ASSERT_EQ(res.toString(), "9999999999999999989.990000000000000000");

    // trim scale from 20 -> 17
    dec128_2 = {22, 10, "999999999999.9999999999"};
    rett = Numeric<128>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128_2.type());
    ASSERT_EQ(rett.scale, 17);
    res = dec64 * dec128_2;
    ASSERT_EQ(res.toString(), "99999999999999999899.99000000000000000");

    // trim scale from 20 -> 6
    dec128_2 = {33, 10, "99999999999999999999999.9999999999"};
    rett = Numeric<128>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128_2.type());
    ASSERT_EQ(rett.scale, 6);
    res = dec64 * dec128_2;
    ASSERT_EQ(res.toString(), "9999999999999999989999999999999.990000");

    dec128_2 = {34, 10, "999999999999999999999999.9999999999"};
    rett = Numeric<128>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128_2.type());
    ASSERT_EQ(rett.scale, 6);
    res = dec64 * dec128_2;
    ASSERT_EQ(res.toString(), "99999999999999999899999999999999.990000");

    dec128_2 = {35, 10, "9999999999999999999999999.9999999999"};
    rett = Numeric<128>::getRetType(OP_TYPE_MULTI, dec64.type(), dec128_2.type());
    ASSERT_EQ(rett.scale, 6);
    ASSERT_OVERFLOW(dec64 * dec128_2);
  }
  {
    // divide not overflow but trim scale
    Numeric<128> dec128{19, 10, "999999999.9999999999"};
    Numeric<64>  dec64{10, 10, "0.10000000"};
    auto         res = dec128 / dec64;
    ASSERT_EQ(res.scale(), 19);
    ASSERT_EQ(res.toString(), "9999999999.9999999990000000000");

    dec64 = {10, 10, "0.1111111111"};
    res = dec128 / dec64;
    ASSERT_EQ(res.scale(), 19);
    ASSERT_EQ(res.toString(), "9000000000.8999999991899999999");

    dec64 = {10, 2, "0.01"};
    res = dec128 / dec64;
    ASSERT_EQ(res.scale(), 21);
    ASSERT_EQ(res.prec(), 32);
    ASSERT_EQ(res.toString(), "99999999999.999999990000000000000");

    dec64 = {10, 2, "7.77"};
    int32_t a = 2;
    res = dec64 % a;
    ASSERT_EQ(res.toString(), "1.77");

    dec128 = {38, 10, "999999999999999999999999999.9999999999"};
    res = dec128 % dec64;
    ASSERT_EQ(res.toString(), "5.4399999999");

    dec64 = {18, 10, "99999999.9999999999"};
    res = dec128 % dec64;
    ASSERT_EQ(res.toString(), "0.0000000009");

    Numeric<128> dec128_2 = {38, 10, "9988888888888888888888888.1111111111"};
    res = dec128 % dec128_2;
    ASSERT_EQ(res.toString(), "1111111111111111111111188.8888888899");

    dec128 = {38, 10, "9999999999999999999999999999.9999999999"};
    dec128_2 = {38, 2, "999999999999999999999999999988123123.88"};
    res = dec128 % dec128_2;
    ASSERT_EQ(res.toString(), "9999999999999999999999999999.9999999999");
  }
}

EOperatorType get_op_type(char op) {
  switch (op) {
    case '+':
      return OP_TYPE_ADD;
    case '-':
      return OP_TYPE_SUB;
    case '*':
      return OP_TYPE_MULTI;
    case '/':
      return OP_TYPE_DIV;
    case '%':
      return OP_TYPE_REM;
    default:
      return OP_TYPE_IS_UNKNOWN;
  }
}

std::string get_op_str(EOperatorType op) {
  switch (op) {
    case OP_TYPE_ADD:
      return "+";
    case OP_TYPE_SUB:
      return "-";
    case OP_TYPE_MULTI:
      return "*";
    case OP_TYPE_DIV:
      return "/";
    case OP_TYPE_REM:
      return "%";
    default:
      return "unknown";
  }
}

struct DecimalRetTypeCheckConfig {
  bool check_res_type = true;
  bool check_bytes = true;
  bool log = true;
};
struct DecimalRetTypeCheckContent {
  DecimalRetTypeCheckContent(const SDataType& a, const SDataType& b, const SDataType& out, EOperatorType op)
      : type_a(a), type_b(b), type_out(out), op_type(op) {}
  SDataType     type_a;
  SDataType     type_b;
  SDataType     type_out;
  EOperatorType op_type;
  // (1, 0) / (1, 1) = (8, 6)
  // (1, 0) / (1, 1) = (8, 6)
  DecimalRetTypeCheckContent(const std::string& s) {
    char op = '\0';
    sscanf(s.c_str(), "(%hhu, %hhu) %c (%hhu, %hhu) = (%hhu, %hhu)", &type_a.precision, &type_a.scale, &op,
           &type_b.precision, &type_b.scale, &type_out.precision, &type_out.scale);
    type_a = getDecimalType(type_a.precision, type_a.scale);
    type_b = getDecimalType(type_b.precision, type_b.scale);
    type_out = getDecimalType(type_out.precision, type_out.scale);
    op_type = get_op_type(op);
  }

  void check(const DecimalRetTypeCheckConfig& config = DecimalRetTypeCheckConfig()) {
    SDataType ret = {0};
    try {
      if (config.log)
        cout << "check ret type for type: (" << (int)type_a.type << " " << (int)type_a.precision << " "
             << (int)type_a.scale << ") " << get_op_str(op_type) << " (" << (int)type_b.type << " "
             << (int)type_b.precision << " " << (int)type_b.scale << ") = \n";
      ret = Numeric<64>::getRetType(op_type, type_a, type_b);
    } catch (std::runtime_error& e) {
      ASSERT_EQ(type_out.type, TSDB_DATA_TYPE_MAX);
      if (config.log) cout << "not support!" << endl;
      return;
    }
    if (config.log)
      cout << "(" << (int)ret.type << " " << (int)ret.precision << " " << (int)ret.scale << ") expect:" << endl
           << "(" << (int)type_out.type << " " << (int)type_out.precision << " " << (int)type_out.scale << ")" << endl;
    if (config.check_res_type) ASSERT_EQ(ret.type, type_out.type);
    ASSERT_EQ(ret.precision, type_out.precision);
    ASSERT_EQ(ret.scale, type_out.scale);
    if (config.check_bytes) ASSERT_EQ(ret.bytes, type_out.bytes);
  }
};

TEST(decimal_all, ret_type_load_from_file) {
  GTEST_SKIP();
  std::string   fname = "/tmp/ret_type.txt";
  std::ifstream ifs(fname, std::ios_base::in);
  if (!ifs.is_open()) {
    std::cerr << "open file " << fname << " failed" << std::endl;
    FAIL();
  }
  char    buf[64];
  int32_t total_lines = 0;
  while (ifs.getline(buf, 64, '\n')) {
    DecimalRetTypeCheckContent dcc(buf);
    DecimalRetTypeCheckConfig  config;
    config.check_res_type = false;
    config.check_bytes = false;
    config.log = false;
    dcc.check(config);
    ++total_lines;
  }
  ASSERT_EQ(total_lines, 3034205);
}

TEST(decimal_all, test_decimal_compare) {
  Numeric<64> dec64 = {10, 2, "123.23"};
  Numeric<64> dec64_2 = {11, 10, "1.23"};
  ASSERT_FALSE(dec64_2 > dec64);
  dec64 = "10123456.23";
  ASSERT_FALSE(dec64_2 > dec64);
  ASSERT_TRUE(dec64 > dec64_2);
  ASSERT_TRUE(dec64_2 < 100);
  Numeric<128> dec128 = {38, 10, "1.23"};
  ASSERT_TRUE(dec128 == dec64_2);
}

TEST(decimal_all, ret_type_for_non_decimal_types) {
  std::vector<DecimalRetTypeCheckContent> non_decimal_types;
  SDataType                               decimal_type = {TSDB_DATA_TYPE_DECIMAL64, 10, 2, 8};
  EOperatorType                           op = OP_TYPE_DIV;
  std::vector<SDataType>                  out_types;
  auto                                    count_digits = [](uint64_t v) { return std::floor(std::log10(v) + 1); };
  std::vector<SDataType>                  equivalent_decimal_types;
  // #define TSDB_DATA_TYPE_NULL       0   // 1 bytes
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_NULL, 0, 0, tDataTypes[TSDB_DATA_TYPE_NULL].bytes});
  // #define TSDB_DATA_TYPE_BOOL       1   // 1 bytes
  equivalent_decimal_types.push_back(getDecimalType(1, 0));
  // #define TSDB_DATA_TYPE_TINYINT    2   // 1 byte
  equivalent_decimal_types.push_back(getDecimalType(count_digits(INT8_MAX), 0));
  // #define TSDB_DATA_TYPE_SMALLINT   3   // 2 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(INT16_MAX), 0));
  // #define TSDB_DATA_TYPE_INT        4   // 4 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(INT32_MAX), 0));
  // #define TSDB_DATA_TYPE_BIGINT     5   // 8 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(INT64_MAX), 0));
  // #define TSDB_DATA_TYPE_FLOAT      6   // 4 bytes
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_DOUBLE, 0, 0, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes});
  // #define TSDB_DATA_TYPE_DOUBLE     7   // 8 bytes
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_DOUBLE, 0, 0, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes});
  // #define TSDB_DATA_TYPE_VARCHAR    8   // string, alias for varchar
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_DOUBLE, 0, 0, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes});
  // #define TSDB_DATA_TYPE_TIMESTAMP  9   // 8 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(INT64_MAX), 0));
  // #define TSDB_DATA_TYPE_NCHAR      10  // unicode string
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_DOUBLE, 0, 0, tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes});
  // #define TSDB_DATA_TYPE_UTINYINT   11  // 1 byte
  equivalent_decimal_types.push_back(getDecimalType(count_digits(UINT8_MAX), 0));
  // #define TSDB_DATA_TYPE_USMALLINT  12  // 2 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(UINT16_MAX), 0));
  // #define TSDB_DATA_TYPE_UINT       13  // 4 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(UINT32_MAX), 0));
  // #define TSDB_DATA_TYPE_UBIGINT    14  // 8 bytes
  equivalent_decimal_types.push_back(getDecimalType(count_digits(UINT64_MAX), 0));
  // #define TSDB_DATA_TYPE_JSON       15  // json string
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});
  // #define TSDB_DATA_TYPE_VARBINARY  16  // binary
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});
  // #define TSDB_DATA_TYPE_DECIMAL    17  // decimal
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});
  // #define TSDB_DATA_TYPE_BLOB       18  // binary
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});
  // #define TSDB_DATA_TYPE_MEDIUMBLOB 19
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});
  // #define TSDB_DATA_TYPE_GEOMETRY   20                      // geometry
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});
  // #define TSDB_DATA_TYPE_DECIMAL64  21                      // decimal64
  equivalent_decimal_types.push_back({TSDB_DATA_TYPE_MAX, 0, 0, 0});

  for (uint8_t i = 0; i < TSDB_DATA_TYPE_MAX; ++i) {
    if (IS_DECIMAL_TYPE(i)) continue;
    SDataType equivalent_out_type = equivalent_decimal_types[i];
    if (equivalent_out_type.type != TSDB_DATA_TYPE_MAX)
      equivalent_out_type = Numeric<128>::getRetType(op, decimal_type, equivalent_decimal_types[i]);
    DecimalRetTypeCheckContent dcc{decimal_type, {i, 0, 0, tDataTypes[i].bytes}, equivalent_out_type, op};
    dcc.check();

    if (equivalent_out_type.type != TSDB_DATA_TYPE_MAX) {
      equivalent_out_type = Numeric<128>::getRetType(op, equivalent_decimal_types[i], decimal_type);
    }
    DecimalRetTypeCheckContent dcc2{{i, 0, 0, tDataTypes[i].bytes}, decimal_type, equivalent_out_type, op};
    dcc2.check();
  }
}

class DecimalTest : public ::testing::Test {
  TAOS* get_connection() {
    auto conn = taos_connect(host, user, passwd, db, 0);
    if (!conn) {
      cout << "taos connect failed: " << host << " " << taos_errstr(NULL);
    }
    return conn;
  }
  TAOS*                              default_conn_ = NULL;
  static constexpr const char*       host = "127.0.0.1";
  static constexpr const char*       user = "root";
  static constexpr const char*       passwd = "taosdata";
  static constexpr const char*       db = "test";
  DecimalStringRandomGenerator       generator_;
  DecimalStringRandomGeneratorConfig generator_config_;

 public:
  void SetUp() override {
    default_conn_ = get_connection();
    if (!default_conn_) {
      FAIL();
    }
  }
  void TearDown() override {
    if (default_conn_) {
      taos_close(default_conn_);
    }
  }

  std::string generate_decimal_str() { return generator_.generate(generator_config_); }
};

TEST(decimal, fillDecimalInfoInBytes) {
  auto    d = getDecimalType(10, 2);
  int32_t bytes = 0;
  fillBytesForDecimalType(&bytes, d.type, d.precision, d.scale);

  uint8_t prec = 0, scale = 0;
  extractDecimalTypeInfoFromBytes(&bytes, &prec, &scale);
  ASSERT_EQ(bytes, tDataTypes[d.type].bytes);
  ASSERT_EQ(prec, d.precision);
  ASSERT_EQ(scale, d.scale);
}

#if 0
TEST_F(DecimalTest, api_taos_fetch_rows) {
  GTEST_SKIP_("");
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  const char* db = "test_api";
  const char* create_tb = "create table if not exists test_api.nt(ts timestamp, c1 decimal(10, 2), c2 decimal(38, 10), c3 varchar(255))";
  const char* sql = "select c1, c2,c3 from test_api.nt";
  const char* sql_insert = "insert into test_api.nt values(now, 123456.123, 98472981092.1209111)";

  TAOS* pTaos = taos_connect(host, user, passwd, NULL, 0);
  if (!pTaos) {
    cout << "taos connect failed: " << host << " " << taos_errstr(NULL);
    FAIL();
  }

  auto* res = taos_query(pTaos, (std::string("create database if not exists ") + db).c_str());
  taos_free_result(res);
  res = taos_query(pTaos, create_tb);
  taos_free_result(res);
  res = taos_query(pTaos, sql_insert);
  taos_free_result(res);

  res = taos_query(pTaos, sql);
  int32_t code = taos_errno(res);
  if (code != 0) {
    cout << "taos_query with sql: " << sql << " failed: " << taos_errstr(res);
    FAIL();
  }

  char  buf[1024] = {0};
  auto* fields = taos_fetch_fields(res);
  auto  fieldNum = taos_field_count(res);
  while (auto row = taos_fetch_row(res)) {
    taos_print_row(buf, row, fields, fieldNum);
    cout << buf << endl;
  }
  auto* fields_e = taos_fetch_fields_e(res);
  ASSERT_EQ(fields_e[0].type, TSDB_DATA_TYPE_DECIMAL64);
  ASSERT_EQ(fields_e[1].type, TSDB_DATA_TYPE_DECIMAL);
  ASSERT_EQ(fields_e[0].precision, 10);
  ASSERT_EQ(fields_e[0].scale, 2);
  ASSERT_EQ(fields_e[1].precision, 38);
  ASSERT_EQ(fields_e[1].scale, 10);
  ASSERT_EQ(fields_e[2].type, TSDB_DATA_TYPE_VARCHAR);
  ASSERT_EQ(fields_e[2].bytes, 255);

  taos_free_result(res);

  res = taos_query(pTaos, sql);
  code = taos_errno(res);
  if (code != 0) {
    cout << "taos_query with sql: " << sql << " failed: " << taos_errstr(res);
    taos_free_result(res);
    FAIL();
  }

  void*   pData = NULL;
  int32_t numOfRows = 0;
  code = taos_fetch_raw_block(res, &numOfRows, &pData);
  if (code != 0) {
    cout << "taos_query with sql: " << sql << " failed: " << taos_errstr(res);
    FAIL();
  }
  if (numOfRows > 0) {
    int32_t version = *(int32_t*)pData;
    ASSERT_EQ(version, BLOCK_VERSION_1);
    int32_t rows = *(int32_t*)((char*)pData + 4 + 4);
    int32_t colNum = *(int32_t*)((char*)pData + 4 + 4 + 4);
    int32_t bytes_skip = 4 + 4 + 4 + 4 + 4 + 8;
    char*   p = (char*)pData + bytes_skip;
    // col1
    int8_t  t = *(int8_t*)p;
    int32_t type_mod = *(int32_t*)(p + 1);

    ASSERT_EQ(t, TSDB_DATA_TYPE_DECIMAL64);
    auto check_type_mod = [](char* pStart, uint8_t prec, uint8_t scale, int32_t bytes) {
      int32_t d = *(int32_t*)pStart;
      ASSERT_EQ(d >> 24, bytes);
      ASSERT_EQ((d & 0xFF00) >> 8, prec);
      ASSERT_EQ(d & 0xFF, scale);
    };
    check_type_mod(p + 1, 10, 2, 8);

    // col2
    p += 5;
    t = *(int8_t*)p;
    type_mod = *(int32_t*)(p + 1);
    check_type_mod(p + 1, 38, 10, 16);

    p = p + 5 + BitmapLen(numOfRows) + colNum * 4;
    int64_t row1Val = *(int64_t*)p;
    ASSERT_EQ(row1Val, 12345612);
  }
  taos_free_result(res);

  taos_close(pTaos);
}
#endif

#if 0
TEST_F(DecimalTest, decimalFromStr) {
  Numeric<64> numeric64 = {10, 2, "0"};

  numeric64 = {18, 0, "0"};

  numeric64 = {18, 18, "0"};

  numeric64 = {18, 2, "0"};
  Numeric<128> numeric128 = {38, 10, "0"};
}
#endif

TEST(decimal, test_add_check_overflow) {
  Numeric<128> dec128 = {38, 10, "9999999999999999999999999999.9999999999"};
  Numeric<64>  dec64 = {18, 2, "123.12"};
  bool         overflow = decimal128AddCheckOverflow((Decimal128*)&dec128.dec(), &dec64.dec(), DECIMAL_WORD_NUM(Decimal64));
  ASSERT_TRUE(overflow);
  auto ret = dec128 + dec64;
  dec128 = {38, 10, "-9999999999999999999999999999.9999999999"};
  ASSERT_FALSE(decimal128AddCheckOverflow((Decimal128*)&dec128.dec(), &dec64.dec(), DECIMAL_WORD_NUM(Decimal64)));
  dec64 = {18, 2, "-123.1"};
  ASSERT_TRUE(decimal128AddCheckOverflow((Decimal128*)&dec128.dec(), &dec64.dec(), DECIMAL_WORD_NUM(Decimal64)));

  dec128 = {38, 0, "99999999999999999999999999999999999999"};
  dec64= {18, 0, "123"};
  Numeric<128> dec128_2 = {38, 2, "999999999999999999999999999999999999.99"};
  ASSERT_OVERFLOW(dec128 + dec128_2);
  ASSERT_OVERFLOW(dec128 + dec64);
  ASSERT_RUNTIME_ERROR(dec128 / 0);

  dec64 = {10, 2, "99999999.99"};
  Decimal64 tmp = {0};
  ASSERT_EQ(TSDB_CODE_DECIMAL_OVERFLOW, TEST_decimal64FromDecimal64((Decimal64*)&dec64.dec(), 10, 2, &tmp, 9, 1));
  dec128 = {20, 12, "99999999.999999999999"};
  ASSERT_EQ(TSDB_CODE_DECIMAL_OVERFLOW, TEST_decimal64FromDecimal128((Decimal128*)&dec128.dec(), 20, 12, &tmp, 9, 1));

  dec64 = {18, 10, "99999999.9999999999"};
  Decimal128 tmp2 = {0};
  ASSERT_EQ(TSDB_CODE_DECIMAL_OVERFLOW, TEST_decimal128FromDecimal64((Decimal64*)&dec64.dec(), 18, 10, &tmp2, 20, 20));
  ASSERT_EQ(TSDB_CODE_DECIMAL_OVERFLOW, TEST_decimal128FromDecimal128((Decimal128*)&dec128.dec(), 20, 12, &tmp2, 19, 11));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
