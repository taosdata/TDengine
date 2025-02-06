#include <gtest/gtest.h>
#include <iostream>
#include <memory>

#include "decimal.h"
#include "libs/nodes/querynodes.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "wideInteger.h"
using namespace std;

template <int N>
void printArray(const std::array<uint64_t, N>& arr) {
  auto it = arr.rbegin();
  for (; it != arr.rend(); ++it) {
    cout << *it;
  }
  cout << endl;
}

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
  static constexpr uint8_t WORD_NUM = WORD_NUM(Decimal64);

 public:
  friend class Numeric128;
  Numeric64() { dec_ = {0}; }
  int32_t fromStr(const string& str, uint8_t prec, uint8_t scale) {
    return decimal64FromStr(str.c_str(), str.size(), prec, scale, &dec_);
  }
  Numeric64& operator+=(const Numeric64& r) {
    getOps()->add(&dec_, &r.dec_, WORD_NUM);
    return *this;
  }
  // Numeric64& operator+=(const Numeric128& r);

  bool       operator==(const Numeric64& r) const { return getOps()->eq(&dec_, &r.dec_, WORD_NUM); }
  Numeric64& operator=(const Numeric64& r);
  Numeric64& operator=(const Numeric128& r);

  static SDecimalOps* getOps() { return getDecimalOps(TSDB_DATA_TYPE_DECIMAL64); }
};

class Numeric128 {
  Decimal128               dec_;
  static constexpr uint8_t WORD_NUM = WORD_NUM(Decimal128);

 public:
  friend Numeric64;
  Numeric128() { dec_ = {0}; }
  Numeric128(const Numeric128& r) = default;
  int32_t fromStr(const string& str, uint8_t prec, uint8_t scale) {
    return decimal128FromStr(str.c_str(), str.size(), prec, scale, &dec_);
  }
  Numeric128& operator+=(const Numeric128& r) { return *this; }
  Numeric128& operator+=(const Numeric64& r) {
    getOps()->add(&dec_, &r.dec_, Numeric64::WORD_NUM);
    return *this;
  }

  static SDecimalOps* getOps() { return getDecimalOps(TSDB_DATA_TYPE_DECIMAL); }
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
};

template <>
struct NumericType<128> {
  using Type = Numeric128;
  static constexpr int8_t dataType = TSDB_DATA_TYPE_DECIMAL;
  static constexpr int8_t maxPrec = TSDB_DECIMAL_MAX_PRECISION;
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
      cout << "failed to init decimal from str: " << str << endl;
      throw std::string(tstrerror(code));
    }
  }
  Numeric(const Numeric& o) = default;
  ~Numeric() = default;
  Numeric& operator=(const Numeric& o) = default;

  SDataType getRetType(EOperatorType op, const SDataType& lt, const SDataType& rt) const {
    SDataType ot = {0};
    decimalGetRetType(&lt, &rt, op, &ot);
    return ot;
  }
  uint8_t     prec() const { return prec_; }
  uint8_t     scale() const { return scale_; }
  const Type& dec() const { return dec_; }

  template <int BitNum2>
  Numeric& binaryOp(const Numeric<BitNum2>& r, EOperatorType op) {
    auto out = binaryOp<BitNum2, BitNum>(r, op);
    return *this = out;
  }

  template <int BitNum2, int BitNumO>
  Numeric<BitNumO> binaryOp(const Numeric<BitNum2>& r, EOperatorType op) {
    SDataType lt{.type = NumericType<BitNum>::dataType, .precision = prec_, .scale = scale_, .bytes = BitNum};
    SDataType rt{.type = NumericType<BitNum2>::dataType, .precision = r.prec(), .scale = r.scale(), .bytes = BitNum2};
    SDataType ot = getRetType(op, lt, rt);
    Numeric<BitNumO> out{ot.precision, ot.scale, "0"};
    int32_t          code = decimalOp(op, &lt, &rt, &ot, &dec_, &r.dec(), &out);
    if (code != 0) throw std::overflow_error(tstrerror(code));
    return out;
  }

  template <int BitNumO, typename T>
  Numeric<BitNumO> binaryOp(const T& r, EOperatorType op) {
    using TypeInfo = TrivialTypeInfo<T>;
    SDataType        lt{.type = NumericType<BitNum>::dataType, .precision = prec_, .scale = scale_, .bytes = BitNum};
    SDataType        rt{.type = TypeInfo::dataType, .precision = 0, .scale = 0, .bytes = TypeInfo::bytes};
    SDataType        ot = getRetType(op, lt, rt);
    Numeric<BitNumO> out{ot.precision, ot.scale, "0"};
    int32_t          code = decimalOp(op, &lt, &rt, &ot, &dec_, &r, &out);
    if (code != 0) throw std::overflow_error(tstrerror(code));
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

#define DEFINE_TYPE_OP(op, op_type)                                               \
  template <typename T, int BitNumO = 128>                                        \
  Numeric<BitNumO> operator op(const T & r) {                                     \
    cout << *this << " " #op " " << r << "(" << typeid(T).name() << ")" << " = "; \
    auto res = binaryOp<BitNumO, T>(r, op_type);                                  \
    cout << res << endl;                                                          \
    return res;                                                                   \
  }
  DEFINE_TYPE_OP(+, OP_TYPE_ADD);
  DEFINE_TYPE_OP(-, OP_TYPE_SUB);
  DEFINE_TYPE_OP(*, OP_TYPE_MULTI);
  DEFINE_TYPE_OP(/, OP_TYPE_DIV);

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

  template <int BitNum2>
  bool operator==(const Numeric<BitNum2>& r) {
    return binaryOp(r, OP_TYPE_EQUAL);
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

  Numeric operator=(const char* str) {
    std::string s = str;
    int32_t code = 0;
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

#define DEFINE_OPERATOR_FROM_FOR_BITNUM(type, BitNum)                              \
  if (std::is_floating_point<type>::value) {                                       \
    code = TEST_decimal##BitNum##From_double((Decimal##BitNum*)&dec_, prec(), scale(), v);   \
  } else if (std::is_signed<type>::value) {                                        \
    code = TEST_decimal##BitNum##From_int64_t((Decimal##BitNum*)&dec_, prec(), scale(), v);  \
  } else if (std::is_unsigned<type>::value) {                                      \
    code = TEST_decimal##BitNum##From_uint64_t((Decimal##BitNum*)&dec_, prec(), scale(), v); \
  }

#define DEFINE_OPERATOR_EQ_T(type)                \
  Numeric operator=(type v) {                     \
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
  ASSERT_EQ(os.toStringTrimTailingZeros(), "15241481344302520.993184");
  ASSERT_EQ(os.toString(), "15241481344302520.993184");

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
  auto min = zero - max;
  ASSERT_EQ(min.toString(), "-99999999999999999999999999999999999999");

  dec = 123.456;
  ASSERT_EQ(dec.toString(), "123.4560");

  dec = 47563.36;
  dec128 = 0;
  o = dec128 + dec; // (37, 10) + (10, 4) = (38, 10)
  ASSERT_EQ(o.toString(), "47563.3600000000");
  dec = 3749.00;
  o = o + dec;// (38, 10) + (10, 4) = (38, 9)
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
  ASSERT_EQ(dec1.toString(), "99.9999");
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
  ASSERT_EQ(uintv, 0);
  ASSERT_EQ(doublev, -123.44);
  intv = dec1 = "-123.99";
  uintv = dec1;
  doublev = dec1;
  ASSERT_EQ(intv, -124);
  ASSERT_EQ(uintv, 0);
  ASSERT_EQ(doublev, -123.99);

  bool boolv = false;
  boolv = dec1;
  ASSERT_TRUE(boolv);
  boolv = dec1 = "0";
  ASSERT_FALSE(boolv);
}

// TEST where decimal column in (...)
// TEST same decimal type with different scale doing comparing or operations
// TEST case when select common type

TEST(decimal, a) {
  __int128 a = generate_big_int128(37);
  extractWideInteger<9>(a);
  ASSERT_TRUE(1);
}

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

TEST(decimal128, divide) {
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
}

TEST(decimal, api_taos_fetch_rows) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  const char* db = "test_api";
  const char* create_tb = "create table if not exists test_api.nt(ts timestamp, c1 decimal(10, 2), c2 decimal(38, 10))";
  const char* sql = "select c1, c2 from test_api.nt";
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
      ASSERT_EQ(*pStart, bytes);
      ASSERT_EQ(*(pStart + 2), prec);
      ASSERT_EQ(*(pStart + 3), scale);
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
  taos_cleanup();
}

TEST(decimal, conversion) {
  // convert uint8 to decimal
  char      buf[64] = {0};
  int8_t    i8 = 22;
  SDataType inputType = {.type = TSDB_DATA_TYPE_TINYINT, .bytes = 1};
  uint8_t   prec = 10, scale = 2;
  SDataType decType = {.type = TSDB_DATA_TYPE_DECIMAL64, .precision = prec, .scale = scale, .bytes = 8};
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
}

SDataType getDecimalType(uint8_t prec, uint8_t scale) {
  if (prec <= 18) {
    return {.type = TSDB_DATA_TYPE_DECIMAL64, .precision = prec, .scale = scale, .bytes = 8};
  } else if (prec <= 38) {
    return {.type = TSDB_DATA_TYPE_DECIMAL, .precision = prec, .scale = scale, .bytes = 16};
  }
  return {};
}

bool operator==(const SDataType& lt, const SDataType& rt) {
  return lt.type == rt.type && lt.precision == rt.precision && lt.scale == rt.scale && lt.bytes == rt.bytes;
}

TEST(decimal, decimalOpRetType) {
  EOperatorType op = OP_TYPE_ADD;
  auto          ta = getDecimalType(10, 2);
  auto          tb = getDecimalType(10, 2);
  SDataType     tc{}, tExpect = {.type = TSDB_DATA_TYPE_DECIMAL, .precision = 11, .scale = 2, .bytes = sizeof(Decimal)};
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

  SDataType tc{}, tExpect{.type = TSDB_DATA_TYPE_DECIMAL, .precision = 11, .scale = 2, .bytes = sizeof(Decimal)};
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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
