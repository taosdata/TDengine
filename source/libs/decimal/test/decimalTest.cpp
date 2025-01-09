#include <gtest/gtest.h>
#include <iostream>

#include "decimal.h"
#include "libs/nodes/querynodes.h"
#include "tcommon.h"
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

void printDecimal128(const Decimal128* pDec, int8_t precision, int8_t scale) {
  char buf[64] = {0};
  decimalToStr(pDec->words, TSDB_DATA_TYPE_DECIMAL, precision, scale, buf, 64);
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
  printDecimal128(&d, precision1, scale1);
  __int128 a = 1;
  while (delta_scale-- > 0) a *= 10;
  Decimal128 multiplier = {0};
  makeDecimal128(&multiplier, a >> 64, a);
  ops->multiply(d.words, multiplier.words, 2);
  cout << " / ";
  printDecimal128(&d2, precision2, scale2);
  cout << " = ";
  ops->divide(d.words, d2.words, 2, remainder.words);
  printDecimal128(&d, out_precision, out_scale);
}

TEST(decimal, cpi_taos_fetch_rows) {
  GTEST_SKIP();
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  const char* db = "test";
  const char* sql = "select c1, c2 from nt";

  TAOS* pTaos = taos_connect(host, user, passwd, db, 0);
  if (!pTaos) {
    cout << "taos connect failed: " << host << " " << taos_errstr(NULL);
    FAIL();
  }

  auto*   res = taos_query(pTaos, sql);
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
    FAIL();
  }

  void*   pData = NULL;
  int32_t numOfRows = 0;
  code = taos_fetch_raw_block(res, &numOfRows, &pData);
  if (code != 0) {
    cout << "taos_query with sql: " << sql << " failed: " << taos_errstr(res);
    FAIL();
  }

  SSDataBlock* pBlock;
  taos_free_result(res);

  taos_close(pTaos);
  taos_cleanup();
}

void printDecimal(const char* pDecStr, uint8_t type, uint8_t prec, uint8_t scale) {
  ASSERT_TRUE(type == prec > 18 ? TSDB_DATA_TYPE_DECIMAL : TSDB_DATA_TYPE_DECIMAL64);
  ASSERT_TRUE(scale <= prec);

  cout << "decimal" << (prec > 18 ? 128 : 64) << " " << (int32_t)prec << ":" << (int32_t)scale << " -> " << pDecStr << endl;
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
  code = decimalToStr(&dec64, TSDB_DATA_TYPE_DECIMAL64, prec, scale, buf, 64);
  ASSERT_EQ(code, 0);
  cout << "convert uint8: " << (int32_t)i8 << " to decimal64 " << (uint32_t)prec << ":" << (uint32_t)scale << "-> "
       << buf << endl;
  ASSERT_STREQ(buf, "22.00");

  Decimal128 dec128 = {0};
  decType.type = TSDB_DATA_TYPE_DECIMAL;
  decType.precision = 38;
  decType.scale = 10;
  decType.bytes = 16;
  code = convertToDecimal(&i8, &inputType, &dec128, &decType);
  ASSERT_TRUE(code == 0);
  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, decType.precision, decType.scale, buf, 64);
  ASSERT_EQ(code, 0);
  cout << "convert uint8: " << (int32_t)i8 << " to ";
  printDecimal(buf, TSDB_DATA_TYPE_DECIMAL, decType.precision, decType.scale);
  const char* expect = "22.0000000000";
  ASSERT_STREQ(buf, expect);

  char inputBuf[64] = "123.000000000000000000000000000000001";
  code = decimal128FromStr(inputBuf, strlen(inputBuf), 38, 35, &dec128);
  ASSERT_EQ(code, 0);
  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 35, buf, 64);
  ASSERT_EQ(code, 0);
  printDecimal(buf, TSDB_DATA_TYPE_DECIMAL, 38, 35);
  expect = "123.00000000000000000000000000000000100";
  ASSERT_STREQ(expect, buf);

  inputType.type = TSDB_DATA_TYPE_DECIMAL64;
  inputType.precision = prec;
  inputType.scale = scale;
  code = convertToDecimal(&dec64, &inputType, &dec128, &decType);
  ASSERT_EQ(code, 0);
  decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf, 64);
  expect = "22.0000000000";
  ASSERT_STREQ(expect, buf);
  printDecimal(buf, TSDB_DATA_TYPE_DECIMAL, 38, 10);
}

static constexpr uint64_t k1E16 = 10000000000000000LL;

TEST(decimal, decimalFromStr) {
  char inputBuf[64] = "123.000000000000000000000000000000001";
  Decimal128 dec128 = {0};
  int32_t code = decimal128FromStr(inputBuf, strlen(inputBuf), 38, 35, &dec128);
  ASSERT_EQ(code, 0);
  __int128 res = decimal128ToInt128(&dec128);
  __int128 resExpect = 123;
  resExpect *= k1E16;
  resExpect *= k1E16;
  resExpect *= 10;
  resExpect += 1;
  resExpect *= 100;
  ASSERT_EQ(res, resExpect);
}

TEST(decimal, toStr) {
  Decimal64 dec = {0};
  char buf[64] = {0};
  int32_t code = decimalToStr(&dec, TSDB_DATA_TYPE_DECIMAL64, 10, 2, buf, 64);
  ASSERT_EQ(code, 0);
  ASSERT_STREQ(buf, "0");

  Decimal128 dec128 = {0};
  code = decimalToStr(&dec128, TSDB_DATA_TYPE_DECIMAL, 38, 10, buf, 64);
  ASSERT_EQ(code, 0);
  ASSERT_STREQ(buf, "0");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
