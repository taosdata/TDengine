#include <gtest/gtest.h>
#include <iostream>
#include "decimal.h"
#include "wideInteger.h"
using namespace std;

template <int N>
void printArray(const std::array<uint64_t, N> &arr) {
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
  __int128 i = generate_big_int128(37);
  int64_t hi = i >> 64;
  uint64_t lo = i;
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
  __int128 i = generate_big_int128(15);
  int64_t hi = i >> 64;
  uint64_t lo = i;
  Decimal128 d;
  makeDecimal128(&d, hi, lo);

  Decimal128 d2 = {0};
  makeDecimal128(&d2, 0, 12345678);

  auto ops = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  Decimal128 remainder = {0};
  int8_t precision1 = 38, scale1 = 5, precision2 = 10, scale2 = 2;
  int8_t out_scale = 25;
  int8_t out_precision = std::min(precision1 - scale1 + scale2 + out_scale, 38);
  int8_t delta_scale = out_scale + scale2 - scale1;
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

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
