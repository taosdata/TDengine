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

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
