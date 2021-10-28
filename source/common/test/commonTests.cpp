#include <gtest/gtest.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tvariant.h"
#include "tdef.h"

namespace {
//
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, toInteger_test) {
  char*    s = "123";
  uint32_t type = 0;

  int64_t val = 0;
  bool sign = true;

  int32_t ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);
  ASSERT_EQ(sign, true);

  s = "9223372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807);
  ASSERT_EQ(sign, true);

  s = "9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9323372036854775807u);
  ASSERT_EQ(sign, false);

  s = "-9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, -1);

  s = "-1";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -1);
  ASSERT_EQ(sign, true);

  s = "-9223372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -9223372036854775807);
  ASSERT_EQ(sign, true);

  s = "1000u";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, -1);

  s = "0x10";
  ret = toInteger(s, strlen(s), 16, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 16);
  ASSERT_EQ(sign, true);

  s = "110";
  ret = toInteger(s, strlen(s), 2, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);
  ASSERT_EQ(sign, true);

  s = "110";
  ret = toInteger(s, strlen(s), 8, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 72);
  ASSERT_EQ(sign, true);

  //18446744073709551615  UINT64_MAX
  s = "18446744073709551615";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18446744073709551615u);
  ASSERT_EQ(sign, false);

  s = "18446744073709551616";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, -1);
}
