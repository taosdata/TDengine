//
// Created by mingming wanng on 2021/11/24.
//
#include <gtest/gtest.h>
#include <inttypes.h>

#include "tscUtil.h"

/* test set config function */
TEST(testCase, json_test1) {
  SStrToken t0 = {.z = "jtag->'location'"};
  getJsonKey(&t0);
  ASSERT_EQ(t0.n, 8);
  t0.z[8] = 0;
  ASSERT_STREQ(t0.z, "location");
}