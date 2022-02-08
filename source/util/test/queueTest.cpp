/**
 * @file queue.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief UTIL module queue tests
 * @version 1.0
 * @date 2022-01-27
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>

#include "os.h"
#include "tqueue.h"

class UtilTestQueue : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}
};

TEST_F(UtilTestQueue, 01_ReadQitemFromQsetByThread) {
  EXPECT_EQ(0, 0);
}