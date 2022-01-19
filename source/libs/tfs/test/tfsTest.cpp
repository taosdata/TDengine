/**
 * @file tfsTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief TFS module tests
 * @version 1.0
 * @date 2022-01-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>
#include "os.h"

class TfsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TfsTest, 01_Open_Close) {
  
}
