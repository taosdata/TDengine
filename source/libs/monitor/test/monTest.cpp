/**
 * @file monTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief monitor module tests
 * @version 1.0
 * @date 2022-03-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>
#include "os.h"

#include "monitor.h"

class MonitorTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { root = "/tmp/monTest"; }
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}

  static const char *root;
};

const char *MonitorTest::root;

TEST_F(MonitorTest, 01_Open_Close) {
  
}
