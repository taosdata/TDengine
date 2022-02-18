/**
 * @file cfgTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief config module tests
 * @version 1.0
 * @date 2022-02-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>
#include "os.h"

#include "tfs.h"

class CfgTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}

  static const char *pConfig;
};

const char *TfsTest::pConfig;

TEST_F(CfgTest, 01_Taos_File) {}
