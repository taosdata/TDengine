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

#include <sys/shm.h>
#include <sys/wait.h>

class UtilTestQueue : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}
};
