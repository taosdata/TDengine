/**
 * @file user.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module user tests
 * @version 1.0
 * @date 2022-01-04
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestUser : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_user", 9011); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestUser::test;

TEST_F(MndTestUser, 01_Show_User) {
  uInfo("show users");
  test.SendShowReq(TSDB_MGMT_TABLE_USER, "user_users", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}
