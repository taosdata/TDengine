/**
 * @file cluster.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module cluster-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "base.h"

class DndTestCluster : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_cluster", 9030); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestCluster::test;

TEST_F(DndTestCluster, 01_ShowCluster) {
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_CLUSTER);
  EXPECT_EQ(test.GetMetaNum(), 3);
  EXPECT_STREQ(test.GetMetaTbName(), "show cluster");

  EXPECT_EQ(test.GetMetaType(0), TSDB_DATA_TYPE_INT);
  EXPECT_EQ(test.GetMetaBytes(0), 4);
  EXPECT_STREQ(test.GetMetaName(0), "id");

  EXPECT_EQ(test.GetMetaType(0), TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(test.GetMetaBytes(0), TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(test.GetMetaName(0), "name");

  EXPECT_EQ(test.GetMetaType(0), TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_EQ(test.GetMetaBytes(0), 8);
  EXPECT_STREQ(test.GetMetaName(0), "create_time");

  test.SendShowRetrieveMsg();
  test.GetShowInt32();
  test.GetShowBinary(TSDB_CLUSTER_ID_LEN);
  EXPECT_GT(test.GetShowTimestamp(), 0);
}