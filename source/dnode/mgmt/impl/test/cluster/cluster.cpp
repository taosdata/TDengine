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

#include "sut.h"

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
  test.SendShowMetaMsg(TSDB_MGMT_TABLE_CLUSTER, "");
  CHECK_META( "show cluster", 3);
  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BIGINT, 8, "id");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");

  test.SendShowRetrieveMsg();
  EXPECT_EQ(test.GetShowRows(), 1);

  IgnoreInt64();
  IgnoreBinary(TSDB_CLUSTER_ID_LEN);
  CheckTimestamp();
}