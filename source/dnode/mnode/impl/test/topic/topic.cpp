/**
 * @file topic.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module topic tests
 * @version 1.0
 * @date 2022-02-16
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestTopic : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_topic", 9039); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}

  void* BuildCreateDbReq(const char* dbname, int32_t* pContLen);
  void* BuildCreateTopicReq(const char* topicName, const char* sql, int32_t* pContLen);
  void* BuildDropTopicReq(const char* topicName, int32_t* pContLen);
};

Testbase MndTestTopic::test;

void* MndTestTopic::BuildCreateDbReq(const char* dbname, int32_t* pContLen) {
  SCreateDbReq createReq = {0};
  strcpy(createReq.db, dbname);
  createReq.numOfVgroups = 2;
  createReq.buffer = -1;
  createReq.pageSize = -1;
  createReq.pages = -1;
  createReq.daysPerFile = 10 * 1440;
  createReq.daysToKeep0 = 3650 * 1440;
  createReq.daysToKeep1 = 3650 * 1440;
  createReq.daysToKeep2 = 3650 * 1440;
  createReq.minRows = 100;
  createReq.maxRows = 4096;
  createReq.walFsyncPeriod = 3000;
  createReq.walLevel = 1;
  createReq.precision = 0;
  createReq.compression = 2;
  createReq.replications = 1;
  createReq.strict = 1;
  createReq.cacheLast = 0;
  createReq.ignoreExist = 1;

  int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSCreateDbReq(pReq, contLen, &createReq);

  *pContLen = contLen;
  return pReq;
}

void* MndTestTopic::BuildCreateTopicReq(const char* topicName, const char* sql, int32_t* pContLen) {
  SCMCreateTopicReq createReq = {0};
  strcpy(createReq.name, topicName);
  createReq.igExists = 0;
  createReq.sql = (char*)sql;
  createReq.ast = NULL;

  int32_t contLen = tSerializeSCMCreateTopicReq(NULL, 0, &createReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSCMCreateTopicReq(pReq, contLen, &createReq);

  *pContLen = contLen;
  return pReq;
}

void* MndTestTopic::BuildDropTopicReq(const char* topicName, int32_t* pContLen) {
  SMDropTopicReq dropReq = {0};
  strcpy(dropReq.name, topicName);

  int32_t contLen = tSerializeSMDropTopicReq(NULL, 0, &dropReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSMDropTopicReq(pReq, contLen, &dropReq);

  *pContLen = contLen;
  return pReq;
}

TEST_F(MndTestTopic, 01_Create_Topic) {
  // TODO add valid ast for unit test
#if 0
  const char* dbname = "1.d1";
  const char* topicName = "1.d1.t1";

  {
    int32_t  contLen = 0;
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  { test.SendShowReq(TSDB_MGMT_TABLE_TOPICS, ""); }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildCreateTopicReq("t1", "sql", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_TOPIC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildCreateTopicReq(topicName, "sql", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_TOPIC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildCreateTopicReq(topicName, "sql", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_TOPIC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TOPIC_ALREADY_EXIST);
  }

  {
    test.SendShowReq(TSDB_MGMT_TABLE_TOPICS, dbname);
    CHECK_META("show topics", 3);

    CHECK_SCHEMA(0, TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE, "name");
    CHECK_SCHEMA(1, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
    CHECK_SCHEMA(2, TSDB_DATA_TYPE_BINARY, TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, "sql");

    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckBinary("t1", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckBinary("sql", TSDB_SHOW_SQL_LEN);

    // restart
    test.Restart();

    test.SendShowReq(TSDB_MGMT_TABLE_TOPICS, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckBinary("t1", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckBinary("sql", TSDB_SHOW_SQL_LEN);
  }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildDropTopicReq(topicName, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_TOPIC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildDropTopicReq(topicName, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_TOPIC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TOPIC_NOT_EXIST);

    test.SendShowReq(TSDB_MGMT_TABLE_TOPICS, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 0);
  }
#endif
}
