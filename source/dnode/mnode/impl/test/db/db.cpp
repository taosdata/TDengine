/**
 * @file db.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module db tests
 * @version 1.0
 * @date 2022-01-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestDb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_db", 9030); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestDb::test;

TEST_F(MndTestDb, 01_ShowDb) {
  test.SendShowReq(TSDB_MGMT_TABLE_DB, "ins_databases", "");
  EXPECT_EQ(test.GetShowRows(), 2);
}

#if 0
TEST_F(MndTestDb, 02_Create_Alter_Drop_Db) {
  {
    SCreateDbReq createReq = {0};
    strcpy(createReq.db, "1.d1");
    createReq.numOfVgroups = 2;
    createReq.buffer = -1;
    createReq.pageSize = -1;
    createReq.pages = -1;
    createReq.daysPerFile = 1000;
    createReq.daysToKeep0 = 3650;
    createReq.daysToKeep1 = 3650;
    createReq.daysToKeep2 = 3650;
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
    createReq.numOfStables = 0;
    createReq.numOfRetensions = 0;

    int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDbReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_DB, "ins_databases", "");
  EXPECT_EQ(test.GetShowRows(), 3);

  test.SendShowReq(TSDB_MGMT_TABLE_VGROUP, "ins_vgroups", "1.d1");
  EXPECT_EQ(test.GetShowRows(), 2);

  {
    SAlterDbReq alterdbReq = {0};
    strcpy(alterdbReq.db, "1.d1");

    alterdbReq.buffer = 12;
    alterdbReq.pageSize = -1;
    alterdbReq.pages = -1;
    alterdbReq.daysPerFile = -1;
    alterdbReq.daysToKeep0 = -1;
    alterdbReq.daysToKeep1 = -1;
    alterdbReq.daysToKeep2 = -1;
    alterdbReq.walFsyncPeriod = 4000;
    alterdbReq.walLevel = 2;
    alterdbReq.strict = 1;
    alterdbReq.cacheLast = 1;
    alterdbReq.replications = 1;

    int32_t contLen = tSerializeSAlterDbReq(NULL, 0, &alterdbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterDbReq(pReq, contLen, &alterdbReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_OPS_NOT_SUPPORT);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_DB, "ins_databases", "");
  EXPECT_EQ(test.GetShowRows(), 3);

  // restart
  test.Restart();

  test.SendShowReq(TSDB_MGMT_TABLE_DB, "ins_databases", "");
  EXPECT_EQ(test.GetShowRows(), 3);

  {
    SDropDbReq dropdbReq = {0};
    strcpy(dropdbReq.db, "1.d1");

    int32_t contLen = tSerializeSDropDbReq(NULL, 0, &dropdbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropDbReq(pReq, contLen, &dropdbReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SDropDbRsp dropdbRsp = {0};
    tDeserializeSDropDbRsp(pRsp->pCont, pRsp->contLen, &dropdbRsp);
    EXPECT_STREQ(dropdbRsp.db, "1.d1");
  }

  test.SendShowReq(TSDB_MGMT_TABLE_DB, "ins_databases", "");
  EXPECT_EQ(test.GetShowRows(), 2);
}
#endif

TEST_F(MndTestDb, 03_Create_Use_Restart_Use_Db) {
  {
    SCreateDbReq createReq = {0};
    strcpy(createReq.db, "1.d2");
    createReq.numOfVgroups = 2;
    createReq.buffer = -1;
    createReq.pageSize = -1;
    createReq.pages = -1;
    createReq.daysPerFile = 1000;
    createReq.daysToKeep0 = 3650;
    createReq.daysToKeep1 = 3650;
    createReq.daysToKeep2 = 3650;
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
    createReq.numOfStables = 0;
    createReq.numOfRetensions = 0;

    int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDbReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_DB, "ins_databases", "");
  EXPECT_EQ(test.GetShowRows(), 3);

  uint64_t d2_uid = 0;

  {
    SUseDbReq usedbReq = {0};
    strcpy(usedbReq.db, "1.d2");
    usedbReq.vgVersion = -1;

    int32_t contLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSUseDbReq(pReq, contLen, &usedbReq);

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_USE_DB, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SUseDbRsp usedbRsp = {0};
    tDeserializeSUseDbRsp(pMsg->pCont, pMsg->contLen, &usedbRsp);
    EXPECT_STREQ(usedbRsp.db, "1.d2");
    EXPECT_EQ(usedbRsp.vgVersion, 1);
    EXPECT_EQ(usedbRsp.vgNum, 2);
    EXPECT_EQ(usedbRsp.hashMethod, 1);
    d2_uid = usedbRsp.uid;

    {
      SVgroupInfo* pInfo = (SVgroupInfo*)taosArrayGet(usedbRsp.pVgroupInfos, 0);
      pInfo->vgId = pInfo->vgId;
      pInfo->hashBegin = pInfo->hashBegin;
      pInfo->hashEnd = pInfo->hashEnd;
      EXPECT_GT(pInfo->vgId, 0);
      EXPECT_EQ(pInfo->hashBegin, 0);
      EXPECT_EQ(pInfo->hashEnd, UINT32_MAX / 2 - 1);
      EXPECT_EQ(pInfo->epSet.inUse, 0);
      EXPECT_EQ(pInfo->epSet.numOfEps, 1);
      SEp* pAddr = &pInfo->epSet.eps[0];
      EXPECT_EQ(pAddr->port, 9030);
      EXPECT_STREQ(pAddr->fqdn, "localhost");
    }

    {
      SVgroupInfo* pInfo = (SVgroupInfo*)taosArrayGet(usedbRsp.pVgroupInfos, 1);
      pInfo->vgId = pInfo->vgId;
      pInfo->hashBegin = pInfo->hashBegin;
      pInfo->hashEnd = pInfo->hashEnd;
      EXPECT_GT(pInfo->vgId, 0);
      EXPECT_EQ(pInfo->hashBegin, UINT32_MAX / 2);
      EXPECT_EQ(pInfo->hashEnd, UINT32_MAX);
      EXPECT_EQ(pInfo->epSet.inUse, 0);
      EXPECT_EQ(pInfo->epSet.numOfEps, 1);
      SEp* pAddr = &pInfo->epSet.eps[0];
      EXPECT_EQ(pAddr->port, 9030);
      EXPECT_STREQ(pAddr->fqdn, "localhost");
    }

    tFreeSUsedbRsp(&usedbRsp);
  }

  {
    SDropDbReq dropdbReq = {0};
    strcpy(dropdbReq.db, "1.d2");

    int32_t contLen = tSerializeSDropDbReq(NULL, 0, &dropdbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropDbReq(pReq, contLen, &dropdbReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SDropDbRsp dropdbRsp = {0};
    tDeserializeSDropDbRsp(pRsp->pCont, pRsp->contLen, &dropdbRsp);
    EXPECT_STREQ(dropdbRsp.db, "1.d2");
    EXPECT_EQ(dropdbRsp.uid, d2_uid);
  }
}
