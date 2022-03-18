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
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_db", 9030); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestDb::test;

TEST_F(MndTestDb, 01_ShowDb) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 17);
  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN - 1 + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_SMALLINT, 2, "vgroups");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_INT, 4, "ntables");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_SMALLINT, 2, "replica");
  CHECK_SCHEMA(5, TSDB_DATA_TYPE_SMALLINT, 2, "quorum");
  CHECK_SCHEMA(6, TSDB_DATA_TYPE_SMALLINT, 2, "days");
  CHECK_SCHEMA(7, TSDB_DATA_TYPE_BINARY, 24 + VARSTR_HEADER_SIZE, "keep0,keep1,keep2");
  CHECK_SCHEMA(8, TSDB_DATA_TYPE_INT, 4, "cache");
  CHECK_SCHEMA(9, TSDB_DATA_TYPE_INT, 4, "blocks");
  CHECK_SCHEMA(10, TSDB_DATA_TYPE_INT, 4, "minrows");
  CHECK_SCHEMA(11, TSDB_DATA_TYPE_INT, 4, "maxrows");
  CHECK_SCHEMA(12, TSDB_DATA_TYPE_TINYINT, 1, "wallevel");
  CHECK_SCHEMA(13, TSDB_DATA_TYPE_INT, 4, "fsync");
  CHECK_SCHEMA(14, TSDB_DATA_TYPE_TINYINT, 1, "comp");
  CHECK_SCHEMA(15, TSDB_DATA_TYPE_TINYINT, 1, "cachelast");
  CHECK_SCHEMA(16, TSDB_DATA_TYPE_BINARY, 3 + VARSTR_HEADER_SIZE, "precision");
//  CHECK_SCHEMA(17, TSDB_DATA_TYPE_TINYINT, 1, "update");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestDb, 02_Create_Alter_Drop_Db) {
  {
    SCreateDbReq createReq = {0};
    strcpy(createReq.db, "1.d1");
    createReq.numOfVgroups = 2;
    createReq.cacheBlockSize = 16;
    createReq.totalBlocks = 10;
    createReq.daysPerFile = 10;
    createReq.daysToKeep0 = 3650;
    createReq.daysToKeep1 = 3650;
    createReq.daysToKeep2 = 3650;
    createReq.minRows = 100;
    createReq.maxRows = 4096;
    createReq.commitTime = 3600;
    createReq.fsyncPeriod = 3000;
    createReq.walLevel = 1;
    createReq.precision = 0;
    createReq.compression = 2;
    createReq.replications = 1;
    createReq.quorum = 1;
    createReq.update = 0;
    createReq.cacheLastRow = 0;
    createReq.ignoreExist = 1;

    int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDbReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 17);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
  CheckBinary("d1", TSDB_DB_NAME_LEN - 1);
  CheckTimestamp();
  CheckInt16(2);                      // vgroups
  CheckInt32(0);                      // ntables
  CheckInt16(1);                      // replica
  CheckInt16(1);                      // quorum
  CheckInt16(10);                     // days
  CheckBinary("3650,3650,3650", 24);  // days
  CheckInt32(16);                     // cache
  CheckInt32(10);                     // blocks
  CheckInt32(100);                    // minrows
  CheckInt32(4096);                   // maxrows
  CheckInt8(1);                       // wallevel
  CheckInt32(3000);                   // fsync
  CheckInt8(2);                       // comp
  CheckInt8(0);                       // cachelast
  CheckBinary("ms", 3);               // precision
  CheckInt8(0);                       // update

  test.SendShowMetaReq(TSDB_MGMT_TABLE_VGROUP, "1.d1");
  CHECK_META("show vgroups", 4);
  CHECK_SCHEMA(0, TSDB_DATA_TYPE_INT, 4, "vgId");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_INT, 4, "tables");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_SMALLINT, 2, "v1_dnode");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_BINARY, 9 + VARSTR_HEADER_SIZE, "v1_status");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);
  CheckInt32(2);
  CheckInt32(3);
  IgnoreInt32();
  IgnoreInt32();
  CheckInt16(1);
  CheckInt16(1);
  CheckBinary("master", 9);
  CheckBinary("master", 9);

  {
    SAlterDbReq alterdbReq = {0};
    strcpy(alterdbReq.db, "1.d1");
    alterdbReq.totalBlocks = 12;
    alterdbReq.daysToKeep0 = 300;
    alterdbReq.daysToKeep1 = 400;
    alterdbReq.daysToKeep2 = 500;
    alterdbReq.fsyncPeriod = 4000;
    alterdbReq.walLevel = 2;
    alterdbReq.quorum = 2;
    alterdbReq.cacheLastRow = 1;

    int32_t contLen = tSerializeSAlterDbReq(NULL, 0, &alterdbReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSAlterDbReq(pReq, contLen, &alterdbReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
  CheckBinary("d1", TSDB_DB_NAME_LEN - 1);
  CheckTimestamp();
  CheckInt16(2);                   // vgroups
  CheckInt32(0);                   // tables
  CheckInt16(1);                   // replica
  CheckInt16(2);                   // quorum
  CheckInt16(10);                  // days
  CheckBinary("300,400,500", 24);  // days
  CheckInt32(16);                  // cache
  CheckInt32(12);                  // blocks
  CheckInt32(100);                 // minrows
  CheckInt32(4096);                // maxrows
  CheckInt8(2);                    // wallevel
  CheckInt32(4000);                // fsync
  CheckInt8(2);                    // comp
  CheckInt8(1);                    // cachelast
  CheckBinary("ms", 3);            // precision
  CheckInt8(0);                    // update

  // restart
  test.Restart();

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 17);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckBinary("d1", TSDB_DB_NAME_LEN - 1);
  CheckTimestamp();
  CheckInt16(2);                   // vgroups
  CheckInt32(0);                   // tables
  CheckInt16(1);                   // replica
  CheckInt16(2);                   // quorum
  CheckInt16(10);                  // days
  CheckBinary("300,400,500", 24);  // days
  CheckInt32(16);                  // cache
  CheckInt32(12);                  // blocks
  CheckInt32(100);                 // minrows
  CheckInt32(4096);                // maxrows
  CheckInt8(2);                    // wallevel
  CheckInt32(4000);                // fsync
  CheckInt8(2);                    // comp
  CheckInt8(1);                    // cachelast
  CheckBinary("ms", 3);            // precision
  CheckInt8(0);                    // update

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

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 17);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestDb, 03_Create_Use_Restart_Use_Db) {
  {
    SCreateDbReq createReq = {0};
    strcpy(createReq.db, "1.d2");
    createReq.numOfVgroups = 2;
    createReq.cacheBlockSize = 16;
    createReq.totalBlocks = 10;
    createReq.daysPerFile = 10;
    createReq.daysToKeep0 = 3650;
    createReq.daysToKeep1 = 3650;
    createReq.daysToKeep2 = 3650;
    createReq.minRows = 100;
    createReq.maxRows = 4096;
    createReq.commitTime = 3600;
    createReq.fsyncPeriod = 3000;
    createReq.walLevel = 1;
    createReq.precision = 0;
    createReq.compression = 2;
    createReq.replications = 1;
    createReq.quorum = 1;
    createReq.update = 0;
    createReq.cacheLastRow = 0;
    createReq.ignoreExist = 1;

    int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateDbReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 17);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
  CheckBinary("d2", TSDB_DB_NAME_LEN - 1);

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
