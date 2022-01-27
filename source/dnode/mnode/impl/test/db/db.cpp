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
  CHECK_META("show databases", 18);
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
  CHECK_SCHEMA(17, TSDB_DATA_TYPE_TINYINT, 1, "update");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestDb, 02_Create_Alter_Drop_Db) {
  {
    int32_t contLen = sizeof(SCreateDbReq);

    SCreateDbReq* pReq = (SCreateDbReq*)rpcMallocCont(contLen);
    strcpy(pReq->db, "1.d1");
    pReq->numOfVgroups = htonl(2);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRows = htonl(100);
    pReq->maxRows = htonl(4096);
    pReq->commitTime = htonl(3600);
    pReq->fsyncPeriod = htonl(3000);
    pReq->walLevel = 1;
    pReq->precision = 0;
    pReq->compression = 2;
    pReq->replications = 1;
    pReq->quorum = 1;
    pReq->update = 0;
    pReq->cacheLastRow = 0;
    pReq->ignoreExist = 1;

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 18);

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
  CheckInt32(0);
  CheckInt32(0);
  CheckInt16(1);
  CheckInt16(1);
  CheckBinary("master", 9);
  CheckBinary("master", 9);

  {
    int32_t contLen = sizeof(SAlterDbReq);

    SAlterDbReq* pReq = (SAlterDbReq*)rpcMallocCont(contLen);
    strcpy(pReq->db, "1.d1");
    pReq->totalBlocks = htonl(12);
    pReq->daysToKeep0 = htonl(300);
    pReq->daysToKeep1 = htonl(400);
    pReq->daysToKeep2 = htonl(500);
    pReq->fsyncPeriod = htonl(4000);
    pReq->walLevel = 2;
    pReq->quorum = 2;
    pReq->cacheLastRow = 1;

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
  CHECK_META("show databases", 18);

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
    int32_t contLen = sizeof(SDropDbReq);

    SDropDbReq* pReq = (SDropDbReq*)rpcMallocCont(contLen);
    strcpy(pReq->db, "1.d1");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SDropDbRsp* pDrop = (SDropDbRsp*)pRsp->pCont;
    pDrop->uid = htobe64(pDrop->uid);
    EXPECT_STREQ(pDrop->db, "1.d1");
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 18);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestDb, 03_Create_Use_Restart_Use_Db) {
  {
    int32_t contLen = sizeof(SCreateDbReq);

    SCreateDbReq* pReq = (SCreateDbReq*)rpcMallocCont(contLen);
    strcpy(pReq->db, "1.d2");
    pReq->numOfVgroups = htonl(2);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRows = htonl(100);
    pReq->maxRows = htonl(4096);
    pReq->commitTime = htonl(3600);
    pReq->fsyncPeriod = htonl(3000);
    pReq->walLevel = 1;
    pReq->precision = 0;
    pReq->compression = 2;
    pReq->replications = 1;
    pReq->quorum = 1;
    pReq->update = 0;
    pReq->cacheLastRow = 0;
    pReq->ignoreExist = 1;

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_DB, "");
  CHECK_META("show databases", 18);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
  CheckBinary("d2", TSDB_DB_NAME_LEN - 1);

  uint64_t d2_uid = 0;

  {
    int32_t contLen = sizeof(SUseDbReq);

    SUseDbReq* pReq = (SUseDbReq*)rpcMallocCont(contLen);
    strcpy(pReq->db, "1.d2");
    pReq->vgVersion = htonl(-1);

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_USE_DB, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SUseDbRsp* pRsp = (SUseDbRsp*)pMsg->pCont;
    EXPECT_STREQ(pRsp->db, "1.d2");
    pRsp->uid = htobe64(pRsp->uid);
    d2_uid = pRsp->uid;
    pRsp->vgVersion = htonl(pRsp->vgVersion);
    pRsp->vgNum = htonl(pRsp->vgNum);
    pRsp->hashMethod = pRsp->hashMethod;
    EXPECT_EQ(pRsp->vgVersion, 1);
    EXPECT_EQ(pRsp->vgNum, 2);
    EXPECT_EQ(pRsp->hashMethod, 1);

    {
      SVgroupInfo* pInfo = &pRsp->vgroupInfo[0];
      pInfo->vgId = htonl(pInfo->vgId);
      pInfo->hashBegin = htonl(pInfo->hashBegin);
      pInfo->hashEnd = htonl(pInfo->hashEnd);
      EXPECT_GT(pInfo->vgId, 0);
      EXPECT_EQ(pInfo->hashBegin, 0);
      EXPECT_EQ(pInfo->hashEnd, UINT32_MAX / 2 - 1);
      EXPECT_EQ(pInfo->epset.inUse, 0);
      EXPECT_EQ(pInfo->epset.numOfEps, 1);
      SEp* pAddr = &pInfo->epset.eps[0];
      pAddr->port = htons(pAddr->port);
      EXPECT_EQ(pAddr->port, 9030);
      EXPECT_STREQ(pAddr->fqdn, "localhost");
    }

    {
      SVgroupInfo* pInfo = &pRsp->vgroupInfo[1];
      pInfo->vgId = htonl(pInfo->vgId);
      pInfo->hashBegin = htonl(pInfo->hashBegin);
      pInfo->hashEnd = htonl(pInfo->hashEnd);
      EXPECT_GT(pInfo->vgId, 0);
      EXPECT_EQ(pInfo->hashBegin, UINT32_MAX / 2);
      EXPECT_EQ(pInfo->hashEnd, UINT32_MAX);
      EXPECT_EQ(pInfo->epset.inUse, 0);
      EXPECT_EQ(pInfo->epset.numOfEps, 1);
      SEp* pAddr = &pInfo->epset.eps[0];
      pAddr->port = htons(pAddr->port);
      EXPECT_EQ(pAddr->port, 9030);
      EXPECT_STREQ(pAddr->fqdn, "localhost");
    }
  }

  {
    int32_t contLen = sizeof(SDropDbReq);

    SDropDbReq* pReq = (SDropDbReq*)rpcMallocCont(contLen);
    strcpy(pReq->db, "1.d2");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SDropDbRsp* pDrop = (SDropDbRsp*)pRsp->pCont;
    pDrop->uid = htobe64(pDrop->uid);
    EXPECT_STREQ(pDrop->db, "1.d2");
    EXPECT_EQ(pDrop->uid, d2_uid);
  }
}
