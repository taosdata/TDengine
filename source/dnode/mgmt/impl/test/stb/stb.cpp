/**
 * @file stb.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module db-msg tests
 * @version 0.1
 * @date 2021-12-17
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "sut.h"

class DndTestStb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_stb", 9101); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestStb::test;

TEST_F(DndTestStb, 01_Create_Show_Meta_Drop_Restart_Stb) {
  {
    int32_t contLen = sizeof(SCreateDbMsg);

    SCreateDbMsg* pReq = (SCreateDbMsg*)rpcMallocCont(contLen);
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

  {
    int32_t cols = 2;
    int32_t tags = 3;
    int32_t contLen = (tags + cols) * sizeof(SSchema) + sizeof(SCreateStbMsg);

    SCreateStbMsg* pReq = (SCreateStbMsg*)rpcMallocCont(contLen);
    strcpy(pReq->name, "1.d1.stb");
    pReq->numOfTags = htonl(tags);
    pReq->numOfColumns = htonl(cols);

    {
      SSchema* pSchema = &pReq->pSchema[0];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_TIMESTAMP;
      strcpy(pSchema->name, "ts");
    }

    {
      SSchema* pSchema = &pReq->pSchema[1];
      pSchema->bytes = htonl(4);
      pSchema->type = TSDB_DATA_TYPE_INT;
      strcpy(pSchema->name, "col1");
    }

    {
      SSchema* pSchema = &pReq->pSchema[2];
      pSchema->bytes = htonl(2);
      pSchema->type = TSDB_DATA_TYPE_TINYINT;
      strcpy(pSchema->name, "tag1");
    }

    {
      SSchema* pSchema = &pReq->pSchema[3];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_BIGINT;
      strcpy(pSchema->name, "tag2");
    }

    {
      SSchema* pSchema = &pReq->pSchema[4];
      pSchema->bytes = htonl(16);
      pSchema->type = TSDB_DATA_TYPE_BINARY;
      strcpy(pSchema->name, "tag3");
    }

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, "1.d1");
  CHECK_META("show stables", 4);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_INT, 4, "columns");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_INT, 4, "tags");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
  CheckBinary("stb", TSDB_TABLE_NAME_LEN);
  CheckTimestamp();
  CheckInt32(2);
  CheckInt32(3);

  // ----- meta ------
  {
    int32_t contLen = sizeof(STableInfoMsg);

    STableInfoMsg* pReq = (STableInfoMsg*)rpcMallocCont(contLen);
    strcpy(pReq->tableFname, "1.d1.stb");

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_STB_META, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    STableMetaMsg* pRsp = (STableMetaMsg*)pMsg->pCont;
    pRsp->numOfTags = htonl(pRsp->numOfTags);
    pRsp->numOfColumns = htonl(pRsp->numOfColumns);
    pRsp->sversion = htonl(pRsp->sversion);
    pRsp->tversion = htonl(pRsp->tversion);
    pRsp->suid = htobe64(pRsp->suid);
    pRsp->tuid = htobe64(pRsp->tuid);
    pRsp->vgId = htobe64(pRsp->vgId);
    for (int32_t i = 0; i < pRsp->numOfTags + pRsp->numOfColumns; ++i) {
      SSchema* pSchema = &pRsp->pSchema[i];
      pSchema->colId = htonl(pSchema->colId);
      pSchema->bytes = htonl(pSchema->bytes);
    }

    EXPECT_STREQ(pRsp->tbFname, "1.d1.stb");
    EXPECT_STREQ(pRsp->stbFname, "");
    EXPECT_EQ(pRsp->numOfColumns, 2);
    EXPECT_EQ(pRsp->numOfTags, 3);
    EXPECT_EQ(pRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRsp->tableType, TSDB_SUPER_TABLE);
    EXPECT_EQ(pRsp->update, 0);
    EXPECT_EQ(pRsp->sversion, 1);
    EXPECT_EQ(pRsp->tversion, 0);
    EXPECT_GT(pRsp->suid, 0);
    EXPECT_EQ(pRsp->tuid, 0);
    EXPECT_EQ(pRsp->vgId, 0);

    {
      SSchema* pSchema = &pRsp->pSchema[0];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
      EXPECT_EQ(pSchema->colId, 1);
      EXPECT_EQ(pSchema->bytes, 8);
      EXPECT_STREQ(pSchema->name, "ts");
    }

    {
      SSchema* pSchema = &pRsp->pSchema[1];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
      EXPECT_EQ(pSchema->colId, 2);
      EXPECT_EQ(pSchema->bytes, 4);
      EXPECT_STREQ(pSchema->name, "col1");
    }

    {
      SSchema* pSchema = &pRsp->pSchema[2];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TINYINT);
      EXPECT_EQ(pSchema->colId, 3);
      EXPECT_EQ(pSchema->bytes, 2);
      EXPECT_STREQ(pSchema->name, "tag1");
    }

    {
      SSchema* pSchema = &pRsp->pSchema[3];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BIGINT);
      EXPECT_EQ(pSchema->colId, 4);
      EXPECT_EQ(pSchema->bytes, 8);
      EXPECT_STREQ(pSchema->name, "tag2");
    }

    {
      SSchema* pSchema = &pRsp->pSchema[4];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
      EXPECT_EQ(pSchema->colId, 5);
      EXPECT_EQ(pSchema->bytes, 16);
      EXPECT_STREQ(pSchema->name, "tag3");
    }
  }

  // restart
  test.Restart();

  test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, "1.d1");
  CHECK_META("show stables", 4);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckBinary("stb", TSDB_TABLE_NAME_LEN);
  CheckTimestamp();
  CheckInt32(2);
  CheckInt32(3);

  {
    int32_t contLen = sizeof(SDropStbMsg);

    SDropStbMsg* pReq = (SDropStbMsg*)rpcMallocCont(contLen);
    strcpy(pReq->name, "1.d1.stb");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, "1.d1");
  CHECK_META("show stables", 4);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}
