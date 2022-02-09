/**
 * @file stb.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module stb tests
 * @version 1.0
 * @date 2022-01-12
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestStb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_stb", 9034); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}

  SCreateDbReq*   BuildCreateDbReq(const char* dbname, int32_t* pContLen);
  SMCreateStbReq* BuildCreateStbReq(const char* stbname, int32_t* pContLen);
  SMAltertbReq*   BuildAlterStbAddTagReq(const char* stbname, const char* tagname, int32_t* pContLen);
  SMAltertbReq*   BuildAlterStbDropTagReq(const char* stbname, const char* tagname, int32_t* pContLen);
  SMAltertbReq*   BuildAlterStbUpdateTagNameReq(const char* stbname, const char* tagname, const char* newtagname,
                                                int32_t* pContLen);
  SMAltertbReq*   BuildAlterStbUpdateTagBytesReq(const char* stbname, const char* tagname, int32_t bytes,
                                                  int32_t* pContLen);
};

Testbase MndTestStb::test;

SCreateDbReq* MndTestStb::BuildCreateDbReq(const char* dbname, int32_t* pContLen) {
  int32_t contLen = sizeof(SCreateDbReq);

  SCreateDbReq* pReq = (SCreateDbReq*)rpcMallocCont(contLen);
  strcpy(pReq->db, dbname);
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

  *pContLen = contLen;
  return pReq;
}

SMCreateStbReq* MndTestStb::BuildCreateStbReq(const char* stbname, int32_t* pContLen) {
  int32_t cols = 2;
  int32_t tags = 3;
  int32_t contLen = (tags + cols) * sizeof(SSchema) + sizeof(SMCreateStbReq);

  SMCreateStbReq* pReq = (SMCreateStbReq*)rpcMallocCont(contLen);
  strcpy(pReq->name, stbname);
  pReq->numOfTags = htonl(tags);
  pReq->numOfColumns = htonl(cols);

  {
    SSchema* pSchema = &pReq->pSchemas[0];
    pSchema->bytes = htonl(8);
    pSchema->type = TSDB_DATA_TYPE_TIMESTAMP;
    strcpy(pSchema->name, "ts");
  }

  {
    SSchema* pSchema = &pReq->pSchemas[1];
    pSchema->bytes = htonl(4);
    pSchema->type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema->name, "col1");
  }

  {
    SSchema* pSchema = &pReq->pSchemas[2];
    pSchema->bytes = htonl(2);
    pSchema->type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema->name, "tag1");
  }

  {
    SSchema* pSchema = &pReq->pSchemas[3];
    pSchema->bytes = htonl(8);
    pSchema->type = TSDB_DATA_TYPE_BIGINT;
    strcpy(pSchema->name, "tag2");
  }

  {
    SSchema* pSchema = &pReq->pSchemas[4];
    pSchema->bytes = htonl(16);
    pSchema->type = TSDB_DATA_TYPE_BINARY;
    strcpy(pSchema->name, "tag3");
  }

  *pContLen = contLen;
  return pReq;
}

TEST_F(MndTestStb, 01_Create_Show_Meta_Drop_Restart_Stb) {
  const char* dbname = "1.d1";
  const char* stbname = "1.d1.stb";

  {
    int32_t       contLen = 0;
    SCreateDbReq* pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t         contLen = 0;
    SMCreateStbReq* pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg*        pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
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
  }

  // ----- meta ------
  {
    int32_t        contLen = sizeof(STableInfoReq);
    STableInfoReq* pReq = (STableInfoReq*)rpcMallocCont(contLen);
    strcpy(pReq->dbFName, dbname);
    strcpy(pReq->tbName, "stb");

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_STB_META, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    STableMetaRsp* pRsp = (STableMetaRsp*)pMsg->pCont;
    pRsp->numOfTags = htonl(pRsp->numOfTags);
    pRsp->numOfColumns = htonl(pRsp->numOfColumns);
    pRsp->sversion = htonl(pRsp->sversion);
    pRsp->tversion = htonl(pRsp->tversion);
    pRsp->suid = be64toh(pRsp->suid);
    pRsp->tuid = be64toh(pRsp->tuid);
    pRsp->vgId = be64toh(pRsp->vgId);
    for (int32_t i = 0; i < pRsp->numOfTags + pRsp->numOfColumns; ++i) {
      SSchema* pSchema = &pRsp->pSchema[i];
      pSchema->colId = htonl(pSchema->colId);
      pSchema->bytes = htonl(pSchema->bytes);
    }

    EXPECT_STREQ(pRsp->dbFName, dbname);
    EXPECT_STREQ(pRsp->tbName, "stb");
    EXPECT_STREQ(pRsp->stbName, "stb");
    EXPECT_EQ(pRsp->numOfColumns, 2);
    EXPECT_EQ(pRsp->numOfTags, 3);
    EXPECT_EQ(pRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRsp->tableType, TSDB_SUPER_TABLE);
    EXPECT_EQ(pRsp->update, 0);
    EXPECT_EQ(pRsp->sversion, 1);
    EXPECT_EQ(pRsp->tversion, 0);
    EXPECT_GT(pRsp->suid, 0);
    EXPECT_GT(pRsp->tuid, 0);
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

  {
    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
    CHECK_META("show stables", 4);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckInt32(2);
    CheckInt32(3);
  }

  {
    int32_t contLen = sizeof(SMDropStbReq);

    SMDropStbReq* pReq = (SMDropStbReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, stbname);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
  CHECK_META("show stables", 4);
  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

SMAltertbReq* MndTestStb::BuildAlterStbAddTagReq(const char* stbname, const char* tagname, int32_t* pContLen) {
  int32_t       contLen = sizeof(SMAltertbReq) + sizeof(SSchema);
  SMAltertbReq* pReq = (SMAltertbReq*)rpcMallocCont(contLen);
  strcpy(pReq->name, stbname);
  pReq->numOfSchemas = htonl(1);
  pReq->alterType = TSDB_ALTER_TABLE_ADD_TAG;

  SSchema* pSchema = &pReq->pSchemas[0];
  pSchema->bytes = htonl(12);
  pSchema->type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema->name, tagname);

  *pContLen = contLen;
  return pReq;
}

SMAltertbReq* MndTestStb::BuildAlterStbDropTagReq(const char* stbname, const char* tagname, int32_t* pContLen) {
  int32_t       contLen = sizeof(SMAltertbReq) + sizeof(SSchema);
  SMAltertbReq* pReq = (SMAltertbReq*)rpcMallocCont(contLen);
  strcpy(pReq->name, stbname);
  pReq->numOfSchemas = htonl(1);
  pReq->alterType = TSDB_ALTER_TABLE_DROP_TAG;

  SSchema* pSchema = &pReq->pSchemas[0];
  pSchema->bytes = htonl(12);
  pSchema->type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema->name, tagname);

  *pContLen = contLen;
  return pReq;
}

SMAltertbReq* MndTestStb::BuildAlterStbUpdateTagNameReq(const char* stbname, const char* tagname,
                                                        const char* newtagname, int32_t* pContLen) {
  int32_t       contLen = sizeof(SMAltertbReq) + 2 * sizeof(SSchema);
  SMAltertbReq* pReq = (SMAltertbReq*)rpcMallocCont(contLen);
  strcpy(pReq->name, stbname);
  pReq->numOfSchemas = htonl(1);
  pReq->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_NAME;

  SSchema* pSchema = &pReq->pSchemas[0];
  pSchema->bytes = htonl(12);
  pSchema->type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema->name, tagname);

  pSchema = &pReq->pSchemas[1];
  pSchema->bytes = htonl(12);
  pSchema->type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema->name, newtagname);

  *pContLen = contLen;
  return pReq;
}

SMAltertbReq* MndTestStb::BuildAlterStbUpdateTagBytesReq(const char* stbname, const char* tagname, int32_t bytes,
                                                          int32_t* pContLen) {
  int32_t       contLen = sizeof(SMAltertbReq) + sizeof(SSchema);
  SMAltertbReq* pReq = (SMAltertbReq*)rpcMallocCont(contLen);
  strcpy(pReq->name, stbname);
  pReq->numOfSchemas = htonl(1);
  pReq->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_BYTES;

  SSchema* pSchema = &pReq->pSchemas[0];
  pSchema->bytes = htonl(bytes);
  pSchema->type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema->name, tagname);

  *pContLen = contLen;
  return pReq;
}

TEST_F(MndTestStb, 02_Alter_Stb_AddTag) {
  const char* dbname = "1.d2";
  const char* stbname = "1.d2.stb";

  {
    int32_t       contLen = 0;
    SCreateDbReq* pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t         contLen = 0;
    SMCreateStbReq* pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg*        pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  int32_t contLen = 0;

  {
    SMAltertbReq* pReq = BuildAlterStbAddTagReq("1.d3.stb", "tag4", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_DB);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbAddTagReq("1.d2.stb3", "tag4", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_STB_NOT_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbAddTagReq(stbname, "tag3", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbAddTagReq(stbname, "col1", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_ALREADY_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbAddTagReq(stbname, "tag4", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckInt32(2);
    CheckInt32(4);
  }
}

TEST_F(MndTestStb, 03_Alter_Stb_DropTag) {
  const char* dbname = "1.d3";
  const char* stbname = "1.d3.stb";
  int32_t     contLen = 0;

  {
    SCreateDbReq* pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SMCreateStbReq* pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg*        pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbDropTagReq(stbname, "tag5", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbDropTagReq(stbname, "tag3", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckInt32(2);
    CheckInt32(2);
  }
}

TEST_F(MndTestStb, 04_Alter_Stb_AlterTagName) {
  const char* dbname = "1.d4";
  const char* stbname = "1.d4.stb";
  int32_t     contLen = 0;

  {
    SCreateDbReq* pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SMCreateStbReq* pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg*        pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag5", "tag6", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagNameReq(stbname, "col1", "tag6", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "col1", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_ALREADY_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "tag2", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
  }
  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "tag2", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "tag4", &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckInt32(2);
    CheckInt32(3);
  }
}


TEST_F(MndTestStb, 04_Alter_Stb_AlterTagBytes) {
  const char* dbname = "1.d5";
  const char* stbname = "1.d5.stb";
  int32_t     contLen = 0;

  {
    SCreateDbReq* pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SMCreateStbReq* pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg*        pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag5", 12, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag1", 13, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_STB_OPTION);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag3", 8, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_ROW_BYTES);
  }

  {
    SMAltertbReq* pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag3", 20, &contLen);
    SRpcMsg*      pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
 
    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
    CheckTimestamp();
    CheckInt32(2);
    CheckInt32(3);
  }
}