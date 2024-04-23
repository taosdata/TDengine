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
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_stb", 9034); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}

  void* BuildCreateDbReq(const char* dbname, int32_t* pContLen);
  void* BuildDropDbReq(const char* dbname, int32_t* pContLen);
  void* BuildCreateStbReq(const char* stbname, int32_t* pContLen);
  void* BuildAlterStbAddTagReq(const char* stbname, const char* tagname, int32_t* pContLen);
  void* BuildAlterStbDropTagReq(const char* stbname, const char* tagname, int32_t* pContLen);
  void* BuildAlterStbUpdateTagNameReq(const char* stbname, const char* tagname, const char* newtagname,
                                      int32_t* pContLen);
  void* BuildAlterStbUpdateTagBytesReq(const char* stbname, const char* tagname, int32_t bytes, int32_t* pContLen);
  void* BuildAlterStbAddColumnReq(const char* stbname, const char* colname, int32_t* pContLen);
  void* BuildAlterStbDropColumnReq(const char* stbname, const char* colname, int32_t* pContLen);
  void* BuildAlterStbUpdateColumnBytesReq(const char* stbname, const char* colname, int32_t bytes, int32_t* pContLen,
                                          int32_t verInBlock);
};

Testbase MndTestStb::test;

void* MndTestStb::BuildCreateDbReq(const char* dbname, int32_t* pContLen) {
  SCreateDbReq createReq = {0};
  strcpy(createReq.db, dbname);
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

  int32_t contLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSCreateDbReq(pReq, contLen, &createReq);

  *pContLen = contLen;
  return pReq;
}

void* MndTestStb::BuildDropDbReq(const char* dbname, int32_t* pContLen) {
  SDropDbReq dropdbReq = {0};
  strcpy(dropdbReq.db, dbname);

  int32_t contLen = tSerializeSDropDbReq(NULL, 0, &dropdbReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSDropDbReq(pReq, contLen, &dropdbReq);

  *pContLen = contLen;
  return pReq;
}

void* MndTestStb::BuildCreateStbReq(const char* stbname, int32_t* pContLen) {
  SMCreateStbReq createReq = {0};
  createReq.numOfColumns = 2;
  createReq.numOfTags = 3;
  createReq.igExists = 0;
  createReq.pColumns = taosArrayInit(createReq.numOfColumns, sizeof(SField));
  createReq.pTags = taosArrayInit(createReq.numOfTags, sizeof(SField));
  strcpy(createReq.name, stbname);

  {
    SField field = {0};
    field.bytes = 8;
    field.type = TSDB_DATA_TYPE_TIMESTAMP;
    strcpy(field.name, "ts");
    taosArrayPush(createReq.pColumns, &field);
  }

  {
    SField field = {0};
    field.bytes = 12;
    field.type = TSDB_DATA_TYPE_BINARY;
    strcpy(field.name, "col1");
    taosArrayPush(createReq.pColumns, &field);
  }

  {
    SField field = {0};
    field.bytes = 2;
    field.type = TSDB_DATA_TYPE_TINYINT;
    strcpy(field.name, "tag1");
    taosArrayPush(createReq.pTags, &field);
  }

  {
    SField field = {0};
    field.bytes = 8;
    field.type = TSDB_DATA_TYPE_BIGINT;
    strcpy(field.name, "tag2");
    taosArrayPush(createReq.pTags, &field);
  }

  {
    SField field = {0};
    field.bytes = 16;
    field.type = TSDB_DATA_TYPE_BINARY;
    strcpy(field.name, "tag3");
    taosArrayPush(createReq.pTags, &field);
  }

  int32_t tlen = tSerializeSMCreateStbReq(NULL, 0, &createReq);
  void*   pHead = rpcMallocCont(tlen);
  tSerializeSMCreateStbReq(pHead, tlen, &createReq);
  tFreeSMCreateStbReq(&createReq);
  *pContLen = tlen;
  return pHead;
}

void* MndTestStb::BuildAlterStbAddTagReq(const char* stbname, const char* tagname, int32_t* pContLen) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 1;
  req.pFields = taosArrayInit(1, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_ADD_TAG;

  SField field = {0};
  field.bytes = 12;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, tagname);
  taosArrayPush(req.pFields, &field);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

void* MndTestStb::BuildAlterStbDropTagReq(const char* stbname, const char* tagname, int32_t* pContLen) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 1;
  req.pFields = taosArrayInit(1, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_DROP_TAG;

  SField field = {0};
  field.bytes = 12;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, tagname);
  taosArrayPush(req.pFields, &field);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

void* MndTestStb::BuildAlterStbUpdateTagNameReq(const char* stbname, const char* tagname, const char* newtagname,
                                                int32_t* pContLen) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 2;
  req.pFields = taosArrayInit(2, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_UPDATE_TAG_NAME;

  SField field = {0};
  field.bytes = 12;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, tagname);
  taosArrayPush(req.pFields, &field);

  SField field2 = {0};
  field2.bytes = 12;
  field2.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field2.name, newtagname);
  taosArrayPush(req.pFields, &field2);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

void* MndTestStb::BuildAlterStbUpdateTagBytesReq(const char* stbname, const char* tagname, int32_t bytes,
                                                 int32_t* pContLen) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 1;
  req.pFields = taosArrayInit(1, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_UPDATE_TAG_BYTES;

  SField field = {0};
  field.bytes = bytes;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, tagname);
  taosArrayPush(req.pFields, &field);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

void* MndTestStb::BuildAlterStbAddColumnReq(const char* stbname, const char* colname, int32_t* pContLen) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 1;
  req.pFields = taosArrayInit(1, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_ADD_COLUMN;

  SField field = {0};
  field.bytes = 12;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, colname);
  taosArrayPush(req.pFields, &field);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

void* MndTestStb::BuildAlterStbDropColumnReq(const char* stbname, const char* colname, int32_t* pContLen) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 1;
  req.pFields = taosArrayInit(1, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_DROP_COLUMN;

  SField field = {0};
  field.bytes = 12;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, colname);
  taosArrayPush(req.pFields, &field);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

void* MndTestStb::BuildAlterStbUpdateColumnBytesReq(const char* stbname, const char* colname, int32_t bytes,
                                                    int32_t* pContLen, int32_t verInBlock) {
  SMAlterStbReq req = {0};
  strcpy(req.name, stbname);
  req.numOfFields = 1;
  req.pFields = taosArrayInit(1, sizeof(SField));
  req.alterType = TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES;

  SField field = {0};
  field.bytes = bytes;
  field.type = TSDB_DATA_TYPE_BINARY;
  strcpy(field.name, colname);
  taosArrayPush(req.pFields, &field);

  int32_t contLen = tSerializeSMAlterStbReq(NULL, 0, &req);
  void*   pHead = rpcMallocCont(contLen);
  tSerializeSMAlterStbReq(pHead, contLen, &req);

  *pContLen = contLen;
  taosArrayDestroy(req.pFields);
  return pHead;
}

TEST_F(MndTestStb, 01_Create_Show_Meta_Drop_Restart_Stb) {
  const char* dbname = "1.d1";
  const char* stbname = "1.d1.stb";

  {
    int32_t  contLen = 0;
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  // ----- meta ------
  {
    STableInfoReq infoReq = {0};
    strcpy(infoReq.dbFName, dbname);
    strcpy(infoReq.tbName, "stb");

    int32_t contLen = tSerializeSTableInfoReq(NULL, 0, &infoReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSTableInfoReq(pReq, contLen, &infoReq);

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_TABLE_META, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    STableMetaRsp metaRsp = {0};
    tDeserializeSTableMetaRsp(pMsg->pCont, pMsg->contLen, &metaRsp);
    rpcFreeCont(pMsg->pCont);

    EXPECT_STREQ(metaRsp.dbFName, dbname);
    EXPECT_STREQ(metaRsp.tbName, "stb");
    EXPECT_STREQ(metaRsp.stbName, "stb");
    EXPECT_EQ(metaRsp.numOfColumns, 2);
    EXPECT_EQ(metaRsp.numOfTags, 3);
    EXPECT_EQ(metaRsp.precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(metaRsp.tableType, TSDB_SUPER_TABLE);
    EXPECT_EQ(metaRsp.sversion, 1);
    EXPECT_EQ(metaRsp.tversion, 1);
    EXPECT_GT(metaRsp.suid, 0);
    EXPECT_GT(metaRsp.tuid, 0);
    EXPECT_EQ(metaRsp.vgId, 0);

    {
      SSchema* pSchema = &metaRsp.pSchemas[0];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
      EXPECT_EQ(pSchema->colId, 1);
      EXPECT_EQ(pSchema->bytes, 8);
      EXPECT_STREQ(pSchema->name, "ts");
    }

    {
      SSchema* pSchema = &metaRsp.pSchemas[1];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
      EXPECT_EQ(pSchema->colId, 2);
      EXPECT_EQ(pSchema->bytes, 12);
      EXPECT_STREQ(pSchema->name, "col1");
    }

    {
      SSchema* pSchema = &metaRsp.pSchemas[2];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TINYINT);
      EXPECT_EQ(pSchema->colId, 3);
      EXPECT_EQ(pSchema->bytes, 2);
      EXPECT_STREQ(pSchema->name, "tag1");
    }

    {
      SSchema* pSchema = &metaRsp.pSchemas[3];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BIGINT);
      EXPECT_EQ(pSchema->colId, 4);
      EXPECT_EQ(pSchema->bytes, 8);
      EXPECT_STREQ(pSchema->name, "tag2");
    }

    {
      SSchema* pSchema = &metaRsp.pSchemas[4];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
      EXPECT_EQ(pSchema->colId, 5);
      EXPECT_EQ(pSchema->bytes, 16);
      EXPECT_STREQ(pSchema->name, "tag3");
    }

    tFreeSTableMetaRsp(&metaRsp);
  }

  // restart
  test.Restart();

  {
    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    SMDropStbReq dropReq = {0};
    strcpy(dropReq.name, stbname);

    int32_t contLen = tSerializeSMDropStbReq(NULL, 0, &dropReq);
    void*   pHead = rpcMallocCont(contLen);
    tSerializeSMDropStbReq(pHead, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_STB, pHead, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 0);
  }

  {
    int32_t  contLen = 0;
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 02_Alter_Stb_AddTag) {
  const char* dbname = "1.d2";
  const char* stbname = "1.d2.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddTagReq("1.d3.stb", "tag4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DB_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddTagReq("1.d2.stb3", "tag4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_STB_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddTagReq(stbname, "tag3", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddTagReq(stbname, "col1", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddTagReq(stbname, "tag4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 03_Alter_Stb_DropTag) {
  const char* dbname = "1.d3";
  const char* stbname = "1.d3.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbDropTagReq(stbname, "tag5", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbDropTagReq(stbname, "tag3", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 04_Alter_Stb_AlterTagName) {
  const char* dbname = "1.d4";
  const char* stbname = "1.d4.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag5", "tag6", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagNameReq(stbname, "col1", "tag6", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "col1", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "tag2", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }
  {
    void*    pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "tag2", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagNameReq(stbname, "tag3", "tag4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 05_Alter_Stb_AlterTagBytes) {
  const char* dbname = "1.d5";
  const char* stbname = "1.d5.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag5", 12, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag1", 13, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_STB_OPTION);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag3", 8, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_ROW_BYTES);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateTagBytesReq(stbname, "tag3", 20, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 06_Alter_Stb_AddColumn) {
  const char* dbname = "1.d6";
  const char* stbname = "1.d6.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddColumnReq("1.d7.stb", "tag4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_DB_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddColumnReq("1.d6.stb3", "tag4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_STB_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddColumnReq(stbname, "tag3", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_TAG_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddColumnReq(stbname, "col1", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddColumnReq(stbname, "col2", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 07_Alter_Stb_DropColumn) {
  const char* dbname = "1.d7";
  const char* stbname = "1.d7.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbDropColumnReq(stbname, "col4", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbDropColumnReq(stbname, "col1", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_STB_ALTER_OPTION);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbDropColumnReq(stbname, "ts", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_STB_ALTER_OPTION);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbAddColumnReq(stbname, "col2", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbDropColumnReq(stbname, "col1", &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}

TEST_F(MndTestStb, 08_Alter_Stb_AlterTagBytes) {
  const char* dbname = "1.d8";
  const char* stbname = "1.d8.stb";
  int32_t     contLen = 0;

  {
    void*    pReq = BuildCreateDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildCreateStbReq(stbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateColumnBytesReq(stbname, "col5", 12, &contLen, 0);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateColumnBytesReq(stbname, "ts", 8, &contLen, 0);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_STB_OPTION);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateColumnBytesReq(stbname, "col1", 8, &contLen, 0);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_ROW_BYTES);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateColumnBytesReq(stbname, "col1", TSDB_MAX_BYTES_PER_ROW, &contLen, 0);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_ROW_BYTES);
    rpcFreeCont(pRsp->pCont);
  }

  {
    void*    pReq = BuildAlterStbUpdateColumnBytesReq(stbname, "col1", 20, &contLen, 0);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildAlterStbUpdateColumnBytesReq(stbname, "col_not_exist", 20, &contLen, 1);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_COLUMN_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);

    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    void*    pReq = BuildDropDbReq(dbname, &contLen);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_DB, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }
}
