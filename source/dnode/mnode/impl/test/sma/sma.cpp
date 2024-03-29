/**
 * @file sma.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module sma tests
 * @version 1.0
 * @date 2022-03-23
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestSma : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_sma", 9035); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}

  void* BuildCreateDbReq(const char* dbname, int32_t* pContLen);
  void* BuildDropDbReq(const char* dbname, int32_t* pContLen);
  void* BuildCreateStbReq(const char* stbname, int32_t* pContLen);
  void* BuildDropStbReq(const char* stbname, int32_t* pContLen);
  void* BuildCreateBSmaStbReq(const char* stbname, int32_t* pContLen);
  void* BuildCreateTSmaReq(const char* smaname, const char* stbname, int8_t igExists, const char* expr,
                           const char* tagsFilter, const char* sql, const char* ast, int32_t* pContLen);
  void* BuildDropTSmaReq(const char* smaname, int8_t igNotExists, int32_t* pContLen);

  void PushField(SArray* pArray, int32_t bytes, int8_t type, const char* name);
};

Testbase MndTestSma::test;

void* MndTestSma::BuildCreateDbReq(const char* dbname, int32_t* pContLen) {
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

void* MndTestSma::BuildDropDbReq(const char* dbname, int32_t* pContLen) {
  SDropDbReq dropdbReq = {0};
  strcpy(dropdbReq.db, dbname);

  int32_t contLen = tSerializeSDropDbReq(NULL, 0, &dropdbReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSDropDbReq(pReq, contLen, &dropdbReq);

  *pContLen = contLen;
  return pReq;
}

void MndTestSma::PushField(SArray* pArray, int32_t bytes, int8_t type, const char* name) {
  SField field = {0};
  field.bytes = bytes;
  field.type = type;
  strcpy(field.name, name);
  taosArrayPush(pArray, &field);
}

void* MndTestSma::BuildCreateStbReq(const char* stbname, int32_t* pContLen) {
  SMCreateStbReq createReq = {0};
  createReq.numOfColumns = 3;
  createReq.numOfTags = 1;
  createReq.igExists = 0;
  createReq.pColumns = taosArrayInit(createReq.numOfColumns, sizeof(SField));
  createReq.pTags = taosArrayInit(createReq.numOfTags, sizeof(SField));
  strcpy(createReq.name, stbname);

  PushField(createReq.pColumns, 8, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  PushField(createReq.pColumns, 2, TSDB_DATA_TYPE_TINYINT, "col1");
  PushField(createReq.pColumns, 8, TSDB_DATA_TYPE_BIGINT, "col2");
  PushField(createReq.pTags, 2, TSDB_DATA_TYPE_TINYINT, "tag1");

  int32_t tlen = tSerializeSMCreateStbReq(NULL, 0, &createReq);
  void*   pHead = rpcMallocCont(tlen);
  tSerializeSMCreateStbReq(pHead, tlen, &createReq);
  tFreeSMCreateStbReq(&createReq);
  *pContLen = tlen;
  return pHead;
}

void* MndTestSma::BuildCreateBSmaStbReq(const char* stbname, int32_t* pContLen) {
  SMCreateStbReq createReq = {0};
  createReq.numOfColumns = 3;
  createReq.numOfTags = 1;
  createReq.igExists = 0;
  createReq.pColumns = taosArrayInit(createReq.numOfColumns, sizeof(SField));
  createReq.pTags = taosArrayInit(createReq.numOfTags, sizeof(SField));
  strcpy(createReq.name, stbname);

  PushField(createReq.pColumns, 8, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  PushField(createReq.pColumns, 2, TSDB_DATA_TYPE_TINYINT, "col1");
  PushField(createReq.pColumns, 8, TSDB_DATA_TYPE_BIGINT, "col2");
  PushField(createReq.pTags, 2, TSDB_DATA_TYPE_TINYINT, "tag1");

  int32_t tlen = tSerializeSMCreateStbReq(NULL, 0, &createReq);
  void*   pHead = rpcMallocCont(tlen);
  tSerializeSMCreateStbReq(pHead, tlen, &createReq);
  tFreeSMCreateStbReq(&createReq);
  *pContLen = tlen;
  return pHead;
}

void* MndTestSma::BuildDropStbReq(const char* stbname, int32_t* pContLen) {
  SMDropStbReq dropstbReq = {0};
  strcpy(dropstbReq.name, stbname);

  int32_t contLen = tSerializeSMDropStbReq(NULL, 0, &dropstbReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSMDropStbReq(pReq, contLen, &dropstbReq);

  *pContLen = contLen;
  return pReq;
}

void* MndTestSma::BuildCreateTSmaReq(const char* smaname, const char* stbname, int8_t igExists, const char* expr,
                                     const char* tagsFilter, const char* sql, const char* ast, int32_t* pContLen) {
  SMCreateSmaReq createReq = {0};
  strcpy(createReq.name, smaname);
  strcpy(createReq.stb, stbname);
  createReq.igExists = igExists;
  createReq.intervalUnit = 1;
  createReq.slidingUnit = 2;
  createReq.timezone = 3;
  createReq.dstVgId = 4;
  createReq.interval = 10;
  createReq.offset = 5;
  createReq.sliding = 6;
  createReq.expr = (char*)expr;
  createReq.exprLen = strlen(createReq.expr) + 1;
  createReq.tagsFilter = (char*)tagsFilter;
  createReq.tagsFilterLen = strlen(createReq.tagsFilter) + 1;
  createReq.sql = (char*)sql;
  createReq.sqlLen = strlen(createReq.sql) + 1;
  createReq.ast = (char*)ast;
  createReq.astLen = strlen(createReq.ast) + 1;

  int32_t tlen = tSerializeSMCreateSmaReq(NULL, 0, &createReq);
  void*   pHead = rpcMallocCont(tlen);
  tSerializeSMCreateSmaReq(pHead, tlen, &createReq);
  *pContLen = tlen;
  return pHead;
}

void* MndTestSma::BuildDropTSmaReq(const char* smaname, int8_t igNotExists, int32_t* pContLen) {
  SMDropSmaReq dropsmaReq = {0};
  dropsmaReq.igNotExists = igNotExists;
  strcpy(dropsmaReq.name, smaname);

  int32_t contLen = tSerializeSMDropSmaReq(NULL, 0, &dropsmaReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSMDropSmaReq(pReq, contLen, &dropsmaReq);

  *pContLen = contLen;
  return pReq;
}

TEST_F(MndTestSma, 01_Create_Show_Meta_Drop_Restart_Stb) {
#if 0
  const char* dbname = "1.d1";
  const char* stbname = "1.d1.stb";
  const char* smaname = "1.d1.sma";
  int32_t     contLen = 0;
  void*       pReq;
  SRpcMsg*    pRsp;

  {
    pReq = BuildCreateDbReq(dbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    pReq = BuildCreateStbReq(stbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    test.SendShowReq(TSDB_MGMT_TABLE_STB, "user_stables",dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    pReq = BuildCreateTSmaReq(smaname, stbname, 0, "expr", "tagsFilter", "sql", "ast", &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_SMA, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    test.SendShowReq(TSDB_MGMT_TABLE_INDEX, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  // restart
  test.Restart();

  {
    test.SendShowReq(TSDB_MGMT_TABLE_INDEX, dbname);
    CHECK_META("show indexes", 3);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckBinary("sma", TSDB_INDEX_NAME_LEN);
    CheckTimestamp();
    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
  }

  {
     pReq = BuildDropTSmaReq(smaname, 0, &contLen);
    pRsp = test.SendReq(TDMT_MND_DROP_SMA, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    test.SendShowReq(TSDB_MGMT_TABLE_INDEX, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 0);
  }
#endif
}

TEST_F(MndTestSma, 02_Create_Show_Meta_Drop_Restart_BSma) {
  const char* dbname = "1.d1";
  const char* stbname = "1.d1.bsmastb";
  int32_t     contLen = 0;
  void*       pReq;
  SRpcMsg*    pRsp;

  {
    pReq = BuildCreateDbReq(dbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_DB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
  }

  {
    pReq = BuildCreateBSmaStbReq(stbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  test.Restart();

  {
    pReq = BuildCreateBSmaStbReq(stbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_STB_ALREADY_EXIST);
    rpcFreeCont(pRsp->pCont);
  }

  {
    pReq = BuildDropStbReq(stbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_DROP_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    rpcFreeCont(pRsp->pCont);
    test.SendShowReq(TSDB_MGMT_TABLE_STB, "ins_stables", dbname);
    EXPECT_EQ(test.GetShowRows(), 0);
  }

  {
    pReq = BuildDropStbReq(stbname, &contLen);
    pRsp = test.SendReq(TDMT_MND_DROP_STB, pReq, contLen);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_STB_NOT_EXIST);
    rpcFreeCont(pRsp->pCont);
  }
}
