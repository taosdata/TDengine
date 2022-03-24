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
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_sma", 9035); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}

  void* BuildCreateDbReq(const char* dbname, int32_t* pContLen);
  void* BuildDropDbReq(const char* dbname, int32_t* pContLen);
  void* BuildCreateStbReq(const char* stbname, int32_t* pContLen);
  void* BuildDropStbReq(const char* stbname, int32_t* pContLen);
  void* BuildCreateSmaReq(const char* smaname, const char* stbname, int8_t igExists, const char* expr,
                          const char* tagsFilter, const char* sql, const char* ast, int32_t* pContLen);
  void* BuildDropSmaReq(const char* smaname, int8_t igNotExists, int32_t* pContLen);
};

Testbase MndTestSma::test;

void* MndTestSma::BuildCreateDbReq(const char* dbname, int32_t* pContLen) {
  SCreateDbReq createReq = {0};
  strcpy(createReq.db, dbname);
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

void* MndTestSma::BuildCreateStbReq(const char* stbname, int32_t* pContLen) {
  SMCreateStbReq createReq = {0};
  createReq.numOfColumns = 3;
  createReq.numOfTags = 1;
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
    field.bytes = 2;
    field.type = TSDB_DATA_TYPE_TINYINT;
    strcpy(field.name, "col1");
    taosArrayPush(createReq.pColumns, &field);
  }

  {
    SField field = {0};
    field.bytes = 8;
    field.type = TSDB_DATA_TYPE_BIGINT;
    strcpy(field.name, "col2");
    taosArrayPush(createReq.pColumns, &field);
  }

  {
    SField field = {0};
    field.bytes = 2;
    field.type = TSDB_DATA_TYPE_TINYINT;
    strcpy(field.name, "tag1");
    taosArrayPush(createReq.pTags, &field);
  }

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

void* MndTestSma::BuildCreateSmaReq(const char* smaname, const char* stbname, int8_t igExists, const char* expr,
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
  createReq.ast = (char*)expr;
  createReq.astLen = strlen(createReq.ast) + 1;

  int32_t tlen = tSerializeSMCreateSmaReq(NULL, 0, &createReq);
  void*   pHead = rpcMallocCont(tlen);
  tSerializeSMCreateSmaReq(pHead, tlen, &createReq);
  *pContLen = tlen;
  return pHead;
}

void* MndTestSma::BuildDropSmaReq(const char* smaname, int8_t igNotExists, int32_t* pContLen) {
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
    test.SendShowMetaReq(TSDB_MGMT_TABLE_STB, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  {
    pReq = BuildCreateSmaReq(smaname, stbname, 0, "expr", "tagsFilter", "sql", "ast", &contLen);
    pRsp = test.SendReq(TDMT_MND_CREATE_SMA, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    test.SendShowMetaReq(TSDB_MGMT_TABLE_INDEX, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);
  }

  // restart
  test.Restart();

  {
    test.SendShowMetaReq(TSDB_MGMT_TABLE_INDEX, dbname);
    CHECK_META("show indexes", 3);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 1);

    CheckBinary("sma", TSDB_INDEX_NAME_LEN);
    CheckTimestamp();
    CheckBinary("stb", TSDB_TABLE_NAME_LEN);
  }

  {
     pReq = BuildDropSmaReq(smaname, 0, &contLen);
    pRsp = test.SendReq(TDMT_MND_DROP_SMA, pReq, contLen);
    ASSERT_EQ(pRsp->code, 0);
    test.SendShowMetaReq(TSDB_MGMT_TABLE_INDEX, dbname);
    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 0);
  }
}
