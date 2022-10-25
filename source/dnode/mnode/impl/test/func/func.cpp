/**
 * @file func.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module func tests
 * @version 1.0
 * @date 2022-01-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestFunc : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "mnode_test_func", 9038); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}

  void SetCode(SCreateFuncReq* pReq, const char* pCode, int32_t size);
  void SetComment(SCreateFuncReq* pReq, const char* pComment);
  void SetBufSize(SCreateFuncReq* pReq, int32_t size);
};

Testbase MndTestFunc::test;

void MndTestFunc::SetCode(SCreateFuncReq* pReq, const char* pCode, int32_t size) {
  pReq->pCode = (char*)taosMemoryMalloc(size);
  memcpy(pReq->pCode, pCode, size);
  pReq->codeLen = size;
}

void MndTestFunc::SetComment(SCreateFuncReq* pReq, const char* pComment) {
  int32_t len = strlen(pComment);
  pReq->pComment = (char*)taosMemoryCalloc(1, len + 1);
  strcpy(pReq->pComment, pComment);
}

void MndTestFunc::SetBufSize(SCreateFuncReq* pReq, int32_t size) { pReq->bufSize = size; }

TEST_F(MndTestFunc, 01_Show_Func) {
  test.SendShowReq(TSDB_MGMT_TABLE_FUNC, "ins_functions", "");
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestFunc, 02_Create_Func) {
  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "");

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_NAME);
  }

  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "f1");
    SetComment(&createReq, "comment1");

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_CODE);
  }

  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "f1");
    SetCode(&createReq, "", 1);
    SetComment(&createReq, "comment1");

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_CODE);
  }

  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "f1");
    SetCode(&createReq, "code1", 6);
    SetComment(&createReq, "comment1");
    SetBufSize(&createReq, -1);

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_BUFSIZE);
  }

  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "f1");
    SetCode(&createReq, "code1", 6);
    SetComment(&createReq, "comment1");
    createReq.bufSize = TSDB_FUNC_BUF_SIZE + 1;

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_BUFSIZE);
  }

  for (int32_t i = 0; i < 3; ++i) {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "f1");
    SetCode(&createReq, "code1", 6);
    SetComment(&createReq, "comment1");
    createReq.bufSize = TSDB_FUNC_BUF_SIZE + 1;
    createReq.igExists = 0;
    if (i == 2) createReq.igExists = 1;
    createReq.funcType = 1;
    createReq.scriptType = 2;
    createReq.outputType = TSDB_DATA_TYPE_SMALLINT;
    createReq.outputLen = 12;
    createReq.bufSize = 4;
    createReq.signature = 5;

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0 || i == 2) {
      ASSERT_EQ(pRsp->code, 0);
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_MND_FUNC_ALREADY_EXIST);
    }
  }

  test.SendShowReq(TSDB_MGMT_TABLE_FUNC, "ins_functions", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestFunc, 03_Retrieve_Func) {
  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 1;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
    taosArrayPush(retrieveReq.pFuncNames, "f1");

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp retrieveRsp = {0};
    tDeserializeSRetrieveFuncRsp(pRsp->pCont, pRsp->contLen, &retrieveRsp);
    EXPECT_EQ(retrieveRsp.numOfFuncs, 1);
    EXPECT_EQ(retrieveRsp.numOfFuncs, (int32_t)taosArrayGetSize(retrieveRsp.pFuncInfos));

    SFuncInfo* pFuncInfo = (SFuncInfo*)taosArrayGet(retrieveRsp.pFuncInfos, 0);

    EXPECT_STREQ(pFuncInfo->name, "f1");
    EXPECT_EQ(pFuncInfo->funcType, 1);
    EXPECT_EQ(pFuncInfo->scriptType, 2);
    EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_SMALLINT);
    EXPECT_EQ(pFuncInfo->outputLen, 12);
    EXPECT_EQ(pFuncInfo->bufSize, 4);
    EXPECT_EQ(pFuncInfo->signature, 5);
    EXPECT_STREQ("comment1", pFuncInfo->pComment);
    EXPECT_STREQ("code1", pFuncInfo->pCode);

    tFreeSRetrieveFuncRsp(&retrieveRsp);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 0;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_RETRIEVE);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = TSDB_FUNC_MAX_RETRIEVE + 1;
    retrieveReq.pFuncNames = taosArrayInit(TSDB_FUNC_MAX_RETRIEVE + 1, TSDB_FUNC_NAME_LEN);
    for (int32_t i = 0; i < TSDB_FUNC_MAX_RETRIEVE + 1; ++i) {
      taosArrayPush(retrieveReq.pFuncNames, "1");
    }

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_RETRIEVE);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 1;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
    taosArrayPush(retrieveReq.pFuncNames, "f2");

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_FUNC_NOT_EXIST);
  }

  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "f2");
    createReq.igExists = 1;
    createReq.funcType = 2;
    createReq.scriptType = 3;
    createReq.outputType = TSDB_DATA_TYPE_BINARY;
    createReq.outputLen = 24;
    createReq.bufSize = 6;
    createReq.signature = 18;
    SetCode(&createReq, "code2", 6);
    SetComment(&createReq, "comment2");

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowReq(TSDB_MGMT_TABLE_FUNC, "ins_functions", "");
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 1;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
    taosArrayPush(retrieveReq.pFuncNames, "f2");

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp retrieveRsp = {0};
    tDeserializeSRetrieveFuncRsp(pRsp->pCont, pRsp->contLen, &retrieveRsp);
    EXPECT_EQ(retrieveRsp.numOfFuncs, 1);
    EXPECT_EQ(retrieveRsp.numOfFuncs, (int32_t)taosArrayGetSize(retrieveRsp.pFuncInfos));

    SFuncInfo* pFuncInfo = (SFuncInfo*)taosArrayGet(retrieveRsp.pFuncInfos, 0);

    EXPECT_STREQ(pFuncInfo->name, "f2");
    EXPECT_EQ(pFuncInfo->funcType, 2);
    EXPECT_EQ(pFuncInfo->scriptType, 3);
    EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pFuncInfo->outputLen, 24);
    EXPECT_EQ(pFuncInfo->bufSize, 6);
    EXPECT_EQ(pFuncInfo->signature, 18);
    EXPECT_EQ(pFuncInfo->commentSize, strlen("comment2") + 1);

    EXPECT_STREQ("comment2", pFuncInfo->pComment);
    EXPECT_STREQ("code2", pFuncInfo->pCode);

    tFreeSRetrieveFuncRsp(&retrieveRsp);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 2;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
    taosArrayPush(retrieveReq.pFuncNames, "f2");
    taosArrayPush(retrieveReq.pFuncNames, "f1");

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp retrieveRsp = {0};
    tDeserializeSRetrieveFuncRsp(pRsp->pCont, pRsp->contLen, &retrieveRsp);
    EXPECT_EQ(retrieveRsp.numOfFuncs, 2);
    EXPECT_EQ(retrieveRsp.numOfFuncs, (int32_t)taosArrayGetSize(retrieveRsp.pFuncInfos));

    {
      SFuncInfo* pFuncInfo = (SFuncInfo*)taosArrayGet(retrieveRsp.pFuncInfos, 0);
      EXPECT_STREQ(pFuncInfo->name, "f2");
      EXPECT_EQ(pFuncInfo->funcType, 2);
      EXPECT_EQ(pFuncInfo->scriptType, 3);
      EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_BINARY);
      EXPECT_EQ(pFuncInfo->outputLen, 24);
      EXPECT_EQ(pFuncInfo->bufSize, 6);
      EXPECT_EQ(pFuncInfo->signature, 18);
      EXPECT_EQ(pFuncInfo->commentSize, strlen("comment2") + 1);
      EXPECT_STREQ("comment2", pFuncInfo->pComment);
      EXPECT_STREQ("code2", pFuncInfo->pCode);
    }

    {
      SFuncInfo* pFuncInfo = (SFuncInfo*)taosArrayGet(retrieveRsp.pFuncInfos, 1);
      EXPECT_STREQ(pFuncInfo->name, "f1");
      EXPECT_EQ(pFuncInfo->funcType, 1);
      EXPECT_EQ(pFuncInfo->scriptType, 2);
      EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_SMALLINT);
      EXPECT_EQ(pFuncInfo->outputLen, 12);
      EXPECT_EQ(pFuncInfo->bufSize, 4);
      EXPECT_EQ(pFuncInfo->signature, 5);
      EXPECT_STREQ("comment1", pFuncInfo->pComment);
      EXPECT_STREQ("code1", pFuncInfo->pCode);
    }

    tFreeSRetrieveFuncRsp(&retrieveRsp);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 2;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
    taosArrayPush(retrieveReq.pFuncNames, "f2");
    taosArrayPush(retrieveReq.pFuncNames, "f3");

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_FUNC_NOT_EXIST);
  }
}

TEST_F(MndTestFunc, 04_Drop_Func) {
  {
    SDropFuncReq dropReq = {0};
    strcpy(dropReq.name, "");

    int32_t contLen = tSerializeSDropFuncReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropFuncReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_NAME);
  }

  {
    SDropFuncReq dropReq = {0};
    strcpy(dropReq.name, "f3");

    int32_t contLen = tSerializeSDropFuncReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropFuncReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_FUNC_NOT_EXIST);
  }

  {
    SDropFuncReq dropReq = {0};
    strcpy(dropReq.name, "f3");
    dropReq.igNotExists = 1;

    int32_t contLen = tSerializeSDropFuncReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropFuncReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SDropFuncReq dropReq = {0};
    strcpy(dropReq.name, "f1");
    dropReq.igNotExists = 1;

    int32_t contLen = tSerializeSDropFuncReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropFuncReq(pReq, contLen, &dropReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowReq(TSDB_MGMT_TABLE_FUNC, "ins_functions", "");
  EXPECT_EQ(test.GetShowRows(), 1);

  // restart
  test.Restart();

  test.SendShowReq(TSDB_MGMT_TABLE_FUNC, "ins_functions", "");
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestFunc, 05_Actual_code) {
  {
    SCreateFuncReq createReq = {0};
    strcpy(createReq.name, "udf1");
    char code[300] = {0};
    for (int32_t i = 0; i < sizeof(code); ++i) {
      code[i] = (i) % 20;
    }
    SetCode(&createReq, code, 300);
    SetComment(&createReq, "comment1");
    createReq.bufSize = 8;
    createReq.igExists = 0;
    createReq.funcType = 1;
    createReq.scriptType = 2;
    createReq.outputType = TSDB_DATA_TYPE_SMALLINT;
    createReq.outputLen = 12;
    createReq.bufSize = 4;
    createReq.signature = 5;

    int32_t contLen = tSerializeSCreateFuncReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateFuncReq(pReq, contLen, &createReq);
    tFreeSCreateFuncReq(&createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    SRetrieveFuncReq retrieveReq = {0};
    retrieveReq.numOfFuncs = 1;
    retrieveReq.pFuncNames = taosArrayInit(1, TSDB_FUNC_NAME_LEN);
    taosArrayPush(retrieveReq.pFuncNames, "udf1");

    int32_t contLen = tSerializeSRetrieveFuncReq(NULL, 0, &retrieveReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSRetrieveFuncReq(pReq, contLen, &retrieveReq);
    tFreeSRetrieveFuncReq(&retrieveReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp retrieveRsp = {0};
    tDeserializeSRetrieveFuncRsp(pRsp->pCont, pRsp->contLen, &retrieveRsp);
    EXPECT_EQ(retrieveRsp.numOfFuncs, 1);
    EXPECT_EQ(retrieveRsp.numOfFuncs, (int32_t)taosArrayGetSize(retrieveRsp.pFuncInfos));

    SFuncInfo* pFuncInfo = (SFuncInfo*)taosArrayGet(retrieveRsp.pFuncInfos, 0);

    EXPECT_STREQ(pFuncInfo->name, "udf1");
    EXPECT_EQ(pFuncInfo->funcType, 1);
    EXPECT_EQ(pFuncInfo->scriptType, 2);
    EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_SMALLINT);
    EXPECT_EQ(pFuncInfo->outputLen, 12);
    EXPECT_EQ(pFuncInfo->bufSize, 4);
    EXPECT_EQ(pFuncInfo->signature, 5);
    EXPECT_STREQ("comment1", pFuncInfo->pComment);
    for (int32_t i = 0; i < 300; ++i) {
      EXPECT_EQ(pFuncInfo->pCode[i], (i) % 20);
    }
    tFreeSRetrieveFuncRsp(&retrieveRsp);
  }
}