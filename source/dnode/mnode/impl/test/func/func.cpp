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
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_func", 9038); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestFunc::test;

TEST_F(MndTestFunc, 01_Show_Func) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 7);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_BINARY, TSDB_FUNC_NAME_LEN + VARSTR_HEADER_SIZE, "name");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, PATH_MAX + VARSTR_HEADER_SIZE, "comment");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_INT, 4, "aggregate");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_BINARY, TSDB_TYPE_STR_MAX_LEN + VARSTR_HEADER_SIZE, "outputtype");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CHECK_SCHEMA(5, TSDB_DATA_TYPE_INT, 4, "code_len");
  CHECK_SCHEMA(6, TSDB_DATA_TYPE_INT, 4, "bufsize");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}

TEST_F(MndTestFunc, 02_Create_Func) {
  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "");
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_NAME);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_COMMENT);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->commentSize = htonl(TSDB_FUNC_COMMENT_LEN + 1);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_COMMENT);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->commentSize = htonl(TSDB_FUNC_COMMENT_LEN);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_CODE);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->commentSize = htonl(TSDB_FUNC_COMMENT_LEN);
    pReq->codeSize = htonl(TSDB_FUNC_CODE_LEN - 1);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_CODE);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq) + 24;

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->commentSize = htonl(TSDB_FUNC_COMMENT_LEN);
    pReq->codeSize = htonl(TSDB_FUNC_CODE_LEN);
    pReq->pCont[0] = 0;
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_CODE);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq) + 24;

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->commentSize = htonl(TSDB_FUNC_COMMENT_LEN);
    pReq->codeSize = htonl(TSDB_FUNC_CODE_LEN);
    pReq->pCont[0] = 'a';
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_BUFSIZE);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq) + 24;

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->commentSize = htonl(TSDB_FUNC_COMMENT_LEN);
    pReq->codeSize = htonl(TSDB_FUNC_CODE_LEN);
    pReq->pCont[0] = 'a';
    pReq->bufSize = htonl(TSDB_FUNC_BUF_SIZE + 1);
    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_BUFSIZE);
  }

  for (int32_t i = 0; i < 3; ++i) {
    int32_t contLen = sizeof(SCreateFuncReq);
    int32_t commentSize = TSDB_FUNC_COMMENT_LEN;
    int32_t codeSize = TSDB_FUNC_CODE_LEN;
    contLen = (contLen + codeSize + commentSize);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f1");
    pReq->igExists = 0;
    if (i == 2) pReq->igExists = 1;
    pReq->funcType = 1;
    pReq->scriptType = 2;
    pReq->outputType = TSDB_DATA_TYPE_SMALLINT;
    pReq->outputLen = htonl(12);
    pReq->bufSize = htonl(4);
    pReq->signature = htobe64(5);
    pReq->commentSize = htonl(commentSize);
    pReq->codeSize = htonl(codeSize);
    for (int32_t i = 0; i < commentSize - 1; ++i) {
      pReq->pCont[i] = 'm';
    }
    for (int32_t i = commentSize; i < commentSize + codeSize - 1; ++i) {
      pReq->pCont[i] = 'd';
    }

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0 || i == 2) {
      ASSERT_EQ(pRsp->code, 0);
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_MND_FUNC_ALREADY_EXIST);
    }
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 7);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);

  CheckBinary("f1", TSDB_FUNC_NAME_LEN);
  CheckBinaryByte('m', TSDB_FUNC_COMMENT_LEN);
  CheckInt32(0);
  CheckBinary("SMALLINT", TSDB_TYPE_STR_MAX_LEN);
  CheckTimestamp();
  CheckInt32(TSDB_FUNC_CODE_LEN);
  CheckInt32(4);
}

TEST_F(MndTestFunc, 03_Retrieve_Func) {
  {
    int32_t contLen = sizeof(SRetrieveFuncReq);
    int32_t numOfFuncs = 1;
    contLen = (contLen + numOfFuncs * TSDB_FUNC_NAME_LEN);

    SRetrieveFuncReq* pReq = (SRetrieveFuncReq*)rpcMallocCont(contLen);
    pReq->numOfFuncs = htonl(1);
    strcpy(pReq->pFuncNames, "f1");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp* pRetrieveRsp = (SRetrieveFuncRsp*)pRsp->pCont;
    pRetrieveRsp->numOfFuncs = htonl(pRetrieveRsp->numOfFuncs);

    SFuncInfo* pFuncInfo = (SFuncInfo*)(pRetrieveRsp->pFuncInfos);
    pFuncInfo->outputLen = htonl(pFuncInfo->outputLen);
    pFuncInfo->bufSize = htonl(pFuncInfo->bufSize);
    pFuncInfo->signature = htobe64(pFuncInfo->signature);
    pFuncInfo->commentSize = htonl(pFuncInfo->commentSize);
    pFuncInfo->codeSize = htonl(pFuncInfo->codeSize);

    EXPECT_STREQ(pFuncInfo->name, "f1");
    EXPECT_EQ(pFuncInfo->funcType, 1);
    EXPECT_EQ(pFuncInfo->scriptType, 2);
    EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_SMALLINT);
    EXPECT_EQ(pFuncInfo->outputLen, 12);
    EXPECT_EQ(pFuncInfo->bufSize, 4);
    EXPECT_EQ(pFuncInfo->signature, 5);
    EXPECT_EQ(pFuncInfo->commentSize, TSDB_FUNC_COMMENT_LEN);
    EXPECT_EQ(pFuncInfo->codeSize, TSDB_FUNC_CODE_LEN);

    char* pComment = pFuncInfo->pCont;
    char* pCode = pFuncInfo->pCont + pFuncInfo->commentSize;
    char  comments[TSDB_FUNC_COMMENT_LEN] = {0};
    for (int32_t i = 0; i < TSDB_FUNC_COMMENT_LEN - 1; ++i) {
      comments[i] = 'm';
    }
    char codes[TSDB_FUNC_CODE_LEN] = {0};
    for (int32_t i = 0; i < TSDB_FUNC_CODE_LEN - 1; ++i) {
      codes[i] = 'd';
    }
    EXPECT_STREQ(pComment, comments);
    EXPECT_STREQ(pCode, codes);
  }

  {
    int32_t contLen = sizeof(SRetrieveFuncReq);
    int32_t numOfFuncs = 0;
    contLen = (contLen + numOfFuncs * TSDB_FUNC_NAME_LEN);

    SRetrieveFuncReq* pReq = (SRetrieveFuncReq*)rpcMallocCont(contLen);
    pReq->numOfFuncs = htonl(numOfFuncs);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_RETRIEVE);
  }

  {
    int32_t contLen = sizeof(SRetrieveFuncReq);
    int32_t numOfFuncs = TSDB_FUNC_MAX_RETRIEVE + 1;
    contLen = (contLen + numOfFuncs * TSDB_FUNC_NAME_LEN);

    SRetrieveFuncReq* pReq = (SRetrieveFuncReq*)rpcMallocCont(contLen);
    pReq->numOfFuncs = htonl(numOfFuncs);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_RETRIEVE);
  }

  {
    int32_t contLen = sizeof(SRetrieveFuncReq);
    int32_t numOfFuncs = 1;
    contLen = (contLen + numOfFuncs * TSDB_FUNC_NAME_LEN);

    SRetrieveFuncReq* pReq = (SRetrieveFuncReq*)rpcMallocCont(contLen);
    pReq->numOfFuncs = htonl(numOfFuncs);
    strcpy(pReq->pFuncNames, "f2");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq);
    int32_t commentSize = 1024;
    int32_t codeSize = 9527;
    contLen = (contLen + codeSize + commentSize);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->name, "f2");
    pReq->igExists = 1;
    pReq->funcType = 2;
    pReq->scriptType = 3;
    pReq->outputType = TSDB_DATA_TYPE_BINARY;
    pReq->outputLen = htonl(24);
    pReq->bufSize = htonl(6);
    pReq->signature = htobe64(18);
    pReq->commentSize = htonl(commentSize);
    pReq->codeSize = htonl(codeSize);
    for (int32_t i = 0; i < commentSize - 1; ++i) {
      pReq->pCont[i] = 'p';
    }
    for (int32_t i = commentSize; i < commentSize + codeSize - 1; ++i) {
      pReq->pCont[i] = 'q';
    }

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
    CHECK_META("show functions", 7);

    test.SendShowRetrieveReq();
    EXPECT_EQ(test.GetShowRows(), 2);
  }

  {
    int32_t contLen = sizeof(SRetrieveFuncReq);
    int32_t numOfFuncs = 1;
    contLen = (contLen + numOfFuncs * TSDB_FUNC_NAME_LEN);

    SRetrieveFuncReq* pReq = (SRetrieveFuncReq*)rpcMallocCont(contLen);
    pReq->numOfFuncs = htonl(1);
    strcpy(pReq->pFuncNames, "f2");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp* pRetrieveRsp = (SRetrieveFuncRsp*)pRsp->pCont;
    pRetrieveRsp->numOfFuncs = htonl(pRetrieveRsp->numOfFuncs);

    SFuncInfo* pFuncInfo = (SFuncInfo*)(pRetrieveRsp->pFuncInfos);
    pFuncInfo->outputLen = htonl(pFuncInfo->outputLen);
    pFuncInfo->bufSize = htonl(pFuncInfo->bufSize);
    pFuncInfo->signature = htobe64(pFuncInfo->signature);
    pFuncInfo->commentSize = htonl(pFuncInfo->commentSize);
    pFuncInfo->codeSize = htonl(pFuncInfo->codeSize);

    EXPECT_STREQ(pFuncInfo->name, "f2");
    EXPECT_EQ(pFuncInfo->funcType, 2);
    EXPECT_EQ(pFuncInfo->scriptType, 3);
    EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pFuncInfo->outputLen, 24);
    EXPECT_EQ(pFuncInfo->bufSize, 6);
    EXPECT_EQ(pFuncInfo->signature, 18);
    EXPECT_EQ(pFuncInfo->commentSize, 1024);
    EXPECT_EQ(pFuncInfo->codeSize, 9527);

    char* pComment = pFuncInfo->pCont;
    char* pCode = pFuncInfo->pCont + pFuncInfo->commentSize;
    char* comments = (char*)calloc(1, 1024);
    for (int32_t i = 0; i < 1024 - 1; ++i) {
      comments[i] = 'p';
    }
    char* codes = (char*)calloc(1, 9527);
    for (int32_t i = 0; i < 9527 - 1; ++i) {
      codes[i] = 'q';
    }
    EXPECT_STREQ(pComment, comments);
    EXPECT_STREQ(pCode, codes);
    free(comments);
    free(codes);
  }

  {
    int32_t contLen = sizeof(SRetrieveFuncReq);
    int32_t numOfFuncs = 2;
    contLen = (contLen + numOfFuncs * TSDB_FUNC_NAME_LEN);

    SRetrieveFuncReq* pReq = (SRetrieveFuncReq*)rpcMallocCont(contLen);
    pReq->numOfFuncs = htonl(1);
    strcpy(pReq->pFuncNames, "f2");
    strcpy((char*)pReq->pFuncNames + TSDB_FUNC_NAME_LEN, "f1");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_RETRIEVE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);

    SRetrieveFuncRsp* pRetrieveRsp = (SRetrieveFuncRsp*)pRsp->pCont;
    pRetrieveRsp->numOfFuncs = htonl(pRetrieveRsp->numOfFuncs);

    {
      SFuncInfo* pFuncInfo = (SFuncInfo*)(pRetrieveRsp->pFuncInfos);
      pFuncInfo->outputLen = htonl(pFuncInfo->outputLen);
      pFuncInfo->bufSize = htonl(pFuncInfo->bufSize);
      pFuncInfo->signature = htobe64(pFuncInfo->signature);
      pFuncInfo->commentSize = htonl(pFuncInfo->commentSize);
      pFuncInfo->codeSize = htonl(pFuncInfo->codeSize);

      EXPECT_STREQ(pFuncInfo->name, "f2");
      EXPECT_EQ(pFuncInfo->funcType, 2);
      EXPECT_EQ(pFuncInfo->scriptType, 3);
      EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_BINARY);
      EXPECT_EQ(pFuncInfo->outputLen, 24);
      EXPECT_EQ(pFuncInfo->bufSize, 6);
      EXPECT_EQ(pFuncInfo->signature, 18);
      EXPECT_EQ(pFuncInfo->commentSize, 1024);
      EXPECT_EQ(pFuncInfo->codeSize, 9527);

      char* pComment = pFuncInfo->pCont;
      char* pCode = pFuncInfo->pCont + pFuncInfo->commentSize;
      char* comments = (char*)calloc(1, 1024);
      for (int32_t i = 0; i < 1024 - 1; ++i) {
        comments[i] = 'p';
      }
      char* codes = (char*)calloc(1, 9527);
      for (int32_t i = 0; i < 9527 - 1; ++i) {
        codes[i] = 'q';
      }
      EXPECT_STREQ(pComment, comments);
      EXPECT_STREQ(pCode, codes);
      free(comments);
      free(codes);
    }

    {
      SFuncInfo* pFuncInfo = (SFuncInfo*)(pRetrieveRsp->pFuncInfos + sizeof(SFuncInfo) + 1024 + 9527);
      pFuncInfo->outputLen = htonl(pFuncInfo->outputLen);
      pFuncInfo->bufSize = htonl(pFuncInfo->bufSize);
      pFuncInfo->signature = htobe64(pFuncInfo->signature);
      pFuncInfo->commentSize = htonl(pFuncInfo->commentSize);
      pFuncInfo->codeSize = htonl(pFuncInfo->codeSize);
      EXPECT_STREQ(pFuncInfo->name, "f1");
      EXPECT_EQ(pFuncInfo->funcType, 1);
      EXPECT_EQ(pFuncInfo->scriptType, 2);
      EXPECT_EQ(pFuncInfo->outputType, TSDB_DATA_TYPE_SMALLINT);
      EXPECT_EQ(pFuncInfo->outputLen, 12);
      EXPECT_EQ(pFuncInfo->bufSize, 4);
      EXPECT_EQ(pFuncInfo->signature, 5);
      EXPECT_EQ(pFuncInfo->commentSize, TSDB_FUNC_COMMENT_LEN);
      EXPECT_EQ(pFuncInfo->codeSize, TSDB_FUNC_CODE_LEN);

      // char* pComment = pFuncInfo->pCont;
      // char* pCode = pFuncInfo->pCont + pFuncInfo->commentSize;
      // char  comments[TSDB_FUNC_COMMENT_LEN] = {0};
      // for (int32_t i = 0; i < TSDB_FUNC_COMMENT_LEN - 1; ++i) {
      //   comments[i] = 'm';
      // }
      // char codes[TSDB_FUNC_CODE_LEN] = {0};
      // for (int32_t i = 0; i < TSDB_FUNC_CODE_LEN - 1; ++i) {
      //   codes[i] = 'd';
      // }
      // EXPECT_STREQ(pComment, comments);
      // EXPECT_STREQ(pCode, codes);
    }
  }
}

#if 0

TEST_F(MndTestFunc, 04_Drop_Func) {
  {
    int32_t contLen = sizeof(SDropFuncReq);

    SDropFuncReq* pReq = (SDropFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_FUNC_FORMAT);
  }

  {
    int32_t contLen = sizeof(SDropFuncReq);

    SDropFuncReq* pReq = (SDropFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u4");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_FUNC_NOT_EXIST);
  }

  {
    int32_t contLen = sizeof(SDropFuncReq);

    SDropFuncReq* pReq = (SDropFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
}

TEST_F(MndTestFunc, 05_Create_Drop_Alter_Func) {
  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "p1");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SCreateFuncReq);

    SCreateFuncReq* pReq = (SCreateFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u2");
    strcpy(pReq->pass, "p2");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_CREATE_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 3);

  CheckBinary("u1", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("u2", TSDB_FUNC_LEN);
  CheckBinary("normal", 10);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);

  {
    int32_t contLen = sizeof(SAlterFuncReq);

    SAlterFuncReq* pReq = (SAlterFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");
    strcpy(pReq->pass, "p2");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_ALTER_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 3);

  CheckBinary("u1", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("u2", TSDB_FUNC_LEN);
  CheckBinary("normal", 10);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);

  {
    int32_t contLen = sizeof(SDropFuncReq);

    SDropFuncReq* pReq = (SDropFuncReq*)rpcMallocCont(contLen);
    strcpy(pReq->user, "u1");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_DROP_FUNC, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("u2", TSDB_FUNC_LEN);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);

  // restart
  test.Restart();

  test.SendShowMetaReq(TSDB_MGMT_TABLE_FUNC, "");
  CHECK_META("show functions", 4);

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 2);

  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("u2", TSDB_FUNC_LEN);
  CheckBinary("super", 10);
  CheckBinary("normal", 10);
  CheckTimestamp();
  CheckTimestamp();
  CheckBinary("root", TSDB_FUNC_LEN);
  CheckBinary("root", TSDB_FUNC_LEN);
}

#endif