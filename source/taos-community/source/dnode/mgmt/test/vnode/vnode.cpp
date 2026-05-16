/**
 * @file db.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module vnode tests
 * @version 0.1
 * @date 2021-12-20
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "sut.h"

class DndTestVnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init(TD_TMP_DIR_PATH "dvnodeTest", 9115); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestVnode::test;

TEST_F(DndTestVnode, 01_Create_Vnode) {
  for (int i = 0; i < 3; ++i) {
    SCreateVnodeReq createReq = {0};
    createReq.vgId = 2;
    createReq.dnodeId = 1;
    strcpy(createReq.db, "1.d1");
    createReq.dbUid = 9527;
    createReq.vgVersion = 1;
    createReq.daysPerFile = 10;
    createReq.daysToKeep0 = 3650;
    createReq.daysToKeep1 = 3650;
    createReq.daysToKeep2 = 3650;
    createReq.minRows = 100;
    createReq.minRows = 4096;
    createReq.fsyncPeriod = 3000;
    createReq.walLevel = 1;
    createReq.precision = 0;
    createReq.compression = 2;
    createReq.replica = 1;
    createReq.strict = 1;
    createReq.cacheLast = 0;
    createReq.selfIndex = 0;
    for (int r = 0; r < createReq.replica; ++r) {
      SReplica* pReplica = &createReq.replicas[r];
      pReplica->id = 1;
      pReplica->port = 9527;
    }

    int32_t contLen = tSerializeSCreateVnodeReq(NULL, 0, &createReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateVnodeReq(pReq, contLen, &createReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0) {
      ASSERT_EQ(pRsp->code, 0);
      test.Restart();
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_VND_ALREADY_EXIST);
    }
  }
}

TEST_F(DndTestVnode, 02_Alter_Vnode) {
  for (int i = 0; i < 3; ++i) {
    SAlterVnodeConfigReq alterReq = {0};
    alterReq.vgVersion = 2;
    alterReq.daysPerFile = 10;
    alterReq.daysToKeep0 = 3650;
    alterReq.daysToKeep1 = 3650;
    alterReq.daysToKeep2 = 3650;
    alterReq.fsyncPeriod = 3000;
    alterReq.walLevel = 1;
    alterReq.replica = 1;
    alterReq.strict = 1;
    alterReq.cacheLast = 0;
    alterReq.selfIndex = 0;
    for (int r = 0; r < alterReq.replica; ++r) {
      SReplica* pReplica = &alterReq.replicas[r];
      pReplica->id = 1;
      pReplica->port = 9527;
    }

    int32_t contLen = tSerializeSCreateVnodeReq(NULL, 0, &alterReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSCreateVnodeReq(pReq, contLen, &alterReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_VND_ALTER_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}

TEST_F(DndTestVnode, 03_Create_Stb) {
  for (int i = 0; i < 1; ++i) {
    SVCreateTbReq req = {0};
    req.ver = 0;
    req.dbFName = (char*)"1.db1";
    req.name = (char*)"stb1";
    req.ttl = 0;
    req.keep = 0;
    req.type = TD_SUPER_TABLE;

    SSchemaEx schemas[2] = {0};
    {
      SSchemaEx* pSchema = &schemas[0];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_TIMESTAMP;
      strcpy(pSchema->name, "ts");
    }

    {
      SSchemaEx* pSchema = &schemas[1];
      pSchema->bytes = htonl(4);
      pSchema->type = TSDB_DATA_TYPE_INT;
      strcpy(pSchema->name, "col1");
    }
    SSchema tagSchemas[3] = {0};
    {
      SSchema* pSchema = &tagSchemas[0];
      pSchema->bytes = htonl(2);
      pSchema->type = TSDB_DATA_TYPE_TINYINT;
      strcpy(pSchema->name, "tag1");
    }

    {
      SSchema* pSchema = &tagSchemas[1];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_BIGINT;
      strcpy(pSchema->name, "tag2");
    }

    {
      SSchema* pSchema = &tagSchemas[2];
      pSchema->bytes = htonl(16);
      pSchema->type = TSDB_DATA_TYPE_BINARY;
      strcpy(pSchema->name, "tag3");
    }

    req.stbCfg.suid = 9527;
    req.stbCfg.nCols = 2;
    req.stbCfg.pSchema = &schemas[0];
    req.stbCfg.nTagCols = 3;
    req.stbCfg.pTagSchema = &tagSchemas[0];

    int32_t   contLen = tSerializeSVCreateTbReq(NULL, &req) + sizeof(SMsgHead);
    SMsgHead* pHead = (SMsgHead*)rpcMallocCont(contLen);

    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(2);

    void* pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
    tSerializeSVCreateTbReq(&pBuf, &req);

    SRpcMsg* pRsp = test.SendReq(TDMT_VND_CREATE_STB, (void*)pHead, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0) {
      ASSERT_EQ(pRsp->code, 0);
      test.Restart();
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_TDB_TABLE_ALREADY_EXIST);
    }
  }
}

TEST_F(DndTestVnode, 04_Alter_Stb) {
  for (int i = 0; i < 1; ++i) {
    SVCreateTbReq req = {0};
    req.ver = 0;
    req.dbFName = (char*)"1.db1";
    req.name = (char*)"stb1";
    req.ttl = 0;
    req.keep = 0;
    req.type = TD_SUPER_TABLE;

    SSchemaEx schemas[2] = {0};
    {
      SSchemaEx* pSchema = &schemas[0];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_TIMESTAMP;
      strcpy(pSchema->name, "ts");
    }

    {
      SSchemaEx* pSchema = &schemas[1];
      pSchema->bytes = htonl(4);
      pSchema->type = TSDB_DATA_TYPE_INT;
      strcpy(pSchema->name, "col1");
    }
    SSchema tagSchemas[3] = {0};
    {
      SSchema* pSchema = &tagSchemas[0];
      pSchema->bytes = htonl(2);
      pSchema->type = TSDB_DATA_TYPE_TINYINT;
      strcpy(pSchema->name, "_tag1");
    }

    {
      SSchema* pSchema = &tagSchemas[1];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_BIGINT;
      strcpy(pSchema->name, "_tag2");
    }

    {
      SSchema* pSchema = &tagSchemas[2];
      pSchema->bytes = htonl(16);
      pSchema->type = TSDB_DATA_TYPE_BINARY;
      strcpy(pSchema->name, "_tag3");
    }

    req.stbCfg.suid = 9527;
    req.stbCfg.nCols = 2;
    req.stbCfg.pSchema = &schemas[0];
    req.stbCfg.nTagCols = 3;
    req.stbCfg.pTagSchema = &tagSchemas[0];

    int32_t   contLen = tSerializeSVCreateTbReq(NULL, &req) + sizeof(SMsgHead);
    SMsgHead* pHead = (SMsgHead*)rpcMallocCont(contLen);

    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(2);

    void* pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
    tSerializeSVCreateTbReq(&pBuf, &req);

    SRpcMsg* pRsp = test.SendReq(TDMT_VND_ALTER_STB, (void*)pHead, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}

TEST_F(DndTestVnode, 05_DROP_Stb) {
  {
    for (int i = 0; i < 3; ++i) {
      SVDropTbReq req = {0};
      req.ver = 0;
      req.name = (char*)"stb1";
      req.suid = 9599;
      req.type = TD_SUPER_TABLE;

      int32_t   contLen = tSerializeSVDropTbReq(NULL, &req) + sizeof(SMsgHead);
      SMsgHead* pHead = (SMsgHead*)rpcMallocCont(contLen);

      pHead->contLen = htonl(contLen);
      pHead->vgId = htonl(2);

      void* pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
      tSerializeSVDropTbReq(&pBuf, &req);

      SRpcMsg* pRsp = test.SendReq(TDMT_VND_DROP_STB, (void*)pHead, contLen);
      ASSERT_NE(pRsp, nullptr);
      ASSERT_EQ(pRsp->code, 0);
    }
  }
}

TEST_F(DndTestVnode, 06_Drop_Vnode) {
  for (int i = 0; i < 3; ++i) {
    SDropVnodeReq dropReq = {0};
    dropReq.vgId = 2;
    dropReq.dnodeId = 1;
    strcpy(dropReq.db, "1.d1");
    dropReq.dbUid = 9527;

    int32_t contLen = tSerializeSDropVnodeReq(NULL, 0, &dropReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSDropVnodeReq(pReq, contLen, &dropReq);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = contLen;
    rpcMsg.msgType = TDMT_DND_DROP_VNODE;

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0) {
      ASSERT_EQ(pRsp->code, 0);
      test.Restart();
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_VND_NOT_EXIST);
    }
  }
}