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
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_vnode", 9115); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestVnode::test;

TEST_F(DndTestVnode, 01_Create_Vnode) {
  for (int i = 0; i < 3; ++i) {
    int32_t contLen = sizeof(SCreateVnodeReq);

    SCreateVnodeReq* pReq = (SCreateVnodeReq*)rpcMallocCont(contLen);
    pReq->vgId = htonl(2);
    pReq->dnodeId = htonl(1);
    strcpy(pReq->db, "1.d1");
    pReq->dbUid = htobe64(9527);
    pReq->vgVersion = htonl(1);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRows = htonl(100);
    pReq->minRows = htonl(4096);
    pReq->commitTime = htonl(3600);
    pReq->fsyncPeriod = htonl(3000);
    pReq->walLevel = 1;
    pReq->precision = 0;
    pReq->compression = 2;
    pReq->replica = 1;
    pReq->quorum = 1;
    pReq->update = 0;
    pReq->cacheLastRow = 0;
    pReq->selfIndex = 0;
    for (int r = 0; r < pReq->replica; ++r) {
      SReplica* pReplica = &pReq->replicas[r];
      pReplica->id = htonl(1);
      pReplica->port = htons(9527);
    }

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0) {
      ASSERT_EQ(pRsp->code, 0);
      test.Restart();
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_DND_VNODE_ALREADY_DEPLOYED);
    }
  }

  {
    int32_t contLen = sizeof(SCreateVnodeReq);

    SCreateVnodeReq* pReq = (SCreateVnodeReq*)rpcMallocCont(contLen);
    pReq->vgId = htonl(2);
    pReq->dnodeId = htonl(3);
    strcpy(pReq->db, "1.d1");
    pReq->dbUid = htobe64(9527);
    pReq->vgVersion = htonl(1);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRows = htonl(100);
    pReq->minRows = htonl(4096);
    pReq->commitTime = htonl(3600);
    pReq->fsyncPeriod = htonl(3000);
    pReq->walLevel = 1;
    pReq->precision = 0;
    pReq->compression = 2;
    pReq->replica = 1;
    pReq->quorum = 1;
    pReq->update = 0;
    pReq->cacheLastRow = 0;
    pReq->selfIndex = 0;
    for (int r = 0; r < pReq->replica; ++r) {
      SReplica* pReplica = &pReq->replicas[r];
      pReplica->id = htonl(1);
      pReplica->port = htons(9527);
    }

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_DND_VNODE_INVALID_OPTION);
  }
}

TEST_F(DndTestVnode, 02_ALTER_Vnode) {
  for (int i = 0; i < 3; ++i) {
    int32_t contLen = sizeof(SAlterVnodeReq);

    SAlterVnodeReq* pReq = (SAlterVnodeReq*)rpcMallocCont(contLen);
    pReq->vgId = htonl(2);
    pReq->dnodeId = htonl(1);
    strcpy(pReq->db, "1.d1");
    pReq->dbUid = htobe64(9527);
    pReq->vgVersion = htonl(2);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRows = htonl(100);
    pReq->minRows = htonl(4096);
    pReq->commitTime = htonl(3600);
    pReq->fsyncPeriod = htonl(3000);
    pReq->walLevel = 1;
    pReq->precision = 0;
    pReq->compression = 2;
    pReq->replica = 1;
    pReq->quorum = 1;
    pReq->update = 0;
    pReq->cacheLastRow = 0;
    pReq->selfIndex = 0;
    for (int r = 0; r < pReq->replica; ++r) {
      SReplica* pReplica = &pReq->replicas[r];
      pReplica->id = htonl(1);
      pReplica->port = htons(9527);
    }

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_ALTER_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }
}

TEST_F(DndTestVnode, 03_Create_Stb) {
  for (int i = 0; i < 1; ++i) {
    SVCreateTbReq req = {0};
    req.ver = 0;
    req.name = (char*)"stb1";
    req.ttl = 0;
    req.keep = 0;
    req.type = TD_SUPER_TABLE;

    SSchema schemas[5] = {0};
    {
      SSchema* pSchema = &schemas[0];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_TIMESTAMP;
      strcpy(pSchema->name, "ts");
    }

    {
      SSchema* pSchema = &schemas[1];
      pSchema->bytes = htonl(4);
      pSchema->type = TSDB_DATA_TYPE_INT;
      strcpy(pSchema->name, "col1");
    }

    {
      SSchema* pSchema = &schemas[2];
      pSchema->bytes = htonl(2);
      pSchema->type = TSDB_DATA_TYPE_TINYINT;
      strcpy(pSchema->name, "tag1");
    }

    {
      SSchema* pSchema = &schemas[3];
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_BIGINT;
      strcpy(pSchema->name, "tag2");
    }

    {
      SSchema* pSchema = &schemas[4];
      pSchema->bytes = htonl(16);
      pSchema->type = TSDB_DATA_TYPE_BINARY;
      strcpy(pSchema->name, "tag3");
    }

    req.stbCfg.suid = 9527;
    req.stbCfg.nCols = 2;
    req.stbCfg.pSchema = &schemas[0];
    req.stbCfg.nTagCols = 3;
    req.stbCfg.pTagSchema = &schemas[2];

    int32_t   bsize = tSerializeSVCreateTbReq(NULL, &req);
    void*     buf = rpcMallocCont(sizeof(SMsgHead) + bsize);
    SMsgHead* pMsgHead = (SMsgHead*)buf;

    pMsgHead->contLen = htonl(sizeof(SMsgHead) + bsize);
    pMsgHead->vgId = htonl(2);

    void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    tSerializeSVCreateTbReq(&pBuf, &req);

    int32_t contLen = sizeof(SMsgHead) + bsize;

    SRpcMsg* pRsp = test.SendReq(TDMT_VND_CREATE_STB, buf, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0) {
      ASSERT_EQ(pRsp->code, 0);
      test.Restart();
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_TDB_TABLE_ALREADY_EXIST);
    }
  }
}

TEST_F(DndTestVnode, 04_ALTER_Stb) {
#if 0
  {
    for (int i = 0; i < 3; ++i) {
      SRpcMsg* pRsp = test.SendReq(TDMT_VND_ALTER_STB, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      ASSERT_EQ(pRsp->code, 0);
    }
  }
#endif
}

TEST_F(DndTestVnode, 05_DROP_Stb) {
#if 0
  {
    for (int i = 0; i < 3; ++i) {
      SRpcMsg* pRsp = test.SendReq(TDMT_VND_DROP_STB, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      if (i == 0) {
        ASSERT_EQ(pRsp->code, 0);
        test.Restart();
      } else {
        ASSERT_EQ(pRsp->code, TSDB_CODE_TDB_INVALID_TABLE_ID);
      }
    }
  }
#endif
}

TEST_F(DndTestVnode, 06_DROP_Vnode) {
  for (int i = 0; i < 3; ++i) {
    int32_t contLen = sizeof(SDropVnodeReq);

    SDropVnodeReq* pReq = (SDropVnodeReq*)rpcMallocCont(contLen);
    pReq->vgId = htonl(2);
    pReq->dnodeId = htonl(1);
    strcpy(pReq->db, "1.d1");
    pReq->dbUid = htobe64(9527);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SDropVnodeReq);
    rpcMsg.msgType = TDMT_DND_DROP_VNODE;

    SRpcMsg* pRsp = test.SendReq(TDMT_DND_DROP_VNODE, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    if (i == 0) {
      ASSERT_EQ(pRsp->code, 0);
      test.Restart();
    } else {
      ASSERT_EQ(pRsp->code, TSDB_CODE_DND_VNODE_NOT_DEPLOYED);
    }
  }
}