/**
 * @file db.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module vgroup-msg tests
 * @version 0.1
 * @date 2021-12-20
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "sut.h"

class DndTestVnode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/dnode_test_vnode", 9150); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase DndTestVnode::test;

TEST_F(DndTestVnode, 01_Create_Restart_Drop_Vnode) {
  {
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
        pReplica->port = htons(9150);
      }

      SRpcMsg* pRsp = test.SendReq(TDMT_DND_CREATE_VNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      ASSERT_EQ(pRsp->code, 0);
    }
  }

  {
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
        pReplica->port = htons(9150);
      }

      SRpcMsg* pRsp = test.SendReq(TDMT_DND_ALTER_VNODE, pReq, contLen);
      ASSERT_NE(pRsp, nullptr);
      ASSERT_EQ(pRsp->code, 0);
    }
  }

  {
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
      ASSERT_EQ(pRsp->code, 0);
    }
  }
}
