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

#include "deploy.h"

class DndTestVgroup : public ::testing::Test {
 protected:
  static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    initLog("/tmp/tdlog");

    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9150";
    pServer = CreateServer("/tmp/dnode_test_vgroup", fqdn, 9150, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9150);
    taosMsleep(1100);
  }

  static void TearDownTestSuite() {
    stopServer(pServer);
    dropClient(pClient);
    pServer = NULL;
    pClient = NULL;
  }

  static SServer* pServer;
  static SClient* pClient;
  static int32_t  connId;

 public:
  void SetUp() override {}
  void TearDown() override {}

  void SendTheCheckShowMetaMsg(int8_t showType, const char* showName, int32_t columns, const char* db) {
    SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
    pShow->type = showType;
    if (db != NULL) {
      strcpy(pShow->db, db);
    }
    SRpcMsg showRpcMsg = {0};
    showRpcMsg.pCont = pShow;
    showRpcMsg.contLen = sizeof(SShowMsg);
    showRpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

    sendMsg(pClient, &showRpcMsg);
    ASSERT_NE(pClient->pRsp, nullptr);
    ASSERT_EQ(pClient->pRsp->code, 0);
    ASSERT_NE(pClient->pRsp->pCont, nullptr);

    SShowRsp* pShowRsp = (SShowRsp*)pClient->pRsp->pCont;
    ASSERT_NE(pShowRsp, nullptr);
    pShowRsp->showId = htonl(pShowRsp->showId);
    pMeta = &pShowRsp->tableMeta;
    pMeta->numOfTags = htonl(pMeta->numOfTags);
    pMeta->numOfColumns = htonl(pMeta->numOfColumns);
    pMeta->sversion = htonl(pMeta->sversion);
    pMeta->tversion = htonl(pMeta->tversion);
    pMeta->tuid = htobe64(pMeta->tuid);
    pMeta->suid = htobe64(pMeta->suid);

    showId = pShowRsp->showId;

    EXPECT_NE(pShowRsp->showId, 0);
    EXPECT_STREQ(pMeta->tbFname, showName);
    EXPECT_EQ(pMeta->numOfTags, 0);
    EXPECT_EQ(pMeta->numOfColumns, columns);
    EXPECT_EQ(pMeta->precision, 0);
    EXPECT_EQ(pMeta->tableType, 0);
    EXPECT_EQ(pMeta->update, 0);
    EXPECT_EQ(pMeta->sversion, 0);
    EXPECT_EQ(pMeta->tversion, 0);
    EXPECT_EQ(pMeta->tuid, 0);
    EXPECT_EQ(pMeta->suid, 0);
  }

  void CheckSchema(int32_t index, int8_t type, int32_t bytes, const char* name) {
    SSchema* pSchema = &pMeta->pSchema[index];
    pSchema->bytes = htonl(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, type);
    EXPECT_EQ(pSchema->bytes, bytes);
    EXPECT_STREQ(pSchema->name, name);
  }

  void SendThenCheckShowRetrieveMsg(int32_t rows) {
    SRetrieveTableMsg* pRetrieve = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
    pRetrieve->showId = htonl(showId);
    pRetrieve->free = 0;

    SRpcMsg retrieveRpcMsg = {0};
    retrieveRpcMsg.pCont = pRetrieve;
    retrieveRpcMsg.contLen = sizeof(SRetrieveTableMsg);
    retrieveRpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

    sendMsg(pClient, &retrieveRpcMsg);

    ASSERT_NE(pClient->pRsp, nullptr);
    ASSERT_EQ(pClient->pRsp->code, 0);
    ASSERT_NE(pClient->pRsp->pCont, nullptr);

    pRetrieveRsp = (SRetrieveTableRsp*)pClient->pRsp->pCont;
    ASSERT_NE(pRetrieveRsp, nullptr);
    pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
    pRetrieveRsp->useconds = htobe64(pRetrieveRsp->useconds);
    pRetrieveRsp->compLen = htonl(pRetrieveRsp->compLen);

    EXPECT_EQ(pRetrieveRsp->numOfRows, rows);
    EXPECT_EQ(pRetrieveRsp->useconds, 0);
    // EXPECT_EQ(pRetrieveRsp->completed, completed);
    EXPECT_EQ(pRetrieveRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRetrieveRsp->compressed, 0);
    EXPECT_EQ(pRetrieveRsp->compLen, 0);

    pData = pRetrieveRsp->data;
    pos = 0;
  }

  void CheckInt8(int8_t val) {
    int8_t data = *((int8_t*)(pData + pos));
    pos += sizeof(int8_t);
    EXPECT_EQ(data, val);
  }

  void CheckInt16(int16_t val) {
    int16_t data = *((int16_t*)(pData + pos));
    pos += sizeof(int16_t);
    EXPECT_EQ(data, val);
  }

  void CheckInt32(int32_t val) {
    int32_t data = *((int32_t*)(pData + pos));
    pos += sizeof(int32_t);
    EXPECT_EQ(data, val);
  }

  void CheckInt64(int64_t val) {
    int64_t data = *((int64_t*)(pData + pos));
    pos += sizeof(int64_t);
    EXPECT_EQ(data, val);
  }

  void CheckTimestamp() {
    int64_t data = *((int64_t*)(pData + pos));
    pos += sizeof(int64_t);
    EXPECT_GT(data, 0);
  }

  void CheckBinary(const char* val, int32_t len) {
    pos += sizeof(VarDataLenT);
    char* data = (char*)(pData + pos);
    pos += len;
    EXPECT_STREQ(data, val);
  }

  int32_t            showId;
  STableMetaMsg*     pMeta;
  SRetrieveTableRsp* pRetrieveRsp;
  char*              pData;
  int32_t            pos;
};

SServer* DndTestVgroup::pServer;
SClient* DndTestVgroup::pClient;
int32_t  DndTestVgroup::connId;

TEST_F(DndTestVgroup, 01_Create_Restart_Drop_Vnode) {
  {
    for (int i = 0; i < 3; ++i) {
      SCreateVnodeMsg* pReq = (SCreateVnodeMsg*)rpcMallocCont(sizeof(SCreateVnodeMsg));
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

      SRpcMsg rpcMsg = {0};
      rpcMsg.pCont = pReq;
      rpcMsg.contLen = sizeof(SCreateVnodeMsg);
      rpcMsg.msgType = TSDB_MSG_TYPE_CREATE_VNODE_IN;

      sendMsg(pClient, &rpcMsg);
      SRpcMsg* pMsg = pClient->pRsp;
      ASSERT_NE(pMsg, nullptr);
      ASSERT_EQ(pMsg->code, 0);
    }
  }

  {
    for (int i = 0; i < 3; ++i) {
      SAlterVnodeMsg* pReq = (SAlterVnodeMsg*)rpcMallocCont(sizeof(SAlterVnodeMsg));
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

      SRpcMsg rpcMsg = {0};
      rpcMsg.pCont = pReq;
      rpcMsg.contLen = sizeof(SAlterVnodeMsg);
      rpcMsg.msgType = TSDB_MSG_TYPE_ALTER_VNODE_IN;

      sendMsg(pClient, &rpcMsg);
      SRpcMsg* pMsg = pClient->pRsp;
      ASSERT_NE(pMsg, nullptr);
      ASSERT_EQ(pMsg->code, 0);
    }
  }

  {
    for (int i = 0; i < 3; ++i) {
      SDropVnodeMsg* pReq = (SDropVnodeMsg*)rpcMallocCont(sizeof(SDropVnodeMsg));
      pReq->vgId = htonl(2);
      pReq->dnodeId = htonl(1);
      strcpy(pReq->db, "1.d1");
      pReq->dbUid = htobe64(9527);

      SRpcMsg rpcMsg = {0};
      rpcMsg.pCont = pReq;
      rpcMsg.contLen = sizeof(SDropVnodeMsg);
      rpcMsg.msgType = TSDB_MSG_TYPE_DROP_VNODE_IN;

      sendMsg(pClient, &rpcMsg);
      SRpcMsg* pMsg = pClient->pRsp;
      ASSERT_NE(pMsg, nullptr);
      ASSERT_EQ(pMsg->code, 0);
    }
  }
}
