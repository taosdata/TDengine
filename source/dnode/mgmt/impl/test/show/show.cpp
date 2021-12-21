/**
 * @file show.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module show-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "deploy.h"

class DndTestShow : public ::testing::Test {
 protected:
  static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    initLog("/tmp/tdlog");

    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9091";
    pServer = CreateServer("/tmp/dnode_test_show", fqdn, 9091, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9091);
    taosMsleep(300);
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

  void SendTheCheckShowMetaMsg(int8_t showType, const char* showName, int32_t columns) {
    SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
    pShow->type = showType;
    strcpy(pShow->db, "");

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

  void IgnoreBinary(int32_t len) {
    pos += sizeof(VarDataLenT);
    char* data = (char*)(pData + pos);
    pos += len;
  }

  int32_t            showId;
  STableMetaMsg*     pMeta;
  SRetrieveTableRsp* pRetrieveRsp;
  char*              pData;
  int32_t            pos;
};

SServer* DndTestShow::pServer;
SClient* DndTestShow::pClient;
int32_t  DndTestShow::connId;

TEST_F(DndTestShow, 01_ShowMsg_InvalidMsgMax) {
  ASSERT_NE(pClient, nullptr);

  SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pReq->type = TSDB_MGMT_TABLE_MAX;
  strcpy(pReq->db, "");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SShowMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}

TEST_F(DndTestShow, 02_ShowMsg_InvalidMsgStart) {
  ASSERT_NE(pClient, nullptr);

  SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pReq->type = TSDB_MGMT_TABLE_START;
  strcpy(pReq->db, "");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SShowMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_MSG_TYPE);
}
