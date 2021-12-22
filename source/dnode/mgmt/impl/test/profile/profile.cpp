/**
 * @file profile.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module profile-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "deploy.h"

class DndTestProfile : public ::testing::Test {
 protected:
  static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    initLog("/tmp/tdlog");

    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9080";
    pServer = CreateServer("/tmp/dnode_test_profile", fqdn, 9080, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9080);
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

SServer* DndTestProfile::pServer;
SClient* DndTestProfile::pClient;
int32_t  DndTestProfile::connId;

TEST_F(DndTestProfile, 01_ConnectMsg) {
  ASSERT_NE(pClient, nullptr);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "dnode_test_profile");
  strcpy(pReq->db, "");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SConnectMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_CONNECT;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);

  SConnectRsp* pRsp = (SConnectRsp*)pMsg->pCont;
  ASSERT_NE(pRsp, nullptr);
  pRsp->acctId = htonl(pRsp->acctId);
  pRsp->clusterId = htonl(pRsp->clusterId);
  pRsp->connId = htonl(pRsp->connId);
  pRsp->epSet.port[0] = htons(pRsp->epSet.port[0]);

  EXPECT_EQ(pRsp->acctId, 1);
  EXPECT_GT(pRsp->clusterId, 0);
  EXPECT_EQ(pRsp->connId, 1);
  EXPECT_EQ(pRsp->superUser, 1);

  EXPECT_EQ(pRsp->epSet.inUse, 0);
  EXPECT_EQ(pRsp->epSet.numOfEps, 1);
  EXPECT_EQ(pRsp->epSet.port[0], 9080);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");

  connId = pRsp->connId;
}

TEST_F(DndTestProfile, 02_ConnectMsg_InvalidDB) {
  ASSERT_NE(pClient, nullptr);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "dnode_test_profile");
  strcpy(pReq->db, "invalid_db");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SConnectMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_CONNECT;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_DB);
  ASSERT_EQ(pMsg->contLen, 0);
}

TEST_F(DndTestProfile, 03_ConnectMsg_Show) {
  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_CONNS, "show connections", 7);
  CheckSchema(0, TSDB_DATA_TYPE_INT, 4, "connId");
  CheckSchema(1, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "user");
  CheckSchema(2, TSDB_DATA_TYPE_BINARY, TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, "program");
  CheckSchema(3, TSDB_DATA_TYPE_INT, 4, "pid");
  CheckSchema(4, TSDB_DATA_TYPE_BINARY, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, "ip:port");
  CheckSchema(5, TSDB_DATA_TYPE_TIMESTAMP, 8, "login_time");
  CheckSchema(6, TSDB_DATA_TYPE_TIMESTAMP, 8, "last_access");

  SendThenCheckShowRetrieveMsg(1);

  CheckInt32(1);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("dnode_test_profile", TSDB_APP_NAME_LEN);
  CheckInt32(1234);
  IgnoreBinary(TSDB_IPv4ADDR_LEN + 6);
  CheckTimestamp();
  CheckTimestamp();
}

TEST_F(DndTestProfile, 04_HeartBeatMsg) {
  ASSERT_NE(pClient, nullptr);

  SHeartBeatMsg* pReq = (SHeartBeatMsg*)rpcMallocCont(sizeof(SHeartBeatMsg));
  pReq->connId = htonl(connId);
  pReq->pid = htonl(1234);
  pReq->numOfQueries = htonl(0);
  pReq->numOfStreams = htonl(0);
  strcpy(pReq->app, "dnode_test_profile");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SHeartBeatMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_HEARTBEAT;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);

  SHeartBeatRsp* pRsp = (SHeartBeatRsp*)pMsg->pCont;
  ASSERT_NE(pRsp, nullptr);
  pRsp->connId = htonl(pRsp->connId);
  pRsp->queryId = htonl(pRsp->queryId);
  pRsp->streamId = htonl(pRsp->streamId);
  pRsp->totalDnodes = htonl(pRsp->totalDnodes);
  pRsp->onlineDnodes = htonl(pRsp->onlineDnodes);
  pRsp->epSet.port[0] = htons(pRsp->epSet.port[0]);

  EXPECT_EQ(pRsp->connId, connId);
  EXPECT_EQ(pRsp->queryId, 0);
  EXPECT_EQ(pRsp->streamId, 0);
  EXPECT_EQ(pRsp->totalDnodes, 1);
  EXPECT_EQ(pRsp->onlineDnodes, 1);
  EXPECT_EQ(pRsp->killConnection, 0);

  EXPECT_EQ(pRsp->epSet.inUse, 0);
  EXPECT_EQ(pRsp->epSet.numOfEps, 1);
  EXPECT_EQ(pRsp->epSet.port[0], 9080);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
}

TEST_F(DndTestProfile, 05_KillConnMsg) {
  ASSERT_NE(pClient, nullptr);

  {
    SKillConnMsg* pReq = (SKillConnMsg*)rpcMallocCont(sizeof(SKillConnMsg));
    pReq->connId = htonl(connId);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SKillConnMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_KILL_CONN;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  {
    SHeartBeatMsg* pReq = (SHeartBeatMsg*)rpcMallocCont(sizeof(SHeartBeatMsg));
    pReq->connId = htonl(connId);
    pReq->pid = htonl(1234);
    pReq->numOfQueries = htonl(0);
    pReq->numOfStreams = htonl(0);
    strcpy(pReq->app, "dnode_test_profile");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SHeartBeatMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_HEARTBEAT;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_CONNECTION);
    ASSERT_EQ(pMsg->contLen, 0);
  }

  {
    SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
    pReq->pid = htonl(1234);
    strcpy(pReq->app, "dnode_test_profile");
    strcpy(pReq->db, "");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SConnectMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_CONNECT;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);

    SConnectRsp* pRsp = (SConnectRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->acctId = htonl(pRsp->acctId);
    pRsp->clusterId = htonl(pRsp->clusterId);
    pRsp->connId = htonl(pRsp->connId);
    pRsp->epSet.port[0] = htons(pRsp->epSet.port[0]);

    EXPECT_EQ(pRsp->acctId, 1);
    EXPECT_GT(pRsp->clusterId, 0);
    EXPECT_GT(pRsp->connId, connId);
    EXPECT_EQ(pRsp->superUser, 1);

    EXPECT_EQ(pRsp->epSet.inUse, 0);
    EXPECT_EQ(pRsp->epSet.numOfEps, 1);
    EXPECT_EQ(pRsp->epSet.port[0], 9080);
    EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");

    connId = pRsp->connId;
  }
}

TEST_F(DndTestProfile, 06_KillConnMsg_InvalidConn) {
  ASSERT_NE(pClient, nullptr);

  SKillConnMsg* pReq = (SKillConnMsg*)rpcMallocCont(sizeof(SKillConnMsg));
  pReq->connId = htonl(2345);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SKillConnMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_KILL_CONN;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_CONN_ID);
}

TEST_F(DndTestProfile, 07_KillQueryMsg) {
  ASSERT_NE(pClient, nullptr);

  {
    SKillQueryMsg* pReq = (SKillQueryMsg*)rpcMallocCont(sizeof(SKillQueryMsg));
    pReq->connId = htonl(connId);
    pReq->queryId = htonl(1234);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SKillQueryMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_KILL_QUERY;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
    ASSERT_EQ(pMsg->contLen, 0);
  }

  {
    SHeartBeatMsg* pReq = (SHeartBeatMsg*)rpcMallocCont(sizeof(SHeartBeatMsg));
    pReq->connId = htonl(connId);
    pReq->pid = htonl(1234);
    pReq->numOfQueries = htonl(0);
    pReq->numOfStreams = htonl(0);
    strcpy(pReq->app, "dnode_test_profile");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SHeartBeatMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_HEARTBEAT;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);

    SHeartBeatRsp* pRsp = (SHeartBeatRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->connId = htonl(pRsp->connId);
    pRsp->queryId = htonl(pRsp->queryId);
    pRsp->streamId = htonl(pRsp->streamId);
    pRsp->totalDnodes = htonl(pRsp->totalDnodes);
    pRsp->onlineDnodes = htonl(pRsp->onlineDnodes);
    pRsp->epSet.port[0] = htons(pRsp->epSet.port[0]);

    EXPECT_EQ(pRsp->connId, connId);
    EXPECT_EQ(pRsp->queryId, 1234);
    EXPECT_EQ(pRsp->streamId, 0);
    EXPECT_EQ(pRsp->totalDnodes, 1);
    EXPECT_EQ(pRsp->onlineDnodes, 1);
    EXPECT_EQ(pRsp->killConnection, 0);

    EXPECT_EQ(pRsp->epSet.inUse, 0);
    EXPECT_EQ(pRsp->epSet.numOfEps, 1);
    EXPECT_EQ(pRsp->epSet.port[0], 9080);
    EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
  }
}

TEST_F(DndTestProfile, 08_KillQueryMsg_InvalidConn) {
  ASSERT_NE(pClient, nullptr);

  SKillQueryMsg* pReq = (SKillQueryMsg*)rpcMallocCont(sizeof(SKillQueryMsg));
  pReq->connId = htonl(2345);
  pReq->queryId = htonl(1234);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SKillQueryMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_KILL_QUERY;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_CONN_ID);
}

TEST_F(DndTestProfile, 09_KillQueryMsg) {
  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_QUERIES, "show queries", 14);
  CheckSchema(0, TSDB_DATA_TYPE_INT, 4, "queryId");
  CheckSchema(1, TSDB_DATA_TYPE_INT, 4, "connId");
  CheckSchema(2, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "user");
  CheckSchema(3, TSDB_DATA_TYPE_BINARY, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, "ip:port");
  CheckSchema(4, TSDB_DATA_TYPE_BINARY, 22 + VARSTR_HEADER_SIZE, "qid");
  CheckSchema(5, TSDB_DATA_TYPE_TIMESTAMP, 8, "created_time");
  CheckSchema(6, TSDB_DATA_TYPE_BIGINT, 8, "time");
  CheckSchema(7, TSDB_DATA_TYPE_BINARY, 18 + VARSTR_HEADER_SIZE, "sql_obj_id");
  CheckSchema(8, TSDB_DATA_TYPE_INT, 4, "pid");
  CheckSchema(9, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "ep");
  CheckSchema(10, TSDB_DATA_TYPE_BOOL, 1, "stable_query");
  CheckSchema(11, TSDB_DATA_TYPE_INT, 4, "sub_queries");
  CheckSchema(12, TSDB_DATA_TYPE_BINARY, TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE, "sub_query_info");
  CheckSchema(13, TSDB_DATA_TYPE_BINARY, TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, "sql");

  SendThenCheckShowRetrieveMsg(0);
}
