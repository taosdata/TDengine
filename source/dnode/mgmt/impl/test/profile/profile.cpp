/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "deploy.h"

class DndTestProfile : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    const char* user = "root";
    const char* pass = "taosdata";
    const char* path = "/tmp/dndTestProfile";
    const char* fqdn = "localhost";
    uint16_t    port = 9522;

    pServer = createServer(path, fqdn, port);
    ASSERT(pServer);
    pClient = createClient(user, pass, fqdn, port);
  }

  static void TearDownTestSuite() {
    dropServer(pServer);
    dropClient(pClient);
  }

  static SServer* pServer;
  static SClient* pClient;
  static int32_t  connId;
};

SServer* DndTestProfile::pServer;
SClient* DndTestProfile::pClient;
int32_t  DndTestProfile::connId;

TEST_F(DndTestProfile, SConnectMsg_01) {
  ASSERT_NE(pClient, nullptr);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "dndTestProfile");
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
  EXPECT_EQ(pRsp->superAuth, 1);
  EXPECT_EQ(pRsp->readAuth, 1);
  EXPECT_EQ(pRsp->writeAuth, 1);

  EXPECT_EQ(pRsp->epSet.inUse, 0);
  EXPECT_EQ(pRsp->epSet.numOfEps, 1);
  EXPECT_EQ(pRsp->epSet.port[0], 9522);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");

  connId = pRsp->connId;
}

TEST_F(DndTestProfile, SConnectMsg_02) {
  ASSERT_NE(pClient, nullptr);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "dndTestProfile");
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

TEST_F(DndTestProfile, SConnectMsg_03) {
  ASSERT_NE(pClient, nullptr);
  int32_t showId = 0;

  {
    SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
    pReq->type = TSDB_MGMT_TABLE_CONNS;
    strcpy(pReq->db, "");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SShowMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);

    SShowRsp* pRsp = (SShowRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->showId = htonl(pRsp->showId);
    STableMetaMsg* pMeta = &pRsp->tableMeta;
    pMeta->contLen = htonl(pMeta->contLen);
    pMeta->numOfColumns = htons(pMeta->numOfColumns);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->tversion = htons(pMeta->tversion);
    pMeta->tid = htonl(pMeta->tid);
    pMeta->uid = htobe64(pMeta->uid);
    pMeta->suid = htobe64(pMeta->suid);

    showId = pRsp->showId;

    EXPECT_NE(pRsp->showId, 0);
    EXPECT_EQ(pMeta->contLen, 0);
    EXPECT_STREQ(pMeta->tableFname, "");
    EXPECT_EQ(pMeta->numOfTags, 0);
    EXPECT_EQ(pMeta->precision, 0);
    EXPECT_EQ(pMeta->tableType, 0);
    EXPECT_EQ(pMeta->numOfColumns, 7);
    EXPECT_EQ(pMeta->sversion, 0);
    EXPECT_EQ(pMeta->tversion, 0);
    EXPECT_EQ(pMeta->tid, 0);
    EXPECT_EQ(pMeta->uid, 0);
    EXPECT_STREQ(pMeta->sTableName, "");
    EXPECT_EQ(pMeta->suid, 0);

    SSchema* pSchema = NULL;
    pSchema = &pMeta->pSchema[0];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "connId");

    pSchema = &pMeta->pSchema[1];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "user");

    pSchema = &pMeta->pSchema[2];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "program");

    pSchema = &pMeta->pSchema[3];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "pid");

    pSchema = &pMeta->pSchema[4];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "ip:port");

    pSchema = &pMeta->pSchema[5];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
    EXPECT_EQ(pSchema->bytes, 8);
    EXPECT_STREQ(pSchema->name, "login_time");

    pSchema = &pMeta->pSchema[6];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
    EXPECT_EQ(pSchema->bytes, 8);
    EXPECT_STREQ(pSchema->name, "last_access");
  }

  {
    SRetrieveTableMsg* pReq = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
    pReq->showId = htonl(showId);
    pReq->free = 0;

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SRetrieveTableMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SRetrieveTableRsp* pRsp = (SRetrieveTableRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->offset = htobe64(pRsp->offset);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->compLen = htonl(pRsp->compLen);

    EXPECT_EQ(pRsp->numOfRows, 1);
    EXPECT_EQ(pRsp->offset, 0);
    EXPECT_EQ(pRsp->useconds, 0);
    EXPECT_EQ(pRsp->completed, 1);
    EXPECT_EQ(pRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRsp->compressed, 0);
    EXPECT_EQ(pRsp->reserved, 0);
    EXPECT_EQ(pRsp->compLen, 0);
  }
}

TEST_F(DndTestProfile, SHeartBeatMsg_01) {
  ASSERT_NE(pClient, nullptr);

  SHeartBeatMsg* pReq = (SHeartBeatMsg*)rpcMallocCont(sizeof(SHeartBeatMsg));
  pReq->connId = htonl(connId);
  pReq->pid = htonl(1234);
  pReq->numOfQueries = htonl(0);
  pReq->numOfStreams = htonl(0);
  strcpy(pReq->app, "dndTestProfile");

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
  EXPECT_EQ(pRsp->epSet.port[0], 9522);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
}

TEST_F(DndTestProfile, SKillConnMsg_01) {
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
    strcpy(pReq->app, "dndTestProfile");

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
    strcpy(pReq->app, "dndTestProfile");
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
    EXPECT_EQ(pRsp->readAuth, 1);
    EXPECT_EQ(pRsp->writeAuth, 1);

    EXPECT_EQ(pRsp->epSet.inUse, 0);
    EXPECT_EQ(pRsp->epSet.numOfEps, 1);
    EXPECT_EQ(pRsp->epSet.port[0], 9522);
    EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");

    connId = pRsp->connId;
  }
}

TEST_F(DndTestProfile, SKillConnMsg_02) {
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

TEST_F(DndTestProfile, SKillQueryMsg_01) {
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
    strcpy(pReq->app, "dndTestProfile");

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
    EXPECT_EQ(pRsp->epSet.port[0], 9522);
    EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
  }
}

TEST_F(DndTestProfile, SKillQueryMsg_02) {
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

TEST_F(DndTestProfile, SKillQueryMsg_03) {
  ASSERT_NE(pClient, nullptr);
  int32_t showId = 0;

  {
    SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
    pReq->type = TSDB_MGMT_TABLE_QUERIES;
    strcpy(pReq->db, "");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SShowMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);

    SShowRsp* pRsp = (SShowRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->showId = htonl(pRsp->showId);
    STableMetaMsg* pMeta = &pRsp->tableMeta;
    pMeta->contLen = htonl(pMeta->contLen);
    pMeta->numOfColumns = htons(pMeta->numOfColumns);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->tversion = htons(pMeta->tversion);
    pMeta->tid = htonl(pMeta->tid);
    pMeta->uid = htobe64(pMeta->uid);
    pMeta->suid = htobe64(pMeta->suid);

    showId = pRsp->showId;

    EXPECT_NE(pRsp->showId, 0);
    EXPECT_EQ(pMeta->contLen, 0);
    EXPECT_STREQ(pMeta->tableFname, "");
    EXPECT_EQ(pMeta->numOfTags, 0);
    EXPECT_EQ(pMeta->precision, 0);
    EXPECT_EQ(pMeta->tableType, 0);
    EXPECT_EQ(pMeta->numOfColumns, 14);
    EXPECT_EQ(pMeta->sversion, 0);
    EXPECT_EQ(pMeta->tversion, 0);
    EXPECT_EQ(pMeta->tid, 0);
    EXPECT_EQ(pMeta->uid, 0);
    EXPECT_STREQ(pMeta->sTableName, "");
    EXPECT_EQ(pMeta->suid, 0);

    SSchema* pSchema = NULL;
    pSchema = &pMeta->pSchema[0];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "queryId");

    pSchema = &pMeta->pSchema[1];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "connId");

    pSchema = &pMeta->pSchema[2];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "user");

    pSchema = &pMeta->pSchema[3];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "ip:port");
  }

  {
    SRetrieveTableMsg* pReq = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
    pReq->showId = htonl(showId);
    pReq->free = 0;

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SRetrieveTableMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SRetrieveTableRsp* pRsp = (SRetrieveTableRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->offset = htobe64(pRsp->offset);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->compLen = htonl(pRsp->compLen);

    EXPECT_EQ(pRsp->numOfRows, 0);
    EXPECT_EQ(pRsp->offset, 0);
    EXPECT_EQ(pRsp->useconds, 0);
    EXPECT_EQ(pRsp->completed, 1);
    EXPECT_EQ(pRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRsp->compressed, 0);
    EXPECT_EQ(pRsp->reserved, 0);
    EXPECT_EQ(pRsp->compLen, 0);
  }
}

TEST_F(DndTestProfile, SKillStreamMsg_01) {
  ASSERT_NE(pClient, nullptr);

  {
    SKillStreamMsg* pReq = (SKillStreamMsg*)rpcMallocCont(sizeof(SKillStreamMsg));
    pReq->connId = htonl(connId);
    pReq->streamId = htonl(3579);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SKillStreamMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_KILL_STREAM;

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
    strcpy(pReq->app, "dndTestProfile");

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
    EXPECT_EQ(pRsp->streamId, 3579);
    EXPECT_EQ(pRsp->totalDnodes, 1);
    EXPECT_EQ(pRsp->onlineDnodes, 1);
    EXPECT_EQ(pRsp->killConnection, 0);

    EXPECT_EQ(pRsp->epSet.inUse, 0);
    EXPECT_EQ(pRsp->epSet.numOfEps, 1);
    EXPECT_EQ(pRsp->epSet.port[0], 9522);
    EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
  }
}

TEST_F(DndTestProfile, SKillStreamMsg_02) {
  ASSERT_NE(pClient, nullptr);

  SKillStreamMsg* pReq = (SKillStreamMsg*)rpcMallocCont(sizeof(SKillStreamMsg));
  pReq->connId = htonl(2345);
  pReq->streamId = htonl(1234);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SKillStreamMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_KILL_QUERY;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, TSDB_CODE_MND_INVALID_CONN_ID);
}

TEST_F(DndTestProfile, SKillStreamMsg_03) {
  ASSERT_NE(pClient, nullptr);
  int32_t showId = 0;

  {
    SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
    pReq->type = TSDB_MGMT_TABLE_STREAMS;
    strcpy(pReq->db, "");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SShowMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);

    SShowRsp* pRsp = (SShowRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->showId = htonl(pRsp->showId);
    STableMetaMsg* pMeta = &pRsp->tableMeta;
    pMeta->contLen = htonl(pMeta->contLen);
    pMeta->numOfColumns = htons(pMeta->numOfColumns);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->tversion = htons(pMeta->tversion);
    pMeta->tid = htonl(pMeta->tid);
    pMeta->uid = htobe64(pMeta->uid);
    pMeta->suid = htobe64(pMeta->suid);

    showId = pRsp->showId;

    EXPECT_NE(pRsp->showId, 0);
    EXPECT_EQ(pMeta->contLen, 0);
    EXPECT_STREQ(pMeta->tableFname, "");
    EXPECT_EQ(pMeta->numOfTags, 0);
    EXPECT_EQ(pMeta->precision, 0);
    EXPECT_EQ(pMeta->tableType, 0);
    EXPECT_EQ(pMeta->numOfColumns, 10);
    EXPECT_EQ(pMeta->sversion, 0);
    EXPECT_EQ(pMeta->tversion, 0);
    EXPECT_EQ(pMeta->tid, 0);
    EXPECT_EQ(pMeta->uid, 0);
    EXPECT_STREQ(pMeta->sTableName, "");
    EXPECT_EQ(pMeta->suid, 0);

    SSchema* pSchema = NULL;
    pSchema = &pMeta->pSchema[0];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "streamId");

    pSchema = &pMeta->pSchema[1];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "connId");

    pSchema = &pMeta->pSchema[2];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "user");
  }

  {
    SRetrieveTableMsg* pReq = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
    pReq->showId = htonl(showId);
    pReq->free = 0;

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SRetrieveTableMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SRetrieveTableRsp* pRsp = (SRetrieveTableRsp*)pMsg->pCont;
    ASSERT_NE(pRsp, nullptr);
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->offset = htobe64(pRsp->offset);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->compLen = htonl(pRsp->compLen);

    EXPECT_EQ(pRsp->numOfRows, 0);
    EXPECT_EQ(pRsp->offset, 0);
    EXPECT_EQ(pRsp->useconds, 0);
    EXPECT_EQ(pRsp->completed, 1);
    EXPECT_EQ(pRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRsp->compressed, 0);
    EXPECT_EQ(pRsp->reserved, 0);
    EXPECT_EQ(pRsp->compLen, 0);
  }
}
