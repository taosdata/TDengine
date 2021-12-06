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

class DndTestShow : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    const char* user = "root";
    const char* pass = "taosdata";
    const char* path = "/tmp/dndTestShow";
    const char* fqdn = "localhost";
    uint16_t    port = 9523;

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

SServer* DndTestShow::pServer;
SClient* DndTestShow::pClient;
int32_t  DndTestShow::connId;

TEST_F(DndTestShow, SShowMsg_01) {
  ASSERT_NE(pClient, nullptr);

  SConnectMsg* pReq = (SConnectMsg*)rpcMallocCont(sizeof(SConnectMsg));
  pReq->pid = htonl(1234);
  strcpy(pReq->app, "dndTestShow");
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
  pRsp->connId = htonl(pRsp->connId);

  EXPECT_EQ(pRsp->connId, 1);
  connId = pRsp->connId;
}

TEST_F(DndTestShow, SShowMsg_02) {
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

TEST_F(DndTestShow, SShowMsg_03) {
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

TEST_F(DndTestShow, SShowMsg_04) {
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
    pSchema = &pMeta->schema[0];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "connId");

    pSchema = &pMeta->schema[1];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "user");

    pSchema = &pMeta->schema[2];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "program");

    pSchema = &pMeta->schema[3];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_INT);
    EXPECT_EQ(pSchema->bytes, 4);
    EXPECT_STREQ(pSchema->name, "pid");

    pSchema = &pMeta->schema[4];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "ip:port");

    pSchema = &pMeta->schema[5];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
    EXPECT_EQ(pSchema->bytes, 8);
    EXPECT_STREQ(pSchema->name, "login_time");

    pSchema = &pMeta->schema[6];
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
