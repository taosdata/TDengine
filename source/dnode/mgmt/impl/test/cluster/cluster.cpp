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

class DndTestCluster : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    const char* user = "root";
    const char* pass = "taosdata";
    const char* path = "/tmp/dndTestCluster";
    const char* fqdn = "localhost";
    uint16_t    port = 9521;

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

SServer* DndTestCluster::pServer;
SClient* DndTestCluster::pClient;
int32_t  DndTestCluster::connId;

TEST_F(DndTestCluster, ShowCluster) {
  ASSERT_NE(pClient, nullptr);
  int32_t showId = 0;

  {
    SShowMsg* pReq = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
    pReq->type = TSDB_MGMT_TABLE_CLUSTER;
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
    EXPECT_STREQ(pMeta->tbFname, "show cluster");
    EXPECT_EQ(pMeta->numOfTags, 0);
    EXPECT_EQ(pMeta->precision, 0);
    EXPECT_EQ(pMeta->tableType, 0);
    EXPECT_EQ(pMeta->numOfColumns, 3);
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
    EXPECT_STREQ(pSchema->name, "id");

    pSchema = &pMeta->pSchema[1];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
    EXPECT_EQ(pSchema->bytes, TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE);
    EXPECT_STREQ(pSchema->name, "name");

    pSchema = &pMeta->pSchema[2];
    pSchema->bytes = htons(pSchema->bytes);
    EXPECT_EQ(pSchema->colId, 0);
    EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
    EXPECT_EQ(pSchema->bytes, 8);
    EXPECT_STREQ(pSchema->name, "create_time");
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

    char*   pData = pRsp->data;
    int32_t pos = 0;

    int32_t id = *((int32_t*)(pData + pos));
    pos += sizeof(int32_t);

    int32_t nameLen = varDataLen(pData + pos);
    pos += sizeof(VarDataLenT);

    char* name = (char*)(pData + pos);
    pos += TSDB_CLUSTER_ID_LEN;

    int64_t create_time = *((int64_t*)(pData + pos));
    pos += sizeof(int64_t);

    EXPECT_NE(id, 0);
    EXPECT_EQ(nameLen, 36);
    EXPECT_STRNE(name, "");
    EXPECT_GT(create_time, 0);
    printf("--- id:%d nameLen:%d name:%s time:%" PRId64 " --- \n", id, nameLen, name, create_time);
  }
}