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

class DndTestDnode : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9521";
    pServer1 = CreateServer("/tmp/dndTestDnode1", fqdn, 9521, firstEp);
    pServer2 = CreateServer("/tmp/dndTestDnode2", fqdn, 9522, firstEp);
    pServer3 = CreateServer("/tmp/dndTestDnode3", fqdn, 9523, firstEp);
    pServer4 = CreateServer("/tmp/dndTestDnode4", fqdn, 9524, firstEp);
    pServer5 = CreateServer("/tmp/dndTestDnode5", fqdn, 9525, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9521);
  }

  static void TearDownTestSuite() {
    dropServer(pServer1);
    dropServer(pServer2);
    dropServer(pServer3);
    dropServer(pServer4);
    dropServer(pServer5);
    dropClient(pClient);
  }

  static SServer* pServer1;
  static SServer* pServer2;
  static SServer* pServer3;
  static SServer* pServer4;
  static SServer* pServer5;
  static SClient* pClient;

  void CheckShowMsg(int8_t msgType) {

  }
};

SServer* DndTestDnode::pServer1;
SServer* DndTestDnode::pServer2;
SServer* DndTestDnode::pServer3;
SServer* DndTestDnode::pServer4;
SServer* DndTestDnode::pServer5;
SClient* DndTestDnode::pClient;

TEST_F(DndTestDnode, ShowDnode) {
  int32_t showId = 0;

  //--- meta ---
  SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pShow->type = TSDB_MGMT_TABLE_DNODE;
  strcpy(pShow->db, "");

  SRpcMsg showRpcMsg = {0};
  showRpcMsg.pCont = pShow;
  showRpcMsg.contLen = sizeof(SShowMsg);
  showRpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &showRpcMsg);
  ASSERT_NE(pClient->pRsp, nullptr);

  SShowRsp* pShowRsp = (SShowRsp*)pClient->pRsp->pCont;
  ASSERT_NE(pShowRsp, nullptr);
  pShowRsp->showId = htonl(pShowRsp->showId);
  STableMetaMsg* pMeta = &pShowRsp->tableMeta;
  pMeta->numOfTags = htons(pMeta->numOfTags);
  pMeta->numOfColumns = htons(pMeta->numOfColumns);
  pMeta->sversion = htons(pMeta->sversion);
  pMeta->tversion = htons(pMeta->tversion);
  pMeta->tuid = htobe64(pMeta->tuid);
  pMeta->suid = htobe64(pMeta->suid);

  showId = pShowRsp->showId;

  EXPECT_NE(pShowRsp->showId, 0);
  EXPECT_STREQ(pMeta->tbFname, "show dnodes");
  EXPECT_EQ(pMeta->numOfTags, 0);
  EXPECT_EQ(pMeta->numOfColumns, 7);
  EXPECT_EQ(pMeta->precision, 0);
  EXPECT_EQ(pMeta->tableType, 0);
  EXPECT_EQ(pMeta->update, 0);
  EXPECT_EQ(pMeta->sversion, 0);
  EXPECT_EQ(pMeta->tversion, 0);
  EXPECT_EQ(pMeta->tuid, 0);
  EXPECT_EQ(pMeta->suid, 0);
  
  SSchema* pSchema = NULL;

  pSchema = &pMeta->pSchema[0];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_SMALLINT);
  EXPECT_EQ(pSchema->bytes, 2);
  EXPECT_STREQ(pSchema->name, "id");

  pSchema = &pMeta->pSchema[1];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(pSchema->bytes, TSDB_EP_LEN + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(pSchema->name, "end point");

  pSchema = &pMeta->pSchema[2];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_SMALLINT);
  EXPECT_EQ(pSchema->bytes, 2);
  EXPECT_STREQ(pSchema->name, "vnodes");

  pSchema = &pMeta->pSchema[3];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_SMALLINT);
  EXPECT_EQ(pSchema->bytes, 2);
  EXPECT_STREQ(pSchema->name, "max vnodes");

  pSchema = &pMeta->pSchema[4];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(pSchema->bytes, 10 + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(pSchema->name, "status");

  pSchema = &pMeta->pSchema[5];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_EQ(pSchema->bytes, 8);
  EXPECT_STREQ(pSchema->name, "create time");

  pSchema = &pMeta->pSchema[6];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(pSchema->name, "offline reason");

  //--- retrieve ---
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

  SRetrieveTableRsp* pRetrieveRsp = (SRetrieveTableRsp*)pClient->pRsp->pCont;
  ASSERT_NE(pRetrieveRsp, nullptr);
  pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
  pRetrieveRsp->offset = htobe64(pRetrieveRsp->offset);
  pRetrieveRsp->useconds = htobe64(pRetrieveRsp->useconds);
  pRetrieveRsp->compLen = htonl(pRetrieveRsp->compLen);

  EXPECT_EQ(pRetrieveRsp->numOfRows, 1);
  EXPECT_EQ(pRetrieveRsp->offset, 0);
  EXPECT_EQ(pRetrieveRsp->useconds, 0);
  EXPECT_EQ(pRetrieveRsp->completed, 1);
  EXPECT_EQ(pRetrieveRsp->precision, TSDB_TIME_PRECISION_MILLI);
  EXPECT_EQ(pRetrieveRsp->compressed, 0);
  EXPECT_EQ(pRetrieveRsp->reserved, 0);
  EXPECT_EQ(pRetrieveRsp->compLen, 0);

  char*   pData = pRetrieveRsp->data;
  int32_t pos = 0;
  char*   strVal = NULL;
  int64_t int64Val = 0;
  int16_t int16Val = 0;

  //--- name ---
  {
    int16Val = *((int16_t*)(pData + pos));
    pos += sizeof(int16_t);
    EXPECT_EQ(int16Val, 1);
  }

  // //--- privilege ---
  // {
  //   pos += sizeof(VarDataLenT);
  //   strVal = (char*)(pData + pos);
  //   pos += 10;
  //   EXPECT_STREQ(strVal, "super");

  //   pos += sizeof(VarDataLenT);
  //   strVal = (char*)(pData + pos);
  //   pos += 10;
  //   EXPECT_STREQ(strVal, "writable");
  // }

  // //--- create_time ---
  // {
  //   int64Val = *((int64_t*)(pData + pos));
  //   pos += sizeof(int64_t);
  //   EXPECT_GT(int64Val, 0);

  //   int64Val = *((int64_t*)(pData + pos));
  //   pos += sizeof(int64_t);
  //   EXPECT_GT(int64Val, 0);
  // }

  // //--- account ---
  // {
  //   pos += sizeof(VarDataLenT);
  //   strVal = (char*)(pData + pos);
  //   pos += TSDB_USER_LEN;
  //   EXPECT_STREQ(strVal, "root");

  //   pos += sizeof(VarDataLenT);
  //   strVal = (char*)(pData + pos);
  //   pos += TSDB_USER_LEN;
  //   EXPECT_STREQ(strVal, "root");
  // }
}

#if 0
TEST_F(DndTestDnode, CreateUser_01) {
  ASSERT_NE(pClient, nullptr);

  //--- create user ---
  SCreateUserMsg* pReq = (SCreateUserMsg*)rpcMallocCont(sizeof(SCreateUserMsg));
  strcpy(pReq->user, "u1");
  strcpy(pReq->pass, "p1");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SCreateUserMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_CREATE_USER;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

  //--- meta ---
  SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pShow->type = TSDB_MGMT_TABLE_USER;
  SRpcMsg showRpcMsg = {0};
  showRpcMsg.pCont = pShow;
  showRpcMsg.contLen = sizeof(SShowMsg);
  showRpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &showRpcMsg);
  SShowRsp*      pShowRsp = (SShowRsp*)pClient->pRsp->pCont;
  STableMetaMsg* pMeta = &pShowRsp->tableMeta;
  pMeta->numOfColumns = htons(pMeta->numOfColumns);
  EXPECT_EQ(pMeta->numOfColumns, 4);

  //--- retrieve ---
  SRetrieveTableMsg* pRetrieve = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
  pRetrieve->showId = pShowRsp->showId;
  SRpcMsg retrieveRpcMsg = {0};
  retrieveRpcMsg.pCont = pRetrieve;
  retrieveRpcMsg.contLen = sizeof(SRetrieveTableMsg);
  retrieveRpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

  sendMsg(pClient, &retrieveRpcMsg);
  SRetrieveTableRsp* pRetrieveRsp = (SRetrieveTableRsp*)pClient->pRsp->pCont;
  pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
  EXPECT_EQ(pRetrieveRsp->numOfRows, 3);

  char*   pData = pRetrieveRsp->data;
  int32_t pos = 0;
  char*   strVal = NULL;

  //--- name ---
  {
    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "u1");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "root");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "_root");
  }
}

TEST_F(DndTestDnode, AlterUser_01) {
  ASSERT_NE(pClient, nullptr);

  //--- drop user ---
  SAlterUserMsg* pReq = (SAlterUserMsg*)rpcMallocCont(sizeof(SAlterUserMsg));
  strcpy(pReq->user, "u1");
  strcpy(pReq->pass, "p2");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SAlterUserMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_ALTER_USER;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

  //--- meta ---
  SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pShow->type = TSDB_MGMT_TABLE_USER;
  SRpcMsg showRpcMsg = {0};
  showRpcMsg.pCont = pShow;
  showRpcMsg.contLen = sizeof(SShowMsg);
  showRpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &showRpcMsg);
  SShowRsp*      pShowRsp = (SShowRsp*)pClient->pRsp->pCont;
  STableMetaMsg* pMeta = &pShowRsp->tableMeta;
  pMeta->numOfColumns = htons(pMeta->numOfColumns);
  EXPECT_EQ(pMeta->numOfColumns, 4);

  //--- retrieve ---
  SRetrieveTableMsg* pRetrieve = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
  pRetrieve->showId = pShowRsp->showId;
  SRpcMsg retrieveRpcMsg = {0};
  retrieveRpcMsg.pCont = pRetrieve;
  retrieveRpcMsg.contLen = sizeof(SRetrieveTableMsg);
  retrieveRpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

  sendMsg(pClient, &retrieveRpcMsg);
  SRetrieveTableRsp* pRetrieveRsp = (SRetrieveTableRsp*)pClient->pRsp->pCont;
  pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
  EXPECT_EQ(pRetrieveRsp->numOfRows, 3);

  char*   pData = pRetrieveRsp->data;
  int32_t pos = 0;
  char*   strVal = NULL;

  //--- name ---
  {
    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "u1");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "root");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "_root");
  }
}

TEST_F(DndTestDnode, DropUser_01) {
  ASSERT_NE(pClient, nullptr);

  //--- drop user ---
  SDropUserMsg* pReq = (SDropUserMsg*)rpcMallocCont(sizeof(SDropUserMsg));
  strcpy(pReq->user, "u1");

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pReq;
  rpcMsg.contLen = sizeof(SDropUserMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_DROP_USER;

  sendMsg(pClient, &rpcMsg);
  SRpcMsg* pMsg = pClient->pRsp;
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

  //--- meta ---
  SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pShow->type = TSDB_MGMT_TABLE_USER;
  SRpcMsg showRpcMsg = {0};
  showRpcMsg.pCont = pShow;
  showRpcMsg.contLen = sizeof(SShowMsg);
  showRpcMsg.msgType = TSDB_MSG_TYPE_SHOW;

  sendMsg(pClient, &showRpcMsg);
  SShowRsp*      pShowRsp = (SShowRsp*)pClient->pRsp->pCont;
  STableMetaMsg* pMeta = &pShowRsp->tableMeta;
  pMeta->numOfColumns = htons(pMeta->numOfColumns);
  EXPECT_EQ(pMeta->numOfColumns, 4);

  //--- retrieve ---
  SRetrieveTableMsg* pRetrieve = (SRetrieveTableMsg*)rpcMallocCont(sizeof(SRetrieveTableMsg));
  pRetrieve->showId = pShowRsp->showId;
  SRpcMsg retrieveRpcMsg = {0};
  retrieveRpcMsg.pCont = pRetrieve;
  retrieveRpcMsg.contLen = sizeof(SRetrieveTableMsg);
  retrieveRpcMsg.msgType = TSDB_MSG_TYPE_SHOW_RETRIEVE;

  sendMsg(pClient, &retrieveRpcMsg);
  SRetrieveTableRsp* pRetrieveRsp = (SRetrieveTableRsp*)pClient->pRsp->pCont;
  pRetrieveRsp->numOfRows = htonl(pRetrieveRsp->numOfRows);
  EXPECT_EQ(pRetrieveRsp->numOfRows, 2);

  char*   pData = pRetrieveRsp->data;
  int32_t pos = 0;
  char*   strVal = NULL;

  //--- name ---
  {
    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "root");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "_root");
  }
}

#endif