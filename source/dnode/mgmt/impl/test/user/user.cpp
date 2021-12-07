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

class DndTestUser : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    const char* user = "root";
    const char* pass = "taosdata";
    const char* path = "/tmp/dndTestUser";
    const char* fqdn = "localhost";
    uint16_t    port = 9524;

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

SServer* DndTestUser::pServer;
SClient* DndTestUser::pClient;
int32_t  DndTestUser::connId;

#if 0
TEST_F(DndTestUser, ShowUser) {
  int32_t showId = 0;

  //--- meta ---
  SShowMsg* pShow = (SShowMsg*)rpcMallocCont(sizeof(SShowMsg));
  pShow->type = TSDB_MGMT_TABLE_USER;
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
  pMeta->contLen = htonl(pMeta->contLen);
  pMeta->numOfColumns = htons(pMeta->numOfColumns);
  pMeta->sversion = htons(pMeta->sversion);
  pMeta->tversion = htons(pMeta->tversion);
  pMeta->tid = htonl(pMeta->tid);
  pMeta->uid = htobe64(pMeta->uid);
  pMeta->suid = htobe64(pMeta->suid);

  showId = pShowRsp->showId;

  EXPECT_NE(pShowRsp->showId, 0);
  EXPECT_EQ(pMeta->contLen, 0);
  EXPECT_STREQ(pMeta->tableFname, "show users");
  EXPECT_EQ(pMeta->numOfTags, 0);
  EXPECT_EQ(pMeta->precision, 0);
  EXPECT_EQ(pMeta->tableType, 0);
  EXPECT_EQ(pMeta->numOfColumns, 4);
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
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(pSchema->name, "name");

  pSchema = &pMeta->schema[1];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(pSchema->bytes, 10 + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(pSchema->name, "privilege");

  pSchema = &pMeta->schema[2];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_EQ(pSchema->bytes, 8);
  EXPECT_STREQ(pSchema->name, "create_time");

  pSchema = &pMeta->schema[3];
  pSchema->bytes = htons(pSchema->bytes);
  EXPECT_EQ(pSchema->colId, 0);
  EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(pSchema->bytes, TSDB_USER_LEN + VARSTR_HEADER_SIZE);
  EXPECT_STREQ(pSchema->name, "account");

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

  EXPECT_EQ(pRetrieveRsp->numOfRows, 2);
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

  //--- privilege ---
  {
    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += 10;
    EXPECT_STREQ(strVal, "super");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += 10;
    EXPECT_STREQ(strVal, "writable");
  }

  //--- create_time ---
  {
    int64Val = *((int64_t*)(pData + pos));
    pos += sizeof(int64_t);
    EXPECT_GT(int64Val, 0);

    int64Val = *((int64_t*)(pData + pos));
    pos += sizeof(int64_t);
    EXPECT_GT(int64Val, 0);
  }

  //--- account ---
  {
    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "root");

    pos += sizeof(VarDataLenT);
    strVal = (char*)(pData + pos);
    pos += TSDB_USER_LEN;
    EXPECT_STREQ(strVal, "root");
  }
}
#endif

TEST_F(DndTestUser, CreateUser_01) {
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
  // taosMsleep(10000000);
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

// TEST_F(DndTestUser, AlterUser) {
//   ASSERT_NE(pClient, nullptr);

//   SAlterUserMsg* pReq = (SAlterUserMsg*)rpcMallocCont(sizeof(SAlterUserMsg));

//   SRpcMsg rpcMsg = {0};
//   rpcMsg.pCont = pReq;
//   rpcMsg.contLen = sizeof(SAlterUserMsg);
//   rpcMsg.msgType = TSDB_MSG_TYPE_ALTER_ACCT;

//   sendMsg(pClient, &rpcMsg);
//   SRpcMsg* pMsg = pClient->pRsp;
//   ASSERT_NE(pMsg, nullptr);
//   ASSERT_EQ(pMsg->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
// }

// TEST_F(DndTestUser, DropUser) {
//   ASSERT_NE(pClient, nullptr);

//   SDropUserMsg* pReq = (SDropUserMsg*)rpcMallocCont(sizeof(SDropUserMsg));

//   SRpcMsg rpcMsg = {0};
//   rpcMsg.pCont = pReq;
//   rpcMsg.contLen = sizeof(SDropUserMsg);
//   rpcMsg.msgType = TSDB_MSG_TYPE_DROP_ACCT;

//   sendMsg(pClient, &rpcMsg);
//   SRpcMsg* pMsg = pClient->pRsp;
//   ASSERT_NE(pMsg, nullptr);
//   ASSERT_EQ(pMsg->code, TSDB_CODE_MND_MSG_NOT_PROCESSED);
// }
