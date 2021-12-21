/**
 * @file stb.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module db-msg tests
 * @version 0.1
 * @date 2021-12-17
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "deploy.h"

class DndTestStb : public ::testing::Test {
 protected:
  static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    initLog("/tmp/tdlog");

    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9101";
    pServer = CreateServer("/tmp/dnode_test_stb", fqdn, 9101, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9101);
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

SServer* DndTestStb::pServer;
SClient* DndTestStb::pClient;
int32_t  DndTestStb::connId;

TEST_F(DndTestStb, 01_Create_Show_Meta_Drop_Restart_Stb) {
  {
    SCreateDbMsg* pReq = (SCreateDbMsg*)rpcMallocCont(sizeof(SCreateDbMsg));
    strcpy(pReq->db, "1.d1");
    pReq->numOfVgroups = htonl(2);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRows = htonl(100);
    pReq->maxRows = htonl(4096);
    pReq->commitTime = htonl(3600);
    pReq->fsyncPeriod = htonl(3000);
    pReq->walLevel = 1;
    pReq->precision = 0;
    pReq->compression = 2;
    pReq->replications = 1;
    pReq->quorum = 1;
    pReq->update = 0;
    pReq->cacheLastRow = 0;
    pReq->ignoreExist = 1;

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SCreateDbMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_CREATE_DB;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  {
    int32_t cols = 2;
    int32_t tags = 3;
    int32_t size = (tags + cols) * sizeof(SSchema) + sizeof(SCreateStbMsg);

    SCreateStbMsg* pReq = (SCreateStbMsg*)rpcMallocCont(size);
    strcpy(pReq->name, "1.d1.stb");
    pReq->numOfTags = htonl(tags);
    pReq->numOfColumns = htonl(cols);

    {
      SSchema* pSchema = &pReq->pSchema[0];
      pSchema->colId = htonl(0);
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_TIMESTAMP;
      strcpy(pSchema->name, "ts");
    }

    {
      SSchema* pSchema = &pReq->pSchema[1];
      pSchema->colId = htonl(1);
      pSchema->bytes = htonl(4);
      pSchema->type = TSDB_DATA_TYPE_INT;
      strcpy(pSchema->name, "col1");
    }

    {
      SSchema* pSchema = &pReq->pSchema[2];
      pSchema->colId = htonl(2);
      pSchema->bytes = htonl(2);
      pSchema->type = TSDB_DATA_TYPE_TINYINT;
      strcpy(pSchema->name, "tag1");
    }

    {
      SSchema* pSchema = &pReq->pSchema[3];
      pSchema->colId = htonl(3);
      pSchema->bytes = htonl(8);
      pSchema->type = TSDB_DATA_TYPE_BIGINT;
      strcpy(pSchema->name, "tag2");
    }

    {
      SSchema* pSchema = &pReq->pSchema[4];
      pSchema->colId = htonl(4);
      pSchema->bytes = htonl(16);
      pSchema->type = TSDB_DATA_TYPE_BINARY;
      strcpy(pSchema->name, "tag3");
    }

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = size;
    rpcMsg.msgType = TSDB_MSG_TYPE_CREATE_STB;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    taosMsleep(100000);
  }

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_STB, "show stables", 4, "1.d1");
  CheckSchema(0, TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE, "name");
  CheckSchema(1, TSDB_DATA_TYPE_TIMESTAMP, 8, "create_time");
  CheckSchema(2, TSDB_DATA_TYPE_INT, 4, "columns");
  CheckSchema(3, TSDB_DATA_TYPE_INT, 4, "tags");

  SendThenCheckShowRetrieveMsg(1);
  CheckBinary("stb", TSDB_TABLE_NAME_LEN);
  CheckTimestamp();
  CheckInt32(2);
  CheckInt32(3);

  // ----- meta ------
  {
    STableInfoMsg* pReq = (STableInfoMsg*)rpcMallocCont(sizeof(STableInfoMsg));
    strcpy(pReq->tableFname, "1.d1.stb");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(STableInfoMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_TABLE_META;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    STableMetaMsg* pRsp = (STableMetaMsg*)pMsg->pCont;
    pRsp->numOfTags = htonl(pRsp->numOfTags);
    pRsp->numOfColumns = htonl(pRsp->numOfColumns);
    pRsp->sversion = htonl(pRsp->sversion);
    pRsp->tversion = htonl(pRsp->tversion);
    pRsp->suid = htobe64(pRsp->suid);
    pRsp->tuid = htobe64(pRsp->tuid);
    pRsp->vgId = htobe64(pRsp->vgId);
    for (int32_t i = 0; i < pRsp->numOfTags + pRsp->numOfColumns; ++i) {
      SSchema* pSchema = &pRsp->pSchema[i];
      pSchema->colId = htonl(pSchema->colId);
      pSchema->bytes = htonl(pSchema->bytes);
    }

    EXPECT_STREQ(pRsp->tbFname, "");
    EXPECT_STREQ(pRsp->stbFname, "1.d1.stb");
    EXPECT_EQ(pRsp->numOfColumns, 2);
    EXPECT_EQ(pRsp->numOfTags, 3);
    EXPECT_EQ(pRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRsp->tableType, TSDB_SUPER_TABLE);
    EXPECT_EQ(pRsp->update, 0);
    EXPECT_EQ(pRsp->sversion, 1);
    EXPECT_EQ(pRsp->tversion, 0);
    EXPECT_GT(pRsp->suid, 0);
    EXPECT_EQ(pRsp->tuid, 0);
    EXPECT_EQ(pRsp->vgId, 0);

    {
      SSchema* pSchema = &pRsp->pSchema[0];
      EXPECT_EQ(pSchema->type, TSDB_DATA_TYPE_TIMESTAMP);
      EXPECT_EQ(pSchema->colId, 0);
      EXPECT_EQ(pSchema->bytes, 8);
      EXPECT_STREQ(pSchema->name, "ts");
    }
  }

  // restart
  stopServer(pServer);
  pServer = NULL;

  uInfo("start all server");

  const char* fqdn = "localhost";
  const char* firstEp = "localhost:9101";
  pServer = startServer("/tmp/dnode_test_stb", fqdn, 9101, firstEp);

  uInfo("all server is running");

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_STB, "show stables", 4, "1.d1");
  SendThenCheckShowRetrieveMsg(1);
  CheckBinary("stb", TSDB_TABLE_NAME_LEN);
  CheckTimestamp();
  CheckInt32(2);
  CheckInt32(3);

  {
    SDropStbMsg* pReq = (SDropStbMsg*)rpcMallocCont(sizeof(SDropStbMsg));
    strcpy(pReq->name, "1.d1.stb");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SDropStbMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_DROP_STB;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_STB, "show stables", 4, "1.d1");
  SendThenCheckShowRetrieveMsg(0);
}
