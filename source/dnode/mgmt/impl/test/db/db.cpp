/**
 * @file vnodeApiTests.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief DNODE module db-msg tests
 * @version 0.1
 * @date 2021-12-15
 *
 * @copyright Copyright (c) 2021
 *
 */

#include "deploy.h"

class DndTestDb : public ::testing::Test {
 protected:
  static SServer* CreateServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
    SServer* pServer = createServer(path, fqdn, port, firstEp);
    ASSERT(pServer);
    return pServer;
  }

  static void SetUpTestSuite() {
    initLog("/tmp/tdlog");

    const char* fqdn = "localhost";
    const char* firstEp = "localhost:9040";
    pServer = CreateServer("/tmp/dnode_test_db", fqdn, 9040, firstEp);
    pClient = createClient("root", "taosdata", fqdn, 9040);
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
    pMeta->numOfTags = htons(pMeta->numOfTags);
    pMeta->numOfColumns = htons(pMeta->numOfColumns);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->tversion = htons(pMeta->tversion);
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
    pSchema->bytes = htons(pSchema->bytes);
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
    pRetrieveRsp->offset = htobe64(pRetrieveRsp->offset);
    pRetrieveRsp->useconds = htobe64(pRetrieveRsp->useconds);
    pRetrieveRsp->compLen = htonl(pRetrieveRsp->compLen);

    EXPECT_EQ(pRetrieveRsp->numOfRows, rows);
    EXPECT_EQ(pRetrieveRsp->offset, 0);
    EXPECT_EQ(pRetrieveRsp->useconds, 0);
    // EXPECT_EQ(pRetrieveRsp->completed, completed);
    EXPECT_EQ(pRetrieveRsp->precision, TSDB_TIME_PRECISION_MILLI);
    EXPECT_EQ(pRetrieveRsp->compressed, 0);
    EXPECT_EQ(pRetrieveRsp->reserved, 0);
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

SServer* DndTestDb::pServer;
SClient* DndTestDb::pClient;
int32_t  DndTestDb::connId;

TEST_F(DndTestDb, 01_ShowDb) {
  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_DB, "show databases", 17, NULL);
  CheckSchema(0, TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN - 1 + VARSTR_HEADER_SIZE, "name");
  CheckSchema(1, TSDB_DATA_TYPE_TIMESTAMP, 8, "create time");
  CheckSchema(2, TSDB_DATA_TYPE_SMALLINT, 2, "vgroups");
  CheckSchema(3, TSDB_DATA_TYPE_SMALLINT, 2, "replica");
  CheckSchema(4, TSDB_DATA_TYPE_SMALLINT, 2, "quorum");
  CheckSchema(5, TSDB_DATA_TYPE_SMALLINT, 2, "days");
  CheckSchema(6, TSDB_DATA_TYPE_BINARY, 24 + VARSTR_HEADER_SIZE, "keep0,keep1,keep2");
  CheckSchema(7, TSDB_DATA_TYPE_INT, 4, "cache(MB)");
  CheckSchema(8, TSDB_DATA_TYPE_INT, 4, "blocks");
  CheckSchema(9, TSDB_DATA_TYPE_INT, 4, "minrows");
  CheckSchema(10, TSDB_DATA_TYPE_INT, 4, "maxrows");
  CheckSchema(11, TSDB_DATA_TYPE_TINYINT, 1, "wallevel");
  CheckSchema(12, TSDB_DATA_TYPE_INT, 4, "fsync");
  CheckSchema(13, TSDB_DATA_TYPE_TINYINT, 1, "comp");
  CheckSchema(14, TSDB_DATA_TYPE_TINYINT, 1, "cachelast");
  CheckSchema(15, TSDB_DATA_TYPE_BINARY, 3 + VARSTR_HEADER_SIZE, "precision");
  CheckSchema(16, TSDB_DATA_TYPE_TINYINT, 1, "update");

  SendThenCheckShowRetrieveMsg(0);
}

TEST_F(DndTestDb, 02_Create_Alter_Drop_Db) {
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
    pReq->minRowsPerFileBlock = htonl(100);
    pReq->maxRowsPerFileBlock = htonl(4096);
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

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_DB, "show databases", 17, NULL);
  SendThenCheckShowRetrieveMsg(1);
  CheckBinary("d1", TSDB_DB_NAME_LEN - 1);
  CheckTimestamp();
  CheckInt16(2);                      // vgroups
  CheckInt16(1);                      // replica
  CheckInt16(1);                      // quorum
  CheckInt16(10);                     // days
  CheckBinary("3650,3650,3650", 24);  // days
  CheckInt32(16);                     // cache
  CheckInt32(10);                     // blocks
  CheckInt32(100);                    // minrows
  CheckInt32(4096);                   // maxrows
  CheckInt8(1);                       // wallevel
  CheckInt32(3000);                   // fsync
  CheckInt8(2);                       // comp
  CheckInt8(0);                       // cachelast
  CheckBinary("ms", 3);               // precision
  CheckInt8(0);                       // update

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_VGROUP, "show vgroups", 4, "1.d1");
  CheckSchema(0, TSDB_DATA_TYPE_INT, 4, "vgId");
  CheckSchema(1, TSDB_DATA_TYPE_INT, 4, "tables");
  CheckSchema(2, TSDB_DATA_TYPE_SMALLINT, 2, "v1_dnode");
  CheckSchema(3, TSDB_DATA_TYPE_BINARY, 9 + VARSTR_HEADER_SIZE, "v1_status");
  SendThenCheckShowRetrieveMsg(2);
  CheckInt32(1);
  CheckInt32(2);
  CheckInt32(0);
  CheckInt32(0);
  CheckInt16(1);
  CheckInt16(1);
  CheckBinary("master", 9);
  CheckBinary("master", 9);

  {
    SAlterDbMsg* pReq = (SAlterDbMsg*)rpcMallocCont(sizeof(SAlterDbMsg));
    strcpy(pReq->db, "1.d1");
    pReq->totalBlocks = htonl(12);
    pReq->daysToKeep0 = htonl(300);
    pReq->daysToKeep1 = htonl(400);
    pReq->daysToKeep2 = htonl(500);
    pReq->fsyncPeriod = htonl(4000);
    pReq->walLevel = 2;
    pReq->quorum = 2;
    pReq->cacheLastRow = 1;

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SAlterDbMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_ALTER_DB;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_DB, "show databases", 17, NULL);
  SendThenCheckShowRetrieveMsg(1);
  CheckBinary("d1", TSDB_DB_NAME_LEN - 1);
  CheckTimestamp();
  CheckInt16(2);                   // vgroups
  CheckInt16(1);                   // replica
  CheckInt16(2);                   // quorum
  CheckInt16(10);                  // days
  CheckBinary("300,400,500", 24);  // days
  CheckInt32(16);                  // cache
  CheckInt32(12);                  // blocks
  CheckInt32(100);                 // minrows
  CheckInt32(4096);                // maxrows
  CheckInt8(2);                    // wallevel
  CheckInt32(4000);                // fsync
  CheckInt8(2);                    // comp
  CheckInt8(1);                    // cachelast
  CheckBinary("ms", 3);            // precision
  CheckInt8(0);                    // update

  // restart
  stopServer(pServer);
  pServer = NULL;

  uInfo("start all server");

  const char* fqdn = "localhost";
  const char* firstEp = "localhost:9040";
  pServer = startServer("/tmp/dnode_test_db", fqdn, 9040, firstEp);

  uInfo("all server is running");

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_DB, "show databases", 17, NULL);
  SendThenCheckShowRetrieveMsg(1);
  CheckBinary("d1", TSDB_DB_NAME_LEN - 1);
  CheckTimestamp();
  CheckInt16(2);                   // vgroups
  CheckInt16(1);                   // replica
  CheckInt16(2);                   // quorum
  CheckInt16(10);                  // days
  CheckBinary("300,400,500", 24);  // days
  CheckInt32(16);                  // cache
  CheckInt32(12);                  // blocks
  CheckInt32(100);                 // minrows
  CheckInt32(4096);                // maxrows
  CheckInt8(2);                    // wallevel
  CheckInt32(4000);                // fsync
  CheckInt8(2);                    // comp
  CheckInt8(1);                    // cachelast
  CheckBinary("ms", 3);            // precision
  CheckInt8(0);                    // update

  {
    SDropDbMsg* pReq = (SDropDbMsg*)rpcMallocCont(sizeof(SDropDbMsg));
    strcpy(pReq->db, "1.d1");

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SDropDbMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_DROP_DB;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);
  }

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_DB, "show databases", 17, NULL);
  SendThenCheckShowRetrieveMsg(0);
}

TEST_F(DndTestDb, 03_Create_Use_Restart_Use_Db) {
  {
    SCreateDbMsg* pReq = (SCreateDbMsg*)rpcMallocCont(sizeof(SCreateDbMsg));
    strcpy(pReq->db, "1.d2");
    pReq->numOfVgroups = htonl(2);
    pReq->cacheBlockSize = htonl(16);
    pReq->totalBlocks = htonl(10);
    pReq->daysPerFile = htonl(10);
    pReq->daysToKeep0 = htonl(3650);
    pReq->daysToKeep1 = htonl(3650);
    pReq->daysToKeep2 = htonl(3650);
    pReq->minRowsPerFileBlock = htonl(100);
    pReq->maxRowsPerFileBlock = htonl(4096);
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

  SendTheCheckShowMetaMsg(TSDB_MGMT_TABLE_DB, "show databases", 17, NULL);
  SendThenCheckShowRetrieveMsg(1);
  CheckBinary("d2", TSDB_DB_NAME_LEN - 1);

  {
    SUseDbMsg* pReq = (SUseDbMsg*)rpcMallocCont(sizeof(SUseDbMsg));
    strcpy(pReq->db, "1.d2");
    pReq->vgVersion = htonl(-1);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = pReq;
    rpcMsg.contLen = sizeof(SUseDbMsg);
    rpcMsg.msgType = TSDB_MSG_TYPE_USE_DB;

    sendMsg(pClient, &rpcMsg);
    SRpcMsg* pMsg = pClient->pRsp;
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SUseDbRsp* pRsp = (SUseDbRsp*)pMsg->pCont;
    EXPECT_STREQ(pRsp->db, "1.d2");
    pRsp->vgVersion = htonl(pRsp->vgVersion);
    pRsp->vgNum = htonl(pRsp->vgNum);
    pRsp->hashMethod = pRsp->hashMethod;
    EXPECT_EQ(pRsp->vgVersion, 1);
    EXPECT_EQ(pRsp->vgNum, 2);
    EXPECT_EQ(pRsp->hashMethod, 1);

    {
      SVgroupInfo* pInfo = &pRsp->vgroupInfo[0];
      pInfo->vgId = htonl(pInfo->vgId);
      pInfo->hashBegin = htonl(pInfo->hashBegin);
      pInfo->hashEnd = htonl(pInfo->hashEnd);
      EXPECT_GT(pInfo->vgId, 0);
      EXPECT_EQ(pInfo->hashBegin, 0);
      EXPECT_EQ(pInfo->hashEnd, UINT32_MAX / 2 - 1);
      EXPECT_EQ(pInfo->inUse, 0);
      EXPECT_EQ(pInfo->numOfEps, 1);
      SEpAddrMsg* pAddr = &pInfo->epAddr[0];
      pAddr->port = htons(pAddr->port);
      EXPECT_EQ(pAddr->port, 9040);
      EXPECT_STREQ(pAddr->fqdn, "localhost");
    }

    {
      SVgroupInfo* pInfo = &pRsp->vgroupInfo[1];
      pInfo->vgId = htonl(pInfo->vgId);
      pInfo->hashBegin = htonl(pInfo->hashBegin);
      pInfo->hashEnd = htonl(pInfo->hashEnd);
      EXPECT_GT(pInfo->vgId, 0);
      EXPECT_EQ(pInfo->hashBegin, UINT32_MAX / 2);
      EXPECT_EQ(pInfo->hashEnd, UINT32_MAX);
      EXPECT_EQ(pInfo->inUse, 0);
      EXPECT_EQ(pInfo->numOfEps, 1);
      SEpAddrMsg* pAddr = &pInfo->epAddr[0];
      pAddr->port = htons(pAddr->port);
      EXPECT_EQ(pAddr->port, 9040);
      EXPECT_STREQ(pAddr->fqdn, "localhost");
    }
  }
}
