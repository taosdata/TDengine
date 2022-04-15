/**
 * @file profile.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module profile tests
 * @version 1.0
 * @date 2022-01-06
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "sut.h"

class MndTestProfile : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { test.Init("/tmp/mnode_test_profile", 9031); }
  static void TearDownTestSuite() { test.Cleanup(); }

  static Testbase test;
  static int32_t  connId;

 public:
  void SetUp() override {}
  void TearDown() override {}
};

Testbase MndTestProfile::test;
int32_t  MndTestProfile::connId;

TEST_F(MndTestProfile, 01_ConnectMsg) {
  SConnectReq connectReq = {0};
  connectReq.pid = 1234;
  strcpy(connectReq.app, "mnode_test_profile");
  strcpy(connectReq.db, "");

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSConnectReq(pReq, contLen, &connectReq);

  SRpcMsg* pMsg = test.SendReq(TDMT_MND_CONNECT, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

  SConnectRsp connectRsp = {0};
  tDeserializeSConnectRsp(pMsg->pCont, pMsg->contLen, &connectRsp);

  EXPECT_EQ(connectRsp.acctId, 1);
  EXPECT_GT(connectRsp.clusterId, 0);
  EXPECT_EQ(connectRsp.connId, 1);
  EXPECT_EQ(connectRsp.superUser, 1);

  EXPECT_EQ(connectRsp.epSet.inUse, 0);
  EXPECT_EQ(connectRsp.epSet.numOfEps, 1);
  EXPECT_EQ(connectRsp.epSet.eps[0].port, 9031);
  EXPECT_STREQ(connectRsp.epSet.eps[0].fqdn, "localhost");

  connId = connectRsp.connId;
}

TEST_F(MndTestProfile, 02_ConnectMsg_InvalidDB) {
  SConnectReq connectReq = {0};
  connectReq.pid = 1234;
  strcpy(connectReq.app, "mnode_test_profile");
  strcpy(connectReq.db, "invalid_db");

  int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSConnectReq(pReq, contLen, &connectReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_CONNECT, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_DB);
  ASSERT_EQ(pRsp->contLen, 0);
}

TEST_F(MndTestProfile, 03_ConnectMsg_Show) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_CONNS, "");
  CHECK_META("show connections", 7);
  CHECK_SCHEMA(0, TSDB_DATA_TYPE_INT, 4, "connId");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "user");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_BINARY, TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, "program");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_INT, 4, "pid");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_BINARY, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, "ip:port");
  CHECK_SCHEMA(5, TSDB_DATA_TYPE_TIMESTAMP, 8, "login_time");
  CHECK_SCHEMA(6, TSDB_DATA_TYPE_TIMESTAMP, 8, "last_access");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 1);
  CheckInt32(1);
  CheckBinary("root", TSDB_USER_LEN);
  CheckBinary("mnode_test_profile", TSDB_APP_NAME_LEN);
  CheckInt32(1234);
  IgnoreBinary(TSDB_IPv4ADDR_LEN + 6);
  CheckTimestamp();
  CheckTimestamp();
}

TEST_F(MndTestProfile, 04_HeartBeatMsg) {
  SClientHbBatchReq batchReq = {0};
  batchReq.reqs = taosArrayInit(0, sizeof(SClientHbReq));
  SClientHbReq req = {0};
  req.connKey = {.connId = 123, .hbType = CONN_TYPE__TMQ};
  req.info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
  SKv kv = {0};
  kv.key = 123;
  kv.value = (void*)"bcd";
  kv.valueLen = 4;
  taosHashPut(req.info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  taosArrayPush(batchReq.reqs, &req);

  int32_t tlen = tSerializeSClientHbBatchReq(NULL, 0, &batchReq);
  void*   buf = (SClientHbBatchReq*)rpcMallocCont(tlen);
  tSerializeSClientHbBatchReq(buf, tlen, &batchReq);

  SRpcMsg* pMsg = test.SendReq(TDMT_MND_HEARTBEAT, buf, tlen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

  SClientHbBatchRsp rsp = {0};
  tDeserializeSClientHbBatchRsp(pMsg->pCont, pMsg->contLen, &rsp);
  int sz = taosArrayGetSize(rsp.rsps);
  ASSERT_EQ(sz, 0);

  // SClientHbRsp* pRsp = (SClientHbRsp*) taosArrayGet(rsp.rsps, 0);
  // EXPECT_EQ(pRsp->connKey.connId, 123);
  // EXPECT_EQ(pRsp->connKey.hbType, HEARTBEAT_TYPE_MQ);
  // EXPECT_EQ(pRsp->status, 0);

#if 0
  int32_t contLen = sizeof(SHeartBeatReq);

  SHeartBeatReq* pReq = (SHeartBeatReq*)rpcMallocCont(contLen);
  pReq->connId = htonl(connId);
  pReq->pid = htonl(1234);
  pReq->numOfQueries = htonl(0);
  pReq->numOfStreams = htonl(0);
  strcpy(pReq->app, "mnode_test_profile");

  SRpcMsg* pMsg = test.SendReq(TDMT_MND_HEARTBEAT, pReq, contLen);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(pMsg->code, 0);

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
  EXPECT_EQ(pRsp->epSet.port[0], 9031);
  EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
#endif
}

TEST_F(MndTestProfile, 05_KillConnMsg) {
  // temporary remove since kill will use new heartbeat msg
#if 0
  {
    SKillConnReq killReq = {0};
    killReq.connId = connId;

    int32_t contLen = tSerializeSKillConnReq(NULL, 0, &killReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSKillConnReq(pReq, contLen, &killReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_KILL_CONN, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
  }

  {
    int32_t contLen = sizeof(SHeartBeatReq);

    SHeartBeatReq* pReq = (SHeartBeatReq*)rpcMallocCont(contLen);
    pReq->connId = htonl(connId);
    pReq->pid = htonl(1234);
    pReq->numOfQueries = htonl(0);
    pReq->numOfStreams = htonl(0);
    strcpy(pReq->app, "mnode_test_profile");

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_HEARTBEAT, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_CONNECTION);
    ASSERT_EQ(pRsp->contLen, 0);
  }

  {
    SConnectReq connectReq = {0};
    connectReq.pid = 1234;
    strcpy(connectReq.app, "mnode_test_profile");
    strcpy(connectReq.db, "invalid_db");

    int32_t contLen = tSerializeSConnectReq(NULL, 0, &connectReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSConnectReq(pReq, contLen, &connectReq);

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_CONNECT, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

    SConnectRsp connectRsp = {0};
    tDeserializeSConnectRsp(pMsg->pCont, pMsg->contLen, &connectRsp);

    EXPECT_EQ(connectRsp.acctId, 1);
    EXPECT_GT(connectRsp.clusterId, 0);
    EXPECT_GT(connectRsp.connId, connId);
    EXPECT_EQ(connectRsp.superUser, 1);

    EXPECT_EQ(connectRsp.epSet.inUse, 0);
    EXPECT_EQ(connectRsp.epSet.numOfEps, 1);
    EXPECT_EQ(connectRsp.epSet.port[0], 9031);
    EXPECT_STREQ(connectRsp.epSet.fqdn[0], "localhost");

    connId = connectRsp.connId;
  }
#endif
}

TEST_F(MndTestProfile, 06_KillConnMsg_InvalidConn) {
  SKillConnReq killReq = {0};
  killReq.connId = 2345;

  int32_t contLen = tSerializeSKillConnReq(NULL, 0, &killReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSKillConnReq(pReq, contLen, &killReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_KILL_CONN, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_CONN_ID);
}

TEST_F(MndTestProfile, 07_KillQueryMsg) {
  // temporary remove since kill will use new heartbeat msg
#if 0
  {
    SKillQueryReq killReq = {0};
    killReq.connId = connId;
    killReq.queryId = 1234;

    int32_t contLen = tSerializeSKillQueryReq(NULL, 0, &killReq);
    void*   pReq = rpcMallocCont(contLen);
    tSerializeSKillQueryReq(pReq, contLen, &killReq);

    SRpcMsg* pRsp = test.SendReq(TDMT_MND_KILL_QUERY, pReq, contLen);
    ASSERT_NE(pRsp, nullptr);
    ASSERT_EQ(pRsp->code, 0);
    ASSERT_EQ(pRsp->contLen, 0);
  }

  {
    int32_t contLen = sizeof(SHeartBeatReq);

    SHeartBeatReq* pReq = (SHeartBeatReq*)rpcMallocCont(contLen);
    pReq->connId = htonl(connId);
    pReq->pid = htonl(1234);
    pReq->numOfQueries = htonl(0);
    pReq->numOfStreams = htonl(0);
    strcpy(pReq->app, "mnode_test_profile");

    SRpcMsg* pMsg = test.SendReq(TDMT_MND_HEARTBEAT, pReq, contLen);
    ASSERT_NE(pMsg, nullptr);
    ASSERT_EQ(pMsg->code, 0);

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
    EXPECT_EQ(pRsp->epSet.port[0], 9031);
    EXPECT_STREQ(pRsp->epSet.fqdn[0], "localhost");
  }
#endif
}

TEST_F(MndTestProfile, 08_KillQueryMsg_InvalidConn) {
  SKillQueryReq killReq = {0};
  killReq.connId = 2345;
  killReq.queryId = 2345;

  int32_t contLen = tSerializeSKillQueryReq(NULL, 0, &killReq);
  void*   pReq = rpcMallocCont(contLen);
  tSerializeSKillQueryReq(pReq, contLen, &killReq);

  SRpcMsg* pRsp = test.SendReq(TDMT_MND_KILL_QUERY, pReq, contLen);
  ASSERT_NE(pRsp, nullptr);
  ASSERT_EQ(pRsp->code, TSDB_CODE_MND_INVALID_CONN_ID);
}

TEST_F(MndTestProfile, 09_KillQueryMsg) {
  test.SendShowMetaReq(TSDB_MGMT_TABLE_QUERIES, "");
  CHECK_META("show queries", 14);

  CHECK_SCHEMA(0, TSDB_DATA_TYPE_INT, 4, "queryId");
  CHECK_SCHEMA(1, TSDB_DATA_TYPE_INT, 4, "connId");
  CHECK_SCHEMA(2, TSDB_DATA_TYPE_BINARY, TSDB_USER_LEN + VARSTR_HEADER_SIZE, "user");
  CHECK_SCHEMA(3, TSDB_DATA_TYPE_BINARY, TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, "ip:port");
  CHECK_SCHEMA(4, TSDB_DATA_TYPE_BINARY, 22 + VARSTR_HEADER_SIZE, "qid");
  CHECK_SCHEMA(5, TSDB_DATA_TYPE_TIMESTAMP, 8, "created_time");
  CHECK_SCHEMA(6, TSDB_DATA_TYPE_BIGINT, 8, "time");
  CHECK_SCHEMA(7, TSDB_DATA_TYPE_BINARY, 18 + VARSTR_HEADER_SIZE, "sql_obj_id");
  CHECK_SCHEMA(8, TSDB_DATA_TYPE_INT, 4, "pid");
  CHECK_SCHEMA(9, TSDB_DATA_TYPE_BINARY, TSDB_EP_LEN + VARSTR_HEADER_SIZE, "ep");
  CHECK_SCHEMA(10, TSDB_DATA_TYPE_BOOL, 1, "stable_query");
  CHECK_SCHEMA(11, TSDB_DATA_TYPE_INT, 4, "sub_queries");
  CHECK_SCHEMA(12, TSDB_DATA_TYPE_BINARY, TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE, "sub_query_info");
  CHECK_SCHEMA(13, TSDB_DATA_TYPE_BINARY, TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, "sql");

  test.SendShowRetrieveReq();
  EXPECT_EQ(test.GetShowRows(), 0);
}
