/**
 * @file trans.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module trans tests
 * @version 1.0
 * @date 2022-05-02
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>

#if 0

#include "mndTrans.h"
#include "mndUser.h"
#include "tcache.h"

void reportStartup(const char *name, const char *desc) {}
void sendRsp(SRpcMsg *pMsg) { rpcFreeCont(pMsg->pCont); }

int32_t sendReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  terrno = TSDB_CODE_INVALID_PTR;
  return -1;
}

int32_t putToQueue(void *pMgmt, SRpcMsg *pMsg) {
  terrno = TSDB_CODE_INVALID_PTR;
  return -1;
}

class MndTestTrans2 : public ::testing::Test {
 protected:
  static void InitLog() {
    dDebugFlag = 143;
    vDebugFlag = 0;
    mDebugFlag = 207;
    cDebugFlag = 0;
    jniDebugFlag = 0;
    tmrDebugFlag = 135;
    uDebugFlag = 135;
    rpcDebugFlag = 143;
    qDebugFlag = 0;
    wDebugFlag = 0;
    sDebugFlag = 0;
    tsdbDebugFlag = 0;
    tsLogEmbedded = 1;
    tsAsyncLog = 0;

    const char *logpath = TD_TMP_DIR_PATH "td";
    taosRemoveDir(logpath);
    taosMkDir(logpath);
    tstrncpy(tsLogDir, logpath, PATH_MAX);
    if (taosInitLog("taosdlog", 1) != 0) {
      printf("failed to init log file\n");
    }
  }

  static void InitMnode() {
    static SMsgCb msgCb = {0};
    msgCb.reportStartupFp = reportStartup;
    msgCb.sendReqFp = sendReq;
    msgCb.sendSyncReqFp = sendSyncReq;
    msgCb.sendRspFp = sendRsp;
    msgCb.queueFps[SYNC_QUEUE] = putToQueue;
    msgCb.queueFps[WRITE_QUEUE] = putToQueue;
     msgCb.queueFps[READ_QUEUE] = putToQueue;
    msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
    tmsgSetDefault(&msgCb);

    SMnodeOpt opt = {0};
    opt.deploy = 1;
    opt.replica = 1;
    opt.replicas[0].id = 1;
    opt.replicas[0].port = 9040;
    strcpy(opt.replicas[0].fqdn, "localhost");
    opt.msgCb = msgCb;

    tsTransPullupInterval = 1;

    const char *mnodepath = TD_TMP_DIR_PATH "mnode_test_trans";
    taosRemoveDir(mnodepath);
    pMnode = mndOpen(mnodepath, &opt);
    mndStart(pMnode);
  }

  static void SetUpTestSuite() {
    InitLog();
    walInit();
    syncInit();
    InitMnode();
  }

  static void TearDownTestSuite() {
    mndStop(pMnode);
    mndClose(pMnode);
    walCleanUp();
    taosCloseLog();
    taosStopCacheRefreshWorker();
  }

  static SMnode *pMnode;

 public:
  void SetUp() override {}
  void TearDown() override {}

  int32_t CreateUserLog(const char *acct, const char *user, ETrnConflct conflict, SDbObj *pDb) {
    SUserObj userObj = {0};
    taosEncryptPass_c((uint8_t *)"taosdata", strlen("taosdata"), userObj.pass);
    tstrncpy(userObj.user, user, TSDB_USER_LEN);
    tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
    userObj.createdTime = taosGetTimestampMs();
    userObj.updateTime = userObj.createdTime;
    userObj.superUser = 1;

    SRpcMsg  rpcMsg = {0};
    STrans  *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, conflict, &rpcMsg, "");
    SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
    mndTransAppendRedolog(pTrans, pRedoRaw);
    sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

    SSdbRaw *pUndoRaw = mndUserActionEncode(&userObj);
    mndTransAppendUndolog(pTrans, pUndoRaw);
    sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

    char *param = taosStrdup("====> test log <=====");
    mndTransSetCb(pTrans, TRANS_START_FUNC_TEST, TRANS_STOP_FUNC_TEST, param, strlen(param) + 1);

    if (pDb != NULL) {
      mndTransSetDbName(pTrans, pDb->name, NULL);
    }

    int32_t code = mndTransPrepare(pMnode, pTrans);
    mndTransDrop(pTrans);

    return code;
  }

  int32_t CreateUserAction(const char *acct, const char *user, bool hasUndoAction, ETrnPolicy policy, ETrnConflct conflict,
                           SDbObj *pDb) {
    SUserObj userObj = {0};
    taosEncryptPass_c((uint8_t *)"taosdata", strlen("taosdata"), userObj.pass);
    tstrncpy(userObj.user, user, TSDB_USER_LEN);
    tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
    userObj.createdTime = taosGetTimestampMs();
    userObj.updateTime = userObj.createdTime;
    userObj.superUser = 1;

    SRpcMsg  rpcMsg = {0};
    STrans  *pTrans = mndTransCreate(pMnode, policy, conflict, &rpcMsg, "");
    SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
    mndTransAppendRedolog(pTrans, pRedoRaw);
    sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

    SSdbRaw *pUndoRaw = mndUserActionEncode(&userObj);
    mndTransAppendUndolog(pTrans, pUndoRaw);
    sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

    char *param = taosStrdup("====> test action <=====");
    mndTransSetCb(pTrans, TRANS_START_FUNC_TEST, TRANS_STOP_FUNC_TEST, param, strlen(param) + 1);

    {
      STransAction action = {0};
      action.epSet.inUse = 0;
      action.epSet.numOfEps = 1;
      action.epSet.eps[0].port = 9040;
      strcpy(action.epSet.eps[0].fqdn, "localhost");

      int32_t contLen = 1024;
      void   *pReq = taosMemoryCalloc(1, contLen);
      strcpy((char *)pReq, "hello world redo");
      action.pCont = pReq;
      action.contLen = contLen;
      action.msgType = TDMT_DND_CREATE_MNODE;
      action.acceptableCode = TSDB_CODE_MNODE_ALREADY_DEPLOYED;
      mndTransAppendRedoAction(pTrans, &action);
    }

    if (hasUndoAction) {
      STransAction action = {0};
      action.epSet.inUse = 0;
      action.epSet.numOfEps = 1;
      action.epSet.eps[0].port = 9040;
      strcpy(action.epSet.eps[0].fqdn, "localhost");

      int32_t contLen = 1024;
      void   *pReq = taosMemoryCalloc(1, contLen);
      strcpy((char *)pReq, "hello world undo");
      action.pCont = pReq;
      action.contLen = contLen;
      action.msgType = TDMT_DND_CREATE_MNODE;
      action.acceptableCode = TSDB_CODE_MNODE_ALREADY_DEPLOYED;
      mndTransAppendUndoAction(pTrans, &action);
    }

    {
      void *pRsp = taosMemoryCalloc(1, 256);
      strcpy((char *)pRsp, "simple rsponse");
      mndTransSetRpcRsp(pTrans, pRsp, 256);
    }

    if (pDb != NULL) {
      mndTransSetDbName(pTrans, pDb->name, NULL);
    }

    int32_t code = mndTransPrepare(pMnode, pTrans);
    mndTransDrop(pTrans);

    return code;
  }

  int32_t CreateUserGlobal(const char *acct, const char *user) {
    SUserObj userObj = {0};
    taosEncryptPass_c((uint8_t *)"taosdata", strlen("taosdata"), userObj.pass);
    tstrncpy(userObj.user, user, TSDB_USER_LEN);
    tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
    userObj.createdTime = taosGetTimestampMs();
    userObj.updateTime = userObj.createdTime;
    userObj.superUser = 1;

    SRpcMsg  rpcMsg = {0};
    STrans  *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, &rpcMsg, "");
    SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
    mndTransAppendRedolog(pTrans, pRedoRaw);
    sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

    SSdbRaw *pUndoRaw = mndUserActionEncode(&userObj);
    mndTransAppendUndolog(pTrans, pUndoRaw);
    sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

    char *param = taosStrdup("====> test log <=====");
    mndTransSetCb(pTrans, TRANS_START_FUNC_TEST, TRANS_STOP_FUNC_TEST, param, strlen(param) + 1);

    int32_t code = mndTransPrepare(pMnode, pTrans);
    mndTransDrop(pTrans);

    return code;
  }
};

SMnode *MndTestTrans2::pMnode;

TEST_F(MndTestTrans2, 01_Log) {
  const char *acct = "root";
  const char *acct_invalid = "root1";
  const char *user1 = "log1";
  const char *user2 = "log2";
  SUserObj   *pUser1 = NULL;
  SUserObj   *pUser2 = NULL;

  ASSERT_NE(pMnode, nullptr);

  EXPECT_EQ(CreateUserLog(acct, user1, TRN_TYPE_CREATE_USER, NULL), 0);
  pUser1 = mndAcquireUser(pMnode, user1);
  ASSERT_NE(pUser1, nullptr);

  // failed to create user and rollback
  EXPECT_EQ(CreateUserLog(acct_invalid, user2, TRN_TYPE_CREATE_USER, NULL), 0);
  pUser2 = mndAcquireUser(pMnode, user2);
  ASSERT_EQ(pUser2, nullptr);

  mndTransPullup(pMnode);
}

TEST_F(MndTestTrans2, 02_Action) {
  const char *acct = "root";
  const char *acct_invalid = "root1";
  const char *user1 = "action1";
  const char *user2 = "action2";
  SUserObj   *pUser1 = NULL;
  SUserObj   *pUser2 = NULL;
  STrans     *pTrans = NULL;
  int32_t     transId = 0;
  int32_t     action = 0;

  ASSERT_NE(pMnode, nullptr);

  {
    // failed to create user and rollback
    EXPECT_EQ(CreateUserAction(acct, user1, false, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_USER, NULL), 0);
    pUser1 = mndAcquireUser(pMnode, user1);
    ASSERT_EQ(pUser1, nullptr);
    mndReleaseUser(pMnode, pUser1);

    // create user, and fake a response
    {
      EXPECT_EQ(CreateUserAction(acct, user1, true, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_USER, NULL), 0);
      pUser1 = mndAcquireUser(pMnode, user1);
      ASSERT_NE(pUser1, nullptr);
      mndReleaseUser(pMnode, pUser1);

      transId = 4;
      pTrans = mndAcquireTrans(pMnode, transId);
      EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
      EXPECT_EQ(pTrans->stage, TRN_STAGE_UNDO_ACTION);
      EXPECT_EQ(pTrans->failedTimes, 1);

      STransAction *pAction = (STransAction *)taosArrayGet(pTrans->undoActions, action);
      pAction->msgSent = 1;

      SRpcMsg rspMsg = {0};
      rspMsg.info.node = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.info.ahandle = (void *)signature;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser1 = mndAcquireUser(pMnode, user1);
      ASSERT_EQ(pUser1, nullptr);
      mndReleaseUser(pMnode, pUser1);
    }
  }

  {
    EXPECT_EQ(CreateUserAction(acct, user1, false, TRN_POLICY_RETRY, TRN_TYPE_CREATE_USER, NULL), 0);
    pUser1 = mndAcquireUser(pMnode, user1);
    ASSERT_NE(pUser1, nullptr);
    mndReleaseUser(pMnode, pUser1);

    {
      transId = 5;
      pTrans = mndAcquireTrans(pMnode, transId);
      EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
      EXPECT_EQ(pTrans->stage, TRN_STAGE_REDO_ACTION);
      EXPECT_EQ(pTrans->failedTimes, 1);

      STransAction *pAction = (STransAction *)taosArrayGet(pTrans->redoActions, action);
      pAction->msgSent = 1;

      SRpcMsg rspMsg = {0};
      rspMsg.info.node = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.info.ahandle = (void *)signature;
      rspMsg.code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser1 = mndAcquireUser(pMnode, user1);
      ASSERT_NE(pUser1, nullptr);
      mndReleaseUser(pMnode, pUser1);
    }

    {
      transId = 5;
      pTrans = mndAcquireTrans(pMnode, transId);
      EXPECT_EQ(pTrans->code, TSDB_CODE_RPC_NETWORK_UNAVAIL);
      EXPECT_EQ(pTrans->stage, TRN_STAGE_REDO_ACTION);
      EXPECT_EQ(pTrans->failedTimes, 2);

      STransAction *pAction = (STransAction *)taosArrayGet(pTrans->redoActions, action);
      pAction->msgSent = 1;

      SRpcMsg rspMsg = {0};
      rspMsg.info.node = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.info.ahandle = (void *)signature;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser1 = mndAcquireUser(pMnode, user1);
      ASSERT_NE(pUser1, nullptr);
      mndReleaseUser(pMnode, pUser1);
    }
  }

  {
    EXPECT_EQ(CreateUserAction(acct, user2, true, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_USER, NULL), 0);
    SUserObj *pUser2 = (SUserObj *)sdbAcquire(pMnode->pSdb, SDB_USER, user2);
    ASSERT_NE(pUser2, nullptr);
    mndReleaseUser(pMnode, pUser2);

    {
      transId = 6;
      pTrans = mndAcquireTrans(pMnode, transId);
      EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
      EXPECT_EQ(pTrans->stage, TRN_STAGE_UNDO_ACTION);
      EXPECT_EQ(pTrans->failedTimes, 1);

      SRpcMsg rspMsg = {0};
      rspMsg.info.node = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.info.ahandle = (void *)signature;
      rspMsg.code = 0;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser2 = mndAcquireUser(pMnode, user2);
      ASSERT_NE(pUser2, nullptr);
      mndReleaseUser(pMnode, pUser2);
    }

    {
      transId = 6;
      pTrans = mndAcquireTrans(pMnode, transId);
      EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
      EXPECT_EQ(pTrans->stage, TRN_STAGE_UNDO_ACTION);
      EXPECT_EQ(pTrans->failedTimes, 2);

      STransAction *pAction = (STransAction *)taosArrayGet(pTrans->undoActions, action);
      pAction->msgSent = 1;

      SRpcMsg rspMsg = {0};
      rspMsg.info.node = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.info.ahandle = (void *)signature;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser2 = mndAcquireUser(pMnode, user2);
      ASSERT_EQ(pUser2, nullptr);
      mndReleaseUser(pMnode, pUser2);
    }
  }
}

TEST_F(MndTestTrans2, 03_Kill) {
  const char *acct = "root";
  const char *user1 = "kill1";
  const char *user2 = "kill2";
  SUserObj   *pUser1 = NULL;
  SUserObj   *pUser2 = NULL;
  STrans     *pTrans = NULL;
  int32_t     transId = 0;
  int32_t     action = 0;

  ASSERT_NE(pMnode, nullptr);

  {
    EXPECT_EQ(CreateUserAction(acct, user1, true, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_USER, NULL), 0);
    pUser1 = mndAcquireUser(pMnode, user1);
    ASSERT_NE(pUser1, nullptr);
    mndReleaseUser(pMnode, pUser1);

    transId = 7;
    pTrans = mndAcquireTrans(pMnode, transId);
    EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
    EXPECT_EQ(pTrans->stage, TRN_STAGE_UNDO_ACTION);
    EXPECT_EQ(pTrans->failedTimes, 1);

    mndKillTrans(pMnode, pTrans);
    mndReleaseTrans(pMnode, pTrans);

    pUser1 = mndAcquireUser(pMnode, user1);
    ASSERT_EQ(pUser1, nullptr);
    mndReleaseUser(pMnode, pUser1);
  }
}

TEST_F(MndTestTrans2, 04_Conflict) {
  const char *acct = "root";
  const char *user1 = "conflict1";
  const char *user2 = "conflict2";
  const char *user3 = "conflict3";
  const char *user4 = "conflict4";
  const char *user5 = "conflict5";
  const char *user6 = "conflict6";
  const char *user7 = "conflict7";
  const char *user8 = "conflict8";
  SUserObj   *pUser = NULL;
  STrans     *pTrans = NULL;
  int32_t     transId = 0;
  int32_t     code = 0;

  ASSERT_NE(pMnode, nullptr);

  {
    SDbObj dbObj1 = {0};
    dbObj1.uid = 9521;
    strcpy(dbObj1.name, "db");
    SDbObj dbObj2 = {0};
    dbObj2.uid = 9522;
    strcpy(dbObj2.name, "conflict db");

    EXPECT_EQ(CreateUserAction(acct, user1, true, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_STB, &dbObj1), 0);
    pUser = mndAcquireUser(pMnode, user1);
    ASSERT_NE(pUser, nullptr);
    mndReleaseUser(pMnode, pUser);

    transId = 8;
    pTrans = mndAcquireTrans(pMnode, transId);
    EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
    EXPECT_EQ(pTrans->stage, TRN_STAGE_UNDO_ACTION);

    // stb scope
    EXPECT_EQ(CreateUserLog(acct, user2, TRN_TYPE_CREATE_DNODE, NULL), -1);
    code = terrno;
    EXPECT_EQ(code, TSDB_CODE_MND_TRANS_CONFLICT);

    EXPECT_EQ(CreateUserLog(acct, user2, TRN_TYPE_CREATE_DB, &dbObj1), -1);
    EXPECT_EQ(CreateUserLog(acct, user2, TRN_TYPE_CREATE_DB, &dbObj2), 0);
    EXPECT_EQ(CreateUserLog(acct, user3, TRN_TYPE_CREATE_STB, &dbObj1), 0);

    // db scope
    pTrans->type = TRN_TYPE_CREATE_DB;
    EXPECT_EQ(CreateUserLog(acct, user4, TRN_TYPE_CREATE_DNODE, NULL), -1);
    EXPECT_EQ(CreateUserLog(acct, user4, TRN_TYPE_CREATE_DB, &dbObj1), -1);
    EXPECT_EQ(CreateUserLog(acct, user4, TRN_TYPE_CREATE_DB, &dbObj2), 0);
    EXPECT_EQ(CreateUserLog(acct, user5, TRN_TYPE_CREATE_STB, &dbObj1), -1);
    EXPECT_EQ(CreateUserLog(acct, user5, TRN_TYPE_CREATE_STB, &dbObj2), 0);

    // global scope
    pTrans->type = TRN_TYPE_CREATE_DNODE;
    EXPECT_EQ(CreateUserLog(acct, user6, TRN_TYPE_CREATE_DNODE, NULL), 0);
    EXPECT_EQ(CreateUserLog(acct, user7, TRN_TYPE_CREATE_DB, &dbObj1), -1);
    EXPECT_EQ(CreateUserLog(acct, user7, TRN_TYPE_CREATE_DB, &dbObj2), -1);
    EXPECT_EQ(CreateUserLog(acct, user7, TRN_TYPE_CREATE_STB, &dbObj1), -1);
    EXPECT_EQ(CreateUserLog(acct, user7, TRN_TYPE_CREATE_STB, &dbObj2), -1);

    // global scope
    pTrans->type = TRN_TYPE_CREATE_USER;
    EXPECT_EQ(CreateUserLog(acct, user7, TRN_TYPE_CREATE_DB, &dbObj1), 0);
    EXPECT_EQ(CreateUserLog(acct, user8, TRN_TYPE_CREATE_DB, &dbObj2), 0);

    mndKillTrans(pMnode, pTrans);
    mndReleaseTrans(pMnode, pTrans);

    pUser = mndAcquireUser(pMnode, user1);
    ASSERT_EQ(pUser, nullptr);
    mndReleaseUser(pMnode, pUser);
  }
}

#endif