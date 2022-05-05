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

#include "mndTrans.h"
#include "mndUser.h"
#include "tcache.h"

void reportStartup(SMgmtWrapper *pWrapper, const char *name, const char *desc) {}

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

    const char *logpath = "/tmp/td";
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
    msgCb.pWrapper = (SMgmtWrapper *)(&msgCb);  // hack
    tmsgSetDefaultMsgCb(&msgCb);

    SMnodeOpt opt = {0};
    opt.deploy = 1;
    opt.replica = 1;
    opt.replicas[0].id = 1;
    opt.replicas[0].port = 9040;
    strcpy(opt.replicas[0].fqdn, "localhost");
    opt.msgCb = msgCb;

    tsTransPullupMs = 1000;

    const char *mnodepath = "/tmp/mnode_test_trans";
    taosRemoveDir(mnodepath);
    pMnode = mndOpen(mnodepath, &opt);
    mndStart(pMnode);
  }

  static void SetUpTestSuite() {
    InitLog();
    walInit();
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

  int32_t CreateUserLog(const char *acct, const char *user) {
    SUserObj userObj = {0};
    taosEncryptPass_c((uint8_t *)"taosdata", strlen("taosdata"), userObj.pass);
    tstrncpy(userObj.user, user, TSDB_USER_LEN);
    tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
    userObj.createdTime = taosGetTimestampMs();
    userObj.updateTime = userObj.createdTime;
    userObj.superUser = 1;

    SRpcMsg  rpcMsg = {0};
    STrans  *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_USER, &rpcMsg);
    SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
    mndTransAppendRedolog(pTrans, pRedoRaw);
    sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

    SSdbRaw *pUndoRaw = mndUserActionEncode(&userObj);
    mndTransAppendUndolog(pTrans, pUndoRaw);
    sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

    char *param = strdup("====> test log <=====");
    mndTransSetCb(pTrans, TEST_TRANS_START_FUNC, TEST_TRANS_STOP_FUNC, param, strlen(param) + 1);

    int32_t code = mndTransPrepare(pMnode, pTrans);
    mndTransDrop(pTrans);

    return code;
  }

  int32_t CreateUserAction(const char *acct, const char *user, bool hasUndoAction, ETrnPolicy policy) {
    SUserObj userObj = {0};
    taosEncryptPass_c((uint8_t *)"taosdata", strlen("taosdata"), userObj.pass);
    tstrncpy(userObj.user, user, TSDB_USER_LEN);
    tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
    userObj.createdTime = taosGetTimestampMs();
    userObj.updateTime = userObj.createdTime;
    userObj.superUser = 1;

    SRpcMsg  rpcMsg = {0};
    STrans  *pTrans = mndTransCreate(pMnode, policy, TRN_TYPE_CREATE_USER, &rpcMsg);
    SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
    mndTransAppendRedolog(pTrans, pRedoRaw);
    sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

    SSdbRaw *pUndoRaw = mndUserActionEncode(&userObj);
    mndTransAppendUndolog(pTrans, pUndoRaw);
    sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

    char *param = strdup("====> test action <=====");
    mndTransSetCb(pTrans, TEST_TRANS_START_FUNC, TEST_TRANS_STOP_FUNC, param, strlen(param) + 1);

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
      action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;
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
      action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;
      mndTransAppendUndoAction(pTrans, &action);
    }

    {
      void *pRsp = taosMemoryCalloc(1, 256);
      strcpy((char *)pRsp, "simple rsponse");
      mndTransSetRpcRsp(pTrans, pRsp, 256);
    }

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

  EXPECT_EQ(CreateUserLog(acct, user1), 0);
  pUser1 = mndAcquireUser(pMnode, user1);
  ASSERT_NE(pUser1, nullptr);

  // failed to create user and rollback
  EXPECT_EQ(CreateUserLog(acct_invalid, user2), 0);
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
    EXPECT_EQ(CreateUserAction(acct, user1, false, TRN_POLICY_ROLLBACK), 0);
    pUser1 = mndAcquireUser(pMnode, user1);
    ASSERT_EQ(pUser1, nullptr);
    mndReleaseUser(pMnode, pUser1);

    // create user, and fake a response
    {
      EXPECT_EQ(CreateUserAction(acct, user1, true, TRN_POLICY_ROLLBACK), 0);
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

      SNodeMsg rspMsg = {0};
      rspMsg.pNode = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.rpcMsg.ahandle = (void *)signature;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser1 = mndAcquireUser(pMnode, user1);
      ASSERT_EQ(pUser1, nullptr);
      mndReleaseUser(pMnode, pUser1);
    }
  }

  {
    EXPECT_EQ(CreateUserAction(acct, user1, false, TRN_POLICY_RETRY), 0);
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

      SNodeMsg rspMsg = {0};
      rspMsg.pNode = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.rpcMsg.ahandle = (void *)signature;
      rspMsg.rpcMsg.code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
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

      SNodeMsg rspMsg = {0};
      rspMsg.pNode = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.rpcMsg.ahandle = (void *)signature;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser1 = mndAcquireUser(pMnode, user1);
      ASSERT_NE(pUser1, nullptr);
      mndReleaseUser(pMnode, pUser1);
    }
  }

  {
    EXPECT_EQ(CreateUserAction(acct, user2, true, TRN_POLICY_ROLLBACK), 0);
    SUserObj *pUser2 = (SUserObj *)sdbAcquire(pMnode->pSdb, SDB_USER, user2);
    ASSERT_NE(pUser2, nullptr);
    mndReleaseUser(pMnode, pUser2);

    {
      transId = 6;
      pTrans = mndAcquireTrans(pMnode, transId);
      EXPECT_EQ(pTrans->code, TSDB_CODE_INVALID_PTR);
      EXPECT_EQ(pTrans->stage, TRN_STAGE_UNDO_ACTION);
      EXPECT_EQ(pTrans->failedTimes, 1);

      SNodeMsg rspMsg = {0};
      rspMsg.pNode = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.rpcMsg.ahandle = (void *)signature;
      rspMsg.rpcMsg.code = 0;
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

      SNodeMsg rspMsg = {0};
      rspMsg.pNode = pMnode;
      int64_t signature = transId;
      signature = (signature << 32);
      signature += action;
      rspMsg.rpcMsg.ahandle = (void *)signature;
      mndTransProcessRsp(&rspMsg);
      mndReleaseTrans(pMnode, pTrans);

      pUser2 = mndAcquireUser(pMnode, user2);
      ASSERT_EQ(pUser2, nullptr);
      mndReleaseUser(pMnode, pUser2);
    }
  }
}
