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

    const char *mnodepath = "/tmp/mnode_test_trans";
    taosRemoveDir(mnodepath);
    pMnode = mndOpen(mnodepath, &opt);
  }

  static void SetUpTestSuite() {
    InitLog();
    walInit();
    InitMnode();
  }

  static void TearDownTestSuite() {
    mndClose(pMnode);
    walCleanUp();
    taosCloseLog();
    taosStopCacheRefreshWorker();
  }

  static SMnode *pMnode;

 public:
  void SetUp() override {}
  void TearDown() override {}

  int32_t CreateUser(const char *acct, const char *user) {
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

    char *param = strdup("====> test param <=====");
    mndTransSetCb(pTrans, TEST_TRANS_START_FUNC, TEST_TRANS_STOP_FUNC, param, strlen(param) + 1);

    int32_t code = mndTransPrepare(pMnode, pTrans);
    mndTransDrop(pTrans);

    return code;
  }
};

SMnode *MndTestTrans2::pMnode;

TEST_F(MndTestTrans2, 01_CbFunc) {
  const char *acct = "root";
  const char *acct_invalid = "root1";
  const char *user1 = "test1";
  const char *user2 = "test2";
  SUserObj   *pUser1 = NULL;
  SUserObj   *pUser2 = NULL;

  ASSERT_NE(pMnode, nullptr);

  // create user success
  EXPECT_EQ(CreateUser(acct, user1), 0);
  pUser1 = mndAcquireUser(pMnode, user1);
  ASSERT_NE(pUser1, nullptr);

  // failed to create user and rollback
  EXPECT_EQ(CreateUser(acct_invalid, user2), 0);
  pUser2 = mndAcquireUser(pMnode, user2);
  ASSERT_EQ(pUser2, nullptr);
}
