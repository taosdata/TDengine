/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include "rpcLog.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tlog.h"
#include "trpc.h"
using namespace std;

const char *label = "APP";
const char *secret = "secret";
const char *user = "user";
const char *ckey = "ckey";

class Server;
int port = 7000;
// server process
// server except

typedef void (*CB)(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);

static void processContinueSend(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static void processReleaseHandleCb(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static void processRegisterFailure(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static void processReq(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
// client process;
static void processResp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
class Client {
 public:
  void Init(int nThread) {
    memset(&rpcInit_, 0, sizeof(rpcInit_));
    rpcInit_.localPort = 0;
    rpcInit_.label = (char *)label;
    rpcInit_.numOfThreads = nThread;
    rpcInit_.cfp = processResp;
    rpcInit_.user = (char *)user;
    rpcInit_.secret = (char *)secret;
    rpcInit_.ckey = (char *)ckey;
    rpcInit_.spi = 1;
    rpcInit_.parent = this;
    rpcInit_.connType = TAOS_CONN_CLIENT;
    this->transCli = rpcOpen(&rpcInit_);
    tsem_init(&this->sem, 0, 0);
  }
  void SetResp(SRpcMsg *pMsg) {
    // set up resp;
    this->resp = *pMsg;
  }
  SRpcMsg *Resp() { return &this->resp; }

  void Restart(CB cb) {
    rpcClose(this->transCli);
    rpcInit_.cfp = cb;
    this->transCli = rpcOpen(&rpcInit_);
  }
  void Stop() {
    rpcClose(this->transCli);
    this->transCli = NULL;
  }

  void SendAndRecv(SRpcMsg *req, SRpcMsg *resp) {
    SEpSet epSet = {0};
    epSet.inUse = 0;
    addEpIntoEpSet(&epSet, "127.0.0.1", 7000);

    rpcSendRequest(this->transCli, &epSet, req, NULL);
    SemWait();
    *resp = this->resp;
  }
  void SendAndRecvNoHandle(SRpcMsg *req, SRpcMsg *resp) {
    if (req->handle != NULL) {
      rpcReleaseHandle(req->handle, TAOS_CONN_CLIENT);
      req->handle = NULL;
    }
    SendAndRecv(req, resp);
  }

  void SemWait() { tsem_wait(&this->sem); }
  void SemPost() { tsem_post(&this->sem); }
  void Reset() {}

  ~Client() {
    if (this->transCli) rpcClose(this->transCli);
  }

 private:
  tsem_t   sem;
  SRpcInit rpcInit_;
  void *   transCli;
  SRpcMsg  resp;
};
class Server {
 public:
  Server() {
    memset(&rpcInit_, 0, sizeof(rpcInit_));
    rpcInit_.localPort = port;
    rpcInit_.label = (char *)label;
    rpcInit_.numOfThreads = 5;
    rpcInit_.cfp = processReq;
    rpcInit_.user = (char *)user;
    rpcInit_.secret = (char *)secret;
    rpcInit_.ckey = (char *)ckey;
    rpcInit_.spi = 1;
    rpcInit_.connType = TAOS_CONN_SERVER;
  }
  void Start() {
    this->transSrv = rpcOpen(&this->rpcInit_);
    taosMsleep(1000);
  }
  void SetSrvContinueSend(CB cb) {
    this->Stop();
    rpcInit_.cfp = cb;
    this->Start();
  }
  void Stop() {
    if (this->transSrv == NULL) return;
    rpcClose(this->transSrv);
    this->transSrv = NULL;
  }
  void SetSrvSend(void (*cfp)(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet)) {
    this->Stop();
    rpcInit_.cfp = cfp;
    this->Start();
  }
  void Restart() {
    this->Stop();
    this->Start();
  }
  ~Server() {
    if (this->transSrv) rpcClose(this->transSrv);
    this->transSrv = NULL;
  }

 private:
  SRpcInit rpcInit_;
  void *   transSrv;
};
static void processReq(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = rpcMallocCont(100);
  rpcMsg.contLen = 100;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.code = 0;
  rpcSendResponse(&rpcMsg);
}

static void processContinueSend(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  for (int i = 0; i < 10; i++) {
    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = rpcMallocCont(100);
    rpcMsg.contLen = 100;
    rpcMsg.handle = pMsg->handle;
    rpcMsg.code = 0;
    rpcSendResponse(&rpcMsg);
  }
}
static void processReleaseHandleCb(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = rpcMallocCont(100);
  rpcMsg.contLen = 100;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.code = 0;
  rpcSendResponse(&rpcMsg);

  rpcReleaseHandle(pMsg->handle, TAOS_CONN_SERVER);
}
static void processRegisterFailure(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  void *handle = pMsg->handle;
  {
    SRpcMsg rpcMsg1 = {0};
    rpcMsg1.pCont = rpcMallocCont(100);
    rpcMsg1.contLen = 100;
    rpcMsg1.handle = handle;
    rpcMsg1.code = 0;
    rpcRegisterBrokenLinkArg(&rpcMsg1);
  }
  taosMsleep(10);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = rpcMallocCont(100);
  rpcMsg.contLen = 100;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.code = 0;
  rpcSendResponse(&rpcMsg);
}
// client process;
static void processResp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  Client *client = (Client *)parent;
  client->SetResp(pMsg);
  client->SemPost();
  tDebug("received resp");
}

static void initEnv() {
  dDebugFlag = 143;
  vDebugFlag = 0;
  mDebugFlag = 143;
  cDebugFlag = 0;
  jniDebugFlag = 0;
  tmrDebugFlag = 143;
  uDebugFlag = 143;
  rpcDebugFlag = 143;
  qDebugFlag = 0;
  wDebugFlag = 0;
  sDebugFlag = 0;
  tsdbDebugFlag = 0;
  tsLogEmbedded = 1;
  tsAsyncLog = 0;

  std::string path = "/tmp/transport";
  // taosRemoveDir(path.c_str());
  taosMkDir(path.c_str());

  tstrncpy(tsLogDir, path.c_str(), PATH_MAX);
  if (taosInitLog("taosdlog", 1) != 0) {
    printf("failed to init log file\n");
  }
}

class TransObj {
 public:
  TransObj() {
    initEnv();
    cli = new Client;
    cli->Init(1);
    srv = new Server;
    srv->Start();
  }

  void RestartCli(CB cb) {
    //
    cli->Restart(cb);
  }
  void StopSrv() {
    //
    srv->Stop();
  }
  // call when link broken, and notify query or fetch stop
  void SetSrvContinueSend(void (*cfp)(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet)) {
    ///////
    srv->SetSrvContinueSend(cfp);
  }
  void RestartSrv() { srv->Restart(); }
  void StopCli() {
    ///////
    cli->Stop();
  }
  void cliSendAndRecv(SRpcMsg *req, SRpcMsg *resp) { cli->SendAndRecv(req, resp); }
  void cliSendAndRecvNoHandle(SRpcMsg *req, SRpcMsg *resp) { cli->SendAndRecvNoHandle(req, resp); }

  ~TransObj() {
    delete cli;
    delete srv;
  }

 private:
  Client *cli;
  Server *srv;
};
class TransEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // set up trans obj
    tr = new TransObj();
  }
  virtual void TearDown() {
    // tear down
    delete tr;
  }

  TransObj *tr = NULL;
};

TEST_F(TransEnv, 01sendAndRec) {
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.msgType = 0;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    assert(resp.code == 0);
  }
}

TEST_F(TransEnv, 02StopServer) {
  for (int i = 0; i < 1; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.msgType = 0;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    assert(resp.code == 0);
  }
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 1;
  req.pCont = rpcMallocCont(10);
  req.contLen = 10;
  tr->StopSrv();
  // tr->RestartSrv();
  tr->cliSendAndRecv(&req, &resp);
  assert(resp.code != 0);
}
TEST_F(TransEnv, clientUserDefined) {
  tr->RestartSrv();
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.msgType = 0;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    assert(resp.code == 0);
  }

  //////////////////
}

TEST_F(TransEnv, cliPersistHandle) {
  SRpcMsg resp = {0};
  void *  handle = NULL;
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0};
    req.handle = resp.handle;
    req.persistHandle = 1;

    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    // if (i == 5) {
    //  std::cout << "stop server" << std::endl;
    //  tr->StopSrv();
    //}
    // if (i >= 6) {
    //  EXPECT_TRUE(resp.code != 0);
    //}
    handle = resp.handle;
  }
  rpcReleaseHandle(handle, TAOS_CONN_CLIENT);
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0};
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
  }

  taosMsleep(1000);
  //////////////////
}

TEST_F(TransEnv, srvReleaseHandle) {
  SRpcMsg resp = {0};
  tr->SetSrvContinueSend(processReleaseHandleCb);
  // tr->Restart(processReleaseHandleCb);
  void *handle = NULL;
  for (int i = 0; i < 1; i++) {
    SRpcMsg req = {0};
    req.handle = resp.handle;
    req.persistHandle = 1;
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    // tr->cliSendAndRecvNoHandle(&req, &resp);
    EXPECT_TRUE(resp.code == 0);
  }
  //////////////////
}
TEST_F(TransEnv, cliReleaseHandleExcept) {
  SRpcMsg resp = {0};
  for (int i = 0; i < 3; i++) {
    SRpcMsg req = {0};
    req.handle = resp.handle;
    req.persistHandle = 1;
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    if (i == 1) {
      std::cout << "stop server" << std::endl;
      tr->StopSrv();
    }
    if (i > 1) {
      EXPECT_TRUE(resp.code != 0);
    }
  }
  //////////////////
}
TEST_F(TransEnv, srvContinueSend) {
  tr->SetSrvContinueSend(processContinueSend);
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
  }
  taosMsleep(1000);
}

TEST_F(TransEnv, srvPersistHandleExcept) {
  tr->SetSrvContinueSend(processContinueSend);
  // tr->SetCliPersistFp(cliPersistHandle);
  SRpcMsg resp = {0};
  for (int i = 0; i < 5; i++) {
    SRpcMsg req = {0};
    req.handle = resp.handle;
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    if (i > 2) {
      tr->StopCli();
      break;
    }
  }
  taosMsleep(2000);
  // conn broken
  //
}
TEST_F(TransEnv, cliPersistHandleExcept) {
  tr->SetSrvContinueSend(processContinueSend);
  SRpcMsg resp = {0};
  for (int i = 0; i < 5; i++) {
    SRpcMsg req = {0};
    req.handle = resp.handle;
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    if (i > 2) {
      tr->StopSrv();
      break;
    }
  }
  taosMsleep(2000);
  // conn broken
  //
}

TEST_F(TransEnv, multiCliPersistHandleExcept) {
  // conn broken
}
TEST_F(TransEnv, queryExcept) {
  tr->SetSrvContinueSend(processRegisterFailure);
  SRpcMsg resp = {0};
  for (int i = 0; i < 5; i++) {
    SRpcMsg req = {0};
    req.handle = resp.handle;
    req.persistHandle = 1;
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
    if (i == 2) {
      rpcReleaseHandle(resp.handle, TAOS_CONN_CLIENT);
      tr->StopCli();
      break;
    }
  }
  taosMsleep(4 * 1000);
}
TEST_F(TransEnv, noResp) {
  SRpcMsg resp = {0};
  for (int i = 0; i < 5; i++) {
    SRpcMsg req = {0};
    req.noResp = 1;
    req.msgType = 1;
    req.pCont = rpcMallocCont(10);
    req.contLen = 10;
    tr->cliSendAndRecv(&req, &resp);
  }
  taosMsleep(2000);

  // no resp
}
