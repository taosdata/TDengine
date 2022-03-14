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
typedef void (*CB)(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
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
  void setPersistFP(bool (*pfp)(void *parent, tmsg_t msgType)) {
    rpcClose(this->transCli);
    rpcInit_.pfp = pfp;
    this->transCli = rpcOpen(&rpcInit_);
  }
  void setConstructFP(void *(*mfp)(void *parent, tmsg_t msgType)) {
    rpcClose(this->transCli);
    rpcInit_.mfp = mfp;
    this->transCli = rpcOpen(&rpcInit_);
  }
  void setPAndMFp(bool (*pfp)(void *parent, tmsg_t msgType), void *(*mfp)(void *parent, tmsg_t msgType)) {
    rpcClose(this->transCli);

    rpcInit_.pfp = pfp;
    rpcInit_.mfp = mfp;
    this->transCli = rpcOpen(&rpcInit_);
  }

  void SendAndRecv(SRpcMsg *req, SRpcMsg *resp) {
    SEpSet epSet = {0};
    epSet.inUse = 0;
    addEpIntoEpSet(&epSet, "127.0.0.1", 7000);

    rpcSendRequest(this->transCli, &epSet, req, NULL);
    SemWait();
    *resp = this->resp;
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
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = port;
    rpcInit.label = (char *)label;
    rpcInit.numOfThreads = 5;
    rpcInit.cfp = processReq;
    rpcInit.user = (char *)user;
    rpcInit.secret = (char *)secret;
    rpcInit.ckey = (char *)ckey;
    rpcInit.spi = 1;
    rpcInit.connType = TAOS_CONN_SERVER;
  }
  void Start() {
    this->transSrv = rpcOpen(&this->rpcInit);
    taosMsleep(1000);
  }
  void Stop() {
    if (this->transSrv == NULL) return;
    rpcClose(this->transSrv);
    this->transSrv = NULL;
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
  SRpcInit rpcInit;
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
// client process;
static void processResp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  Client *client = (Client *)parent;
  client->SetResp(pMsg);
  client->SemPost();
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
  taosRemoveDir(path.c_str());
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

  void RestartCli(CB cb) { cli->Restart(cb); }
  void StopSrv() { srv->Stop(); }
  void SetCliPersistFp(bool (*pfp)(void *parent, tmsg_t msgType)) {
    // do nothing
    cli->setPersistFP(pfp);
  }
  void SetCliMFp(void *(*mfp)(void *parent, tmsg_t msgType)) {
    // do nothing
    cli->setConstructFP(mfp);
  }
  void SetMAndPFp(bool (*pfp)(void *parent, tmsg_t msgType), void *(*mfp)(void *parent, tmsg_t msgType)) {
    // do nothing
    cli->setPAndMFp(pfp, mfp);
  }
  void RestartSrv() { srv->Restart(); }
  void cliSendAndRecv(SRpcMsg *req, SRpcMsg *resp) { cli->SendAndRecv(req, resp); }
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
  for (int i = 0; i < 1; i++) {
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
TEST_F(TransEnv, clientUserDefined) {}

TEST_F(TransEnv, cliPersistHandle) {
  // impl late
}
TEST_F(TransEnv, srvPersistHandle) {
  // impl later
}

TEST_F(TransEnv, srvPersisHandleExcept) {
  // conn breken
  //
}
TEST_F(TransEnv, cliPersisHandleExcept) {
  // conn breken
}

TEST_F(TransEnv, multiCliPersisHandleExcept) {
  // conn breken
}
TEST_F(TransEnv, multiSrvPersisHandleExcept) {
  // conn breken
}
TEST_F(TransEnv, queryExcept) {
  // query and conn is broken
}
