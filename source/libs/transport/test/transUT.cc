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
#include "tep.h"
#include "tglobal.h"
#include "trpc.h"
#include "ulog.h"
using namespace std;

const char *label = "APP";
const char *secret = "secret";
const char *user = "user";
const char *ckey = "ckey";

class Server;
int port = 7000;
// server process
static void processReq(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
// client process;
static void processResp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
class Client {
 public:
  void Init(int nThread) {
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = (char *)label;
    rpcInit.numOfThreads = nThread;
    rpcInit.cfp = processResp;
    rpcInit.user = (char *)user;
    rpcInit.secret = (char *)secret;
    rpcInit.ckey = (char *)ckey;
    rpcInit.spi = 1;
    rpcInit.parent = this;
    rpcInit.connType = TAOS_CONN_CLIENT;
    this->transCli = rpcOpen(&rpcInit);
    tsem_init(&this->sem, 0, 0);
  }
  void SetResp(SRpcMsg *pMsg) {
    // set up resp;
    this->resp = *pMsg;
  }
  SRpcMsg *Resp() { return &this->resp; }

  void Restart() {
    rpcClose(this->transCli);
    this->transCli = rpcOpen(&rpcInit);
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
  SRpcInit rpcInit;
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
class TransObj {
 public:
  TransObj() {
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
    cqDebugFlag = 0;
    tscEmbeddedInUtil = 1;
    tsAsyncLog = 0;

    std::string path = "/tmp/transport";
    taosRemoveDir(path.c_str());
    taosMkDir(path.c_str());

    char temp[PATH_MAX];
    snprintf(temp, PATH_MAX, "%s/taosdlog", path.c_str());
    if (taosInitLog(temp, tsNumOfLogLines, 1) != 0) {
      printf("failed to init log file\n");
    }
    cli = new Client;
    cli->Init(1);
    srv = new Server;
    srv->Start();
  }
  void RestartCli() { cli->Restart(); }
  void StopSrv() { srv->Stop(); }
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

// TEST_F(TransEnv, 01sendAndRec) {
//  for (int i = 0; i < 1; i++) {
//    SRpcMsg req = {0}, resp = {0};
//    req.msgType = 0;
//    req.pCont = rpcMallocCont(10);
//    req.contLen = 10;
//    tr->cliSendAndRecv(&req, &resp);
//    assert(resp.code == 0);
//  }
//}

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
