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
#include <cstring>
#include <thread>
#include <vector>

#include "tdatablock.h"
#include "tglobal.h"
#include "tlog.h"
#include "tmisce.h"
#include "transLog.h"
#include "trpc.h"
#include "tversion.h"

using namespace std;

const char *label = "APP";
const char *secret = "secret";
const char *user = "user";
const char *ckey = "ckey";

int port = 7000;

typedef void (*CB)(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);

static void processReq(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);
static void processResp(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet);

class Client {
 public:
  void Init(int nThread, const char *lbl = "client") {
    memcpy(tsTempDir, TD_TMP_DIR_PATH, strlen(TD_TMP_DIR_PATH));
    memset(&rpcInit_, 0, sizeof(rpcInit_));
    rpcInit_.localPort = 0;
    rpcInit_.label = (char *)lbl;
    rpcInit_.numOfThreads = nThread;
    rpcInit_.cfp = processResp;
    rpcInit_.user = (char *)user;
    rpcInit_.parent = this;
    rpcInit_.connType = TAOS_CONN_CLIENT;
    rpcInit_.shareConnLimit = 200;
    rpcInit_.ipv6 = 1;

    taosVersionStrToInt(td_version, &(rpcInit_.compatibilityVer));
    this->transCli = rpcOpen(&rpcInit_);
    tsem_init(&this->sem, 0, 0);
  }

  void SendAndRecv(const char *ip, SRpcMsg *req, SRpcMsg *resp) {
    SEpSet epSet = {0};
    epSet.inUse = 0;
    addEpIntoEpSet(&epSet, ip, port);

    rpcSendRequest(this->transCli, &epSet, req, NULL);
    SemWait();
    *resp = this->resp;
    rpcFreeCont(resp->pCont);
  }

  void SendAndRecv(SRpcMsg *req, SRpcMsg *resp) { SendAndRecv("127.0.0.1", req, resp); }

  void SetResp(SRpcMsg *pMsg) { this->resp = *pMsg; }

  void SemWait() { tsem_wait(&this->sem); }
  void SemPost() { tsem_post(&this->sem); }

  void Stop() {
    if (this->transCli) {
      rpcClose(this->transCli);
      this->transCli = NULL;
    }
  }

  ~Client() {
    if (this->transCli) rpcClose(this->transCli);
  }

 private:
  tsem_t   sem;
  SRpcInit rpcInit_;
  void    *transCli;
  SRpcMsg  resp;
};

class Server {
 public:
  Server() {
    memcpy(tsTempDir, TD_TMP_DIR_PATH, strlen(TD_TMP_DIR_PATH));
    memset(&rpcInit_, 0, sizeof(rpcInit_));

    memcpy(rpcInit_.localFqdn, "localhost", strlen("localhost"));
    rpcInit_.localPort = port;
    rpcInit_.label = (char *)"server";
    rpcInit_.numOfThreads = 5;
    rpcInit_.cfp = processReq;
    rpcInit_.user = (char *)user;
    rpcInit_.connType = TAOS_CONN_SERVER;
    rpcInit_.ipv6 = 1;
    taosVersionStrToInt(td_version, &(rpcInit_.compatibilityVer));
  }

  void Start() {
    this->transSrv = rpcOpen(&this->rpcInit_);
    taosMsleep(1000);
  }

  void Stop() {
    if (this->transSrv == NULL) return;
    rpcClose(this->transSrv);
    this->transSrv = NULL;
  }

  ~Server() {
    if (this->transSrv) {
      rpcClose(this->transSrv);
      this->transSrv = NULL;
    }
  }

 private:
  SRpcInit rpcInit_;
  void    *transSrv;
};

static void processReq(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = rpcMallocCont(100);
  rpcMsg.contLen = 100;
  rpcMsg.info = pMsg->info;
  rpcMsg.code = 0;
  rpcFreeCont(pMsg->pCont);
  rpcSendResponse(&rpcMsg);
}

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
  tsEnableIpv6 = 1;

  std::string path = TD_TMP_DIR_PATH "transport";
  taosMkDir(path.c_str());

  tstrncpy(tsLogDir, path.c_str(), PATH_MAX);
  if (taosInitLog("taosdlog", 1, false) != 0) {
    printf("failed to init log file\n");
  }
}

class IPv6DualStackTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    initEnv();
    srv = new Server;
    srv->Start();
    cli = new Client;
    cli->Init(1);
  }

  virtual void TearDown() {
    delete cli;
    delete srv;
  }

  Client *cli;
  Server *srv;
};

TEST_F(IPv6DualStackTest, BasicIPv4Connection) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 0;
  req.pCont = rpcMallocCont(100);
  req.contLen = 100;
  req.info.ahandle = (void *)0x1;

  cli->SendAndRecv("127.0.0.1", &req, &resp);

  EXPECT_EQ(resp.code, 0);
}

TEST_F(IPv6DualStackTest, BasicIPv6Connection) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 1;
  req.pCont = rpcMallocCont(100);
  req.contLen = 100;
  req.info.ahandle = (void *)0x2;

  cli->SendAndRecv("::1", &req, &resp);

  EXPECT_EQ(resp.code, 0);
}

TEST_F(IPv6DualStackTest, IPv4MappedIPv6Address) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 2;
  req.pCont = rpcMallocCont(100);
  req.contLen = 100;
  req.info.ahandle = (void *)0x3;

  cli->SendAndRecv("::ffff:127.0.0.1", &req, &resp);

  EXPECT_EQ(resp.code, 0);
}

TEST_F(IPv6DualStackTest, IPv6LoopbackAddress) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 3;
  req.pCont = rpcMallocCont(100);
  req.contLen = 100;
  req.info.ahandle = (void *)0x4;

  cli->SendAndRecv("::1", &req, &resp);

  EXPECT_EQ(resp.code, 0);
}

TEST_F(IPv6DualStackTest, LargePayloadIPv4) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 4;
  req.pCont = rpcMallocCont(1024 * 1024);
  req.contLen = 1024 * 1024;
  req.info.ahandle = (void *)0x5;

  cli->SendAndRecv("127.0.0.1", &req, &resp);

  EXPECT_EQ(resp.code, 0);
}

TEST_F(IPv6DualStackTest, LargePayloadIPv6) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 5;
  req.pCont = rpcMallocCont(1024 * 1024);
  req.contLen = 1024 * 1024;
  req.info.ahandle = (void *)0x6;

  cli->SendAndRecv("::1", &req, &resp);

  EXPECT_EQ(resp.code, 0);
}

TEST_F(IPv6DualStackTest, MultipleRequestsIPv4) {
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.msgType = 6;
    req.pCont = rpcMallocCont(100);
    req.contLen = 100;
    req.info.ahandle = (void *)(int64_t)i;

    cli->SendAndRecv("127.0.0.1", &req, &resp);
    EXPECT_EQ(resp.code, 0);
  }
}

TEST_F(IPv6DualStackTest, MultipleRequestsIPv6) {
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.msgType = 7;
    req.pCont = rpcMallocCont(100);
    req.contLen = 100;
    req.info.ahandle = (void *)(int64_t)i;

    cli->SendAndRecv("::1", &req, &resp);
    EXPECT_EQ(resp.code, 0);
  }
}

TEST_F(IPv6DualStackTest, MixedIPv4IPv6Requests) {
  for (int i = 0; i < 10; i++) {
    SRpcMsg req = {0}, resp = {0};
    req.pCont = rpcMallocCont(100);
    req.contLen = 100;
    req.info.ahandle = (void *)(int64_t)i;

    if (i % 2 == 0) {
      req.msgType = 8;
      cli->SendAndRecv("127.0.0.1", &req, &resp);
    } else {
      req.msgType = 9;
      cli->SendAndRecv("::1", &req, &resp);
    }

    EXPECT_EQ(resp.code, 0);
  }
}

TEST_F(IPv6DualStackTest, ServerRestart) {
  SRpcMsg req = {0}, resp = {0};
  req.msgType = 10;
  req.pCont = rpcMallocCont(100);
  req.contLen = 100;

  cli->SendAndRecv("127.0.0.1", &req, &resp);
  EXPECT_EQ(resp.code, 0);

  srv->Stop();
  taosMsleep(200);

  srv->Start();
  taosMsleep(500);

  req.pCont = rpcMallocCont(100);
  cli->SendAndRecv("127.0.0.1", &req, &resp);
  EXPECT_EQ(resp.code, 0);
}
