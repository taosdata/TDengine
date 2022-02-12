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
#include "trpc.h"
using namespace std;

class TransObj {
 public:
  TransObj() {
    const char *label = "APP";
    const char *secret = "secret";
    const char *user = "user";
    const char *ckey = "ckey";

    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = (char *)label;
    rpcInit.numOfThreads = 5;
    rpcInit.cfp = NULL;
    rpcInit.sessions = 100;
    rpcInit.idleTime = 100;
    rpcInit.user = (char *)user;
    rpcInit.secret = (char *)secret;
    rpcInit.ckey = (char *)ckey;
    rpcInit.spi = 1;
  }
  bool startCli() {
    trans = NULL;
    rpcInit.connType = TAOS_CONN_CLIENT;
    trans = rpcOpen(&rpcInit);
    return trans != NULL ? true : false;
  }
  bool startSrv() {
    trans = NULL;
    rpcInit.connType = TAOS_CONN_SERVER;
    trans = rpcOpen(&rpcInit);
    return trans != NULL ? true : false;
  }

  bool sendAndRecv() {
    SEpSet epSet = {0};
    epSet.inUse = 0;
    addEpIntoEpSet(&epSet, "192.168.1.1", 7000);
    addEpIntoEpSet(&epSet, "192.168.0.1", 7000);

    if (trans == NULL) {
      return false;
    }
    SRpcMsg rpcMsg = {0}, reqMsg = {0};
    reqMsg.pCont = rpcMallocCont(10);
    reqMsg.contLen = 10;
    reqMsg.ahandle = NULL;
    rpcSendRecv(trans, &epSet, &reqMsg, &rpcMsg);
    int code = rpcMsg.code;
    std::cout << tstrerror(code) << std::endl;
    return true;
  }
  bool stop() {
    rpcClose(trans);
    trans = NULL;
    return true;
  }

 private:
  void *   trans;
  SRpcInit rpcInit;
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
TEST_F(TransEnv, test_start_stop) {
  assert(tr->startCli());
  assert(tr->sendAndRecv());
  assert(tr->stop());

  assert(tr->startSrv());
  assert(tr->stop());
}
