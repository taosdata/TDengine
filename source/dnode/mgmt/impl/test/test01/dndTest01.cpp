/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
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
#include <iostream>
#include <queue>
#include "tthread.h"

#include "dnode.h"
#include "trpc.h"

typedef struct {
  SDnode*    pDnode;
  pthread_t* threadId;
} SServer;

void* runServer(void* param) {
  SServer* pServer = param;
  while (1) {
    taosMsleep(100);
    pthread_testcancel();
  }
}

void initOption(SDnodeOpt* pOption) {
  pOption->sver = 1;
  pOption->numOfCores = 1;
  pOption->numOfSupportMnodes = 1;
  pOption->numOfSupportVnodes = 1;
  pOption->numOfSupportQnodes = 1;
  pOption->statusInterval = 1;
  pOption->mnodeEqualVnodeNum = 1;
  pOption->numOfThreadsPerCore = 1;
  pOption->ratioOfQueryCores = 1;
  pOption->maxShellConns = 1000;
  pOption->shellActivityTimer = 30;
  pOption->serverPort = 9527;
  strncpy(pOption->dataDir, "./test01");
  strncpy(pOption->localEp, "localhost:9527");
  strncpy(pOption->localFqdn, "localhost");
  tstrncpy(pOption->firstEp, "localhost:9527");
}

Server* createServer() {
  SDnodeOpt option = {0};
  initOption(&option);

  SDnode* pDnode = dndInit(&option);
  ASSERT(pDnode);

  Server* pServer = calloc(1, sizeof(SServer));
  ASSERT(pServer);

  pServer->pDnode = pDnode;
  pServer->threadId = taosCreateThread(runServer, pServer);
  ASSERT(pServer->threadId);

  return pServer;
}

void dropServer(SServer* pServer) {
  if (pServer->threadId != NULL) {
    taosDestoryThread(pServer->threadId);
  }
}

typedef struct {
  void*    clientRpc;
  SRpcMsg* pRsp;
  tsem_t   sem;
} SClient;

static void processClientRsp(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SClient* pClient = parent;
  pClient->pRsp = pMsg;
  tsem_post(pMgmt->clientRpc);
}

SClient* createClient() {
  SClient* pClient = calloc(1, sizeof(SClient));
  ASSERT(pClient);

  char  secretEncrypt[32] = {0};
  char* pass = "taosdata";
  taosEncryptPass((uint8_t*)pass, strlen(pass), secretEncrypt);

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processClientRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = 30 * 1000;
  rpcInit.user = "root";
  rpcInit.ckey = "key";
  rpcInit.parent = pDnode;
  rpcInit.secret = (char*)secretEncrypt;
  rpcInit.parent = pClient;
  // rpcInit.spi = 1;

  pClient->clientRpc = rpcOpen(&rpcInit);
  ASSERT(pClient->clientRpc);

  tsem_init(&pClient->sem, 0, 0);
}

void dropClient(SClient* pClient) {
  tsem_destroy(&pClient->sem);
  rpcClose(pClient->clientRpc);
}

void sendMsg(SClient* pClient, SRpcMsg* pMsg) {
  SEpSet epSet = {0};
  epSet.inUse = 0;
  epSet.numOfEps = 1;
  epSet.port[0] = 9527;
  strcpy(epSet.fqdn[0], "localhost");

  rpcSendRequest(pMgmt->clientRpc, &epSet, pMsg, NULL);
  tsem_wait(pMgmt->clientRpc);
}

class DndTest01 : public ::testing::Test {
 protected:
  void SetUp() override {
    pServer = createServer();
    pClient = createClient();
  }
  void TearDown() override {
    dropServer(pServer);
    dropClient(pClient);
  }

  SServer* pServer;
  SClient* pClient;
};

TEST_F(DndTest01, connectMsg) {
  SConnectMsg *pReq = rpcMallocCont()    


}
