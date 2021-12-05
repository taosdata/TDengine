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

#include "dndTestDeploy.h"

void initLog(char *path) {
  mDebugFlag = 207;
  char temp[PATH_MAX];
  snprintf(temp, PATH_MAX, "%s/taosdlog", path);
  if (taosInitLog(temp, tsNumOfLogLines, 1) != 0) {
    printf("failed to init log file\n");
  }
}

void* runServer(void* param) {
  SServer* pServer = (SServer*)param;
  while (1) {
    taosMsleep(100);
    pthread_testcancel();
  }
}

void initOption(SDnodeOpt* pOption, char *path) {
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
  strcpy(pOption->dataDir, path);
  strcpy(pOption->localEp, "localhost:9527");
  strcpy(pOption->localFqdn, "localhost");
  strcpy(pOption->firstEp, "localhost:9527");

  taosRemoveDir(path);
  taosMkDir(path);
}

SServer* createServer(char *path) {
  SDnodeOpt option = {0};
  initOption(&option, path);

  SDnode* pDnode = dndInit(&option);
  ASSERT(pDnode);

  SServer* pServer = (SServer*)calloc(1, sizeof(SServer));
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

void processClientRsp(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SClient* pClient = (SClient*)parent;
  pClient->pRsp = pMsg;
  //taosMsleep(1000000);
  tsem_post(&pClient->sem);
}

SClient* createClient(char *user, char *pass) {
  SClient* pClient = (SClient*)calloc(1, sizeof(SClient));
  ASSERT(pClient);

  char secretEncrypt[32] = {0};
  taosEncryptPass((uint8_t*)pass, strlen(pass), secretEncrypt);

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processClientRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = 30 * 1000;
  rpcInit.user = user;
  rpcInit.ckey = "key";
  rpcInit.parent = pClient;
  rpcInit.secret = (char*)secretEncrypt;
  rpcInit.parent = pClient;
  // rpcInit.spi = 1;

  pClient->clientRpc = rpcOpen(&rpcInit);
  ASSERT(pClient->clientRpc);

  tsem_init(&pClient->sem, 0, 0);

  return pClient;
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

  rpcSendRequest(pClient->clientRpc, &epSet, pMsg, NULL);
  tsem_wait(&pClient->sem);
}
