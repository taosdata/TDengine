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

#include "deploy.h"

void initLog(const char* path) {
  dDebugFlag = 143;
  vDebugFlag = 0;
  mDebugFlag = 143;
  cDebugFlag = 0;
  jniDebugFlag = 0;
  tmrDebugFlag = 0;
  sdbDebugFlag = 0;
  httpDebugFlag = 0;
  mqttDebugFlag = 0;
  monDebugFlag = 0;
  uDebugFlag = 0;
  rpcDebugFlag = 0;
  odbcDebugFlag = 0;
  qDebugFlag = 0;
  wDebugFlag = 0;
  sDebugFlag = 0;
  tsdbDebugFlag = 0;
  cqDebugFlag = 0;

  taosMkDir(path);

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

void initOption(SDnodeOpt* pOption, const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
  pOption->sver = 1;
  pOption->numOfCores = 1;
  pOption->numOfSupportMnodes = 1;
  pOption->numOfSupportVnodes = 1;
  pOption->numOfSupportQnodes = 1;
  pOption->statusInterval = 1;
  pOption->numOfThreadsPerCore = 1;
  pOption->ratioOfQueryCores = 1;
  pOption->maxShellConns = 1000;
  pOption->shellActivityTimer = 30;
  pOption->serverPort = port;
  strcpy(pOption->dataDir, path);
  snprintf(pOption->localEp, TSDB_EP_LEN, "%s:%u", fqdn, port);
  snprintf(pOption->localFqdn, TSDB_FQDN_LEN, "%s", fqdn);
  snprintf(pOption->firstEp, TSDB_EP_LEN, "%s", firstEp);
}

SServer* createServer(const char* path, const char* fqdn, uint16_t port, const char* firstEp) {
  taosRemoveDir(path);
  taosMkDir(path);

  SDnodeOpt option = {0};
  initOption(&option, path, fqdn, port, firstEp);

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
  if (pServer == NULL) return;
  if (pServer->threadId != NULL) {
    taosDestoryThread(pServer->threadId);
  }
}

void processClientRsp(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SClient* pClient = (SClient*)parent;
  pClient->pRsp = pMsg;
  uInfo("response:%s from dnode, pCont:%p contLen:%d code:0x%X", taosMsg[pMsg->msgType], pMsg->pCont, pMsg->contLen,
        pMsg->code);
  tsem_post(&pClient->sem);
}

SClient* createClient(const char* user, const char* pass, const char* fqdn, uint16_t port) {
  SClient* pClient = (SClient*)calloc(1, sizeof(SClient));
  ASSERT(pClient);

  char secretEncrypt[32] = {0};
  taosEncryptPass((uint8_t*)pass, strlen(pass), secretEncrypt);

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label = (char*)"DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processClientRsp;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = 30 * 1000;
  rpcInit.user = (char*)user;
  rpcInit.ckey = (char*)"key";
  rpcInit.parent = pClient;
  rpcInit.secret = (char*)secretEncrypt;
  rpcInit.parent = pClient;
  // rpcInit.spi = 1;

  pClient->clientRpc = rpcOpen(&rpcInit);
  ASSERT(pClient->clientRpc);

  tsem_init(&pClient->sem, 0, 0);
  strcpy(pClient->fqdn, fqdn);
  pClient->port = port;

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
  epSet.port[0] = pClient->port;
  memcpy(epSet.fqdn[0], pClient->fqdn, TSDB_FQDN_LEN);

  rpcSendRequest(pClient->clientRpc, &epSet, pMsg, NULL);
  tsem_wait(&pClient->sem);
}
