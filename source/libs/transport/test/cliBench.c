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

#include "os.h"
#include "taoserror.h"
#include "tglobal.h"
#include "transLog.h"
#include "trpc.h"
#include "tutil.h"
#include "tversion.h"

typedef struct {
  int      index;
  SEpSet   epSet;
  int      num;
  int      numOfReqs;
  int      msgSize;
  tsem_t   rspSem;
  tsem_t  *pOverSem;
  TdThread thread;
  void    *pRpc;
} SInfo;

void initLogEnv() {
  const char   *logDir = "/tmp/trans_cli";
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10000;
  tsAsyncLog = 0;
  // idxDebugFlag = 143;
  strcpy(tsLogDir, (char *)logDir);
  taosRemoveDir(tsLogDir);
  taosMkDir(tsLogDir);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

static void processResponse(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SInfo *pInfo = (SInfo *)pMsg->info.ahandle;
  tDebug("thread:%d, response is received, type:%d contLen:%d code:0x%x", pInfo->index, pMsg->msgType, pMsg->contLen,
         pMsg->code);

  if (pEpSet) pInfo->epSet = *pEpSet;

  rpcFreeCont(pMsg->pCont);
  tsem_post(&pInfo->rspSem);
}

static int tcount = 0;

static void *sendRequest(void *param) {
  SInfo  *pInfo = (SInfo *)param;
  SRpcMsg rpcMsg = {0};

  tDebug("thread:%d, start to send request", pInfo->index);

  while (pInfo->numOfReqs == 0 || pInfo->num < pInfo->numOfReqs) {
    pInfo->num++;
    rpcMsg.pCont = rpcMallocCont(pInfo->msgSize);
    rpcMsg.contLen = pInfo->msgSize;
    rpcMsg.info.ahandle = pInfo;
    rpcMsg.info.noResp = 1;
    rpcMsg.msgType = 1;
    tDebug("thread:%d, send request, contLen:%d num:%d", pInfo->index, pInfo->msgSize, pInfo->num);
    rpcSendRequest(pInfo->pRpc, &pInfo->epSet, &rpcMsg, NULL);
    if (pInfo->num % 20000 == 0) tInfo("thread:%d, %d requests have been sent", pInfo->index, pInfo->num);
    // tsem_wait(&pInfo->rspSem);
  }

  tDebug("thread:%d, it is over", pInfo->index);
  tcount++;

  return NULL;
}

int main(int argc, char *argv[]) {
  SRpcInit       rpcInit;
  SEpSet         epSet;
  int            msgSize = 128;
  int            numOfReqs = 0;
  int            appThreads = 1;
  char           serverIp[40] = "127.0.0.1";
  struct timeval systemTime;
  int64_t        startTime, endTime;

  // server info
  epSet.numOfEps = 1;
  epSet.inUse = 0;
  epSet.eps[0].port = 7000;
  epSet.eps[1].port = 7000;
  strcpy(epSet.eps[0].fqdn, serverIp);
  strcpy(epSet.eps[1].fqdn, "192.168.0.1");

  // client info
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "APP";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processResponse;
  rpcInit.sessions = 100;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.user = "michael";

  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.connLimitNum = 10;
  rpcInit.connLimitLock = 1;
  rpcInit.batchSize = 16 * 1024;
  rpcInit.supportBatch = 1;

  rpcDebugFlag = 135;
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-p") == 0 && i < argc - 1) {
    } else if (strcmp(argv[i], "-i") == 0 && i < argc - 1) {
    } else if (strcmp(argv[i], "-t") == 0 && i < argc - 1) {
      rpcInit.numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-m") == 0 && i < argc - 1) {
      msgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0 && i < argc - 1) {
      rpcInit.sessions = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0 && i < argc - 1) {
      numOfReqs = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0 && i < argc - 1) {
      appThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-o") == 0 && i < argc - 1) {
      tsCompressMsgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-u") == 0 && i < argc - 1) {
    } else if (strcmp(argv[i], "-k") == 0 && i < argc - 1) {
    } else if (strcmp(argv[i], "-spi") == 0 && i < argc - 1) {
    } else if (strcmp(argv[i], "-d") == 0 && i < argc - 1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-i ip]: first server IP address, default is:%s\n", serverIp);
      printf("  [-t threads]: number of rpc threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-a threads]: number of app threads, default is:%d\n", appThreads);
      printf("  [-n requests]: number of requests per thread, default is:%d\n", numOfReqs);
      printf("  [-u user]: user name for the connection, default is:%s\n", rpcInit.user);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }

  initLogEnv();
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));
  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    tError("failed to initialize RPC");
    return -1;
  }

  tInfo("client is initialized");
  tInfo("threads:%d msgSize:%d requests:%d", appThreads, msgSize, numOfReqs);

  int64_t now = taosGetTimestampUs();

  SInfo *pInfo = (SInfo *)taosMemoryCalloc(1, sizeof(SInfo) * appThreads);
  SInfo *p = pInfo;
  for (int i = 0; i < appThreads; ++i) {
    pInfo->index = i;
    pInfo->epSet = epSet;
    pInfo->numOfReqs = numOfReqs;
    pInfo->msgSize = msgSize;
    tsem_init(&pInfo->rspSem, 0, 0);
    pInfo->pRpc = pRpc;

    taosThreadCreate(&pInfo->thread, NULL, sendRequest, pInfo);
    pInfo++;
  }

  do {
    taosUsleep(1);
  } while (tcount < appThreads);

  float usedTime = (taosGetTimestampUs() - now) / 1000.0f;

  tInfo("it takes %.3f mseconds to send %d requests to server", usedTime, numOfReqs * appThreads);
  tInfo("Performance: %.3f requests per second, msgSize:%d bytes", 1000.0 * numOfReqs * appThreads / usedTime, msgSize);

  for (int i = 0; i < appThreads; i++) {
    SInfo *pInfo = p;
    taosThreadJoin(pInfo->thread, NULL);
    p++;
  }
  int ch = getchar();
  UNUSED(ch);

  taosCloseLog();

  return 0;
}
