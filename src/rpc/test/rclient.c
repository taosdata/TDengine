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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include "os.h"
#include "tlog.h"
#include "trpc.h"
#include "taoserror.h"
#include <stdint.h>
#include <unistd.h>

typedef struct {
  int       index;
  SRpcIpSet ipSet;
  int       num;
  int       numOfReqs;
  int       msgSize;
  sem_t     rspSem; 
  sem_t    *pOverSem; 
  pthread_t thread;
  void     *pRpc;
} SInfo;

void processResponse(char type, void *pCont, int contLen, void *ahandle, int32_t code) {
  SInfo *pInfo = (SInfo *)ahandle;
  tTrace("thread:%d, response is received, type:%d contLen:%d code:0x%x", pInfo->index, type, contLen, code);

  if (pCont) rpcFreeCont(pCont);

  sem_post(&pInfo->rspSem); 
}

void processUpdateIpSet(void *handle, SRpcIpSet *pIpSet) {
  SInfo *pInfo = (SInfo *)handle;

  tTrace("thread:%d, ip set is changed, index:%d", pInfo->index, pIpSet->index);
  pInfo->ipSet = *pIpSet;
}

int tcount = 0;

void *sendRequest(void *param) {
  SInfo *pInfo = (SInfo *)param;
  char  *cont;
  
  tTrace("thread:%d, start to send request", pInfo->index);

  while ( pInfo->numOfReqs == 0 || pInfo->num < pInfo->numOfReqs) {
    pInfo->num++;
    cont = rpcMallocCont(pInfo->msgSize);
    tTrace("thread:%d, send request, contLen:%d num:%d", pInfo->index, pInfo->msgSize, pInfo->num);
    rpcSendRequest(pInfo->pRpc, &pInfo->ipSet, 1, cont, pInfo->msgSize, pInfo);
    if ( pInfo->num % 20000 == 0 ) 
      tPrint("thread:%d, %d requests have been sent", pInfo->index, pInfo->num);
    sem_wait(&pInfo->rspSem);
  }

  tTrace("thread:%d, it is over", pInfo->index);
  tcount++;

  return NULL;
}

int main(int argc, char *argv[]) {
  SRpcInit  rpcInit;
  SRpcIpSet ipSet;
  int      msgSize = 128;
  int      numOfReqs = 0;
  int      appThreads = 1;
  char     socketType[20] = "udp";
  char     serverIp[40] = "127.0.0.1";
  struct   timeval systemTime;
  int64_t  startTime, endTime;
  pthread_attr_t thattr;

  // server info
  ipSet.numOfIps = 1;
  ipSet.index = 0;
  ipSet.port = 7000;
  ipSet.ip[0] = inet_addr(serverIp);
  ipSet.ip[1] = inet_addr("192.168.0.1");

  // client info
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = "0.0.0.0";
  rpcInit.localPort    = 0;
  rpcInit.label        = "APP";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = processResponse;
  rpcInit.ufp          = processUpdateIpSet;
  rpcInit.sessions     = 100;
  rpcInit.idleTime     = 2000;
  rpcInit.user         = "michael";
  rpcInit.secret       = "mypassword";
  rpcInit.ckey         = "key";

  for (int i=1; i<argc; ++i) { 
    if ( strcmp(argv[i], "-c")==0 && i < argc-1 ) {
      strcpy(socketType, argv[++i]); 
    } else if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      ipSet.port = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i") ==0 && i < argc-1) {
      ipSet.ip[0] = inet_addr(argv[++i]); 
    } else if (strcmp(argv[i], "-l")==0 && i < argc-1) {
      strcpy(rpcInit.localIp, argv[++i]); 
    } else if (strcmp(argv[i], "-t")==0 && i < argc-1) {
      rpcInit.numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-m")==0 && i < argc-1) {
      msgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s")==0 && i < argc-1) {
      rpcInit.sessions = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "-n")==0 && i < argc-1) {
      numOfReqs = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "-a")==0 && i < argc-1) {
      appThreads = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-o")==0 && i < argc-1) {
      tsCompressMsgSize = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-c ctype]: connection type:udp or tpc, default is:%s\n", socketType);
      printf("  [-i ip]: first server IP address, default is:%s\n", serverIp);
      printf("  [-p port]: server port number, default is:%d\n", ipSet.port);
      printf("  [-t threads]: number of rpc threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-s sessions]: number of rpc sessions, default is:%d\n", rpcInit.sessions);
      printf("  [-l localIp]: local IP address, default is:%s\n", rpcInit.localIp);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-a threads]: number of app threads, default is:%d\n", appThreads);
      printf("  [-n requests]: number of requests per thread, default is:%d\n", numOfReqs);
      printf("  [-o compSize]: compression message size, default is:%d\n", tsCompressMsgSize);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }

  rpcInit.connType = strcasecmp(socketType, "udp") == 0 ? TAOS_CONN_UDPC : TAOS_CONN_TCPC;
  taosInitLog("client.log", 100000, 10);

  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    dError("failed to initialize RPC");
    return -1;
  }

  tPrint("client is initialized");

  gettimeofday(&systemTime, NULL);
  startTime = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  SInfo *pInfo = (SInfo *)calloc(1, sizeof(SInfo)*appThreads);
 
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i=0; i<appThreads; ++i) {
    pInfo->index = i;
    pInfo->ipSet = ipSet;
    pInfo->numOfReqs = numOfReqs;
    pInfo->msgSize = msgSize;
    sem_init(&pInfo->rspSem, 0, 0);
    pInfo->pRpc = pRpc;
    pthread_create(&pInfo->thread, &thattr, sendRequest, pInfo);
    pInfo++;
  }

  do {
    usleep(1);
  } while ( tcount < appThreads);

  gettimeofday(&systemTime, NULL);
  endTime = systemTime.tv_sec*1000000 + systemTime.tv_usec;  
  float usedTime = (endTime - startTime)/1000.0;  // mseconds

  tPrint("it takes %.3f mseconds to send %d requests to server", usedTime, numOfReqs*appThreads);
  tPrint("Performance: %.3f requests per second, msgSize:%d bytes", 1000*numOfReqs*appThreads/usedTime, msgSize);

  taosCloseLog();

  return 0;
}


