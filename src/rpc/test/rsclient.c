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
#include "tutil.h"
#include "tglobal.h"
#include "rpcLog.h"
#include "trpc.h"
#include "taoserror.h"

typedef struct {
  int       index;
  SRpcEpSet epSet;
  int       num;
  int       numOfReqs;
  int       msgSize;
  tsem_t    rspSem; 
  tsem_t   *pOverSem; 
  pthread_t thread;
  void     *pRpc;
} SInfo;


static int tcount = 0;
static int terror = 0;

static void *sendRequest(void *param) {
  SInfo  *pInfo = (SInfo *)param;
  SRpcMsg rpcMsg, rspMsg;
  
  setThreadName("sendSrvReq");
 
  tDebug("thread:%d, start to send request", pInfo->index);

  while ( pInfo->numOfReqs == 0 || pInfo->num < pInfo->numOfReqs) {
    pInfo->num++;
    rpcMsg.pCont = rpcMallocCont(pInfo->msgSize);
    rpcMsg.contLen = pInfo->msgSize;
    rpcMsg.handle = pInfo;
    rpcMsg.msgType = 1;
    tDebug("thread:%d, send request, contLen:%d num:%d", pInfo->index, pInfo->msgSize, pInfo->num);

    rpcSendRecv(pInfo->pRpc, &pInfo->epSet, &rpcMsg, &rspMsg);
    
    // handle response
    if (rspMsg.code != 0) terror++;

    tDebug("thread:%d, rspLen:%d code:%d", pInfo->index, rspMsg.contLen, rspMsg.code);

    rpcFreeCont(rspMsg.pCont);

    if ( pInfo->num % 20000 == 0 ) 
      tInfo("thread:%d, %d requests have been sent", pInfo->index, pInfo->num);
  }

  tDebug("thread:%d, it is over", pInfo->index);
  tcount++;

  return NULL;
}

int main(int argc, char *argv[]) {
  SRpcInit  rpcInit;
  SRpcEpSet epSet;
  int      msgSize = 128;
  int      numOfReqs = 0;
  int      appThreads = 1;
  char     serverIp[40] = "127.0.0.1";
  char     secret[TSDB_KEY_LEN] = "mypassword";
  struct   timeval systemTime;
  int64_t  startTime, endTime;
  pthread_attr_t thattr;

  // server info
  epSet.numOfEps = 1;
  epSet.inUse = 0;
  epSet.port[0] = 7000;
  epSet.port[1] = 7000;
  strcpy(epSet.fqdn[0], serverIp);
  strcpy(epSet.fqdn[1], "192.168.0.1");

  // client info
  memset(&rpcInit, 0, sizeof(rpcInit));
  //rpcInit.localIp      = "0.0.0.0";
  rpcInit.localPort    = 0;
  rpcInit.label        = "APP";
  rpcInit.numOfThreads = 1;
  rpcInit.sessions     = 100;
  rpcInit.idleTime     = tsShellActivityTimer*1000;
  rpcInit.user         = "michael";
  rpcInit.secret       = secret;
  rpcInit.ckey         = "key";
  rpcInit.spi          = 1;
  rpcInit.connType     = TAOS_CONN_CLIENT;

  for (int i=1; i<argc; ++i) { 
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      epSet.port[0] = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i") ==0 && i < argc-1) {
      tstrncpy(epSet.fqdn[0], argv[++i], sizeof(epSet.fqdn[0])); 
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
    } else if (strcmp(argv[i], "-o")==0 && i < argc-1) {
      tsCompressMsgSize = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "-u")==0 && i < argc-1) {
      rpcInit.user = argv[++i];
    } else if (strcmp(argv[i], "-k")==0 && i < argc-1) {
      rpcInit.secret = argv[++i];
    } else if (strcmp(argv[i], "-spi")==0 && i < argc-1) {
      rpcInit.spi = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-i ip]: first server IP address, default is:%s\n", serverIp);
      printf("  [-p port]: server port number, default is:%d\n", epSet.port[0]);
      printf("  [-t threads]: number of rpc threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-s sessions]: number of rpc sessions, default is:%d\n", rpcInit.sessions);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-a threads]: number of app threads, default is:%d\n", appThreads);
      printf("  [-n requests]: number of requests per thread, default is:%d\n", numOfReqs);
      printf("  [-o compSize]: compression message size, default is:%d\n", tsCompressMsgSize);
      printf("  [-u user]: user name for the connection, default is:%s\n", rpcInit.user);
      printf("  [-k secret]: password for the connection, default is:%s\n", rpcInit.secret);
      printf("  [-spi SPI]: security parameter index, default is:%d\n", rpcInit.spi);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }

  taosInitLog("client.log", 100000, 10);

  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    tError("failed to initialize RPC");
    return -1;
  }

  tInfo("client is initialized");

  gettimeofday(&systemTime, NULL);
  startTime = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  SInfo *pInfo = (SInfo *)calloc(1, sizeof(SInfo)*appThreads);
 
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i=0; i<appThreads; ++i) {
    pInfo->index = i;
    pInfo->epSet = epSet;
    pInfo->numOfReqs = numOfReqs;
    pInfo->msgSize = msgSize;
    tsem_init(&pInfo->rspSem, 0, 0);
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

  tInfo("it takes %.3f mseconds to send %d requests to server, error num:%d", usedTime, numOfReqs*appThreads, terror);
  tInfo("Performance: %.3f requests per second, msgSize:%d bytes", 1000.0*numOfReqs*appThreads/usedTime, msgSize);

  taosCloseLog();

  return 0;
}


