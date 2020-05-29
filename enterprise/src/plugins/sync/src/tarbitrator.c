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

//#define _DEFAULT_SOURCE
#include "os.h"
#include "hash.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "ttime.h"
#include "tsocket.h"
#include "tglobal.h"
#include "taoserror.h"
#include "taosTcpPool.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

<<<<<<< HEAD
static char     arbLogPath[TSDB_FILENAME_LEN + 16] = {0};
static ttpool_h tsTcpPool;

typedef struct {
  char  id[TSDB_EP_LEN];
  int   nodeFd;
  void *pThread;
} SNodeConn;

static void arbProcessIncommingConnection(int connFd, uint32_t sourceIp)
{
  char  ipstr[24];
  tinet_ntoa(ipstr, sourceIp);
  sTrace("peer TCP connection from ip:%s", ipstr);

  SFirstPkt firstPkt;
  if (taosReadMsg(connFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt)) {
    sError("failed to read peer first pkt from ip:%s(%s)", ipstr, strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  SNodeConn *pNode = (SNodeConn *) calloc(sizeof(SNodeConn), 1);
  if (pNode == NULL) {
    sError("failed to allocate memory(%s)", strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  sprintf(pNode->id, "vgId:%d peer:%s:%d", firstPkt.sourceId, firstPkt.fqdn, firstPkt.port); 
  if (firstPkt.syncHead.vgId) {  
    sTrace("%s, vgId in head is not zero, close the connection", pNode->id);
    tfree(pNode);
    taosCloseSocket(connFd);
    return;
  }

  sTrace("%s, arbitrator request is accepted", pNode->id);
  pNode->nodeFd = connFd;
  pNode->pThread = taosAllocateTcpThread(tsTcpPool, pNode, connFd);

  return;
}

static void arbProcessBrokenLink(void *param) {
  SNodeConn *pNode = param;

  sTrace("%s, TCP link is broken(%s), close connection", pNode->id, strerror(errno));
  taosFreeTcpThread(pNode->pThread, &pNode->nodeFd);

  tfree(pNode);
}

static int arbProcessPeerMsg(void *param, void *buffer)
{
  SNodeConn  *pNode = param;
  SSyncHead   head;
  int         bytes = 0;
  char       *cont = (char *)buffer;

  int hlen = taosReadMsg(pNode->nodeFd, &head, sizeof(head));
  if (hlen != sizeof(head)) {
    sTrace("%s, failed to read msg, hlen:%d", pNode->id, hlen);
    return -1;
  }

  bytes = taosReadMsg(pNode->nodeFd, cont, head.len);
  if (bytes != head.len) {
    sTrace("%s, failed to read, bytes:%d len:%d", pNode->id, bytes, head.len);
    return -1;
  }

  sTrace("%s, msg is received, len:%d", pNode->id, head.len);
  return 0;
}

=======
char arbitratorLogFilePath[TSDB_FILENAME_LEN + 16] = {0};

>>>>>>> develop
int main(int argc, char *argv[]) {
  
  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      tsServerPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      debugFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-g")==0 && i < argc-1) {
      if (strlen(argv[++i]) > TSDB_FILENAME_LEN) continue; 
<<<<<<< HEAD
      strcpy(arbLogPath, argv[i]);
=======
      strcpy(arbitratorLogFilePath, argv[i]);
>>>>>>> develop
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p port]: server port number, default is:%d\n", tsServerPort);
      printf("  [-d debugFlag]: debug flag, default:%d\n", debugFlag);
<<<<<<< HEAD
      printf("  [-g logFilePath]: log file pathe, default:%s\n", arbLogPath);
=======
      printf("  [-g logFilePath]: log file pathe, default:%s\n", arbitratorLogFilePath);
>>>>>>> develop
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }
 
  tsAsyncLog = 0;
<<<<<<< HEAD
  strcat(arbLogPath, "/arbitrator.log");
  taosInitLog(arbLogPath, 1000000, 10);

  taosGetFqdn(tsNodeFqdn);
  tsSyncPort = tsServerPort + TSDB_PORT_SYNC;

  SPoolInfo info;
  info.numOfThreads = 1;
  info.serverIp = 0;
  info.port = tsSyncPort;
  info.bufferSize = 640000;
  info.processBrokenLink = arbProcessBrokenLink;
  info.processIncomingMsg = arbProcessPeerMsg;
  info.processIncomingConn = arbProcessIncommingConnection;
  tsTcpPool = taosOpenTcpThreadPool(&info);
  
  sPrint("TAOS arbitrator: %s:%d is running\n", tsNodeFqdn, tsServerPort);
=======
  strcat(arbitratorLogFilePath, "/arbitrator.log");
  taosInitLog(arbitratorLogFilePath, 1000000, 10);

  SSyncInfo syncInfo;
  memset(&syncInfo, 0, sizeof(syncInfo));

  syncInfo.syncCfg.replica = 1;
  syncInfo.syncCfg.quorum = 1;
  syncInfo.vgId = 1;
  syncInfo.ahandle = &syncInfo;
  syncInfo.syncCfg.nodeInfo[0].nodeId = 1;
  taosGetFqdn(syncInfo.syncCfg.nodeInfo[0].nodeFqdn);
  syncInfo.syncCfg.nodeInfo[0].nodePort = tsServerPort + TSDB_PORT_SYNC;
  tsSyncPort = tsServerPort + TSDB_PORT_SYNC;

  void *syncHandle = syncStart(&syncInfo);
  if (syncHandle == NULL) {
    uError("failed to init arbitrator");
    return -1;
  }

  uPrint("TAOS arbitrator: %s:%d is running\n", syncInfo.syncCfg.nodeInfo[0].nodeFqdn, tsServerPort);
>>>>>>> develop

  while (1) {
    sleep(1);
  }

  return 0;
}


