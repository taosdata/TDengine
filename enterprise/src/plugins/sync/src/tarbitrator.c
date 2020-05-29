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

  int32_t vgId = firstPkt.syncHead.vgId;
  if (vgId) {  
    sTrace("%s:%d, vgId is not zero, close the connection", firstPkt.fqdn, firstPkt.port);
    close(connFd);
    return;
  }

  SNodeConn *pNode = (SNodeConn *) calloc(sizeof(SNodeConn), 1);
  if (pNode == NULL) {
    sError("%s:%d, failed to allocate syncPeer(%s)", firstPkt.fqdn, firstPkt.port, strerror(errno));
    close(connFd);
    return;
  }

  sprintf(pNode->id, "%s:%d", firstPkt.fqdn, firstPkt.port); 
  sTrace("%s:%d, arbitrator request is accepted", firstPkt.fqdn, firstPkt.port);
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

int main(int argc, char *argv[]) {
  
  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      tsServerPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      debugFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-g")==0 && i < argc-1) {
      if (strlen(argv[++i]) > TSDB_FILENAME_LEN) continue; 
      strcpy(arbLogPath, argv[i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p port]: server port number, default is:%d\n", tsServerPort);
      printf("  [-d debugFlag]: debug flag, default:%d\n", debugFlag);
      printf("  [-g logFilePath]: log file pathe, default:%s\n", arbLogPath);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }
 
  tsAsyncLog = 0;
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

  while (1) {
    sleep(1);
  }

  return 0;
}


