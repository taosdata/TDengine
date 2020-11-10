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
#include "tsocket.h"
#include "tglobal.h"
#include "taoserror.h"
#include "taosTcpPool.h"
#include "twal.h"
#include "tsync.h"
#include "syncInt.h"

static void     arbSignalHandler(int32_t signum, siginfo_t *sigInfo, void *context);
static void     arbProcessIncommingConnection(int32_t connFd, uint32_t sourceIp);
static void     arbProcessBrokenLink(void *param);
static int32_t  arbProcessPeerMsg(void *param, void *buffer);
static tsem_t   tsArbSem;
static ttpool_h tsArbTcpPool;

typedef struct {
  char    id[TSDB_EP_LEN + 24];
  int32_t nodeFd;
  void *  pConn;
} SNodeConn;

int32_t main(int32_t argc, char *argv[]) {
  char arbLogPath[TSDB_FILENAME_LEN + 16] = {0};

  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-p") == 0 && i < argc - 1) {
      tsArbitratorPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d") == 0 && i < argc - 1) {
      debugFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-g") == 0 && i < argc - 1) {
      if (strlen(argv[++i]) > TSDB_FILENAME_LEN) continue;
      tstrncpy(arbLogPath, argv[i], sizeof(arbLogPath));
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p port]: arbitrator server port number, default is:%d\n", tsServerPort + TSDB_PORT_ARBITRATOR);
      printf("  [-d debugFlag]: debug flag, option 131 | 135 | 143, default:0\n");
      printf("  [-g logFilePath]: log file pathe, default:/arbitrator.log\n");
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }

  sDebugFlag = debugFlag;

  if (tsem_init(&tsArbSem, 0, 0) != 0) {
    printf("failed to create exit semphore\n");
    exit(EXIT_FAILURE);
  }

  /* Set termination handler. */
  struct sigaction act = {{0}};
  act.sa_flags = SA_SIGINFO;
  act.sa_sigaction = arbSignalHandler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGHUP, &act, NULL);
  sigaction(SIGINT, &act, NULL);

  tsAsyncLog = 0;
  strcat(arbLogPath, "/arbitrator.log");
  taosInitLog(arbLogPath, 1000000, 10);

  taosGetFqdn(tsNodeFqdn);

  SPoolInfo info;
  info.numOfThreads = 1;
  info.serverIp = 0;
  info.port = tsArbitratorPort;
  info.bufferSize = 640000;
  info.processBrokenLink = arbProcessBrokenLink;
  info.processIncomingMsg = arbProcessPeerMsg;
  info.processIncomingConn = arbProcessIncommingConnection;
  tsArbTcpPool = taosOpenTcpThreadPool(&info);

  if (tsArbTcpPool == NULL) {
    sDebug("failed to open TCP thread pool, exit...");
    return -1;
  }

  sInfo("TAOS arbitrator: %s:%d is running", tsNodeFqdn, tsArbitratorPort);

  tsem_wait(&tsArbSem);

  taosCloseTcpThreadPool(tsArbTcpPool);
  sInfo("TAOS arbitrator is shut down\n");
  closelog();

  return 0;
}

static void arbProcessIncommingConnection(int32_t connFd, uint32_t sourceIp) {
  char ipstr[24];
  tinet_ntoa(ipstr, sourceIp);
  sDebug("peer TCP connection from ip:%s", ipstr);

  SFirstPkt firstPkt;
  if (taosReadMsg(connFd, &firstPkt, sizeof(firstPkt)) != sizeof(firstPkt)) {
    sError("failed to read peer first pkt from ip:%s(%s)", ipstr, strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  SNodeConn *pNode = (SNodeConn *)calloc(sizeof(SNodeConn), 1);
  if (pNode == NULL) {
    sError("failed to allocate memory(%s)", strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  firstPkt.fqdn[sizeof(firstPkt.fqdn) - 1] = 0;
  snprintf(pNode->id, sizeof(pNode->id), "vgId:%d peer:%s:%d", firstPkt.sourceId, firstPkt.fqdn, firstPkt.port);
  if (firstPkt.syncHead.vgId) {
    sDebug("%s, vgId in head is not zero, close the connection", pNode->id);
    tfree(pNode);
    taosCloseSocket(connFd);
    return;
  }

  sDebug("%s, arbitrator request is accepted", pNode->id);
  pNode->nodeFd = connFd;
  pNode->pConn = taosAllocateTcpConn(tsArbTcpPool, pNode, connFd);

  return;
}

static void arbProcessBrokenLink(void *param) {
  SNodeConn *pNode = param;

  sDebug("%s, TCP link is broken(%s), close connection", pNode->id, strerror(errno));
  tfree(pNode);
}

static int32_t arbProcessPeerMsg(void *param, void *buffer) {
  SNodeConn *pNode = param;
  SSyncHead  head;
  int32_t    bytes = 0;
  char *     cont = (char *)buffer;

  int32_t hlen = taosReadMsg(pNode->nodeFd, &head, sizeof(head));
  if (hlen != sizeof(head)) {
    sDebug("%s, failed to read msg, hlen:%d", pNode->id, hlen);
    return -1;
  }

  bytes = taosReadMsg(pNode->nodeFd, cont, head.len);
  if (bytes != head.len) {
    sDebug("%s, failed to read, bytes:%d len:%d", pNode->id, bytes, head.len);
    return -1;
  }

  sDebug("%s, msg is received, len:%d", pNode->id, head.len);
  return 0;
}

static void arbSignalHandler(int32_t signum, siginfo_t *sigInfo, void *context) {
  struct sigaction act = {{0}};
  act.sa_handler = SIG_IGN;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGHUP, &act, NULL);
  sigaction(SIGINT, &act, NULL);

  sInfo("shut down signal is %d, sender PID:%d", signum, sigInfo->si_pid);

  // inform main thread to exit
  tsem_post(&tsArbSem);
}
