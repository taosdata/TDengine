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

#define _DEFAULT_SOURCE

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include "dnodeSystem.h"
#include "mgmt.h"
#include "tsdb.h"

extern void *      mgmtStatisticTimer;
extern void *      pDnodeConn;
extern void *      pShellConn;
extern void **     rpcQhandle;
extern SMgmtIpList mgmtIpList;
extern SMgmtIpList mgmtPublicIpList;
extern char        mgmtIpStr[TSDB_MAX_MGMT_IPS][20];
extern bool        tsClusterExist;
extern void *      acctSdb;

int   mgmtInitRedirect();
void  mgmtCleanUpRedirect();
void *mgmtRedirectAllMsgs(char *msg, void *ahandle, void *thandle);
void  mgmtSdbWorkAsMasterCallback();
void  mgmtSetDnodeOfflineOnSdbChanged();

int mgmtInitSystem() {
  struct stat dirstat;
  sdbWorkAsMasterCallback = mgmtSdbWorkAsMasterCallback;

  if (stat(mgmtDirectory, &dirstat) && strcmp(tsMasterIp, tsPrivateIp)) {
    return mgmtInitRedirect();
  } else if (tsClusterExist) {
    return mgmtInitRedirect();
  } else {
    return mgmtStartSystem();
  }
}

int mgmtStartCheckMgmtRunning() {

  if (tsModuleStatus & (1 << TSDB_MOD_MGMT)) {
    return -1;
  }

  tsetModuleStatus(TSDB_MOD_MGMT);

  mgmtCleanUpRedirect();

  strcpy(sdbMasterIp, mgmtIpStr[0]);
  strcpy(sdbPrivateIp, tsPrivateIp);
  sdbPeerPort = tsMgmtMgmtPort;
  sdbSyncPort = tsMgmtSyncPort;
  sdbHbTimer = tsMgmtPeerHBTimer;
  sdbPublicIp = inet_addr(tsPublicIp);

  return 0;
}

void mgmtStartMgmtTimer() {
}

void mgmtDoStatistic(void *handle, void *tmrId) {
  SAcctObj *pAcct = NULL;
  void *    pNode = NULL;
  mgmtStatisticTimer = NULL;

  int64_t totalStorage = 0;
  while (1) {
    pNode = sdbFetchRow(acctSdb, pNode, (void **)&pAcct);
    if (pAcct == NULL) break;
    totalStorage += mgmtGetAcctStatistic(pAcct);
  }

  grantResetCurStorage(totalStorage);
  taosTmrReset(mgmtDoStatistic, tsStatusInterval * 30000, NULL, mgmtTmr, &mgmtStatisticTimer);
}

void mgmtStopSystem() {
  if (sdbMaster) {
    mTrace("it is a master mgmt node, it could not be stopped");
    return;
  }

  mgmtCleanUpSystem();
  remove(mgmtDirectory);
  mgmtInitRedirect();
}

int mgmtInitRedirect() {
  SRpcInit rpcInit;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsPrivateIp;
  rpcInit.localPort = tsMgmtVnodePort;
  rpcInit.label = "MND-dnode";
  rpcInit.numOfThreads = 1;
  rpcInit.fp = mgmtRedirectAllMsgs;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = 100;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_C();
  rpcInit.qhandle = rpcQhandle[0];

  pDnodeConn = taosOpenRpc(&rpcInit);
  if (pDnodeConn == NULL) {
    mError("failed to init tcp connection to vnode");
    return -1;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = tsInternalIp;
  rpcInit.localPort = tsMgmtShellPort;
  rpcInit.label = "MND-shell";
  rpcInit.numOfThreads = 1;
  rpcInit.fp = mgmtRedirectAllMsgs;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = 100;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_C();
  rpcInit.qhandle = rpcQhandle[0];

  pShellConn = taosOpenRpc(&rpcInit);
  if (pShellConn == NULL) {
    mError("failed to init tcp connection to shell");
    return -1;
  }

  mPrint("all mgmt messages will be redirected by this node");
  return 0;
}

void mgmtCleanUpRedirect() {
  if (pDnodeConn) taosCloseRpc(pDnodeConn);
  pDnodeConn = NULL;

  if (pShellConn) taosCloseRpc(pShellConn);
  pShellConn = NULL;
}

void *mgmtRedirectAllMsgs(char *msg, void *ahandle, void *thandle) {
  char *    pMsg, *pStart;
  int       msgLen;
  STaosRsp *pRsp;

  SIntMsg *pHead = (SIntMsg *)msg;

  pStart = taosBuildRspMsgWithSize(thandle, pHead->msgType + 1, 128);
  if (pStart == NULL) return 0;

  pMsg = pStart;
  pRsp = (STaosRsp *)pMsg;
  pRsp->code = TSDB_CODE_REDIRECT;
  pMsg = pRsp->more;

  SIpList *pIpList = (SIpList *)pMsg;
  pIpList->numOfIps = mgmtPublicIpList.numOfIps;
  int size = mgmtPublicIpList.numOfIps * 4;
  memcpy(pIpList->ip, mgmtPublicIpList.ip, size);
  pMsg += sizeof(SIpList) + size;

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(thandle, pStart, msgLen);

  return NULL;
}

void mgmtSdbWorkAsMasterCallback() {
  mgmtSetDnodeOfflineOnSdbChanged();
  grantReset();
}
