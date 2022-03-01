/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include "mndTelem.h"
#include "mndCluster.h"
#include "mndSync.h"
#include "tbuffer.h"
#include "tjson.h"

#define TELEMETRY_SERVER "localhost"
#define TELEMETRY_PORT   80

static void mndBuildRuntimeInfo(SMnode* pMnode, SJson* pJson) {
  SMnodeLoad load = {0};
  if (mndGetLoad(pMnode, &load) != 0) return;

  tjsonAddDoubleToObject(pJson, "numOfDnode", load.numOfDnode);
  tjsonAddDoubleToObject(pJson, "numOfMnode", load.numOfMnode);
  tjsonAddDoubleToObject(pJson, "numOfVgroup", load.numOfVgroup);
  tjsonAddDoubleToObject(pJson, "numOfDatabase", load.numOfDatabase);
  tjsonAddDoubleToObject(pJson, "numOfSuperTable", load.numOfSuperTable);
  tjsonAddDoubleToObject(pJson, "numOfChildTable", load.numOfChildTable);
  tjsonAddDoubleToObject(pJson, "numOfColumn", load.numOfColumn);
  tjsonAddDoubleToObject(pJson, "numOfPoint", load.totalPoints);
  tjsonAddDoubleToObject(pJson, "totalStorage", load.totalStorage);
  tjsonAddDoubleToObject(pJson, "compStorage", load.compStorage);
}

static char* mndBuildTelemetryReport(SMnode* pMnode) {
  char        tmp[4096] = {0};
  STelemMgmt* pMgmt = &pMnode->telemMgmt;

  SJson* pJson = tjsonCreateObject();
  if (pJson == NULL) return NULL;

  char clusterName[64] = {0};
  mndGetClusterName(pMnode, clusterName, sizeof(clusterName));
  tjsonAddStringToObject(pJson, "instanceId", clusterName);
  tjsonAddDoubleToObject(pJson, "reportVersion", 1);

  if (taosGetOsReleaseName(tmp, sizeof(tmp))) {
    tjsonAddStringToObject(pJson, "os", tmp);
  }

  int32_t numOfCores = 0;
  if (taosGetCpuInfo(tmp, sizeof(tmp), &numOfCores)) {
    tjsonAddStringToObject(pJson, "cpuModel", tmp);
    tjsonAddDoubleToObject(pJson, "numOfCpu", numOfCores);
  } else {
    tjsonAddDoubleToObject(pJson, "numOfCpu", taosGetCpuCores());
  }

  uint64_t memoryKB = 0;
  if (taosGetTotalSysMemoryKB(&memoryKB)) {
    snprintf(tmp, sizeof(tmp), "%" PRIu64 " kB", memoryKB);
    tjsonAddStringToObject(pJson, "memory", tmp);
  }

  tjsonAddStringToObject(pJson, "version", version);
  tjsonAddStringToObject(pJson, "buildInfo", buildinfo);
  tjsonAddStringToObject(pJson, "gitInfo", gitinfo);
  tjsonAddStringToObject(pJson, "email", pMgmt->email);

  mndBuildRuntimeInfo(pMnode, pJson);

  char* pCont = tjsonToString(pJson);
  tjsonDelete(pJson);
  return pCont;
}

static void mndSendTelemetryReport(const char* pCont) {
  int32_t code = -1;
  char    buf[128] = {0};
  SOCKET  fd = 0;
  int32_t contLen = strlen(pCont);

  uint32_t ip = taosGetIpv4FromFqdn(TELEMETRY_SERVER);
  if (ip == 0xffffffff) {
    mError("failed to get telemetry server ip");
    goto SEND_OVER;
  }

  fd = taosOpenTcpClientSocket(ip, TELEMETRY_PORT, 0);
  if (fd < 0) {
    mError("failed to create telemetry socket");
    goto SEND_OVER;
  }

  const char* header =
      "POST /report HTTP/1.1\n"
      "Host: " TELEMETRY_SERVER
      "\n"
      "Content-Type: application/json\n"
      "Content-Length: ";
  if (taosWriteSocket(fd, (void*)header, (int32_t)strlen(header)) < 0) {
    mError("failed to send telemetry header");
    goto SEND_OVER;
  }

  snprintf(buf, sizeof(buf), "%d\n\n", contLen);
  if (taosWriteSocket(fd, buf, (int32_t)strlen(buf)) < 0) {
    mError("failed to send telemetry contlen");
    goto SEND_OVER;
  }

  if (taosWriteSocket(fd, (void*)pCont, contLen) < 0) {
    mError("failed to send telemetry content");
    goto SEND_OVER;
  }

  // read something to avoid nginx error 499
  if (taosReadSocket(fd, buf, 10) < 0) {
    mError("failed to receive telemetry response");
    goto SEND_OVER;
  }

  mInfo("send telemetry to %s:%d, len:%d content: %s", TELEMETRY_SERVER, TELEMETRY_PORT, contLen, pCont);
  code = 0;

SEND_OVER:
  if (code != 0) {
    mError("failed to send telemetry to %s:%d since %s", TELEMETRY_SERVER, TELEMETRY_PORT, terrstr());
  }
  taosCloseSocket(fd);
}

static int32_t mndProcessTelemTimer(SMnodeMsg* pReq) {
  SMnode*     pMnode = pReq->pMnode;
  STelemMgmt* pMgmt = &pMnode->telemMgmt;
  if (!pMgmt->enable) return 0;

  taosWLockLatch(&pMgmt->lock);
  char* pCont = mndBuildTelemetryReport(pMnode);
  if (pCont != NULL) {
    mndSendTelemetryReport(pCont);
    free(pCont);
  }
  taosWUnLockLatch(&pMgmt->lock);
  return 0;
}

int32_t mndInitTelem(SMnode* pMnode) {
  STelemMgmt* pMgmt = &pMnode->telemMgmt;

  taosInitRWLatch(&pMgmt->lock);
  pMgmt->enable = tsEnableTelemetryReporting;
  taosGetEmail(pMgmt->email, sizeof(pMgmt->email));
  mndSetMsgHandle(pMnode, TDMT_MND_TELEM_TIMER, mndProcessTelemTimer);

  mDebug("mnode telemetry is initialized");
  return 0;
}

void mndCleanupTelem(SMnode* pMnode) {}
