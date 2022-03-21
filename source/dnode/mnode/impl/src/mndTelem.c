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
#include "thttp.h"
#include "tjson.h"

#define TELEMETRY_SERVER "telemetry.taosdata.com"
#define TELEMETRY_PORT   80

static void mndBuildRuntimeInfo(SMnode* pMnode, SJson* pJson) {
  SMnodeLoad load = {0};
  mndGetLoad(pMnode, &load);

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

  if (taosGetOsReleaseName(tmp, sizeof(tmp)) == 0) {
    tjsonAddStringToObject(pJson, "os", tmp);
  }

  float numOfCores = 0;
  if (taosGetCpuInfo(tmp, sizeof(tmp), &numOfCores) == 0) {
    tjsonAddStringToObject(pJson, "cpuModel", tmp);
    tjsonAddDoubleToObject(pJson, "numOfCpu", numOfCores);
  } else {
    tjsonAddDoubleToObject(pJson, "numOfCpu", tsNumOfCores);
  }

  snprintf(tmp, sizeof(tmp), "%" PRId64 " kB", tsTotalMemoryKB);
  tjsonAddStringToObject(pJson, "memory", tmp);

  tjsonAddStringToObject(pJson, "version", version);
  tjsonAddStringToObject(pJson, "buildInfo", buildinfo);
  tjsonAddStringToObject(pJson, "gitInfo", gitinfo);
  tjsonAddStringToObject(pJson, "email", pMgmt->email);

  mndBuildRuntimeInfo(pMnode, pJson);

  char* pCont = tjsonToString(pJson);
  tjsonDelete(pJson);
  return pCont;
}

static int32_t mndProcessTelemTimer(SNodeMsg* pReq) {
  SMnode*     pMnode = pReq->pNode;
  STelemMgmt* pMgmt = &pMnode->telemMgmt;
  if (!pMgmt->enable) return 0;

  taosWLockLatch(&pMgmt->lock);
  char* pCont = mndBuildTelemetryReport(pMnode);
  if (pCont != NULL) {
    taosSendHttpReport(TELEMETRY_SERVER, TELEMETRY_PORT, pCont, strlen(pCont), HTTP_FLAT);
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
