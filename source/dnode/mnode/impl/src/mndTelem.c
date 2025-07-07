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
#include "thttp.h"
#include "tjson.h"
#include "mndAnode.h"

typedef struct {
  int64_t numOfDnode;
  int64_t numOfMnode;
  int64_t numOfVgroup;
  int64_t numOfDatabase;
  int64_t numOfSuperTable;
  int64_t numOfChildTable;
  int64_t numOfNormalTable;
  int64_t numOfColumn;
  int64_t totalPoints;
  int64_t totalStorage;
  int64_t compStorage;
  int32_t numOfAnalysisAlgos;
} SMnodeStat;

static void mndGetStat(SMnode* pMnode, SMnodeStat* pStat) {
  memset(pStat, 0, sizeof(SMnodeStat));

  SSdb* pSdb = pMnode->pSdb;
  pStat->numOfDnode = sdbGetSize(pSdb, SDB_DNODE);
  pStat->numOfMnode = sdbGetSize(pSdb, SDB_MNODE);
  pStat->numOfVgroup = sdbGetSize(pSdb, SDB_VGROUP);
  pStat->numOfDatabase = sdbGetSize(pSdb, SDB_DB);
  pStat->numOfSuperTable = sdbGetSize(pSdb, SDB_STB);

  void* pIter = NULL;
  while (1) {
    SVgObj* pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pStat->numOfChildTable += pVgroup->numOfTables;
    pStat->numOfColumn += pVgroup->numOfTimeSeries;
    pStat->totalPoints += pVgroup->pointsWritten;
    pStat->totalStorage += pVgroup->totalStorage;
    pStat->compStorage += pVgroup->compStorage;

    sdbRelease(pSdb, pVgroup);
  }
}

static int32_t algoToJson(const void* pObj, SJson* pJson) {
  const SAnodeAlgo* pNode = (const SAnodeAlgo*)pObj;
  int32_t code = tjsonAddStringToObject(pJson, "name", pNode->name);
  return code;
}

static void mndBuildRuntimeInfo(SMnode* pMnode, SJson* pJson) {
  SMnodeStat mstat = {0};
  int32_t    code = 0;
  int32_t    lino = 0;
  SArray*    pFcList = NULL;
  SArray*    pAdList = NULL;

  mndGetStat(pMnode, &mstat);

  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfDnode", mstat.numOfDnode), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfMnode", mstat.numOfMnode), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfVgroup", mstat.numOfVgroup), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfDatabase", mstat.numOfDatabase), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfSuperTable", mstat.numOfSuperTable), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfChildTable", mstat.numOfChildTable), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfColumn", mstat.numOfColumn), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfPoint", mstat.totalPoints), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "totalStorage", mstat.totalStorage), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "compStorage", mstat.compStorage), &lino, _OVER);

  pFcList = taosArrayInit(4, sizeof(SAnodeAlgo));
  pAdList = taosArrayInit(4, sizeof(SAnodeAlgo));
  if (pFcList == NULL || pAdList == NULL) {
    lino = __LINE__;
    goto _OVER;
  }

  mndRetrieveAlgoList(pMnode, pFcList, pAdList);

  if (taosArrayGetSize(pFcList) > 0) {
    SJson* items = tjsonAddArrayToObject(pJson, "forecast");
    TSDB_CHECK_NULL(items, code, lino, _OVER, terrno);

    for (int32_t i = 0; i < taosArrayGetSize(pFcList); ++i) {
      SJson* item = tjsonCreateObject();

      TSDB_CHECK_NULL(item, code, lino, _OVER, terrno);
      TAOS_CHECK_GOTO(tjsonAddItemToArray(items, item), &lino, _OVER);

      SAnodeAlgo* p = taosArrayGet(pFcList, i);
      TSDB_CHECK_NULL(p, code, lino, _OVER, terrno);
      TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "name", p->name), &lino, _OVER);
    }
  }

  if (taosArrayGetSize(pAdList) > 0) {
    SJson* items1 = tjsonAddArrayToObject(pJson, "anomaly_detection");
    TSDB_CHECK_NULL(items1, code, lino, _OVER, terrno);

    for (int32_t i = 0; i < taosArrayGetSize(pAdList); ++i) {
      SJson* item = tjsonCreateObject();

      TSDB_CHECK_NULL(item, code, lino, _OVER, terrno);
      TAOS_CHECK_GOTO(tjsonAddItemToArray(items1, item), &lino, _OVER);

      SAnodeAlgo* p = taosArrayGet(pAdList, i);
      TSDB_CHECK_NULL(p, code, lino, _OVER, terrno);
      TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "name", p->name), &lino, _OVER);
    }
  }

_OVER:
  taosArrayDestroy(pFcList);
  taosArrayDestroy(pAdList);

  if (code != 0) {
    mError("failed to mndBuildRuntimeInfo at line:%d since %s", lino, tstrerror(code));
  }
}

static char* mndBuildTelemetryReport(SMnode* pMnode) {
  char        tmp[4096] = {0};
  STelemMgmt* pMgmt = &pMnode->telemMgmt;
  int32_t     code = 0;
  int32_t     lino = 0;

  SJson* pJson = tjsonCreateObject();
  if (pJson == NULL) return NULL;

  char clusterName[64] = {0};
  if ((terrno = mndGetClusterName(pMnode, clusterName, sizeof(clusterName))) != 0) return NULL;
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "instanceId", clusterName), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "reportVersion", 1), &lino, _OVER);

  if (taosGetOsReleaseName(tmp, NULL, NULL, sizeof(tmp)) == 0) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "os", tmp), &lino, _OVER);
  }

  float numOfCores = 0;
  if (taosGetCpuInfo(tmp, sizeof(tmp), &numOfCores) == 0) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "cpuModel", tmp), &lino, _OVER);
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfCpu", numOfCores), &lino, _OVER);
  } else {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfCpu", tsNumOfCores), &lino, _OVER);
  }

  snprintf(tmp, sizeof(tmp), "%" PRId64 " kB", tsTotalMemoryKB);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "memory", tmp), &lino, _OVER);

  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "version", td_version), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "buildInfo", td_buildinfo), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "gitInfo", td_gitinfo), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "email", pMgmt->email), &lino, _OVER);

  mndBuildRuntimeInfo(pMnode, pJson);

_OVER:
  if (code != 0) {
    mError("failed to build telemetry report at lino:%d, since %s", lino, tstrerror(code));
  }
  char* pCont = tjsonToString(pJson);
  tjsonDelete(pJson);
  return pCont;
}

static int32_t mndProcessTelemTimer(SRpcMsg* pReq) {
  int32_t     code = 0;
  int32_t     line = 0;
  SMnode*     pMnode = pReq->info.node;
  STelemMgmt* pMgmt = &pMnode->telemMgmt;

  if (!tsEnableTelem) {
    return 0;
  }

  (void)taosThreadMutexLock(&pMgmt->lock);
  char* pCont = mndBuildTelemetryReport(pMnode);
  (void)taosThreadMutexUnlock(&pMgmt->lock);

  TSDB_CHECK_NULL(pCont, code, line, _end, terrno);

  code = taosSendTelemReport(&pMgmt->addrMgt, tsTelemUri, tsTelemPort, pCont, strlen(pCont), HTTP_FLAT);
  taosMemoryFree(pCont);
  return code;

_end:
  if (code != 0) {
    mError("%s failed to send telemetry report, line %d since %s", __func__, line, tstrerror(code));
  }
  taosMemoryFree(pCont);
  return code;
}

int32_t mndInitTelem(SMnode* pMnode) {
  int32_t     code = 0;
  STelemMgmt* pMgmt = &pMnode->telemMgmt;

  (void)taosThreadMutexInit(&pMgmt->lock, NULL);
  if ((code = taosGetEmail(pMgmt->email, sizeof(pMgmt->email))) != 0) {
    mWarn("failed to get email since %s", tstrerror(code));
  }

  code = taosTelemetryMgtInit(&pMgmt->addrMgt, tsTelemServer);
  if (code != 0) {
    mError("failed to init telemetry management since %s", tstrerror(code));
    return code;
  }

  mndSetMsgHandle(pMnode, TDMT_MND_TELEM_TIMER, mndProcessTelemTimer);
  return 0;
}

void mndCleanupTelem(SMnode* pMnode) {
  STelemMgmt* pMgmt = &pMnode->telemMgmt;
  taosTelemetryDestroy(&pMgmt->addrMgt);
  (void)taosThreadMutexDestroy(&pMgmt->lock);
}
