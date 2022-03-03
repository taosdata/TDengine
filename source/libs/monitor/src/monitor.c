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
#include "monInt.h"
#include "taoserror.h"
#include "thttp.h"
#include "tlog.h"
#include "ttime.h"

static SMonitor tsMonitor = {0};

int32_t monInit(const SMonCfg *pCfg) {
  tsMonitor.logs = taosArrayInit(16, sizeof(SMonInfo));
  if (tsMonitor.logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tsMonitor.maxLogs = pCfg->maxLogs;
  tsMonitor.server = pCfg->server;
  tsMonitor.port = pCfg->port;
  taosInitRWLatch(&tsMonitor.lock);
  return 0;
}

void monCleanup() {
  taosArrayDestroy(tsMonitor.logs);
  tsMonitor.logs = NULL;
}

void monAddLogItem(SMonLogItem *pItem) {
  taosWLockLatch(&tsMonitor.lock);
  int32_t size = taosArrayGetSize(tsMonitor.logs);
  if (size > tsMonitor.maxLogs) {
    uInfo("too many logs for monitor");
  } else {
    taosArrayPush(tsMonitor.logs, pItem);
  }
  taosWUnLockLatch(&tsMonitor.lock);
}

SMonInfo *monCreateMonitorInfo() {
  SMonInfo *pMonitor = calloc(1, sizeof(SMonInfo));
  if (pMonitor == NULL) return NULL;

  taosWLockLatch(&tsMonitor.lock);
  pMonitor->logs = taosArrayDup(tsMonitor.logs);
  taosArrayClear(tsMonitor.logs);
  taosWUnLockLatch(&tsMonitor.lock);

  pMonitor->pJson = tjsonCreateObject();
  if (pMonitor->pJson == NULL || pMonitor->logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    monCleanupMonitorInfo(pMonitor);
    return NULL;
  }

  return pMonitor;
}

void monCleanupMonitorInfo(SMonInfo *pMonitor) {
  taosArrayDestroy(pMonitor->logs);
  tjsonDelete(pMonitor->pJson);
  free(pMonitor);
}

void monSendReport(SMonInfo *pMonitor) {
  char *pCont = tjsonToString(pMonitor->pJson);
  if (pCont != NULL) {
    taosSendHttpReport(tsMonitor.server, tsMonitor.port, pCont, strlen(pCont));
    free(pCont);
  }
}

void monSetBasicInfo(SMonInfo *pMonitor, SMonBasicInfo *pInfo) {
  SJson *pJson = pMonitor->pJson;
  tjsonAddDoubleToObject(pJson, "dnode_id", pInfo->dnode_id);
  tjsonAddStringToObject(pJson, "dnode_ep", pInfo->dnode_ep);

  int64_t ms = taosGetTimestampMs();
  char    buf[40] = {0};
  taosFormatUtcTime(buf, sizeof(buf), ms, TSDB_TIME_PRECISION_MILLI);
  tjsonAddStringToObject(pJson, "ts", buf);
}

void monSetClusterInfo(SMonInfo *pMonitor, SMonClusterInfo *pInfo) {

}

void monSetVgroupInfo(SMonInfo *pMonitor, SMonVgroupInfo *pInfo) {

}

void monSetGrantInfo(SMonInfo *pMonitor, SMonGrantInfo *pInfo) {

}

void monSetDnodeInfo(SMonInfo *pMonitor, SMonDnodeInfo *pInfo) {

}

void monSetDiskInfo(SMonInfo *pMonitor, SMonDiskInfo *pInfo) {
    
}
