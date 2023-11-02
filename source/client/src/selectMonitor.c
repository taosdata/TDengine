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
#include "clientInt.h"
#include "clientMonitor.h"
#include "clientLog.h"

const char* selectMonitorName = "slow_query";
const char* selectMonitorHelp = "slow query log when cost > 3s";
const int   selectMonitorLabelCount = 1;
const char* selectMonitorLabels[] = {"default"};

void clusterSelectMonitorInit(const char* clusterKey) {
  SAppInstInfo* pAppInstInfo = getAppInstInfo(clusterKey);
  SEpSet epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
  clusterMonitorInit(clusterKey, epSet, pAppInstInfo->pTransporter);
  createClusterCounter(clusterKey, selectMonitorName, selectMonitorHelp, selectMonitorLabelCount, selectMonitorLabels);
}

void clusterSelectLog(const char* clusterKey) {
  const char* selectMonitorLabelValues[] = {"default"};
  taosClusterCounterInc(clusterKey, selectMonitorName, selectMonitorLabelValues);
}

void selectLog(int64_t connId) {
  STscObj* pTscObj = acquireTscObj(connId);
  if (pTscObj != NULL) {
    if(pTscObj->pAppInfo == NULL) {
      tscLog("selectLog, not found pAppInfo");
    }
    return clusterSelectLog(pTscObj->pAppInfo->instKey);
  } else {
    tscLog("selectLog, not found connect ID");
  }
}
