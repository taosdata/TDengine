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
#include "tglobal.h"

const char* slowQueryName = "slow_query";
const char* slowQueryHelp = "slow query log when cost > 3s";
const int   slowQueryLabelCount = 1;
const char* slowQueryLabels[] = {"cost"};

const int64_t msInSeconds = 1000;
const int64_t msInMinutes = 60 * msInSeconds;

static const char* getSlowQueryLableCostDesc(int64_t cost) {
  if (cost >= 30 * msInMinutes) {
    return " > 30 min";
  } else if (cost >= 10 * msInMinutes) {
    return " > 10 min";
  } else if (cost >= 5 * msInMinutes) {
    return " > 5 min";
  } else if (cost >= 1 * msInMinutes) {
    return " > 1 min";
  } else if (cost >= 30 * msInSeconds) {
    return " > 30 seconds";
  } else if (cost >= 10 * msInSeconds) {
    return " > 10 seconds";
  } else if (cost >= 5 * msInSeconds) {
    return " > 5 seconds";
  } else if (cost >= 3 * msInSeconds) {
    return " > 3 seconds";
  }
  return "< 3 s";
}

void clusterSlowQueryMonitorInit(const char* clusterKey) {
  if (!enableSlowQueryMonitor) return;
  SAppInstInfo* pAppInstInfo = getAppInstInfo(clusterKey);
  SEpSet        epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
  clusterMonitorInit(clusterKey, epSet, pAppInstInfo->pTransporter);
  createClusterCounter(clusterKey, slowQueryName, slowQueryHelp, slowQueryLabelCount, slowQueryLabels);
}

void clusterSlowQueryLog(const char* clusterKey, int32_t cost) {
  const char* slowQueryLabelValues[] = {getSlowQueryLableCostDesc(cost)};
  taosClusterCounterInc(clusterKey, slowQueryName, slowQueryLabelValues);
}

void SlowQueryLog(int64_t connId, int32_t cost) {
  if (!enableSlowQueryMonitor) return;
  STscObj* pTscObj = acquireTscObj(connId);
  if (pTscObj != NULL) {
    if(pTscObj->pAppInfo == NULL) {
      tscLog("SlowQueryLog, not found pAppInfo");
    }
    return clusterSlowQueryLog(pTscObj->pAppInfo->instKey, cost);
  } else {
    tscLog("SlowQueryLog, not found connect ID");
  }
}
