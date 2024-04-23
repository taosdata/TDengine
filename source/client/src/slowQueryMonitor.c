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

const char* slowQueryName = "taos_slow_sql:count";
const char* slowQueryHelp = "slow query log when cost over than config duration";
const int   slowQueryLabelCount = 4;
const char* slowQueryLabels[] = {"cluster_id", "username", "result", "duration"};
static const char* defaultClusterID = "";

const int64_t usInSeconds = 1000 * 1000;
const int64_t msInMinutes = 60 * 1000;

static const char* getSlowQueryLableCostDesc(int64_t cost) {
  if (cost >= 1000 * usInSeconds) {
    return "1000s-";
  } else if (cost >= 100 * usInSeconds) {
    return "100-1000s";
  } else if (cost >= 10 * usInSeconds) {
    return "10-100s";
  } else if (cost >= 3 * usInSeconds) {
    return "3-10s";
  }
  return "0-3s";
}

void clientSlowQueryMonitorInit(const char* clusterKey) {
  if (!tsEnableMonitor) return;
  SAppInstInfo* pAppInstInfo = getAppInstInfo(clusterKey);
  SEpSet        epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
  clusterMonitorInit(clusterKey, epSet, pAppInstInfo->pTransporter);
  createClusterCounter(clusterKey, slowQueryName, slowQueryHelp, slowQueryLabelCount, slowQueryLabels);
}

void clientSlowQueryLog(const char* clusterKey, const char* user, SQL_RESULT_CODE result, int32_t cost) {
  const char* slowQueryLabelValues[] = {defaultClusterID, user, resultStr(result), getSlowQueryLableCostDesc(cost)};
  taosClusterCounterInc(clusterKey, slowQueryName, slowQueryLabelValues);
}

void SlowQueryLog(int64_t rid, bool killed, int32_t code, int32_t cost) {
  if (!tsEnableMonitor) return;
  SQL_RESULT_CODE result = SQL_RESULT_SUCCESS;
  if (TSDB_CODE_SUCCESS != code) {
    result = SQL_RESULT_FAILED;
  }
  // to do Distinguish active Kill events
  // else if (killed) {
  //   result = SQL_RESULT_CANCEL;
  // }

  STscObj* pTscObj = acquireTscObj(rid);
  if (pTscObj != NULL) {
    if(pTscObj->pAppInfo == NULL) {
      tscLog("SlowQueryLog, not found pAppInfo");
    } else {
      clientSlowQueryLog(pTscObj->pAppInfo->instKey, pTscObj->user, result, cost);
    }
    releaseTscObj(rid);
  } else {
    tscLog("SlowQueryLog, not found rid");
  }
}
