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

void monitorClientSlowQueryInit(int64_t clusterid) {
  monitorCreateClient(clusterid);
  monitorCreateClientCounter(clusterid, slowQueryName, slowQueryHelp, slowQueryLabelCount, slowQueryLabels);
}

void clientSlowQueryLog(int64_t clusterId, const char* user, SQL_RESULT_CODE result, int32_t cost) {
  char clusterIdStr[32] = {0};
  if (snprintf(clusterIdStr, sizeof(clusterIdStr), "%" PRId64, clusterId) < 0){
    uError("failed to generate clusterId:%" PRId64, clusterId);
  }
  const char* slowQueryLabelValues[] = {clusterIdStr, user, monitorResultStr(result), getSlowQueryLableCostDesc(cost)};
  monitorCounterInc(clusterId, slowQueryName, slowQueryLabelValues);
}

void slowQueryLog(int64_t rid, bool killed, int32_t code, int32_t cost) {
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
      tscLog("slowQueryLog, not found pAppInfo");
    } else {
      clientSlowQueryLog(pTscObj->pAppInfo->clusterId, pTscObj->user, result, cost);
    }
    releaseTscObj(rid);
  } else {
    tscLog("slowQueryLog, not found rid");
  }
}
