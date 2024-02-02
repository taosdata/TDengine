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

const char* selectMonitorName = "taos_sql_req:count";
const char* selectMonitorHelp = "count for select sql";
const int   selectMonitorLabelCount = 4;
const char* selectMonitorLabels[] = {"cluster_id", "sql_type", "username", "result"};

static const char* defaultClusterID = "";

void clusterSelectMonitorInit(const char* clusterKey) {
  if (!tsEnableMonitor) return;
  SAppInstInfo* pAppInstInfo = getAppInstInfo(clusterKey);
  SEpSet epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
  clusterMonitorInit(clusterKey, epSet, pAppInstInfo->pTransporter);
  createClusterCounter(clusterKey, selectMonitorName, selectMonitorHelp, selectMonitorLabelCount, selectMonitorLabels);
}

void clusterSelectLog(const char* clusterKey, const char* user, SQL_RESULT_CODE result) {
  const char* selectMonitorLabelValues[] = {defaultClusterID, "select", user, resultStr(result)};
  taosClusterCounterInc(clusterKey, selectMonitorName, selectMonitorLabelValues);
}

void selectLog(int64_t rid,  bool killed, int32_t code) {
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
    if (pTscObj->pAppInfo == NULL) {
      tscLog("selectLog, not found pAppInfo");
    }
    return clusterSelectLog(pTscObj->pAppInfo->instKey, pTscObj->user, result);
  } else {
    tscLog("selectLog, not found rid");
  }
}
