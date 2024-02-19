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

void clientSQLReqMonitorInit(const char* clusterKey) {
  if (!tsEnableMonitor) return;
  SAppInstInfo* pAppInstInfo = getAppInstInfo(clusterKey);
  SEpSet epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
  clusterMonitorInit(clusterKey, epSet, pAppInstInfo->pTransporter);
  createClusterCounter(clusterKey, selectMonitorName, selectMonitorHelp, selectMonitorLabelCount, selectMonitorLabels);
}

void clientSQLReqLog(const char* clusterKey, const char* user, SQL_RESULT_CODE result, int8_t type) {
  const char* typeStr;
  switch (type) {
    case MONITORSQLTYPEDELETE:
      typeStr = "delete";
      break;
    case MONITORSQLTYPEINSERT:
      typeStr = "insert";
      break;
    default:
      typeStr = "select";
      break;
  }
  const char* selectMonitorLabelValues[] = {defaultClusterID, typeStr, user, resultStr(result)};
  taosClusterCounterInc(clusterKey, selectMonitorName, selectMonitorLabelValues);
}

void sqlReqLog(int64_t rid,  bool killed, int32_t code, int8_t type) {
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
      tscLog("sqlReqLog, not found pAppInfo");
    } else {
      clientSQLReqLog(pTscObj->pAppInfo->instKey, pTscObj->user, result, type);
    }
    releaseTscObj(rid);
  } else {
    tscLog("sqlReqLog, not found rid");
  }
}

void clientMonitorClose(const char* clusterKey) {
  tscLog("clientMonitorClose, key:%s", clusterKey);
  clusterMonitorClose(clusterKey);
}
