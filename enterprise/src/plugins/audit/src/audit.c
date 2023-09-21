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

#include "auditInt.h"
#include "taoserror.h"
#include "thttp.h"
#include "ttime.h"
#include "tjson.h"
#include "tglobal.h"

extern char *tsAuditUri;
extern SAudit tsAudit;

void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  
  if(len > AUDIT_DETAIL_MAX){
    uError("can't record audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  int32_t min = len > AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len;
  char* buf = taosMemoryMalloc(min);
  if(detail == NULL && len > 0){
    uError("audit detail shound not be null, len:%d", len);
  }
  if(detail != NULL && min > 1){
    memcpy(buf, detail, min - 1);
  }

  char *user = pReq->info.conn.user;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    taosMemoryFreeClear(buf);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return;
  }

  char   ts[40] = {0};
  int64_t curTime = taosGetTimestampMs();
  taosFormatUtcTime(ts, sizeof(ts), curTime, TSDB_TIME_PRECISION_MILLI);

  char strClusterId[65] = {0};
  sprintf(strClusterId, "%" PRId64, clusterId);

  tjsonAddDoubleToObject(pJson, "timestamp", curTime);
  tjsonAddStringToObject(pJson, "cluster_id", strClusterId);
  tjsonAddStringToObject(pJson, "user", user);
  tjsonAddStringToObject(pJson, "operation", operation);
  tjsonAddStringToObject(pJson, "target_1", target1);
  tjsonAddStringToObject(pJson, "target_2", target2);
  tjsonAddStringToObject(pJson, "details", buf);

  auditSend(pJson);

  taosMemoryFreeClear(buf);
}

void auditSend(SJson *pJson) {
  char *pCont = tjsonToString(pJson);
  uDebug("audit record cont:%s\n", pCont);
  if (pCont != NULL) {
    EHttpCompFlag flag = tsAudit.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReport(tsAudit.cfg.server, tsAuditUri, tsAudit.cfg.port, pCont, strlen(pCont), flag) != 0) {
      uError("failed to send audit msg, cont:%s", pCont);
    }
    taosMemoryFree(pCont);
  }
}