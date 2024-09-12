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
#include "osMemory.h"
#include "osThread.h"
#include "taoserror.h"
#include "tarray.h"
#include "thttp.h"
#include "tjson.h"
#include "tglobal.h"

extern char *tsAuditUri;
extern char *tsAuditBatchUri;
extern SAudit tsAudit;

void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  if(len > AUDIT_DETAIL_MAX){
    uWarn("can't record total audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  if(detail == NULL || len == 0){
    uWarn("audit detail shound not be null, len:%d", len);
  }

  int32_t min = len >= AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len + 1;
  char* buf = taosMemoryMalloc(min);
  if(buf == NULL){
    uError("failed to audit since can't alloc a tmp buf");
    return;
  }

  memset(buf, 0, min);
  if(detail != NULL && len > 0){
    if(len >= AUDIT_DETAIL_MAX){
      memcpy(buf, detail, min - 1);
    }
    else{
      memcpy(buf, detail, len);
    }
  }

  char user[TSDB_USER_LEN] = {0};
  if(pReq != NULL && pReq->info.conn.user[0] != 0){
    memcpy(user, pReq->info.conn.user, 24);
  }
  uDebug("audit record user:%s, len:%"PRId32, user, (int32_t)strlen(user));

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    taosMemoryFreeClear(buf);
    uError("failed to aduit since failed to create json object")
    return;
  }

  //char   ts[40] = {0};
  int64_t curTime = taosGetTimestampNs();
  //taosFormatUtcTime(ts, sizeof(ts), curTime, TSDB_TIME_PRECISION_MILLI);

  char strClusterId[TSDB_CLUSTER_ID_LEN] = {0};
  sprintf(strClusterId, "%" PRId64, clusterId);

  char clientAddress[50] = {0};
  if(pReq != NULL){
    char ip[24] = {0};
    taosIp2String(pReq->info.conn.clientIp, ip);

    sprintf(clientAddress, "%s:%d", ip, pReq->info.conn.clientPort);
  }

  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "timestamp", curTime), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "cluster_id", strClusterId), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "user", user), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "operation", operation), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "client_add", clientAddress), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "db", target1), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "resource", target2), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "details", buf), &lino, _error);

  TAOS_CHECK_GOTO(auditSend(pJson), &lino, _error);

  goto _exit;

_error:
  uError("failed to aduit, %s at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));

_exit:
  tjsonDelete(pJson);
  taosMemoryFreeClear(buf);
}

int32_t auditSend(SJson *pJson) {
  int32_t code = 0;

  char *pCont = tjsonToString(pJson);
  if(pCont == NULL){
    code = TSDB_CODE_AUDIT_NOT_FORMAT_TO_JSON;
    return code;
  }

  char tmp[100] = {0};
  (void)sprintf(tmp, "%" PRId64, tGenQid64(tsAudit.dnodeId));
  uDebug("audit record with QID:%s cont:%s\n", tmp, pCont);
  EHttpCompFlag flag = tsAudit.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
  if (taosSendHttpReportWithQID(tsAudit.cfg.server, tsAuditUri, tsAudit.cfg.port, pCont, strlen(pCont), flag, tmp) !=
      0) {
    uError("failed to send audit msg, cont:%s", pCont);
    code = TSDB_CODE_AUDIT_FAIL_SEND_AUDIT_RECORD;
    taosMemoryFree(pCont);
    return code;
  }

  taosMemoryFree(pCont);
  return code;
}

void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  if(len > AUDIT_DETAIL_MAX){
    uWarn("can't record total audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  if(detail == NULL || len == 0){
    uWarn("audit detail shound not be null, len:%d", len);
  }

  int32_t min = len >= AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len + 1;
  char* buf = taosMemoryMalloc(min);
  if(buf == NULL){
    uError("failed to audit since can't alloc a tmp buf");
    return;
  }

  memset(buf, 0, min);
  if(detail != NULL && len > 0){
    if(len >= AUDIT_DETAIL_MAX){
      memcpy(buf, detail, min - 1);
    }
    else{
      memcpy(buf, detail, len);
    }
  }

  SAuditRecord *record = taosMemoryMalloc(sizeof(SAuditRecord));
  if(record == NULL){
    uError("failed to audit since can't alloc a audit record");
    return;
  }

  if(pReq != NULL && pReq->info.conn.user[0] != 0){
    strncpy(record->user, pReq->info.conn.user, 24);
  }
  uDebug("audit record user:%s, len:%"PRId32, record->user, (int32_t)strlen(record->user));

  record->curTime = taosGetTimestampNs();

  sprintf(record->strClusterId, "%" PRId64, clusterId);

  if(pReq != NULL){
    char ip[24] = {0};
    taosIp2String(pReq->info.conn.clientIp, ip);

    sprintf(record->clientAddress, "%s:%d", ip, pReq->info.conn.clientPort);
  }

  strcpy(record->operation, operation);
  strcpy(record->target1, target1);
  strcpy(record->target2, target2);
  record->detail = buf;

  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_GOTO(taosThreadMutexLock(&tsAudit.lock), &lino, _exit);
  if (taosArrayPush(tsAudit.records, &record) == NULL) {
    TAOS_CHECK_GOTO(taosThreadMutexUnlock(&tsAudit.lock), &lino, _exit);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }
  TAOS_CHECK_GOTO(taosThreadMutexUnlock(&tsAudit.lock), &lino, _exit);

  return;

_exit:
  uError("failed to aduit, %s at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
}

void auditSendRecordsInBatchImp(){
  if(taosThreadMutexLock(&tsAudit.lock) != 0){
    uError("failed to send audit in batch since failed to lock");
    return;
  }

  int setSize = taosArrayGetSize(tsAudit.records);
  if(setSize == 0){
    (void)taosThreadMutexUnlock(&tsAudit.lock);
    return;
  }

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    uError("failed to send audit in batch since failed to create json object");
    (void)taosThreadMutexUnlock(&tsAudit.lock);
    return;
  }

  int32_t code = 0;
  int32_t lino = 0;
  SJson *items = tjsonAddArrayToObject(pJson, "records");
  if(items == NULL){
    code = TSDB_CODE_AUDIT_FAIL_GENERATE_JSON;
    lino = __LINE__;
    goto _error;
  }

  for (int i = 0; i < setSize; i++) {
    SAuditRecord *pRecord = *(SAuditRecord **)taosArrayPop(tsAudit.records);

    SJson *item = tjsonCreateObject();
    if(item == NULL){
      code = TSDB_CODE_AUDIT_FAIL_GENERATE_JSON;
      lino = __LINE__;
      goto _error;
    }

    TAOS_CHECK_GOTO(tjsonAddItemToArray(items, item), &lino, _error);

    TAOS_CHECK_GOTO(tjsonAddIntegerToObject(item, "timestamp", pRecord->curTime), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "cluster_id", pRecord->strClusterId), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "user", pRecord->user), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "operation", pRecord->operation), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "client_add", pRecord->clientAddress), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "db", pRecord->target1), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "resource", pRecord->target2), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "details", pRecord->detail), &lino, _error);

    taosMemoryFree(pRecord->detail);
    taosMemoryFree(pRecord);
  }

  (void)taosThreadMutexUnlock(&tsAudit.lock);

  char *pCont = tjsonToString(pJson);
  if (pCont != NULL) {
    char tmp[100] = {0};
    (void)sprintf(tmp, "%" PRId64, tGenQid64(tsAudit.dnodeId));
    uDebug("audit batch record with QID:%s cont: %d\n", tmp, setSize);
    EHttpCompFlag flag = tsAudit.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReportWithQID(tsAudit.cfg.server, tsAuditBatchUri, tsAudit.cfg.port, pCont, strlen(pCont), flag,
                                  tmp) != 0) {
      uError("failed to send audit msg, cont:%s", pCont);
    }
    taosMemoryFree(pCont);
  }
  else{
    uError("failed to send audit msg since failed format to json string");
  }

  goto _exit;

_error:
  uError("failed to aduit, %s at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
  (void)taosThreadMutexUnlock(&tsAudit.lock);
  
_exit:
  tjsonDelete(pJson);
}