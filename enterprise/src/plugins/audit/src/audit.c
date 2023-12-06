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
#include "ttime.h"
#include "tjson.h"
#include "tglobal.h"

extern char *tsAuditUri;
extern char *tsAuditBatchUri;
extern SAudit tsAudit;

void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  if(len > AUDIT_DETAIL_MAX){
    uError("can't record audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  if(detail == NULL || len == 0){
    uError("audit detail shound not be null, len:%d", len);
  }

  int32_t min = len >= AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len + 1;
  char* buf = taosMemoryMalloc(min);
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
  if(pReq != NULL && pReq->info.conn.user != NULL && strlen(pReq->info.conn.user) > 0){
    strncpy(user, pReq->info.conn.user, 24);
  }
  uDebug("audit record user:%s, len:%"PRId32, user, (int32_t)strlen(user));

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    taosMemoryFreeClear(buf);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
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

  tjsonAddIntegerToObject(pJson, "timestamp", curTime);
  tjsonAddStringToObject(pJson, "cluster_id", strClusterId);
  tjsonAddStringToObject(pJson, "user", user);
  tjsonAddStringToObject(pJson, "operation", operation);
  tjsonAddStringToObject(pJson, "client_add", clientAddress);
  tjsonAddStringToObject(pJson, "db", target1);
  tjsonAddStringToObject(pJson, "resource", target2);
  tjsonAddStringToObject(pJson, "details", buf);

  auditSend(pJson);

  tjsonDelete(pJson);

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

void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  if(len > AUDIT_DETAIL_MAX){
    uError("can't record audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  if(detail == NULL || len == 0){
    uError("audit detail shound not be null, len:%d", len);
  }

  int32_t min = len >= AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len + 1;
  char* buf = taosMemoryMalloc(min);
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

  if(pReq != NULL && pReq->info.conn.user != NULL && strlen(pReq->info.conn.user) > 0){
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

  taosThreadMutexLock(&tsAudit.lock);
  taosArrayPush(tsAudit.records, &record);
  taosThreadMutexUnlock(&tsAudit.lock);
}

void auditSendRecordsInBatchImp(){
  taosThreadMutexLock(&tsAudit.lock);

  int setSize = taosArrayGetSize(tsAudit.records);
  if(setSize == 0){
    taosThreadMutexUnlock(&tsAudit.lock);
    return;
  }

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosThreadMutexUnlock(&tsAudit.lock);
    return;
  }

  SJson *items = tjsonAddArrayToObject(pJson, "records");

  for (int i = 0; i < setSize; i++) {
    SAuditRecord *pRecord = *(SAuditRecord **)taosArrayPop(tsAudit.records);

    SJson *item = tjsonCreateObject();
    tjsonAddItemToArray(items, item);

    tjsonAddIntegerToObject(item, "timestamp", pRecord->curTime);
    tjsonAddStringToObject(item, "cluster_id", pRecord->strClusterId);
    tjsonAddStringToObject(item, "user", pRecord->user);
    tjsonAddStringToObject(item, "operation", pRecord->operation);
    tjsonAddStringToObject(item, "client_add", pRecord->clientAddress);
    tjsonAddStringToObject(item, "db", pRecord->target1);
    tjsonAddStringToObject(item, "resource", pRecord->target2);
    tjsonAddStringToObject(item, "details", pRecord->detail);

    taosMemoryFree(pRecord->detail);
    taosMemoryFree(pRecord);
  }

  taosThreadMutexUnlock(&tsAudit.lock);

  char *pCont = tjsonToString(pJson);
  uDebug("audit record cont:%s\n", pCont);
  if (pCont != NULL) {
    EHttpCompFlag flag = tsAudit.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReport(tsAudit.cfg.server, tsAuditBatchUri, tsAudit.cfg.port, pCont, strlen(pCont), flag) != 0) {
      uError("failed to send audit msg, cont:%s", pCont);
    }
    taosMemoryFree(pCont);
  }

  tjsonDelete(pJson);
}