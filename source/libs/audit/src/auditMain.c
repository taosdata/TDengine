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

#include "tarray.h"
#include "auditInt.h"
#include "taoserror.h"
#include "thttp.h"
#include "ttime.h"
#include "tjson.h"
#include "tglobal.h"
#include "audit.h"
#include "osMemory.h"

SAudit tsAudit = {0};
char* tsAuditUri = "/audit_v2";
char* tsAuditBatchUri = "/audit-batch";

static FORCE_INLINE void auditDeleteRecord(SAuditRecord *record) {
  if (record) {
    taosMemoryFree(record->detail);
    taosMemoryFree(record);
  }
}

int32_t auditInit(const SAuditCfg *pCfg) {
  int32_t code = 0;
  tsAudit.cfg = *pCfg;
  tsAudit.records = taosArrayInit(0, sizeof(SAuditRecord *));
  if(tsAudit.records == NULL) return terrno;
  if (taosThreadRwlockInit(&tsAudit.infoLock, NULL) != 0) {
    taosArrayDestroyP(tsAudit.records, (FDelete)auditDeleteRecord);
    return -1;
  }
  if (taosThreadMutexInit(&tsAudit.recordLock, NULL) != 0) {
    (void)taosThreadRwlockDestroy(&tsAudit.infoLock);
    taosArrayDestroyP(tsAudit.records, (FDelete)auditDeleteRecord);
    return -1;
  }
  return 0;
}

void auditSetDnodeId(int32_t dnodeId) { tsAudit.dnodeId = dnodeId; }

void auditCleanup() {
  tsLogFp = NULL;
  (void)taosThreadMutexLock(&tsAudit.recordLock);
  taosArrayDestroyP(tsAudit.records, (FDelete)auditDeleteRecord);
  (void)taosThreadMutexUnlock(&tsAudit.recordLock);
  tsAudit.records = NULL;
  (void)taosThreadMutexDestroy(&tsAudit.recordLock);
  (void)taosThreadRwlockDestroy(&tsAudit.infoLock);
}

extern void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                          char *detail, int32_t len, double duration, int64_t affectedRows);
extern void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                          char *detail, int32_t len, double duration, int64_t affectedRows);
extern void auditSendRecordsInBatchImp();

void auditRecord(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                char *detail, int32_t len, double duration, int64_t affectedRows) {
  auditRecordImp(pReq, clusterId, operation, target1, target2, detail, len, duration, affectedRows);
}

void auditAddRecord(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                char *detail, int32_t len, double duration, int64_t affectedRows) {
  auditAddRecordImp(pReq, clusterId, operation, target1, target2, detail, len, duration, affectedRows);
}

void auditSendRecordsInBatch(){
  auditSendRecordsInBatchImp();
}

#ifndef TD_ENTERPRISE
void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len, double duration, int64_t affectedRows) {
}

void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
}

void auditSendRecordsInBatchImp(){

}
#endif
