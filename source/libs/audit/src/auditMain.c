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
#include "mnode.h"
#include "audit.h"
#include "osMemory.h"

SAudit tsAudit = {0};
char* tsAuditUri = "/audit";
char* tsAuditBatchUri = "/audit-batch";

int32_t auditInit(const SAuditCfg *pCfg) {
  tsAudit.cfg = *pCfg;
  tsAudit.records = taosArrayInit(0, sizeof(SAuditRecord *));
  taosThreadMutexInit(&tsAudit.lock, NULL);
  return 0;
}

static FORCE_INLINE void auditDeleteRecord(SAuditRecord * record) {
  if (record) {
    taosMemoryFree(record->detail);
    taosMemoryFree(record);
  }
}

void auditCleanup() {
  tsLogFp = NULL;
  taosThreadMutexLock(&tsAudit.lock);
  taosArrayDestroyP(tsAudit.records, (FDelete)auditDeleteRecord);
  taosThreadMutexUnlock(&tsAudit.lock);
  tsAudit.records = NULL;
  taosThreadMutexDestroy(&tsAudit.lock);
}

extern void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                          char *detail, int32_t len);
extern void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                          char *detail, int32_t len);
extern void auditSendRecordsInBatchImp();

void auditRecord(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                char *detail, int32_t len) {
  auditRecordImp(pReq, clusterId, operation, target1, target2, detail, len);
}

void auditAddRecord(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                char *detail, int32_t len) {
  auditAddRecordImp(pReq, clusterId, operation, target1, target2, detail, len);
}

void auditSendRecordsInBatch(){
  auditSendRecordsInBatchImp();
}

#ifndef TD_ENTERPRISE
void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
}

void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len) {
}

void auditSendRecordsInBatchImp(){

}
#endif
