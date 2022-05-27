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

#include "catalog.h"
#include "command.h"
#include "query.h"
#include "schedulerInt.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

void schCloseJobRef(void) {
  if (!atomic_load_8((int8_t *)&schMgmt.exit)) {
    return;
  }

  SCH_LOCK(SCH_WRITE, &schMgmt.lock);
  if (atomic_load_32(&schMgmt.jobNum) <= 0 && schMgmt.jobRef >= 0) {
    taosCloseRef(schMgmt.jobRef);
    schMgmt.jobRef = -1;
  }
  SCH_UNLOCK(SCH_WRITE, &schMgmt.lock);
}

uint64_t schGenTaskId(void) { return atomic_add_fetch_64(&schMgmt.taskId, 1); }

uint64_t schGenUUID(void) {
  static uint64_t hashId = 0;
  static int32_t requestSerialId = 0;

  if (hashId == 0) {
    char    uid[64];
    int32_t code = taosGetSystemUUID(uid, tListLen(uid));
    if (code != TSDB_CODE_SUCCESS) {
      qError("Failed to get the system uid, reason:%s", tstrerror(TAOS_SYSTEM_ERROR(errno)));
    } else {
      hashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t ts      = taosGetTimestampMs();
  uint64_t pid    = taosGetPId();
  int32_t val     = atomic_add_fetch_32(&requestSerialId, 1);

  uint64_t id = ((hashId & 0x0FFF) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
  return id;
}


void schFreeRpcCtxVal(const void *arg) {
  if (NULL == arg) {
    return;
  }

  SMsgSendInfo *pMsgSendInfo = (SMsgSendInfo *)arg;
  taosMemoryFreeClear(pMsgSendInfo->param);
  taosMemoryFreeClear(pMsgSendInfo->msgInfo.pData);  
  taosMemoryFreeClear(pMsgSendInfo);
}

void schFreeRpcCtx(SRpcCtx *pCtx) {
  if (NULL == pCtx) {
    return;
  }
  void *pIter = taosHashIterate(pCtx->args, NULL);
  while (pIter) {
    SRpcCtxVal *ctxVal = (SRpcCtxVal *)pIter;

    (*pCtx->freeFunc)(ctxVal->val);

    pIter = taosHashIterate(pCtx->args, pIter);
  }

  taosHashCleanup(pCtx->args);

  (*pCtx->freeFunc)(pCtx->brokenVal.val);
}


