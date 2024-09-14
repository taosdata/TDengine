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

#include "streamInt.h"

void streamMutexLock(TdThreadMutex *pMutex) {
  int32_t code = taosThreadMutexLock(pMutex);
  if (code) {
    stError("%p mutex lock failed, code:%s", pMutex, tstrerror(code));
  }
}

void streamMutexUnlock(TdThreadMutex *pMutex) {
  int32_t code = taosThreadMutexUnlock(pMutex);
  if (code) {
    stError("%p mutex unlock failed, code:%s", pMutex, tstrerror(code));
  }
}

void streamMutexDestroy(TdThreadMutex *pMutex) {
  int32_t code = taosThreadMutexDestroy(pMutex);
  if (code) {
    stError("%p mutex destroy, code:%s", pMutex, tstrerror(code));
  }
}

void streamMetaRLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-rlock", pMeta->vgId);
  int32_t code = taosThreadRwlockRdlock(&pMeta->lock);
  if (code) {
    stError("vgId:%d meta-rlock failed, code:%s", pMeta->vgId, tstrerror(code));
  }
}

void streamMetaRUnLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-runlock", pMeta->vgId);
  int32_t code = taosThreadRwlockUnlock(&pMeta->lock);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d meta-runlock failed, code:%s", pMeta->vgId, tstrerror(code));
  } else {
    //    stTrace("vgId:%d meta-runlock completed", pMeta->vgId);
  }
}

void streamMetaWLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-wlock", pMeta->vgId);
  int32_t code = taosThreadRwlockWrlock(&pMeta->lock);
  if (code) {
    stError("vgId:%d failed to apply wlock, code:%s", pMeta->vgId, tstrerror(code));
  }
}

void streamMetaWUnLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-wunlock", pMeta->vgId);
  int32_t code = taosThreadRwlockUnlock(&pMeta->lock);
  if (code) {
    stError("vgId:%d failed to apply wunlock, code:%s", pMeta->vgId, tstrerror(code));
  }
}

void streamSetFatalError(SStreamMeta* pMeta, int32_t code, const char* funcName, int32_t lino) {
  int32_t oldCode = atomic_val_compare_exchange_32(&pMeta->fatalInfo.code, 0, code);
  if (oldCode == 0) {
    pMeta->fatalInfo.ts = taosGetTimestampMs();
    pMeta->fatalInfo.threadId = taosGetSelfPthreadId();
    tstrncpy(pMeta->fatalInfo.func, funcName, tListLen(pMeta->fatalInfo.func));
    pMeta->fatalInfo.line = lino;
    stInfo("vgId:%d set global fatal error, code:%s %s line:%d", pMeta->vgId, tstrerror(code), funcName, lino);
  } else {
    stFatal("vgId:%d existed global fatal eror:%s, failed to set new fatal error code:%s", pMeta->vgId, code);
  }
}

int32_t streamGetFatalError(const SStreamMeta* pMeta) {
  return atomic_load_32((volatile int32_t*) &pMeta->fatalInfo.code);
}
