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
#include "osMemPool.h"
#include "tmempoolInt.h"
#include "tlog.h"
#include "tutil.h"

int32_t mpDirectAlloc(SMemPool* pPool, SMPSession* pSession, int64_t* size, uint32_t alignment, void** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  void* res = NULL;
  int64_t nSize = *size;
  
  taosRLockLatch(&pPool->cfgLock);

  MP_ERR_JRET(mpChkQuotaOverflow(pPool, pSession, *size));
  
  res = alignment ? taosMemMallocAlign(alignment, *size) : taosMemMalloc(*size);
  if (NULL != res) {
    nSize = taosMemSize(res);
    mpUpdateAllocSize(pPool, pSession, nSize, nSize - *size);
  } else {
    (void)atomic_sub_fetch_64(&pSession->pJob->job.allocMemSize, *size);
    (void)atomic_sub_fetch_64(&pPool->allocMemSize, *size);
    
    uError("malloc %" PRId64 " alignment %d failed, code: 0x%x", *size, alignment, terrno);

    code = terrno;
  }

_return:

  taosRUnLockLatch(&pPool->cfgLock);
  
  *ppRes = res;
  *size = nSize;
  
  MP_RET(code);
}

int64_t mpDirectGetMemSize(SMemPool* pPool, SMPSession* pSession, void *ptr) {
  return taosMemSize(ptr);
}

void mpDirectFree(SMemPool* pPool, SMPSession* pSession, void *ptr, int64_t* origSize) {
  int64_t oSize = taosMemSize(ptr);
  if (origSize) {
    *origSize = oSize;
  }
  
  taosRLockLatch(&pPool->cfgLock); // tmp test

  taosMemFree(ptr);
  
  (void)atomic_sub_fetch_64(&pSession->allocMemSize, oSize);
  (void)atomic_sub_fetch_64(&pSession->pJob->job.allocMemSize, oSize);
  (void)atomic_sub_fetch_64(&pPool->allocMemSize, oSize);

  taosRUnLockLatch(&pPool->cfgLock);
}

int32_t mpDirectRealloc(SMemPool* pPool, SMPSession* pSession, void **pPtr, int64_t* size, int64_t* origSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t nSize = *size;

  taosRLockLatch(&pPool->cfgLock);

  MP_ERR_JRET(mpChkQuotaOverflow(pPool, pSession, *size - *origSize));
  
  *pPtr = taosMemRealloc(*pPtr, *size);
  if (NULL != *pPtr) {
    nSize = taosMemSize(*pPtr);
    mpUpdateAllocSize(pPool, pSession, nSize - *origSize, nSize - *size + *origSize);
  } else {
    MP_ERR_RET(terrno);
  }

_return:

  taosRUnLockLatch(&pPool->cfgLock);

  if (code) {
    mpDirectFree(pPool, pSession, *pPtr, origSize);
    *pPtr = NULL;
  }

  *size = nSize;
  
  return TSDB_CODE_SUCCESS;
}

int32_t mpDirectTrim(SMemPool* pPool, SMPSession* pSession, int32_t size, bool* trimed) {
  return taosMemTrim(size, trimed);
}


