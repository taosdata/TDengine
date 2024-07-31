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

int32_t mpDirectAlloc(SMemPool* pPool, SMPSession* pSession, int64_t size, uint32_t alignment, void** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  MP_ERR_RET(mpChkQuotaOverflow(pPool, pSession, size));
  
  void* res = alignment ? taosMemMallocAlign(alignment, size) : taosMemMalloc(size);
  if (NULL != res) {
    mpUpdateAllocSize(pPool, pSession, size);
  } else {
    (void)atomic_sub_fetch_64(&pSession->pJob->job.allocMemSize, size);
    (void)atomic_sub_fetch_64(&pPool->allocMemSize, size);
    
    uError("malloc %" PRId64 " alignment %d failed, code: 0x%x", size, alignment, terrno);

    code = terrno;
  }

  *ppRes = res;
  
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
  taosMemFree(ptr);
  
  (void)atomic_sub_fetch_64(&pSession->allocMemSize, oSize);
  (void)atomic_sub_fetch_64(&pSession->pJob->job.allocMemSize, oSize);
  (void)atomic_sub_fetch_64(&pPool->allocMemSize, oSize);
}

int32_t mpDirectRealloc(SMemPool* pPool, SMPSession* pSession, void **pPtr, int64_t size, int64_t* origSize) {
  int32_t code = TSDB_CODE_SUCCESS;

  MP_ERR_RET(mpChkQuotaOverflow(pPool, pSession, size));
  
  *pPtr = taosMemRealloc(*pPtr, size);
  if (NULL != *pPtr) {
    mpUpdateAllocSize(pPool, pSession, size - *origSize);
  } else {
    MP_ERR_RET(terrno);
  }

  return TSDB_CODE_SUCCESS;
}


