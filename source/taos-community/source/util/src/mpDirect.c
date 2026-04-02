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

void* mpDirectAlloc(SMemPool* pPool, SMPJob* pJob, int64_t size) {
  MP_CHECK_QUOTA(pPool, pJob, size);
  
  return taosMemMalloc(size);
}


void* mpDirectAlignAlloc(SMemPool* pPool, SMPJob* pJob, uint32_t alignment, int64_t size) {
  MP_CHECK_QUOTA(pPool, pJob, size);
  
  return taosMemMallocAlign(alignment, size);
}


void* mpDirectCalloc(SMemPool* pPool, SMPJob* pJob, int64_t num, int64_t size) {
  int64_t tSize = num * size;
  MP_CHECK_QUOTA(pPool, pJob, tSize);

  return taosMemCalloc(num, size);
}

void mpDirectFree(SMemPool* pPool, SMPJob* pJob, void *ptr) {
  if (*pPool->cfg.jobQuota > 0) {
    (void)atomic_sub_fetch_64(&pJob->job.allocMemSize, taosMemSize(ptr));
  }
  taosMemFree(ptr);
}


void* mpDirectRealloc(SMemPool* pPool, SMPJob* pJob, void* ptr, int64_t size) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == ptr) {
    return mpDirectAlloc(pPool, pJob, size);
  }

  if (0 == size) {
    mpDirectFree(pPool, pJob, ptr);
    return NULL;
  }

  int64_t oSize = taosMemSize(ptr);

  MP_CHECK_QUOTA(pPool, pJob, size - oSize);

  return taosMemRealloc(ptr, size);
}

void* mpDirectStrdup(SMemPool* pPool, SMPJob* pJob, const void* ptr) {
  if (NULL == ptr) {
    return NULL;
  }
  
  int64_t oSize = strlen(ptr);
  MP_CHECK_QUOTA(pPool, pJob, oSize);
  
  return taosStrdupi(ptr);
}

void* mpDirectStrndup(SMemPool* pPool, SMPJob* pJob, const void* ptr, int64_t size) {
  if (NULL == ptr) {
    return NULL;
  }
  
  int64_t oSize = strlen(ptr);
  MP_CHECK_QUOTA(pPool, pJob, TMIN(oSize, size) + 1);
  
  return taosStrndupi(ptr, size);
}




int64_t mpDirectGetMemSize(SMemPool* pPool, SMPSession* pSession, void *ptr) {
  return taosMemSize(ptr);
}

void mpDirectFullFree(SMemPool* pPool, SMPSession* pSession, void *ptr, int64_t* origSize) {
  int64_t oSize = taosMemSize(ptr);
  if (origSize) {
    *origSize = oSize;
  }
  
  MP_LOCK(MP_READ, &pPool->cfgLock); // tmp test

  taosMemFree(ptr);

  if (NULL != pSession) {
    (void)atomic_sub_fetch_64(&pSession->allocMemSize, oSize);
    (void)atomic_sub_fetch_64(&pSession->pJob->job.allocMemSize, oSize);
  }
  
  (void)atomic_sub_fetch_64(&pPool->allocMemSize, oSize);

  MP_UNLOCK(MP_READ, &pPool->cfgLock);
}



int32_t mpDirectFullAlloc(SMemPool* pPool, SMPSession* pSession, int64_t* size, uint32_t alignment, void** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  void* res = NULL;
  int64_t nSize = *size;
  
  MP_LOCK(MP_READ, &pPool->cfgLock);

  MP_ERR_JRET(mpChkFullQuota(pPool, pSession, *size));
  
  res = alignment ? taosMemMallocAlign(alignment, *size) : taosMemMalloc(*size);
  if (NULL != res) {
    nSize = taosMemSize(res);
    mpUpdateAllocSize(pPool, pSession, nSize, nSize - *size);
  } else {
    if (NULL != pSession) {
      (void)atomic_sub_fetch_64(&pSession->pJob->job.allocMemSize, *size);
    }
    
    (void)atomic_sub_fetch_64(&pPool->allocMemSize, *size);
    
    uError("malloc %" PRId64 " alignment %d failed, code: 0x%x", *size, alignment, terrno);

    code = terrno;
  }

_return:

  MP_UNLOCK(MP_READ, &pPool->cfgLock);
  
  *ppRes = res;
  *size = nSize;
  
  MP_RET(code);
}

int32_t mpDirectFullRealloc(SMemPool* pPool, SMPSession* pSession, void **pPtr, int64_t* size, int64_t* origSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t nSize = *size;

  MP_LOCK(MP_READ, &pPool->cfgLock);

  MP_ERR_JRET(mpChkFullQuota(pPool, pSession, *size - *origSize));
  
  *pPtr = taosMemRealloc(*pPtr, *size);
  if (NULL != *pPtr) {
    nSize = taosMemSize(*pPtr);
    mpUpdateAllocSize(pPool, pSession, nSize - *origSize, nSize - *size + *origSize);
  } else {
    MP_ERR_JRET(terrno);
  }

_return:

  MP_UNLOCK(MP_READ, &pPool->cfgLock);

  if (code) {
    mpDirectFullFree(pPool, pSession, *pPtr, origSize);
    *pPtr = NULL;
  }

  *size = nSize;
  
  return TSDB_CODE_SUCCESS;
}

int32_t mpDirectTrim(SMemPool* pPool, SMPSession* pSession, int32_t size, bool* trimed) {
  return taosMemTrim(size, trimed);
}


