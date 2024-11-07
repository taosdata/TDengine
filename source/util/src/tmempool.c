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
#include "taos.h"
#include "tglobal.h"

static TdThreadOnce  gMPoolInit = PTHREAD_ONCE_INIT;
void* gMemPoolHandle = NULL;
threadlocal void* threadPoolSession = NULL;
SMemPoolMgmt gMPMgmt = {0};
SMPStrategyFp gMPFps[] = {
  {NULL}, 
  {NULL,        mpDirectAlloc, mpDirectFree, mpDirectGetMemSize, mpDirectRealloc, NULL,               NULL,             mpDirectTrim},
  {mpChunkInit, mpChunkAlloc,  mpChunkFree,  mpChunkGetMemSize,  mpChunkRealloc,  mpChunkInitSession, mpChunkUpdateCfg, NULL}
};


int32_t mpCheckCfg(SMemPoolCfg* cfg) {
  if (cfg->chunkSize < MEMPOOL_MIN_CHUNK_SIZE || cfg->chunkSize > MEMPOOL_MAX_CHUNK_SIZE) {
    uError("invalid memory pool chunkSize:%d", cfg->chunkSize);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  if (cfg->evicPolicy <= 0 || cfg->evicPolicy >= E_EVICT_MAX_VALUE) {
    uError("invalid memory pool evicPolicy:%d", cfg->evicPolicy);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  if (cfg->threadNum <= 0) {
    uError("invalid memory pool threadNum:%d", cfg->threadNum);
    return TSDB_CODE_INVALID_MEM_POOL_PARAM;
  }

  return TSDB_CODE_SUCCESS;
}


void mpFreeCacheGroup(SMPCacheGroup* pGrp) {
  if (NULL == pGrp) {
    return;
  }

  taosMemoryFree(pGrp->pNodes);
  taosMemoryFree(pGrp);
}


int32_t mpAddCacheGroup(SMemPool* pPool, SMPCacheGroupInfo* pInfo, SMPCacheGroup* pHead) {
  SMPCacheGroup* pGrp = NULL;
  if (NULL == pInfo->pGrpHead) {
    pInfo->pGrpHead = taosMemoryCalloc(1, sizeof(*pInfo->pGrpHead));
    if (NULL == pInfo->pGrpHead) {
      uError("malloc chunkCache failed");
      MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    pGrp = pInfo->pGrpHead;
  } else {
    pGrp = (SMPCacheGroup*)taosMemoryCalloc(1, sizeof(SMPCacheGroup));
    pGrp->pNext = pHead;
  }

  pGrp->nodesNum = pInfo->groupNum;
  pGrp->pNodes = taosMemoryCalloc(pGrp->nodesNum, pInfo->nodeSize);
  if (NULL == pGrp->pNodes) {
    uError("calloc %d %d nodes in cache group failed", pGrp->nodesNum, pInfo->nodeSize);
    MP_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (pHead && atomic_val_compare_exchange_ptr(&pInfo->pGrpHead, pHead, pGrp) != pHead) {
    mpFreeCacheGroup(pGrp);
    return TSDB_CODE_SUCCESS;
  }

  atomic_add_fetch_64(&pInfo->allocNum, pGrp->nodesNum);

  return TSDB_CODE_SUCCESS;
}

void mpDestroyCacheGroup(SMPCacheGroupInfo* pInfo) {
  SMPCacheGroup* pGrp = pInfo->pGrpHead;
  SMPCacheGroup* pNext = NULL;
  while (NULL != pGrp) {
    pNext = pGrp->pNext;

    mpFreeCacheGroup(pGrp);

    pGrp = pNext;
  }
}


int32_t mpPopIdleNode(SMemPool* pPool, SMPCacheGroupInfo* pInfo, void** ppRes) {
  SMPCacheGroup* pGrp = NULL;
  SMPListNode* pNode = NULL;
  
  while (true) {
    pNode = (SMPListNode*)atomic_load_ptr(&pInfo->pIdleList);
    if (NULL == pNode) {
      break;
    }

    if (atomic_val_compare_exchange_ptr(&pInfo->pIdleList, pNode, pNode->pNext) != pNode) {
      continue;
    }

    pNode->pNext = NULL;
    goto _return;
  }

  while (true) {
    pGrp = atomic_load_ptr(&pInfo->pGrpHead);
    int32_t offset = atomic_fetch_add_32(&pGrp->idleOffset, 1);
    if (offset < pGrp->nodesNum) {
      pNode = (SMPListNode*)((char*)pGrp->pNodes + offset * pInfo->nodeSize);
      break;
    } else {
      atomic_sub_fetch_32(&pGrp->idleOffset, 1);
    }
    
    MP_ERR_RET(mpAddCacheGroup(pPool, pInfo, pGrp));
  }

_return:

  *ppRes = pNode;

  return TSDB_CODE_SUCCESS;
}

void mpPushIdleNode(SMemPool* pPool, SMPCacheGroupInfo* pInfo, SMPListNode* pNode) {
  SMPCacheGroup* pGrp = NULL;
  SMPListNode* pOrig = NULL;
  
  while (true) {
    pOrig = (SMPListNode*)atomic_load_ptr(&pInfo->pIdleList);
    pNode->pNext = pOrig;
    
    if (atomic_val_compare_exchange_ptr(&pInfo->pIdleList, pOrig, pNode) != pOrig) {
      continue;
    }

    break;
  }
}


int32_t mpUpdateCfg(SMemPool* pPool) {
  if (gMPFps[gMPMgmt.strategy].updateCfgFp) {
    MP_ERR_RET((*gMPFps[gMPMgmt.strategy].updateCfgFp)(pPool));
  }

  uDebug("memPool %s cfg updated, reserveSize:%dMB, jobQuota:%dMB, threadNum:%d", 
      pPool->name, *pPool->cfg.reserveSize, *pPool->cfg.jobQuota, pPool->cfg.threadNum);

  return TSDB_CODE_SUCCESS;
}

uint32_t mpFileIdHashFp(const char* fileId, uint32_t len) {
  return *(uint32_t*)fileId;
}


int32_t mpInitStat(SMPStatPos* pStat, bool sessionStat) {
  pStat->remainHash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pStat->remainHash) {
    uError("memPool init posStat remainHash failed, error:%s, sessionStat:%d", tstrerror(terrno), sessionStat);
    return terrno;
  }

  pStat->allocHash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pStat->allocHash) {
    uError("memPool init posStat allocHash failed, error:%s, sessionStat:%d", tstrerror(terrno), sessionStat);
    return terrno;
  }

  pStat->freeHash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pStat->freeHash) {
    uError("memPool init posStat freeHash failed, error:%s, sessionStat:%d", tstrerror(terrno), sessionStat);
    return terrno;
  }

  pStat->fileHash = taosHashInit(1024, mpFileIdHashFp, false, HASH_ENTRY_LOCK);
  if (NULL == pStat->fileHash) {
    uError("memPool init posStat fileHash failed, error:%s, sessionStat:%d", tstrerror(terrno), sessionStat);
    return terrno;
  }

  uDebug("memPool stat initialized, sessionStat:%d", sessionStat);

  return TSDB_CODE_SUCCESS;
}

int32_t mpInit(SMemPool* pPool, char* poolName, SMemPoolCfg* cfg) {
  MP_ERR_RET(mpCheckCfg(cfg));
  
  TAOS_MEMCPY(&pPool->cfg, cfg, sizeof(*cfg));
  
  pPool->name = taosStrdup(poolName);
  if (NULL == pPool->name) {
    uError("calloc memory pool name %s failed", poolName);
    MP_ERR_RET(terrno);
  }

  MP_ERR_RET(mpUpdateCfg(pPool));

  pPool->ctrl.statFlags = MP_STAT_FLAG_LOG_ALL;
  pPool->ctrl.funcFlags = MP_CTRL_FLAG_PRINT_STAT | MP_CTRL_FLAG_CHECK_STAT;

  pPool->sessionCache.groupNum = MP_SESSION_CACHE_ALLOC_BATCH_SIZE;
  pPool->sessionCache.nodeSize = sizeof(SMPSession);

  MP_ERR_RET(mpAddCacheGroup(pPool, &pPool->sessionCache, NULL));

  if (gMPFps[gMPMgmt.strategy].initFp) {
    MP_ERR_RET((*gMPFps[gMPMgmt.strategy].initFp)(pPool, poolName, cfg));
  }

  MP_ERR_RET(mpInitStat(&pPool->stat.posStat, false));
  
  return TSDB_CODE_SUCCESS;
}

FORCE_INLINE void mpUpdateMaxAllocSize(int64_t* pMaxAllocMemSize, int64_t newSize) {
  int64_t maxAllocMemSize = atomic_load_64(pMaxAllocMemSize);
  while (true) {
    if (newSize <= maxAllocMemSize) {
      break;
    }
    
    if (maxAllocMemSize == atomic_val_compare_exchange_64(pMaxAllocMemSize, maxAllocMemSize, newSize)) {
      break;
    }

    maxAllocMemSize = atomic_load_64(pMaxAllocMemSize);
  }
}

void mpUpdateAllocSize(SMemPool* pPool, SMPSession* pSession, int64_t size, int64_t addSize) {
  if (addSize) {
    if (NULL != pSession) {
      atomic_add_fetch_64(&pSession->pJob->job.allocMemSize, addSize);
    }
    atomic_add_fetch_64(&pPool->allocMemSize, addSize);
  }

  if (NULL != pSession) {
    int64_t allocMemSize = atomic_add_fetch_64(&pSession->allocMemSize, size);
    mpUpdateMaxAllocSize(&pSession->maxAllocMemSize, allocMemSize);

    allocMemSize = atomic_load_64(&pSession->pJob->job.allocMemSize);
    mpUpdateMaxAllocSize(&pSession->pJob->job.maxAllocMemSize, allocMemSize);
  }
  
  int64_t allocMemSize = atomic_load_64(&pPool->allocMemSize);
  mpUpdateMaxAllocSize(&pPool->maxAllocMemSize, allocMemSize);
}

int32_t mpPutRetireMsgToQueue(SMemPool* pPool, bool retireLowLevel) {
  if (retireLowLevel) {
    if (0 == atomic_val_compare_exchange_8(&gMPMgmt.msgQueue.lowLevelRetire, 0, 1)) {
      atomic_store_ptr(&gMPMgmt.msgQueue.pPool, pPool);
      MP_ERR_RET(tsem2_post(&gMPMgmt.threadSem));
    }
    
    return TSDB_CODE_SUCCESS;
  }

  if (0 == atomic_val_compare_exchange_8(&gMPMgmt.msgQueue.midLevelRetire, 0, 1)) {
    atomic_store_ptr(&gMPMgmt.msgQueue.pPool, pPool);
    MP_ERR_RET(tsem2_post(&gMPMgmt.threadSem));
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t mpChkQuotaOverflow(SMemPool* pPool, SMPSession* pSession, int64_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pSession) {
    (void)atomic_add_fetch_64(&pPool->allocMemSize, size);
    return code;
  }
  
  SMPJob* pJob = pSession->pJob;  
  int64_t cAllocSize = atomic_add_fetch_64(&pJob->job.allocMemSize, size);
  int64_t quota = atomic_load_32(pPool->cfg.jobQuota);
  if (quota > 0 && cAllocSize / 1048576UL > quota) {
    code = TSDB_CODE_QRY_REACH_QMEM_THRESHOLD;
    uWarn("job 0x%" PRIx64 " allocSize %" PRId64 " is over than quota %" PRId64, pJob->job.jobId, cAllocSize, quota);
    pPool->cfg.cb.reachFp(pJob->job.jobId, code);
    (void)atomic_sub_fetch_64(&pJob->job.allocMemSize, size);
    MP_RET(code);
  }

  if (atomic_load_64(&tsCurrentAvailMemorySize) <= ((atomic_load_32(pPool->cfg.reserveSize) * 1048576UL) + size)) {
    code = TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED;
    uWarn("%s pool sysAvailMemSize %" PRId64 " can't alloc %" PRId64" while keeping reserveSize %dMB", 
        pPool->name, atomic_load_64(&tsCurrentAvailMemorySize), size, *pPool->cfg.reserveSize);
    pPool->cfg.cb.reachFp(pJob->job.jobId, code);
    (void)atomic_sub_fetch_64(&pJob->job.allocMemSize, size);
    MP_RET(code);
  }

  (void)atomic_add_fetch_64(&pPool->allocMemSize, size);

/*
  int64_t pAllocSize = atomic_add_fetch_64(&pPool->allocMemSize, size);
  if (pAllocSize >= atomic_load_32(pPool->cfg.upperLimitSize) * 1048576UL) {
    code = TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED;
    uWarn("%s pool allocSize %" PRId64 " reaches the upperLimit %" PRId64, pPool->name, pAllocSize, atomic_load_32(pPool->cfg.upperLimitSize) * 1048576UL);
    pPool->cfg.cb.retireJobFp(&pJob->job, code);
    (void)atomic_sub_fetch_64(&pJob->job.allocMemSize, size);
    (void)atomic_sub_fetch_64(&pPool->allocMemSize, size);
    MP_RET(code);
  }
*/

  return TSDB_CODE_SUCCESS;
}

int64_t mpGetMemorySizeImpl(SMemPool* pPool, SMPSession* pSession, void *ptr) {
  return (*gMPFps[gMPMgmt.strategy].getSizeFp)(pPool, pSession, ptr);
}

int32_t mpMalloc(SMemPool* pPool, SMPSession* pSession, int64_t* size, uint32_t alignment, void** ppRes) {
  MP_RET((*gMPFps[gMPMgmt.strategy].allocFp)(pPool, pSession, size, alignment, ppRes));
}

int32_t mpCalloc(SMemPool* pPool, SMPSession* pSession, int64_t* size, void** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  void *res = NULL;

  MP_ERR_RET(mpMalloc(pPool, pSession, size, 0, &res));

  if (NULL != res) {
    TAOS_MEMSET(res, 0, *size);
  }

_return:

  *ppRes = res;

  return code;
}


void mpFree(SMemPool* pPool, SMPSession* pSession, void *ptr, int64_t* origSize) {
  if (NULL == ptr) {
    if (origSize) {
      *origSize = 0;
    }
    
    return;
  }

  (*gMPFps[gMPMgmt.strategy].freeFp)(pPool, pSession, ptr, origSize);
}

int32_t mpRealloc(SMemPool* pPool, SMPSession* pSession, void **pPtr, int64_t* size, int64_t* origSize) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == *pPtr) {
    *origSize = 0;
    MP_RET(mpMalloc(pPool, pSession, size, 0, pPtr));
  }

  if (0 == *size) {
    mpFree(pPool, pSession, *pPtr, origSize);
    *pPtr = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *origSize = mpGetMemorySizeImpl(pPool, pSession, *pPtr);

  MP_RET((*gMPFps[gMPMgmt.strategy].reallocFp)(pPool, pSession, pPtr, size, origSize));
}

int32_t mpTrim(SMemPool* pPool, SMPSession* pSession, int32_t size, bool* trimed) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (gMPFps[gMPMgmt.strategy].trimFp) {
    MP_RET((*gMPFps[gMPMgmt.strategy].trimFp)(pPool, pSession, size, trimed));
  }

  return code;
}


void mpPrintStatDetail(SMPCtrlInfo* pCtrl, SMPStatDetail* pDetail, char* detailName, int64_t maxAllocSize) {
  if (!MP_GET_FLAG(pCtrl->funcFlags, MP_CTRL_FLAG_PRINT_STAT)) {
    return;
  }

  uInfo("MemPool [%s] stat detail:", detailName);

  uInfo("Max Used Memory Size: %" PRId64, maxAllocSize);
  
  uInfo("[times]:");
  switch (gMPMgmt.strategy) {
    case E_MP_STRATEGY_DIRECT:
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Malloc", pDetail->times.memMalloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Calloc", pDetail->times.memCalloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Realloc", pDetail->times.memRealloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Strdup", pDetail->times.memStrdup));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Strndup", pDetail->times.memStrndup));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Free", pDetail->times.memFree));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Trim", pDetail->times.memTrim));
      break;
    case E_MP_STRATEGY_CHUNK:
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkMalloc", pDetail->times.chunkMalloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkRecycle", pDetail->times.chunkRecycle));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkReUse", pDetail->times.chunkReUse));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkFree", pDetail->times.chunkFree));
      break;
    default:
      break;
  }
  
  uInfo("[bytes]:");
  switch (gMPMgmt.strategy) {
    case E_MP_STRATEGY_DIRECT:  
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Malloc", pDetail->bytes.memMalloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Calloc", pDetail->bytes.memCalloc));
      uInfo(MP_STAT_ORIG_FORMAT, MP_STAT_ORIG_VALUE("Realloc", pDetail->bytes.memRealloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Strdup", pDetail->bytes.memStrdup));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Strndup", pDetail->bytes.memStrndup));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Free", pDetail->bytes.memFree));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("Trim", pDetail->bytes.memTrim));
      break;
  case E_MP_STRATEGY_CHUNK:
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkMalloc", pDetail->bytes.chunkMalloc));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkRecycle", pDetail->bytes.chunkRecycle));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkReUse", pDetail->bytes.chunkReUse));
      uInfo(MP_STAT_FORMAT, MP_STAT_VALUE("chunkFree", pDetail->bytes.chunkFree));
      break;
    default:
      break;
  }
}

int32_t mpAddToRemainAllocHash(SHashObj* pHash, SMPFileLine* pFileLine) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPAllocStat stat = {0}, *pStat = NULL;
  
  while (true) {
    pStat = (SMPAllocStat*)taosHashGet(pHash, &pFileLine->fl, sizeof(pFileLine->fl));
    if (NULL == pStat) {
      code = taosHashPut(pHash, &pFileLine->fl, sizeof(pFileLine->fl), &stat, sizeof(stat));
      if (TSDB_CODE_SUCCESS != code) {
        if (TSDB_CODE_DUP_KEY == code) {
          continue;
        }
        
        uError("taosHashPut to remain alloc hash failed, error:%s", tstrerror(code));
        return code;
      }

      continue;
    }

    atomic_add_fetch_64(&pStat->allocBytes, pFileLine->size);
    atomic_add_fetch_64(&pStat->allocTimes, 1);
    break;
  }

  return TSDB_CODE_SUCCESS;
}

void mpPrintPosRemainStat(SMPStatPos* pStat) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t remainNum = taosHashGetSize(pStat->remainHash);
  if (remainNum <= 0) {
    uInfo("no alloc remaining memory");
    return;
  }
  
  SHashObj* pAllocHash = taosHashInit(remainNum / 10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pAllocHash) {
    uError("taosHashInit pAllocHash failed, error:%s, remainNum:%d", tstrerror(terrno), remainNum);
    return;
  }

  SMPFileLine* pFileLine = NULL;
  void* pIter = taosHashIterate(pStat->remainHash, NULL);
  while (pIter) {
    pFileLine = (SMPFileLine*)pIter;

    MP_ERR_JRET(mpAddToRemainAllocHash(pAllocHash, pFileLine));

    pIter = taosHashIterate(pStat->remainHash, pIter);
  }

  SMPAllocStat* pAlloc = NULL;
  pIter = taosHashIterate(pAllocHash, NULL);
  while (pIter) {
    pAlloc = (SMPAllocStat*)pIter;
    SMPFileLineId* pId = (SMPFileLineId*)taosHashGetKey(pIter, NULL);
    SMPAllocStat* pAlloc = (SMPAllocStat*)taosHashGet(pStat->allocHash, pId, sizeof(*pId));
    char* pFileName = (char*)taosHashGet(pStat->fileHash, &pId->fileId, sizeof(pId->fileId));
    if (NULL == pAlloc || NULL == pFileName) {
      uError("fail to get pId in allocHash or fileHash, pAlloc:%p, pFileName:%p", pAlloc, pFileName);
      goto _return;
    }

    uInfo("REMAINING: %" PRId64 " bytes alloced by %s:%d in %" PRId64 " times", pAlloc->allocBytes, pFileName, pId->line, pAlloc->allocTimes);
    
    pIter = taosHashIterate(pAllocHash, pIter);
  }

_return:

  taosHashCleanup(pAllocHash);
}

void mpPrintPosAllocStat(SMPStatPos* pStat) {

}

void mpPrintPosFreeStat(SMPStatPos* pStat) {

}

void mpPrintPosStat(SMPCtrlInfo* pCtrl, SMPStatPos* pStat, char* detailName) {
  if (!MP_GET_FLAG(pCtrl->funcFlags, MP_CTRL_FLAG_PRINT_STAT)) {
    return;
  }

  uInfo("MemPool [%s] Pos Stat:", detailName);
  uInfo("error times: %" PRId64, pStat->logErrTimes);

  mpPrintPosRemainStat(pStat);

  mpPrintPosAllocStat(pStat);

  mpPrintPosFreeStat(pStat);
}

void mpPrintNodeStat(SMPCtrlInfo* pCtrl, SHashObj* pHash, char* detailName) {
  //TODO
}

void mpPrintSessionStat(SMPCtrlInfo* pCtrl, SMPStatSession* pSessStat, char* detailName) {
  if (!MP_GET_FLAG(pCtrl->funcFlags, MP_CTRL_FLAG_PRINT_STAT)) {
    return;
  }

  uInfo("MemPool [%s] session stat:", detailName);
  uInfo("init session succeed num: %" PRId64, pSessStat->initSucc);
  uInfo("init session failed num: %" PRId64, pSessStat->initFail);
  uInfo("session destroyed num: %" PRId64, pSessStat->destroyNum);
}



void mpLogDetailStat(SMPStatDetail* pDetail, EMPStatLogItem item, SMPStatInput* pInput) {
  switch (item) {
    case E_MP_STAT_LOG_MEM_MALLOC: {
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memMalloc.exec, 1);
        atomic_add_fetch_64(&pDetail->bytes.memMalloc.exec, pInput->size);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memMalloc.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memMalloc.succ, pInput->size);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memMalloc.fail, 1);
        atomic_add_fetch_64(&pDetail->bytes.memMalloc.fail, pInput->size);
      } 
      break;
    }
    case E_MP_STAT_LOG_MEM_CALLOC:{
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memCalloc.exec, 1);
        atomic_add_fetch_64(&pDetail->bytes.memCalloc.exec, pInput->size);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memCalloc.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memCalloc.succ, pInput->size);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memCalloc.fail, 1);
        atomic_add_fetch_64(&pDetail->bytes.memCalloc.fail, pInput->size);
      } 
      break;
    }
    case E_MP_STAT_LOG_MEM_REALLOC:{
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memRealloc.exec, 1);
        atomic_add_fetch_64(&pDetail->bytes.memRealloc.exec, pInput->size);
        atomic_add_fetch_64(&pDetail->bytes.memRealloc.origExec, pInput->origSize);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memRealloc.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memRealloc.succ, pInput->size);
        atomic_add_fetch_64(&pDetail->bytes.memRealloc.origSucc, pInput->origSize);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memRealloc.fail, 1);
        atomic_add_fetch_64(&pDetail->bytes.memRealloc.fail, pInput->size);
        atomic_add_fetch_64(&pDetail->bytes.memRealloc.origFail, pInput->origSize);
      } 
      break;
    }
    case E_MP_STAT_LOG_MEM_FREE:{
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memFree.exec, 1);
        atomic_add_fetch_64(&pDetail->bytes.memFree.exec, pInput->size);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memFree.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memFree.succ, pInput->size);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memFree.fail, 1);
        atomic_add_fetch_64(&pDetail->bytes.memFree.fail, pInput->size);
      } 
      break;
    }
    case E_MP_STAT_LOG_MEM_STRDUP: {
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memStrdup.exec, 1);
        atomic_add_fetch_64(&pDetail->bytes.memStrdup.exec, pInput->size);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memStrdup.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memStrdup.succ, pInput->size);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memStrdup.fail, 1);
        atomic_add_fetch_64(&pDetail->bytes.memStrdup.fail, pInput->size);
      } 
      break;
    }
    case E_MP_STAT_LOG_MEM_STRNDUP: {
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memStrndup.exec, 1);
        atomic_add_fetch_64(&pDetail->bytes.memStrndup.exec, pInput->size);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memStrndup.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memStrndup.succ, pInput->size);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memStrndup.fail, 1);
        atomic_add_fetch_64(&pDetail->bytes.memStrndup.fail, pInput->size);
      } 
      break;
    }
    case E_MP_STAT_LOG_MEM_TRIM: {
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_EXEC)) {
        atomic_add_fetch_64(&pDetail->times.memTrim.exec, 1);
      }
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
        atomic_add_fetch_64(&pDetail->times.memTrim.succ, 1);
        atomic_add_fetch_64(&pDetail->bytes.memTrim.succ, pInput->size);
      } 
      if (MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_FAIL)) {
        atomic_add_fetch_64(&pDetail->times.memTrim.fail, 1);
      } 
      break;
    }
    case E_MP_STAT_LOG_CHUNK_MALLOC:  
    case E_MP_STAT_LOG_CHUNK_RECYCLE:  
    case E_MP_STAT_LOG_CHUNK_REUSE:
    case E_MP_STAT_LOG_CHUNK_FREE: {

    }
    default:
      uError("Invalid stat item: %d", item);
      break;
  }
}

int32_t mpGetAllocFreeStat(SHashObj* pHash, void* pKey, int32_t keyLen, void* pNew, int32_t newSize, void** ppRes) {
  void* pStat = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  while (true) {
    pStat = taosHashGet(pHash, pKey, keyLen);
    if (NULL != pStat) {
      *ppRes = pStat;
      break;
    }

    code = taosHashPut(pHash, pKey, keyLen, pNew, newSize);
    if (code) {
      if (TSDB_CODE_DUP_KEY == code) {
        continue;
      }

      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mpGetPosStatFileId(SMPStatPos* pStat, char* fileName, uint32_t* pId, bool sessionStat) {
  uint32_t hashVal = MurmurHash3_32(fileName, strlen(fileName));
  int32_t code = taosHashPut(pStat->fileHash, &hashVal, sizeof(hashVal), fileName, strlen(fileName) + 1);
  if (code && TSDB_CODE_DUP_KEY != code) {
    return code;
  }

  *pId = hashVal;
  
  return TSDB_CODE_SUCCESS;
}

void mpLogPosStat(SMPStatPos* pStat, EMPStatLogItem item, SMPStatInput* pInput, bool sessionStat) {
  if (!MP_GET_FLAG(pInput->procFlags, MP_STAT_PROC_FLAG_RES_SUCC)) {
    return;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  
  switch (item) {
    case E_MP_STAT_LOG_MEM_MALLOC: 
    case E_MP_STAT_LOG_MEM_CALLOC: 
    case E_MP_STAT_LOG_MEM_STRDUP: 
    case E_MP_STAT_LOG_MEM_STRNDUP: {
      SMPAllocStat allocStat = {0}, *pAlloc = NULL;
      SMPFileLine fileLine = {.fl.line = pInput->line, .size = pInput->size};
      code = mpGetPosStatFileId(pStat, pInput->file, &fileLine.fl.fileId, sessionStat);
      if (TSDB_CODE_SUCCESS != code) {
        uError("add pMem:%p file:%s line:%d to fileHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }
      code = taosHashPut(pStat->remainHash, &pInput->pMem, POINTER_BYTES, &fileLine, sizeof(fileLine));
      if (TSDB_CODE_SUCCESS != code) {
        uError("add pMem:%p file:%s line:%d to remainHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }
      code = mpGetAllocFreeStat(pStat->allocHash, &fileLine.fl, sizeof(fileLine.fl), (void*)&allocStat, sizeof(allocStat), (void**)&pAlloc);
      if (TSDB_CODE_SUCCESS != code) {
        uError("add pMem:%p file:%s line:%d to allocHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }
      
      atomic_add_fetch_64(&pAlloc->allocTimes, 1);
      atomic_add_fetch_64(&pAlloc->allocBytes, pInput->size);
      break;
    }
    case E_MP_STAT_LOG_MEM_REALLOC: {
      SMPAllocStat allocStat = {0}, *pAlloc = NULL;
      SMPFreeStat freeStat = {0}, *pFree = NULL;
      SMPFileLine fileLine = {.fl.line = pInput->line, .size = pInput->size};
      code = mpGetPosStatFileId(pStat, pInput->file, &fileLine.fl.fileId, sessionStat);
      if (TSDB_CODE_SUCCESS != code) {
        uError("realloc: add pMem:%p file:%s line:%d to fileHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }

      ASSERT((pInput->pOrigMem && pInput->origSize > 0) || (NULL == pInput->pOrigMem && pInput->origSize == 0));
      
      if (pInput->pOrigMem && pInput->origSize > 0) {
        code = taosHashRemove(pStat->remainHash, &pInput->pOrigMem, POINTER_BYTES);
        if (TSDB_CODE_SUCCESS != code) {
          uError("realloc: rm pOrigMem:%p file:%s line:%d from remainHash failed, error:%s, sessionStat:%d", 
            pInput->pOrigMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
          MP_ERR_JRET(code);
        }
        code = mpGetAllocFreeStat(pStat->freeHash, &fileLine.fl, sizeof(fileLine.fl), (void*)&freeStat, sizeof(freeStat), (void**)&pFree);
        if (TSDB_CODE_SUCCESS != code) {
          uError("realloc: add pOrigMem:%p file:%s line:%d to freeHash failed, error:%s, sessionStat:%d", 
            pInput->pOrigMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
          MP_ERR_JRET(code);
        }
        
        atomic_add_fetch_64(&pFree->freeTimes, 1);
        atomic_add_fetch_64(&pFree->freeBytes, pInput->origSize);
      }
      
      code = taosHashPut(pStat->remainHash, &pInput->pMem, POINTER_BYTES, &fileLine, sizeof(fileLine));
      if (TSDB_CODE_SUCCESS != code) {
        uError("realloc: add pMem:%p file:%s line:%d to remainHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }
      
      code = mpGetAllocFreeStat(pStat->allocHash, &fileLine.fl, sizeof(fileLine.fl), (void*)&allocStat, sizeof(allocStat), (void**)&pAlloc);
      if (TSDB_CODE_SUCCESS != code) {
        uError("realloc: add pMem:%p file:%s line:%d to allocHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }

      atomic_add_fetch_64(&pAlloc->allocTimes, 1);
      atomic_add_fetch_64(&pAlloc->allocBytes, pInput->size);
      break;
    }
    case E_MP_STAT_LOG_MEM_FREE: {
      SMPAllocStat allocStat = {0}, *pAlloc = NULL;
      SMPFreeStat freeStat = {0}, *pFree = NULL;
      SMPFileLineId fl = {.line = pInput->line};
      code = mpGetPosStatFileId(pStat, pInput->file, &fl.fileId, sessionStat);
      if (TSDB_CODE_SUCCESS != code) {
        uError("free: add pMem:%p file:%s line:%d to fileHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }

      code = taosHashRemove(pStat->remainHash, &pInput->pMem, POINTER_BYTES);
      if (TSDB_CODE_SUCCESS != code) {
        uError("free: rm pMem:%p file:%s line:%d to remainHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }

      code = mpGetAllocFreeStat(pStat->freeHash, &fl, sizeof(fl), (void*)&freeStat, sizeof(freeStat), (void**)&pFree);
      if (TSDB_CODE_SUCCESS != code) {
        uError("realloc: add pMem:%p file:%s line:%d to freeHash failed, error:%s, sessionStat:%d", 
          pInput->pMem, pInput->file, pInput->line, tstrerror(code), sessionStat);
        MP_ERR_JRET(code);
      }
        
      atomic_add_fetch_64(&pFree->freeTimes, 1);
      atomic_add_fetch_64(&pFree->freeBytes, pInput->size);
      break;
    }
    case E_MP_STAT_LOG_MEM_TRIM:
      break;
    case E_MP_STAT_LOG_CHUNK_MALLOC:  
    case E_MP_STAT_LOG_CHUNK_RECYCLE:  
    case E_MP_STAT_LOG_CHUNK_REUSE:
    case E_MP_STAT_LOG_CHUNK_FREE: {
      break;
    }
    default:
      uError("Invalid stat item: %d", item);
      break;
  }

  return;

_return:
  
  atomic_add_fetch_64(&pStat->logErrTimes, 1);
}


void mpLogStat(SMemPool* pPool, SMPSession* pSession, EMPStatLogItem item, SMPStatInput* pInput) {
  switch (item) {
    case E_MP_STAT_LOG_MEM_MALLOC:
    case E_MP_STAT_LOG_MEM_CALLOC:
    case E_MP_STAT_LOG_MEM_REALLOC:
    case E_MP_STAT_LOG_MEM_FREE:
    case E_MP_STAT_LOG_MEM_STRDUP: 
    case E_MP_STAT_LOG_MEM_STRNDUP: 
    case E_MP_STAT_LOG_MEM_TRIM: {
      if (pSession && MP_GET_FLAG(pSession->ctrl.statFlags, MP_LOG_FLAG_ALL_MEM)) {
        mpLogDetailStat(&pSession->stat.statDetail, item, pInput);
      }
      if (MP_GET_FLAG(pPool->ctrl.statFlags, MP_LOG_FLAG_ALL_MEM)) {
        mpLogDetailStat(&pPool->stat.statDetail, item, pInput);
      }
      if (pSession && MP_GET_FLAG(pSession->ctrl.statFlags, MP_LOG_FLAG_ALL_POS)) {
        mpLogPosStat(&pSession->stat.posStat, item, pInput, true);
      }
      if (MP_GET_FLAG(pPool->ctrl.statFlags, MP_LOG_FLAG_ALL_POS)) {
        mpLogPosStat(&pPool->stat.posStat, item, pInput, false);
      }
      break;
    }
    case E_MP_STAT_LOG_CHUNK_MALLOC:  
    case E_MP_STAT_LOG_CHUNK_RECYCLE:  
    case E_MP_STAT_LOG_CHUNK_REUSE:
    case E_MP_STAT_LOG_CHUNK_FREE: {
      break;
    }
    default:
      uError("Invalid stat item: %d", item);
      break;
  }
}

void mpCheckStatDetail(void* poolHandle, void* session, char* detailName) {
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  SMPCtrlInfo* pCtrl = NULL;
  SMPStatDetail* pDetail = NULL;

  if (NULL != session) {
    pCtrl = &pSession->ctrl;
    pDetail = &pSession->stat.statDetail;
    if (MP_GET_FLAG(pCtrl->funcFlags, MP_CTRL_FLAG_CHECK_STAT)) {
      int64_t allocSize = MEMPOOL_GET_ALLOC_SIZE(pDetail);
      int64_t freeSize = MEMPOOL_GET_FREE_SIZE(pDetail);

      if (allocSize != freeSize) {
        uError("%s Session in JOB:0x%" PRIx64 " stat check failed, allocSize:%" PRId64 ", freeSize:%" PRId64, 
            detailName, pSession->pJob->job.jobId, allocSize, freeSize);

        taosMemPoolPrintStat(NULL, pSession, detailName);
      } else {
        uDebug("%s Session in JOB:0x%" PRIx64 " stat check succeed, allocSize:%" PRId64 ", freeSize:%" PRId64, 
            detailName, pSession->pJob->job.jobId, allocSize, freeSize);
      }
    }
  }

  if (NULL != poolHandle) {
    pCtrl = &pPool->ctrl;
    pDetail = &pPool->stat.statDetail;
    int64_t sessInit = atomic_load_64(&pPool->stat.statSession.initFail) + atomic_load_64(&pPool->stat.statSession.initSucc);
    if (MP_GET_FLAG(pCtrl->funcFlags, MP_CTRL_FLAG_CHECK_STAT) && sessInit == atomic_load_64(&pPool->stat.statSession.destroyNum)) {
      int64_t allocSize = pDetail->bytes.memMalloc.succ + pDetail->bytes.memCalloc.succ + pDetail->bytes.memRealloc.succ + pDetail->bytes.memStrdup.succ + pDetail->bytes.memStrndup.succ;
      int64_t freeSize = pDetail->bytes.memRealloc.origSucc + pDetail->bytes.memFree.succ;

      if (allocSize != freeSize) {
        uError("%s MemPool %s stat check failed, allocSize:%" PRId64 ", freeSize:%" PRId64, detailName, pPool->name, allocSize, freeSize);

        taosMemPoolPrintStat(poolHandle, NULL, detailName);
      } else {
        uDebug("%s MemPool %s stat check succeed, allocSize:%" PRId64 ", freeSize:%" PRId64, detailName, pPool->name, allocSize, freeSize);
      }
    }
  }
}


void mpCheckUpateCfg(void) {
/*
  taosRLockLatch(&gMPMgmt.poolLock);
  int32_t poolNum = taosArrayGetSize(gMPMgmt.poolList);
  for (int32_t i = 0; i < poolNum; ++i) {
    SMemPool* pPool = (SMemPool*)taosArrayGetP(gMPMgmt.poolList, i);
    if (pPool->cfg.cb.cfgUpdateFp) {
      (*pPool->cfg.cb.cfgUpdateFp)((void*)pPool, &pPool->cfg);
    }
  }
  taosRUnLockLatch(&gMPMgmt.poolLock);
*/  
}

void mpUpdateSystemAvailableMemorySize() {
  static int64_t errorTimes = 0;
  int64_t sysAvailSize = 0;
  
  int32_t code = taosGetSysAvailMemory(&sysAvailSize);
  if (TSDB_CODE_SUCCESS != code) {
    errorTimes++;
    if (0 == errorTimes % 1000) {
      uError("get system available memory size failed, error: %s, errorTimes:%" PRId64, tstrerror(code), errorTimes);
    }
    
    return;
  }

  atomic_store_64(&tsCurrentAvailMemorySize, sysAvailSize);
}

void* mpMgmtThreadFunc(void* param) {
  int32_t timeout = 0;
  int64_t retireSize = 0;
  SMemPool* pPool = (SMemPool*)gMemPoolHandle;
  
  while (0 == atomic_load_8(&gMPMgmt.modExit)) {
    mpUpdateSystemAvailableMemorySize();

    retireSize = atomic_load_32(pPool->cfg.reserveSize) * 1048576UL - tsCurrentAvailMemorySize;
    if (retireSize > 0) {
      (*pPool->cfg.cb.failFp)(retireSize, TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED);
    }

    taosMsleep(MP_DEFAULT_MEM_CHK_INTERVAL_MS);
/*
    timeout = tsem2_timewait(&gMPMgmt.threadSem, gMPMgmt.waitMs);
    if (0 != timeout) {
      mpUpdateSystemAvailableMemorySize();
      continue;
    }

    if (atomic_load_8(&gMPMgmt.msgQueue.midLevelRetire)) {
      (*gMPMgmt.msgQueue.pPool->cfg.cb.retireJobsFp)(gMPMgmt.msgQueue.pPool, atomic_load_64(&gMPMgmt.msgQueue.pPool->cfg.retireUnitSize), false, TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED);
    } else if (atomic_load_8(&gMPMgmt.msgQueue.lowLevelRetire)) {
      (*gMPMgmt.msgQueue.pPool->cfg.cb.retireJobsFp)(gMPMgmt.msgQueue.pPool, atomic_load_64(&gMPMgmt.msgQueue.pPool->cfg.retireUnitSize), true, TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED);
    }
*/    
  }
  
  return NULL;
}

int32_t mpCreateMgmtThread() {
  int32_t code = TSDB_CODE_SUCCESS;
  TdThreadAttr thAttr;
  MP_ERR_RET(taosThreadAttrInit(&thAttr));
  MP_ERR_JRET(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE));
  code = taosThreadCreate(&gMPMgmt.poolMgmtThread, &thAttr, mpMgmtThreadFunc, NULL);
  if (code != 0) {
    uError("failed to create memPool mgmt thread, error: 0x%x", code);
    (void)taosThreadAttrDestroy(&thAttr);
    MP_ERR_JRET(code);
  }

_return:

  MP_ERR_RET(taosThreadAttrDestroy(&thAttr));

  return code;
}

void mpModInit(void) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  taosInitRWLatch(&gMPMgmt.poolLock);
  
  gMPMgmt.poolList = taosArrayInit(10, POINTER_BYTES);
  if (NULL == gMPMgmt.poolList) {
    MP_ERR_JRET(terrno);
  }

  gMPMgmt.strategy = E_MP_STRATEGY_DIRECT;

  gMPMgmt.code = tsem2_init(&gMPMgmt.threadSem, 0, 0);
  if (TSDB_CODE_SUCCESS != gMPMgmt.code) {
    uError("failed to init sem2, error: 0x%x", gMPMgmt.code);
    return;
  }

  gMPMgmt.waitMs = MP_DEFAULT_MEM_CHK_INTERVAL_MS;

_return:

  gMPMgmt.code = code;
}

void taosMemPoolPrintStat(void* poolHandle, void* session, char* procName) {
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  char detailName[128];

  if (NULL != pSession) {
    snprintf(detailName, sizeof(detailName) - 1, "%s - %s", procName, "Session");
    detailName[sizeof(detailName) - 1] = 0;
    mpPrintStatDetail(&pSession->ctrl, &pSession->stat.statDetail, detailName, pSession->maxAllocMemSize);

    snprintf(detailName, sizeof(detailName) - 1, "%s - %s", procName, "SessionPos");
    detailName[sizeof(detailName) - 1] = 0;
    mpPrintPosStat(&pSession->ctrl, &pSession->stat.posStat, detailName);
  }

  if (NULL != pPool) {
    snprintf(detailName, sizeof(detailName) - 1, "%s - %s", procName, pPool->name);
    detailName[sizeof(detailName) - 1] = 0;
    mpPrintSessionStat(&pPool->ctrl, &pPool->stat.statSession, detailName);
    mpPrintStatDetail(&pPool->ctrl, &pPool->stat.statDetail, detailName, pPool->maxAllocMemSize);

    snprintf(detailName, sizeof(detailName) - 1, "%s - %s", procName, "MemPoolNode");
    detailName[sizeof(detailName) - 1] = 0;
    mpPrintNodeStat(&pPool->ctrl, pPool->stat.nodeStat, detailName);
    
    snprintf(detailName, sizeof(detailName) - 1, "%s - %s", procName, "MemPoolPos");
    detailName[sizeof(detailName) - 1] = 0;
    mpPrintPosStat(&pPool->ctrl, &pPool->stat.posStat, detailName);
  }
}


int32_t taosMemPoolOpen(char* poolName, SMemPoolCfg* cfg, void** poolHandle) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMemPool* pPool = NULL;
  
  MP_ERR_JRET(taosThreadOnce(&gMPoolInit, mpModInit));
  if (TSDB_CODE_SUCCESS != gMPMgmt.code) {
    uError("init memory pool failed, code: 0x%x", gMPMgmt.code);
    MP_ERR_JRET(gMPMgmt.code);
  }

  pPool = (SMemPool*)taosMemoryCalloc(1, sizeof(SMemPool));
  if (NULL == pPool) {
    uError("calloc memory pool failed, code: 0x%x", terrno);
    MP_ERR_JRET(terrno);
  }

  MP_ERR_JRET(mpInit(pPool, poolName, cfg));

  taosWLockLatch(&gMPMgmt.poolLock);
  
  if (NULL == taosArrayPush(gMPMgmt.poolList, &pPool)) {
    taosWUnLockLatch(&gMPMgmt.poolLock);
    MP_ERR_JRET(terrno);
  }
  
  pPool->slotId = taosArrayGetSize(gMPMgmt.poolList) - 1;
  
  taosWUnLockLatch(&gMPMgmt.poolLock);

  uInfo("mempool %s opened", poolName);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolClose(pPool);
    pPool = NULL;
  }

  *poolHandle = pPool;

  return code;
}

void taosMemPoolCfgUpdate(void* poolHandle, SMemPoolCfg* pCfg) {
  SMemPool* pPool = (SMemPool*)poolHandle;

  (void)mpUpdateCfg(pPool);
}

void taosMemPoolDestroySession(void* poolHandle, void* session) {
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  if (NULL == poolHandle || NULL == pSession) {
    uWarn("null pointer of poolHandle %p or session %p", poolHandle, session);
    return;
  }

  (void)atomic_sub_fetch_32(&pSession->pJob->remainSession, 1);
  
  //TODO;

  (void)atomic_add_fetch_64(&pPool->stat.statSession.destroyNum, 1);

  mpCheckStatDetail(pPool, pSession, "DestroySession");

  TAOS_MEMSET(pSession, 0, sizeof(*pSession));

  mpPushIdleNode(pPool, &pPool->sessionCache, (SMPListNode*)pSession);
}

int32_t taosMemPoolInitSession(void* poolHandle, void** ppSession, void* pJob) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = NULL;

  MP_ERR_JRET(mpPopIdleNode(pPool, &pPool->sessionCache, (void**)&pSession));

  TAOS_MEMCPY(&pSession->ctrl, &pPool->ctrl, sizeof(pSession->ctrl));

  if (gMPFps[gMPMgmt.strategy].initSessionFp) {
    MP_ERR_JRET((*gMPFps[gMPMgmt.strategy].initSessionFp)(pPool, pSession));
  }

  MP_ERR_JRET(mpInitStat(&pSession->stat.posStat, true));
  
  pSession->pJob = (SMPJob*)pJob;
  (void)atomic_add_fetch_32(&pSession->pJob->remainSession, 1);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    taosMemPoolDestroySession(poolHandle, pSession);
    pSession = NULL;
    (void)atomic_add_fetch_64(&pPool->stat.statSession.initFail, 1);
  } else {
    (void)atomic_add_fetch_64(&pPool->stat.statSession.initSucc, 1);
  }

  *ppSession = pSession;

  return code;
}


void *taosMemPoolMalloc(void* poolHandle, void* session, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPStatInput input = {.size = size, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .pMem = NULL};
  
  if (NULL == poolHandle || NULL == fileName || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, size:%" PRId64, __FUNCTION__, poolHandle, session, fileName, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

  code = mpMalloc(pPool, pSession, &input.size, 0, &input.pMem);

  MP_SET_FLAG(input.procFlags, (NULL != input.pMem ? MP_STAT_PROC_FLAG_RES_SUCC : MP_STAT_PROC_FLAG_RES_FAIL));
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_MALLOC, &input);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
  }
  
  return input.pMem;
}

void   *taosMemPoolCalloc(void* poolHandle, void* session, int64_t num, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t totalSize = num * size;
  SMPStatInput input = {.size = totalSize, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .pMem = NULL};
  
  if (NULL == poolHandle || NULL == fileName || num < 0 || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, num:%" PRId64 ", size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, num, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

  code = mpCalloc(pPool, pSession, &input.size, &input.pMem);

  MP_SET_FLAG(input.procFlags, (NULL != input.pMem ? MP_STAT_PROC_FLAG_RES_SUCC : MP_STAT_PROC_FLAG_RES_FAIL));
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_CALLOC, &input);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
  }

  return input.pMem;
}

void *taosMemPoolRealloc(void* poolHandle, void* session, void *ptr, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPStatInput input = {.size = size, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .origSize = 0, .pMem = ptr, .pOrigMem = ptr};
  
  if (NULL == poolHandle || NULL == fileName || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

  code = mpRealloc(pPool, pSession, &input.pMem, &input.size, &input.origSize);

  if (NULL != input.pMem) {
    MP_SET_FLAG(input.procFlags, MP_STAT_PROC_FLAG_RES_SUCC);
    mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_REALLOC, &input);
  } else if (0 == size){
    input.pMem = input.pOrigMem;
    MP_SET_FLAG(input.procFlags, MP_STAT_PROC_FLAG_RES_SUCC);
    mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_FREE, &input);
    input.pMem = NULL;
  } else {
    MP_SET_FLAG(input.procFlags, MP_STAT_PROC_FLAG_RES_FAIL);
    mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_REALLOC, &input);

    input.pMem = input.pOrigMem;
    input.procFlags = MP_STAT_PROC_FLAG_EXEC;
    MP_SET_FLAG(input.procFlags, MP_STAT_PROC_FLAG_RES_SUCC);
    mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_FREE, &input);
    input.pMem = NULL;
  }

_return:

  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
  }

  return input.pMem;
}

char *taosMemPoolStrdup(void* poolHandle, void* session, const char *ptr, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t size = (ptr ? strlen(ptr) : 0) + 1;
  SMPStatInput input = {.size = size, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .pMem = NULL};
  
  if (NULL == poolHandle || NULL == fileName || NULL == ptr) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, ptr:%p", 
      __FUNCTION__, poolHandle, session, fileName, ptr);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

  code = mpMalloc(pPool, pSession, &input.size, 0, &input.pMem);
  if (NULL != input.pMem) {
    TAOS_STRCPY(input.pMem, ptr);
    *((char*)input.pMem + size - 1) = 0;
  }

  MP_SET_FLAG(input.procFlags, (NULL != input.pMem ? MP_STAT_PROC_FLAG_RES_SUCC : MP_STAT_PROC_FLAG_RES_FAIL));
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_STRDUP, &input);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
  }

  return input.pMem;
}

char *taosMemPoolStrndup(void* poolHandle, void* session, const char *ptr, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t origSize = ptr ? strlen(ptr) : 0;
  size = TMIN(size, origSize) + 1;
  SMPStatInput input = {.size = size, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .pMem = NULL};
  
  if (NULL == poolHandle || NULL == fileName || NULL == ptr || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, ptr:%p, size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, ptr, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

  code = mpMalloc(pPool, pSession, &input.size, 0, &input.pMem);
  if (NULL != input.pMem) {
    TAOS_MEMCPY(input.pMem, ptr, size - 1);
    *((char*)input.pMem + size - 1) = 0;
  }

  MP_SET_FLAG(input.procFlags, (NULL != input.pMem ? MP_STAT_PROC_FLAG_RES_SUCC : MP_STAT_PROC_FLAG_RES_FAIL));
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_STRNDUP, &input);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
  }

  return input.pMem;
}


void taosMemPoolFree(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo) {
  if (NULL == ptr) {
    return;
  }
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == poolHandle || NULL == fileName) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p", 
      __FUNCTION__, poolHandle, session, fileName);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  SMPStatInput input = {.file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .pMem = ptr};

  mpFree(pPool, pSession, ptr, &input.size);

  MP_SET_FLAG(input.procFlags, MP_STAT_PROC_FLAG_RES_SUCC);
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_FREE, &input);
}

int64_t taosMemPoolGetMemorySize(void* poolHandle, void* session, void *ptr, char* fileName, int32_t lineNo) {
  int64_t code = 0;
  if (NULL == poolHandle || NULL == fileName) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p", 
      __FUNCTION__, poolHandle, session, fileName);
    MP_ERR_RET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  if (NULL == ptr) {
    return 0;
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  
  return mpGetMemorySizeImpl(pPool, pSession, ptr);
}

void* taosMemPoolMallocAlign(void* poolHandle, void* session, uint32_t alignment, int64_t size, char* fileName, int32_t lineNo) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPStatInput input = {.size = size, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC, .pMem = NULL};
  
  if (NULL == poolHandle || NULL == fileName || size < 0 || alignment < POINTER_BYTES || alignment % POINTER_BYTES) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, alignment:%u, size:%" PRId64, 
      __FUNCTION__, poolHandle, session, fileName, alignment, size);
    MP_ERR_JRET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;

  code = mpMalloc(pPool, pSession, &input.size, alignment, &input.pMem);

  MP_SET_FLAG(input.procFlags, (NULL != input.pMem ? MP_STAT_PROC_FLAG_RES_SUCC : MP_STAT_PROC_FLAG_RES_FAIL));
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_MALLOC, &input);

_return:

  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
  }

  return input.pMem;
}

void taosMemPoolClose(void* poolHandle) {
  SMemPool* pPool = (SMemPool*)poolHandle;

  mpCheckStatDetail(pPool, NULL, "PoolClose");

  taosMemoryFree(pPool->name);
  mpDestroyCacheGroup(&pPool->sessionCache);
}

void taosMemPoolModDestroy(void) {

}


int32_t taosMemPoolTrim(void* poolHandle, void* session, int32_t size, char* fileName, int32_t lineNo, bool* trimed) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (NULL == poolHandle || NULL == fileName || size < 0) {
    uError("%s invalid input param, handle:%p, session:%p, fileName:%p, size:%d", 
      __FUNCTION__, poolHandle, session, fileName, size);
    MP_ERR_RET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMemPool* pPool = (SMemPool*)poolHandle;
  SMPSession* pSession = (SMPSession*)session;
  SMPStatInput input = {.size = 0, .file = fileName, .line = lineNo, .procFlags = MP_STAT_PROC_FLAG_EXEC};

  code = mpTrim(pPool, pSession, size, trimed);

  input.size = (trimed) ? (*trimed) : 0;

  MP_SET_FLAG(input.procFlags, ((0 == code) ? MP_STAT_PROC_FLAG_RES_SUCC : MP_STAT_PROC_FLAG_RES_FAIL));
  mpLogStat(pPool, pSession, E_MP_STAT_LOG_MEM_TRIM, &input);

  return code;
}

int32_t taosMemPoolCallocJob(uint64_t jobId, void** ppJob) {
  int32_t code = TSDB_CODE_SUCCESS;
  *ppJob = taosMemoryCalloc(1, sizeof(SMPJob));
  if (NULL == *ppJob) {
    uError("calloc mp job failed, code: 0x%x", terrno);
    MP_ERR_RET(terrno);
  }

  SMPJob* pJob = (SMPJob*)*ppJob;
  pJob->job.jobId = jobId;

  return code;
}

void taosMemPoolGetUsedSizeBegin(void* poolHandle, int64_t* usedSize, bool* needEnd) {
  if (NULL == poolHandle) {
    *usedSize = 0;
    *needEnd = false;
    return;
  }
  
  SMemPool* pPool = (SMemPool*)poolHandle;

  taosWLockLatch(&pPool->cfgLock);
  *needEnd = true;
  *usedSize = atomic_load_64(&pPool->allocMemSize);
}

void taosMemPoolGetUsedSizeEnd(void* poolHandle) {
  SMemPool* pPool = (SMemPool*)poolHandle;
  if (NULL == pPool) {
    return;
  }
  
  taosWUnLockLatch(&pPool->cfgLock);
}

int32_t taosMemPoolGetSessionStat(void* session, SMPStatDetail** ppStat, int64_t* allocSize, int64_t* maxAllocSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == session || (NULL == ppStat && NULL == allocSize && NULL == maxAllocSize)) {
    uError("%s invalid input param, session:%p, ppStat:%p, allocSize:%p, maxAllocSize:%p", __FUNCTION__, session, ppStat, allocSize, maxAllocSize);
    MP_ERR_RET(TSDB_CODE_INVALID_MEM_POOL_PARAM);
  }

  SMPSession* pSession = (SMPSession*)session;

  if (ppStat) {
    *ppStat = &pSession->stat.statDetail;
  }
  if (allocSize) {
    *allocSize = atomic_load_64(&pSession->allocMemSize);
  }
  if (maxAllocSize) {
    *maxAllocSize = atomic_load_64(&pSession->maxAllocMemSize);
  }
  
  return code;
}

int32_t taosMemoryPoolInit(mpReserveFailFp failFp, mpReserveReachFp reachFp) {
  int32_t code = TSDB_CODE_SUCCESS;
  
#ifdef LINUX  
  if (!tsQueryUseMemoryPool) {
#endif  
    uInfo("memory pool disabled cause of configured disabled");
    return code;
#ifdef LINUX  
  }
#endif

  taosGetTotalMemory(&tsTotalMemoryKB);
  if (tsTotalMemoryKB <= 0) {
    uInfo("memory pool disabled since no enough system total memory, size: %" PRId64 "KB", tsTotalMemoryKB);
    return code;
  }

  uInfo("total memory size: %" PRId64 "KB", tsTotalMemoryKB);

  tsMinReservedMemorySize = TMAX(MIN_RESERVE_MEM_SIZE, tsTotalMemoryKB / 1024 * MP_DEFAULT_RESERVE_MEM_PERCENT / 100);

  SMemPoolCfg cfg = {0};
  int64_t sysAvailSize = 0;
  
  code = taosGetSysAvailMemory(&sysAvailSize);
  if (code || sysAvailSize < MP_MIN_MEM_POOL_SIZE) {
    uInfo("memory pool disabled since no enough system available memory, size: %" PRId64, sysAvailSize);
    code = TSDB_CODE_SUCCESS;
    return code;
  }

  cfg.reserveSize = &tsMinReservedMemorySize;

  int64_t freeSizeAfterRes = sysAvailSize - tsMinReservedMemorySize * 1048576UL;
  if (freeSizeAfterRes < MP_MIN_FREE_SIZE_AFTER_RESERVE) {
    uInfo("memory pool disabled since no enough system available memory after reservied, size: %" PRId64, freeSizeAfterRes);
    return code;
  }

  atomic_store_64(&tsCurrentAvailMemorySize, sysAvailSize);

  cfg.evicPolicy = E_EVICT_AUTO; //TODO
  cfg.chunkSize = 1048576;
  cfg.jobQuota = &tsSingleQueryMaxMemorySize;
  cfg.cb.failFp = failFp;
  cfg.cb.reachFp  = reachFp;
  
  code = taosMemPoolOpen("taosd", &cfg, &gMemPoolHandle);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  code = mpCreateMgmtThread();
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  uInfo("memory pool initialized");

  return code;
}


void taosAutoMemoryFree(void *ptr) {
  if (NULL != gMemPoolHandle) {
    taosMemPoolFree(gMemPoolHandle, threadPoolSession, ptr, __FILE__, __LINE__);
  } else {
    taosMemFree(ptr);
  }
}


