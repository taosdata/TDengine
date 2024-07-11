#include "qwInt.h"
#include "qworker.h"

int32_t qwGetMemPoolMaxMemSize(int64_t totalSize, int64_t* maxSize) {
  int64_t reserveSize = TMAX(totalSize * QW_DEFAULT_RESERVE_MEM_PERCENT / 100 / 1048576UL * 1048576UL, QW_MIN_RESERVE_MEM_SIZE);
  int64_t availSize = (totalSize - reserveSize) / 1048576UL * 1048576UL;
  if (availSize < QW_MIN_MEM_POOL_SIZE) {
    qError("too little available query memory, totalAvailable: %" PRId64 ", reserveSize: %" PRId64, totalSize, reserveSize);
    return TSDB_CODE_QRY_TOO_FEW_AVAILBLE_MEM;
  }

  *maxSize = availSize;

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetMemPoolChunkSize(int64_t totalSize, int32_t threadNum, int32_t* chunkSize) {
  *chunkSize = 2 * 1048576;

  return TSDB_CODE_SUCCESS;
}

void qwSetConcurrentTaskNumCb(int32_t taskNum) {
  int32_t finTaskNum = TMIN(taskNum, tsNumOfQueryThreads * QW_DEFAULT_THREAD_TASK_NUM);
  
  if (tsQueryMaxConcurrentTaskNum > 0) {
    finTaskNum = TMIN(taskNum, tsQueryMaxConcurrentTaskNum);
  }
  finTaskNum = TMAX(finTaskNum, tsQueryMinConcurrentTaskNum);

  atomic_store_32(&tsQueryConcurrentTaskNum, finTaskNum);

  atomic_store_32(&gQueryMgmt.concTaskLevel, QW_CONC_TASK_LEVEL_FULL);
}

void qwDecConcurrentTaskNumCb(void) {
  int32_t concTaskLevel = atomic_load_32(&gQueryMgmt.concTaskLevel);
  if (concTaskLevel <= QW_CONC_TASK_LEVEL_LOW) {
    qError("Unable to decrease concurrent task num, current task level:%d", concTaskLevel);
    return;
  }

  //TODO
}

void qwIncConcurrentTaskNumCb(void) {
  int32_t concTaskLevel = atomic_load_32(&gQueryMgmt.concTaskLevel);
  if (concTaskLevel >= QW_CONC_TASK_LEVEL_FULL) {
    qError("Unable to increase concurrent task num, current task level:%d", concTaskLevel);
    return;
  }

  //TODO
}

int32_t qwInitQueryInfo(uint64_t qId, SQWQueryInfo* pQuery) {
  pQuery->pSessions= taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pQuery->pSessions) {
    qError("fail to init session hash");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = taosMemPoolCallocCollection(qId, &pQuery->pCollection);
  if (TSDB_CODE_SUCCESS != code) {
    taosHashCleanup(pQuery->pSessions);
    return code;
  }

  return code;
}

int32_t qwInitSession(QW_FPARAMS_DEF, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQWQueryInfo* pQuery = NULL;
  
  while (true) {
    pQuery = (SQWQueryInfo*)taosHashGet(gQueryMgmt.pQueryInfo, &qId, sizeof(qId));
    if (NULL == pQuery) {
      SQWQueryInfo queryInfo = {0};
      code = qwInitQueryInfo(qId, &queryInfo);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      
      code = taosHashPut(gQueryMgmt.pQueryInfo, &qId, sizeof(qId), &queryInfo, sizeof(queryInfo));
      if (TSDB_CODE_SUCCESS != code) {
        qwDestroyQueryInfo(&queryInfo);
        if (-2 == code) {
          code = TSDB_CODE_SUCCESS;
          continue;
        }
        
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pQuery = (SQWQueryInfo*)taosHashGet(gQueryMgmt.pQueryInfo, &qId, sizeof(qId));
    }

    break;
  }

  QW_ERR_RET(taosMemPoolInitSession(gQueryMgmt.memPoolHandle, ppSession, pQuery->pCollection));

  char id[sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_TEID(id, tId, eId);

  code = taosHashPut(pQuery->pSessions, id, sizeof(id), ppSession, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS != code) {
    qError("fail to put session into query session hash, errno:%d", terrno);
    return terrno;
  }

  return code;
}

bool qwLowLevelRetire() {

}

bool qwRetireCollectionCb(int64_t collectionId, int64_t collectionAllocSize, int64_t retireSize, bool retireLow) {
  if (retireLow && collectionAllocSize > retireSize) {
    return qwLowLevelRetire();
  }
  
  return false;
}

int32_t qwGetQueryMemPoolMaxSize(int64_t* pMaxSize, bool* autoMaxSize) {
  if (tsQueryBufferPoolSize > 0) {
    *pMaxSize = tsQueryBufferPoolSize * 1048576UL;
    *autoMaxSize = false;

    return TSDB_CODE_SUCCESS;
  }
  
  int64_t memSize = 0;
  int32_t code = taosGetSysAvailMemory(&memSize);
  if (TSDB_CODE_SUCCESS != code) {
    qError("get system avaiable memory size failed, errno: %d", errno);
    return TAOS_SYSTEM_ERROR(errno);
  }

  code = qwGetMemPoolMaxMemSize(memSize, pMaxSize);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *autoMaxSize = true;

  return code;
}

void qwCheckUpateCfgCb(void* pHandle, SMemPoolCfg* pCfg) {
  int64_t newCollectionQuota = tsSingleQueryMaxMemorySize * 1048576UL;
  if (pCfg->collectionQuota != newCollectionQuota) {
    atomic_store_64(&pCfg->collectionQuota, newCollectionQuota);
  }
  
  int64_t maxSize = 0;
  bool autoMaxSize = false;
  int32_t code = qwGetQueryMemPoolMaxSize(&maxSize, &autoMaxSize);
  if (TSDB_CODE_SUCCESS != code) {
    pCfg->maxSize = 0;
    qError("get query memPool maxSize failed, reset maxSize to %" PRId64, pCfg->maxSize);
    return;
  }
  
  if (pCfg->autoMaxSize != autoMaxSize || pCfg->maxSize != maxSize) {
    pCfg->autoMaxSize = autoMaxSize;
    atomic_store_64(&pCfg->maxSize, maxSize);
    taosMemPoolCfgUpdate(pHandle, pCfg);
  }
}

int32_t qwInitQueryPool(void) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (!tsQueryUseMemoryPool) {
    qDebug("query memory pool disabled");
    return code;
  }

  SMemPoolCfg cfg = {0};
  int64_t maxSize = 0;
  bool autoMaxSize = false;
  code = qwGetQueryMemPoolMaxSize(&maxSize, &autoMaxSize);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  cfg.threadNum = 10; //TODO
  cfg.evicPolicy = E_EVICT_AUTO; //TODO
  cfg.collectionQuota = tsSingleQueryMaxMemorySize * 1048576UL;
  cfg.cb.setSessFp = qwSetConcurrentTaskNumCb;
  cfg.cb.decSessFp = qwDecConcurrentTaskNumCb;
  cfg.cb.incSessFp = qwIncConcurrentTaskNumCb;
  cfg.cb.retireFp = qwRetireCollectionCb;
  cfg.cb.cfgUpdateFp = qwCheckUpateCfgCb;

  code = qwGetMemPoolChunkSize(cfg.maxSize, cfg.threadNum, &cfg.chunkSize);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  code = taosMemPoolOpen(QW_QUERY_MEM_POOL_NAME, &cfg, &gQueryMgmt.memPoolHandle);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  gQueryMgmt.pQueryInfo = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == gQueryMgmt.pQueryInfo) {
    qError("init query hash failed");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  qDebug("query memory pool initialized");

  return code;
}


