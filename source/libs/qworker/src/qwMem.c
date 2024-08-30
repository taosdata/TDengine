#include "qwInt.h"
#include "qworker.h"

int32_t qwGetMemPoolMaxMemSize(void* pHandle, int64_t* maxSize) {
  int64_t freeSize = 0;
  int64_t usedSize = 0;
  bool needEnd = false;

  taosMemPoolGetUsedSizeBegin(pHandle, &usedSize, &needEnd);
  int32_t code = taosGetSysAvailMemory(&freeSize);
  if (needEnd) {
    taosMemPoolGetUsedSizeEnd(pHandle);
  }
  
  if (TSDB_CODE_SUCCESS != code) {
    qError("get system avaiable memory size failed, error: 0x%x", code);
    return code;
  }

  int64_t totalSize = freeSize + usedSize;
  int64_t reserveSize = TMAX(tsTotalMemoryKB * 1024 * QW_DEFAULT_RESERVE_MEM_PERCENT / 100 / 1048576UL * 1048576UL, QW_MIN_RESERVE_MEM_SIZE);
  int64_t availSize = (totalSize - reserveSize) / 1048576UL * 1048576UL;
  if (availSize < QW_MIN_MEM_POOL_SIZE) {
    qError("too little available query memory, totalAvailable: %" PRId64 ", reserveSize: %" PRId64, totalSize, reserveSize);
    return TSDB_CODE_QRY_TOO_FEW_AVAILBLE_MEM;
  }

  uDebug("new pool maxSize:%" PRId64 ", usedSize:%" PRId64 ", freeSize:%" PRId64, availSize, usedSize, freeSize);

  *maxSize = availSize;

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetMemPoolChunkSize(int64_t totalSize, int32_t threadNum, int32_t* chunkSize) {
  //TODO 
  
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

int32_t qwInitJobInfo(uint64_t qId, SQWJobInfo* pJob) {
  pJob->pSessions= taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->pSessions) {
    qError("fail to init session hash, code: 0x%x", terrno);
    return terrno;
  }

  int32_t code = taosMemPoolCallocJob(qId, (void**)&pJob->memInfo);
  if (TSDB_CODE_SUCCESS != code) {
    taosHashCleanup(pJob->pSessions);
    pJob->pSessions = NULL;
    return code;
  }

  return code;
}

int32_t qwInitSession(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQWJobInfo* pJob = NULL;
  
  while (true) {
    pJob = (SQWJobInfo*)taosHashAcquire(gQueryMgmt.pJobInfo, &qId, sizeof(qId));
    if (NULL == pJob) {
      SQWJobInfo jobInfo = {0};
      code = qwInitJobInfo(qId, &jobInfo);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      
      code = taosHashPut(gQueryMgmt.pJobInfo, &qId, sizeof(qId), &jobInfo, sizeof(jobInfo));
      if (TSDB_CODE_SUCCESS != code) {
        qwDestroyJobInfo(&jobInfo);
        if (TSDB_CODE_DUP_KEY == code) {
          code = TSDB_CODE_SUCCESS;
          continue;
        }
        
        return code;
      }

      pJob = (SQWJobInfo*)taosHashAcquire(gQueryMgmt.pJobInfo, &qId, sizeof(qId));
      if (NULL == pJob) {
        qError("QID:0x%" PRIx64 " not in joj hash, may be dropped", qId);
        return TSDB_CODE_QRY_JOB_NOT_EXIST;
      }
    }

    break;
  }

  ctx->pJobInfo = pJob;

  char id[sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_TEID(id, tId, eId);

  QW_ERR_JRET(taosMemPoolInitSession(gQueryMgmt.memPoolHandle, ppSession, pJob->memInfo));

  code = taosHashPut(pJob->pSessions, id, sizeof(id), ppSession, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS != code) {
    qError("fail to put session into query session hash, code: 0x%x", code);
    QW_ERR_JRET(code);
  }

_return:

  if (NULL != pJob) {
    taosHashRelease(gQueryMgmt.pJobInfo, pJob);
  }

  return code;
}

void qwRetireJobCb(SMemPoolJob* mpJob, int32_t errCode) {
  SQWJobInfo* pJob = (SQWJobInfo*)taosHashGet(gQueryMgmt.pJobInfo, &mpJob->jobId, sizeof(mpJob->jobId));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 " fail to get job from job hash", mpJob->jobId);
    return;
  }

  if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
    qInfo("QID:0x%" PRIx64 " mark retired, errCode: 0x%x, allocSize:%" PRId64, mpJob->jobId, errCode, atomic_load_64(&pJob->memInfo->allocMemSize));
  } else {
    qDebug("QID:0x%" PRIx64 " already retired, retired: %d, errCode: 0x%x, allocSize:%" PRId64, mpJob->jobId, atomic_load_8(&pJob->retired), atomic_load_32(&pJob->errCode), atomic_load_64(&pJob->memInfo->allocMemSize));
  }
}

void qwLowLevelRetire(void* pHandle, int64_t retireSize, int32_t errCode) {
  SQWJobInfo* pJob = (SQWJobInfo*)taosHashIterate(gQueryMgmt.pJobInfo, NULL);
  while (pJob) {
    if (!taosMemPoolNeedRetireJob(pHandle)) {
      taosHashCancelIterate(gQueryMgmt.pJobInfo, pJob);
      return;
    }

    uint64_t jobId = pJob->memInfo->jobId;
    int64_t aSize = atomic_load_64(&pJob->memInfo->allocMemSize);
    if (aSize >= retireSize && 0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
      qwRetireJob(pJob);

      qDebug("QID:0x%" PRIx64 " job retired cause of low level memory retire, usedSize:%" PRId64 ", retireSize:%" PRId64, 
          jobId, aSize, retireSize);
          
      taosHashCancelIterate(gQueryMgmt.pJobInfo, pJob);
      break;
    }
    
    pJob = (SQWJobInfo*)taosHashIterate(gQueryMgmt.pJobInfo, pJob);
  }
}

void qwMidLevelRetire(void* pHandle, int64_t retireSize, int32_t errCode) {
  SQWJobInfo* pJob = (SQWJobInfo*)taosHashIterate(gQueryMgmt.pJobInfo, NULL);
  PriorityQueueNode qNode;
  while (NULL != pJob) {
    if (0 == atomic_load_8(&pJob->retired)) {
      qNode.data = pJob;
      (void)taosBQPush(gQueryMgmt.retireCtx.pJobQueue, &qNode);
    }
    
    pJob = (SQWJobInfo*)taosHashIterate(gQueryMgmt.pJobInfo, pJob);
  }

  PriorityQueueNode* pNode = NULL;
  uint64_t jobId = 0;
  int64_t retiredSize = 0;
  while (retiredSize < retireSize) {
    if (!taosMemPoolNeedRetireJob(pHandle)) {
      break;
    }

    pNode = taosBQTop(gQueryMgmt.retireCtx.pJobQueue);
    if (NULL == pNode) {
      break;
    }

    pJob = (SQWJobInfo*)pNode->data;
    if (atomic_load_8(&pJob->retired)) {
      taosBQPop(gQueryMgmt.retireCtx.pJobQueue);
      continue;
    }

    if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
      int64_t aSize = atomic_load_64(&pJob->memInfo->allocMemSize);
      jobId = pJob->memInfo->jobId;

      qwRetireJob(pJob);

      qDebug("QID:0x%" PRIx64 " job retired cause of mid level memory retire, usedSize:%" PRId64 ", retireSize:%" PRId64, 
          jobId, aSize, retireSize);

      retiredSize += aSize;    
    }

    taosBQPop(gQueryMgmt.retireCtx.pJobQueue);
  }

  taosBQClear(gQueryMgmt.retireCtx.pJobQueue);
}


void qwRetireJobsCb(void* pHandle, int64_t retireSize, bool lowLevelRetire, int32_t errCode) {
  (lowLevelRetire) ? qwLowLevelRetire(pHandle, retireSize, errCode) : qwMidLevelRetire(pHandle, retireSize, errCode);
}

int32_t qwUpdateQueryMemPoolCfg(void* pHandle, int64_t* pFreeSize, bool* autoMaxSize, int64_t* pReserveSize, int64_t* pRetireUnitSize) {
  if (tsQueryBufferPoolSize > 0) {
    *pFreeSize = tsQueryBufferPoolSize * 1048576UL;
    *autoMaxSize = false;

    return TSDB_CODE_SUCCESS;
  }
  
  int32_t code = qwGetMemPoolMaxMemSize(pHandle, pFreeSize);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *autoMaxSize = true;

  return code;
}

void qwCheckUpateCfgCb(void* pHandle, void* cfg) {
  SMemPoolCfg* pCfg = (SMemPoolCfg*)cfg;
  int64_t newJobQuota = tsSingleQueryMaxMemorySize * 1048576UL;
  if (pCfg->jobQuota != newJobQuota) {
    atomic_store_64(&pCfg->jobQuota, newJobQuota);
  }
  
  int64_t freeSize = 0, reserveSize = 0, retireUnitSize = 0;
  bool autoMaxSize = false;
  int32_t code = qwUpdateQueryMemPoolCfg(pHandle, &freeSize, &autoMaxSize, &reserveSize, &retireUnitSize);
  if (TSDB_CODE_SUCCESS != code) {
    pCfg->freeSize = 0;
    qError("get query memPool freeSize failed, reset freeSize to %" PRId64, pCfg->freeSize);
  }
  
  if (pCfg->autoMaxSize != autoMaxSize || pCfg->freeSize != freeSize) {
    pCfg->autoMaxSize = autoMaxSize;
    atomic_store_64(&pCfg->freeSize, freeSize);
    taosMemPoolCfgUpdate(pHandle, pCfg);
  }
}

static bool qwJobMemSizeCompFn(void* l, void* r, void* param) {
  SQWJobInfo* left = (SQWJobInfo*)l;
  SQWJobInfo* right = (SQWJobInfo*)r;
  if (atomic_load_8(&right->retired)) {
    return true;
  }
  
  return atomic_load_64(&right->memInfo->allocMemSize) < atomic_load_64(&left->memInfo->allocMemSize);
}

void qwDeleteJobQueueData(void* pData) {}


int32_t qwInitQueryPool(void) {
  int32_t code = TSDB_CODE_SUCCESS;

#ifdef LINUX  
  if (!tsQueryUseMemoryPool) {
#endif  
    qInfo("query memory pool disabled");
    return code;
#ifdef LINUX  
  }
#endif

  taosGetTotalMemory(&tsTotalMemoryKB);

  SMemPoolCfg cfg = {0};
  code = qwUpdateQueryMemPoolCfg(NULL, &cfg.freeSize, &cfg.autoMaxSize, &cfg.reserveSize, &cfg.retireUnitSize);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  cfg.threadNum = 10; //TODO
  cfg.evicPolicy = E_EVICT_AUTO; //TODO
  cfg.jobQuota = tsSingleQueryMaxMemorySize * 1048576UL;
  cfg.cb.setSessFp = qwSetConcurrentTaskNumCb;
  cfg.cb.decSessFp = qwDecConcurrentTaskNumCb;
  cfg.cb.incSessFp = qwIncConcurrentTaskNumCb;
  cfg.cb.retireJobsFp = qwRetireJobsCb;
  cfg.cb.retireJobFp  = qwRetireJobCb;
  cfg.cb.cfgUpdateFp = qwCheckUpateCfgCb;

  gQueryMgmt.pJobInfo = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == gQueryMgmt.pJobInfo) {
    qError("init job hash failed, error:0x%x", terrno);
    return terrno;
  }

  gQueryMgmt.retireCtx.pJobQueue = createBoundedQueue(QW_MAX_RETIRE_JOB_NUM, qwJobMemSizeCompFn, qwDeleteJobQueueData, NULL);
  if (NULL == gQueryMgmt.retireCtx.pJobQueue) {
    qError("init job bounded queue failed, error:0x%x", terrno);
    return terrno;
  }
  
  code = taosMemPoolOpen(QW_QUERY_MEM_POOL_NAME, &cfg, &gQueryMgmt.memPoolHandle);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }  

  qInfo("query memory pool initialized");

  return code;
}


