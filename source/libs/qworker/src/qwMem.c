#include "qwInt.h"
#include "qworker.h"

int32_t qwGetMemPoolMaxMemSize(int64_t totalSize, int64_t* maxSize) {
  int64_t reserveSize = TMAX(totalSize * QW_DEFAULT_RESERVE_MEM_PERCENT / 100 / 1048576 * 1048576, QW_MIN_RESERVE_MEM_SIZE);
  int64_t availSize = (totalSize - reserveSize) / 1048576 * 1048576;
  if (availSize < QW_MIN_MEM_POOL_SIZE) {
    return -1;
  }

  *maxSize = availSize;

  return TSDB_CODE_SUCCESS;
}

int32_t qwGetMemPoolChunkSize(int64_t totalSize, int32_t threadNum, int32_t* chunkSize) {
  *chunkSize = 2 * 1048576;

  return TSDB_CODE_SUCCESS;
}

void qwSetConcurrentTaskNum(int32_t taskNum) {
  int32_t finTaskNum = TMIN(taskNum, tsNumOfQueryThreads * QW_DEFAULT_THREAD_TASK_NUM);
  
  if (tsQueryMaxConcurrentTaskNum > 0) {
    finTaskNum = TMIN(taskNum, tsQueryMaxConcurrentTaskNum);
  }
  finTaskNum = TMAX(finTaskNum, tsQueryMinConcurrentTaskNum);

  atomic_store_32(&tsQueryConcurrentTaskNum, finTaskNum);

  atomic_store_32(&gQueryMgmt.concTaskLevel, QW_CONC_TASK_LEVEL_FULL);
}

void qwDecConcurrentTaskNum(void) {
  int32_t concTaskLevel = atomic_load_32(&gQueryMgmt.concTaskLevel);
  if (concTaskLevel < QW_CONC_TASK_LEVEL_LOW) {
    qError("Unable to decrease concurrent task num, current task level:%d", concTaskLevel);
    return;
  }

  
}

void qwInitQueryPool(void) {
  int64_t memSize = 0;
  int32_t code = taosGetSysAvailMemory(&memSize);
  if (TSDB_CODE_SUCCESS != code) {
    return;
  }

  SMemPoolCfg cfg = {0};
  code = qwGetMemPoolMaxMemSize(memSize, &cfg.maxSize);
  if (TSDB_CODE_SUCCESS != code) {
    return;
  }

  cfg.threadNum = 10; //TODO
  cfg.evicPolicy = E_EVICT_AUTO; //TODO
  cfg.cb.setSessFp = qwSetConcurrentTaskNum;
  cfg.cb.decSessFp = qwDecConcurrentTaskNum;
  cfg.cb.incSessFp = qwIncConcurrentTaskNum;

  code = qwGetMemPoolChunkSize(cfg.maxSize, cfg.threadNum, &cfg.chunkSize);
  if (TSDB_CODE_SUCCESS != code) {
    return;
  }  

  code = taosMemPoolOpen(QW_QUERY_MEM_POOL_NAME, &cfg, &gQueryMgmt.memPoolHandle);
  if (TSDB_CODE_SUCCESS != code) {
    return;
  }  
}


