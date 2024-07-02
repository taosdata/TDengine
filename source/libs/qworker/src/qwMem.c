#include "qwInt.h"
#include "qworker.h"

void* gQueryPoolHandle = NULL;

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

  code = qwGetMemPoolChunkSize(cfg.maxSize, cfg.threadNum, &cfg.chunkSize);
  if (TSDB_CODE_SUCCESS != code) {
    return;
  }  

  taosMemPoolOpen(QW_QUERY_MEM_POOL_NAME, &cfg, &gQueryPoolHandle);
}


