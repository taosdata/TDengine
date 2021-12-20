#include "queryInt.h"
#include "query.h"
#include "taosmsg.h"
#include "queryWorker.h"
#include "qWorkerInt.h"

SQWorkerMgmt qWorkerMgmt = {0};

int32_t qWorkerInit(SQWorkerCfg *cfg) {
  if (cfg) {
    qWorkerMgmt.cfg = *cfg;
  } else {
    qWorkerMgmt.cfg.maxSchedulerNum = QWORKER_DEFAULT_SCHEDULER_NUMBER;
    qWorkerMgmt.cfg.maxResCacheNum = QWORKER_DEFAULT_RES_CACHE_NUMBER;
  }

  qWorkerMgmt.scheduleHash = taosHashInit(qWorkerMgmt.cfg.maxSchedulerNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == qWorkerMgmt.scheduleHash) {
    SCH_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d schduler hash failed", qWorkerMgmt.cfg.maxSchedulerNum);
  }

  qWorkerMgmt.resHash = taosHashInit(qWorkerMgmt.cfg.maxResCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == qWorkerMgmt.resHash) {
    taosHashCleanup(qWorkerMgmt.scheduleHash);
    qWorkerMgmt.scheduleHash = NULL;
    
    SCH_ERR_LRET(TSDB_CODE_QRY_OUT_OF_MEMORY, "init %d res cache hash failed", qWorkerMgmt.cfg.maxResCacheNum);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessQueryMsg(SSchedulerQueryMsg *msg, int32_t msgLen, int32_t *code, char **rspMsg) {

  // TODO
  return 0;
}

