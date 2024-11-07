#include "qwInt.h"
#include "qworker.h"

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


int32_t qwInitJobHash(void) {
  int32_t code = TSDB_CODE_SUCCESS;
  SHashObj* pHash = NULL;

  if (NULL == atomic_load_ptr(&gQueryMgmt.pJobInfo)) {
    pHash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (NULL == pHash) {
      qError("init job hash failed, error:0x%x", terrno);
      return terrno;
    }

    if (NULL != atomic_val_compare_exchange_ptr(&gQueryMgmt.pJobInfo, NULL, pHash)) {
      taosHashCleanup(pHash);
    }
  }

  return code;
}


int32_t qwInitSession(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQWJobInfo* pJob = NULL;

  if (NULL == gQueryMgmt.pJobInfo) {
    QW_ERR_RET(qwInitJobHash());
  }
  
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

  QW_ERR_JRET(taosMemPoolInitSession(gMemPoolHandle, ppSession, pJob->memInfo));

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


