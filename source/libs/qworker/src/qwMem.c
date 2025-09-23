#include <stdint.h>
#include "qwInt.h"
#include "qworker.h"

#if 0
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
#endif

int32_t qwInitJobInfo(QW_FPARAMS_DEF, SQWJobInfo* pJob) {
  pJob->pSessions= taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->pSessions) {
    QW_TASK_ELOG("fail to init session hash, code: 0x%x", terrno);
    return terrno;
  }

  int32_t code = taosMemPoolCallocJob(qId, cId, (void**)&pJob->memInfo);
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
      return code;
    }

    taosHashSetFreeFp(gQueryMgmt.pJobInfo, qwDestroyJobInfo);
  }

  return code;
}


void qwDestroySession(QW_FPARAMS_DEF, SQWJobInfo *pJobInfo, void* session, bool needRemoveHashKey) {
  char id[sizeof(tId) + sizeof(eId) + 1] = {0};
  QW_SET_TEID(id, tId, eId);

  int32_t code = taosHashRemove(pJobInfo->pSessions, id, sizeof(id));
  if (TSDB_CODE_SUCCESS != code && needRemoveHashKey) {
    QW_TASK_ELOG("fail to remove session from query session hash, error(0x%x):%s", code, tstrerror(code));
    return;
  }

  taosMemPoolDestroySession(gMemPoolHandle, session);

  QW_LOCK(QW_WRITE, &pJobInfo->lock);
  int32_t remainSessions = atomic_sub_fetch_32(&pJobInfo->memInfo->remainSession, 1);
  if (remainSessions != 0) {
    QW_UNLOCK(QW_WRITE, &pJobInfo->lock);
  }
  
  QW_TASK_DLOG("task session destoryed, remainSessions:%d", remainSessions);

  if (0 == remainSessions) {
//    if (/*0 == taosHashGetSize(pJobInfo->pSessions) && */0 == atomic_load_32(&pJobInfo->memInfo->remainSession)) {
      atomic_store_8(&pJobInfo->destroyed, 1);
      QW_UNLOCK(QW_WRITE, &pJobInfo->lock);

      char id2[sizeof(qId) + sizeof(cId) + 1] = {0};
      QW_SET_QCID(id2, qId, cId);
      TAOS_UNUSED(taosHashRemove(gQueryMgmt.pJobInfo, id2, sizeof(id2)));
      
      QW_TASK_DLOG_E("the whole query job removed");
//    } else {
//      QW_TASK_DLOG("job not removed, remainSessions:%d, %d", taosHashGetSize(pJobInfo->pSessions), pJobInfo->memInfo->remainSession);
//      QW_UNLOCK(QW_WRITE, &pJobInfo->lock);
//    }
  }
}

int32_t qwRetrieveJobInfo(QW_FPARAMS_DEF, SQWJobInfo** ppJob) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQWJobInfo* pJob = NULL;
  char id[sizeof(qId) + sizeof(cId) + 1] = {0};

  if (NULL == gQueryMgmt.pJobInfo) {
    QW_ERR_RET(qwInitJobHash());
  }

  QW_SET_QCID(id, qId, cId);
  
  while (true) {
    pJob = (SQWJobInfo*)taosHashAcquire(gQueryMgmt.pJobInfo, id, sizeof(id));
    if (NULL == pJob) {
      SQWJobInfo jobInfo = {0};
      code = qwInitJobInfo(QW_FPARAMS(), &jobInfo);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      
      code = taosHashPut(gQueryMgmt.pJobInfo, id, sizeof(id), &jobInfo, sizeof(jobInfo));
      if (TSDB_CODE_SUCCESS != code) {
        qwDestroyJobInfo(&jobInfo);
        if (TSDB_CODE_DUP_KEY == code) {
          code = TSDB_CODE_SUCCESS;
          continue;
        }
        
        QW_TASK_ELOG("fail to put job to job hash, error: %s", tstrerror(code));
        return code;
      }

      QW_TASK_DLOG_E("job added to hash");

      pJob = (SQWJobInfo*)taosHashAcquire(gQueryMgmt.pJobInfo, id, sizeof(id));
      if (NULL == pJob) {
        QW_TASK_WLOG_E("job not in job hash, may be dropped");
        continue;
      }
    }

    QW_LOCK(QW_READ, &pJob->lock);
    
    if (atomic_load_8(&pJob->destroyed)) {
      QW_UNLOCK(QW_READ, &pJob->lock);
      taosHashRelease(gQueryMgmt.pJobInfo, pJob);
      continue;
    }

    (void)atomic_add_fetch_32(&pJob->memInfo->remainSession, 1);
   
    QW_UNLOCK(QW_READ, &pJob->lock);

    break;
  }

  *ppJob = pJob;

  return code;
}

int32_t qwInitSession(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQWJobInfo* pJob = NULL;
  SQWSessionInfo session = {.mgmt = mgmt,
                            .sId = sId,
                            .qId = qId,
                            .cId = cId,
                            .tId = tId,
                            .rId = rId,
                            .eId = eId
  };

  bool sessionKeyInHash = false;
  do {
    QW_ERR_JRET(qwRetrieveJobInfo(QW_FPARAMS(), &pJob));

    ctx->pJobInfo = pJob;

    char id[sizeof(tId) + sizeof(eId) + 1] = {0};
    QW_SET_TEID(id, tId, eId);

    QW_ERR_JRET(taosMemPoolInitSession(gMemPoolHandle, ppSession, pJob->memInfo, id));
    session.sessionMp = *ppSession;

    sessionKeyInHash = true;
    code = taosHashPut(pJob->pSessions, id, sizeof(id), &session, sizeof(session));
    if (TSDB_CODE_SUCCESS != code) {
      sessionKeyInHash = false;
      QW_TASK_ELOG("fail to put session into query session hash, code: 0x%x", code);
      QW_ERR_JRET(code);
    }

    break;
  } while (true);

  QW_TASK_DLOG_E("session initialized");

_return:

  if (NULL != pJob) {
    if (TSDB_CODE_SUCCESS != code) {
      qwDestroySession(QW_FPARAMS(), pJob, *ppSession, sessionKeyInHash);
      *ppSession = NULL;
    }
    
    taosHashRelease(gQueryMgmt.pJobInfo, pJob);
  }

  return code;
}


