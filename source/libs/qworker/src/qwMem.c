/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <stdint.h>
#include "qwInt.h"
#include "qworker.h"

#define QW_SESSION_KEY_SIZE (sizeof(uint64_t) * 3 + sizeof(int32_t))

static void qwMakeSessionKey(char key[QW_SESSION_KEY_SIZE], uint64_t queryId, uint64_t clientId,
                             uint64_t taskId, int32_t execId) {
  memcpy(key, &queryId, sizeof(queryId));
  memcpy(key + sizeof(queryId), &clientId, sizeof(clientId));
  memcpy(key + sizeof(queryId) + sizeof(clientId), &taskId, sizeof(taskId));
  memcpy(key + sizeof(queryId) + sizeof(clientId) + sizeof(taskId), &execId, sizeof(execId));
}

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

int32_t qwInitJobInfo(QW_FPARAMS_DEF, SQWJobInfo* pJob, uint64_t rootQueryId, uint64_t rootClientId) {
  pJob->pSessions= taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->pSessions) {
    QW_TASK_ELOG("fail to init session hash, code: 0x%x", terrno);
    return terrno;
  }

  int32_t code = taosMemPoolCallocJob(rootQueryId, rootClientId, (void**)&pJob->memInfo);
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
  char id[QW_SESSION_KEY_SIZE];
  qwMakeSessionKey(id, qId, cId, tId, eId);

  if (needRemoveHashKey) {
    int32_t code = taosHashRemove(pJobInfo->pSessions, id, sizeof(id));
    if (TSDB_CODE_SUCCESS != code) {
      QW_TASK_ELOG("fail to remove session from query session hash, session:%p, job:%p, sessionHash:%p, hashSize:%d, "
                   "remainSessions:%d, thread:%" PRId64 ", error(0x%x):%s",
                   session, pJobInfo, pJobInfo->pSessions, taosHashGetSize(pJobInfo->pSessions),
                   atomic_load_32(&pJobInfo->memInfo->remainSession), taosGetSelfPthreadId(), code, tstrerror(code));
      return;
    }
  }

  if (NULL != session) {
    taosMemPoolDestroySession(gMemPoolHandle, session);
  }

  QW_LOCK(QW_WRITE, &pJobInfo->lock);
  int32_t remainSessions = atomic_sub_fetch_32(&pJobInfo->memInfo->remainSession, 1);
  if (remainSessions != 0) {
    QW_UNLOCK(QW_WRITE, &pJobInfo->lock);
  }
  
  QW_TASK_DLOG("task session destroyed, session:%p, job:%p, remainSessions:%d, thread:%" PRId64, session, pJobInfo,
               remainSessions, taosGetSelfPthreadId());

  if (0 == remainSessions) {
//    if (/*0 == taosHashGetSize(pJobInfo->pSessions) && */0 == atomic_load_32(&pJobInfo->memInfo->remainSession)) {
      atomic_store_8(&pJobInfo->destroyed, 1);
      QW_UNLOCK(QW_WRITE, &pJobInfo->lock);

      char id2[sizeof(qId) + sizeof(cId) + 1] = {0};
      QW_SET_QCID(id2, pJobInfo->memInfo->jobId, pJobInfo->memInfo->clientId);
      TAOS_UNUSED(taosHashRemove(gQueryMgmt.pJobInfo, id2, sizeof(id2)));
      
      QW_TASK_DLOG_E("the whole query job removed");
//    } else {
//      QW_TASK_DLOG("job not removed, remainSessions:%d, %d", taosHashGetSize(pJobInfo->pSessions), pJobInfo->memInfo->remainSession);
//      QW_UNLOCK(QW_WRITE, &pJobInfo->lock);
//    }
  }
}

int32_t qwRetrieveJobInfo(QW_FPARAMS_DEF, SQWTaskCtx* ctx, SQWJobInfo** ppJob) {
  int32_t code = TSDB_CODE_SUCCESS;
  SQWJobInfo* pJob = NULL;
  char id[sizeof(qId) + sizeof(cId) + 1] = {0};
  uint64_t rootQueryId = 0 != ctx->correlation.rootExecutionId ? ctx->correlation.rootQueryId : qId;
  uint64_t rootClientId = 0 != ctx->correlation.rootExecutionId ? ctx->correlation.rootClientId : cId;

  if (NULL == gQueryMgmt.pJobInfo) {
    QW_ERR_RET(qwInitJobHash());
  }

  QW_SET_QCID(id, rootQueryId, rootClientId);
  
  while (true) {
    pJob = (SQWJobInfo*)taosHashAcquire(gQueryMgmt.pJobInfo, id, sizeof(id));
    if (NULL == pJob) {
      SQWJobInfo jobInfo = {0};
      code = qwInitJobInfo(QW_FPARAMS(), &jobInfo, rootQueryId, rootClientId);
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
    QW_ERR_JRET(qwRetrieveJobInfo(QW_FPARAMS(), ctx, &pJob));

    ctx->pJobInfo = pJob;

    char id[QW_SESSION_KEY_SIZE];
    qwMakeSessionKey(id, qId, cId, tId, eId);

    char sessionId[sizeof(id) * 2 + 1];
    (void)snprintf(sessionId, sizeof(sessionId), "%016" PRIx64 "%016" PRIx64 "%016" PRIx64 "%08" PRIx32,
                   qId, cId, tId, (uint32_t)eId);

    QW_ERR_JRET(taosMemPoolInitSession(gMemPoolHandle, ppSession, pJob->memInfo, sessionId));
    session.sessionMp = *ppSession;

    sessionKeyInHash = true;
    code = taosHashPut(pJob->pSessions, id, sizeof(id), &session, sizeof(session));
    if (TSDB_CODE_SUCCESS != code) {
      sessionKeyInHash = false;
      QW_TASK_ELOG("fail to put session into query session hash, session:%p, job:%p, sessionHash:%p, hashSize:%d, "
                   "remainSessions:%d, thread:%" PRId64 ", code: 0x%x",
                   *ppSession, pJob, pJob->pSessions, taosHashGetSize(pJob->pSessions),
                   atomic_load_32(&pJob->memInfo->remainSession), taosGetSelfPthreadId(), code);
      QW_ERR_JRET(code);
    }

    break;
  } while (true);

  QW_TASK_DLOG("session initialized, session:%p, job:%p, sessionHash:%p, hashSize:%d, remainSessions:%d, thread:%" PRId64,
               *ppSession, pJob, pJob->pSessions, taosHashGetSize(pJob->pSessions),
               atomic_load_32(&pJob->memInfo->remainSession), taosGetSelfPthreadId());

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
