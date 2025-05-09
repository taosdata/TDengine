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

#include "streamInt.h"

int32_t streamGetThreadIdx(int32_t threadNum, int64_t streamGId) {
  return streamGId % threadNum;
}

int32_t stmAddFetchStreamGid(void) {
  if (++gStreamMgmt.stmGrpIdx >= STREAM_MAX_GROUP_NUM) {
    gStreamMgmt.stmGrpIdx = 0;
  }

  return gStreamMgmt.stmGrpIdx;
}



int32_t stmAddStreamStatus(SArray** ppStatus, SStreamTasksInfo* pStream, int64_t streamId, int32_t gid) {
  taosWLockLatch(&pStream->taskLock);

  if (taosArrayGetSize(pStream->undeployReaders) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, true);
  }

  if (taosArrayGetSize(pStream->undeployRunners) > 0) {
    smHandleRemovedTask(pStream, streamId, gid, false);
  }

  if (pStream->taskNum <= 0) {
    mstDebug("ignore stream status update since stream taskNum %d is invalid", pStream->taskNum);
    goto _exit;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  if (NULL == *ppStatus) {
    *ppStatus = taosArrayInit(pStream->taskNum, sizeof(SStmTaskStatusMsg));
    TSDB_CHECK_NULL(*ppStatus, code, lino, _exit, terrno);
  }

  int32_t origTaskNum = taosArrayGetSize(*ppStatus);
  int32_t taskNum = taosArrayGetSize(pStream->readerList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamReaderTask* pReader = taosArrayGet(pStream->readerList, i);
    TSDB_CHECK_NULL(taosArrayPush(*ppStatus, &pReader->task), code, lino, _exit, terrno);
  }

  mstDebug("%d reader tasks status added to hb", taskNum);

  if (pStream->triggerTask) {
    TSDB_CHECK_NULL(taosArrayPush(*ppStatus, &pStream->triggerTask->task), code, lino, _exit, terrno);
    mstDebug("%d trigger tasks status added to hb", 1);
  }

  taskNum = taosArrayGetSize(pStream->runnerList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamRunnerTask* pRunner = taosArrayGet(pStream->runnerList, i);
    TSDB_CHECK_NULL(taosArrayPush(*ppStatus, &pRunner->task), code, lino, _exit, terrno);
  }

  mstDebug("%d runner tasks status added to hb", taskNum);

  mstDebug("total %d:%d tasks status added to hb", taosArrayGetSize(*ppStatus) - origTaskNum, pStream->taskNum);

_exit:

  taosWUnLockLatch(&pStream->taskLock);

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t stmBuildStreamsStatus(SArray** ppStatus, int32_t gid) {
  SHashObj* pHash = gStreamMgmt.stmGrp[gid];
  if (NULL == pHash) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  void *pIter = NULL;
  while (true) {
    pIter = taosHashIterate(pHash, pIter);
    if (NULL == pIter) {
      break;
    }

    SStreamTasksInfo* pStream = (SStreamTasksInfo*)pIter;
    int64_t* streamId = taosHashGetKey(pIter, NULL);

    stmAddStreamStatus(ppStatus, pStream, *streamId, gid);
  }

  return code;
}

void stmDestroySStreamTasksInfo(SStreamTasksInfo* p) {
  // STREAMTODO
}

int32_t readStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, void** ppCache) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SStreamTriggerTask* pTask = NULL;

  *ppCache = NULL;

  code = streamGetTask(streamId, taskId, (SStreamTask**)&pTask);
  QUERY_CHECK_CODE(code, lino, _end);

  QUERY_CHECK_CONDITION(pTask->task.type == STREAM_TRIGGER_TASK, code, lino, _end, TSDB_CODE_STREAM_TASK_NOT_EXIST);

  if (pTask->pRealtimeCtx->sessionId == sessionId) {
    *ppCache = pTask->pRealtimeCtx->pCalcDataCache;
  } else {
    stError("sessionId %ld not match with task %ld", sessionId, pTask->pRealtimeCtx->sessionId);
    code = TSDB_CODE_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
