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

#include "cos.h"
#include "rsync.h"
#include "streamBackendRocksdb.h"
#include "streamInt.h"

typedef struct {
  ECHECKPOINT_BACKUP_TYPE type;
  char*                   taskId;
  int64_t                 chkpId;

  SStreamTask* pTask;
  int64_t      dbRefId;
  void*        pMeta;
} SAsyncUploadArg;

static int32_t downloadCheckpointDataByName(const char* id, const char* fname, const char* dstName);
static int32_t deleteCheckpointFile(const char* id, const char* name);
static int32_t streamTaskUploadCheckpoint(const char* id, const char* path);
static int32_t deleteCheckpoint(const char* id);
static int32_t downloadCheckpointByNameS3(const char* id, const char* fname, const char* dstName);
static int32_t continueDispatchCheckpointTriggerBlock(SStreamDataBlock* pBlock, SStreamTask* pTask);
static int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType);
static int32_t streamAlignCheckpoint(SStreamTask* pTask);

int32_t streamAlignCheckpoint(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  int64_t old = atomic_val_compare_exchange_32(&pTask->chkInfo.downstreamAlignNum, 0, num);
  if (old == 0) {
    stDebug("s-task:%s set initial align upstream num:%d", pTask->id.idStr, num);
  }

  return atomic_sub_fetch_32(&pTask->chkInfo.downstreamAlignNum, 1);
}

int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType) {
  SStreamDataBlock* pChkpoint = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock));
  if (pChkpoint == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChkpoint->type = checkpointType;

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    taosFreeQitem(pChkpoint);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pBlock->info.type = STREAM_CHECKPOINT;
  pBlock->info.version = pTask->chkInfo.checkpointingId;
  pBlock->info.window.ekey = pBlock->info.window.skey = pTask->chkInfo.transId;  // NOTE: set the transId
  pBlock->info.rows = 1;
  pBlock->info.childId = pTask->info.selfChildId;

  pChkpoint->blocks = taosArrayInit(4, sizeof(SSDataBlock));  // pBlock;
  taosArrayPush(pChkpoint->blocks, pBlock);

  taosMemoryFree(pBlock);
  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pChkpoint) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  streamTrySchedExec(pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  // todo this status may not be set here.
  // 1. set task status to be prepared for check point, no data are allowed to put into inputQ.
  int32_t code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
  ASSERT(code == TSDB_CODE_SUCCESS);

  pTask->chkInfo.transId = pReq->transId;
  pTask->chkInfo.checkpointingId = pReq->checkpointId;
  pTask->chkInfo.numOfNotReady = streamTaskGetNumOfDownstream(pTask);
  pTask->chkInfo.startTs = taosGetTimestampMs();
  pTask->execInfo.checkpoint += 1;

  // 2. Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
  return appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER);
}

int32_t continueDispatchCheckpointTriggerBlock(SStreamDataBlock* pBlock, SStreamTask* pTask) {
  pBlock->srcTaskId = pTask->id.taskId;
  pBlock->srcVgId = pTask->pMeta->vgId;

  int32_t code = taosWriteQitem(pTask->outputq.queue->pQueue, pBlock);
  if (code == 0) {
    ASSERT(pTask->chkInfo.dispatchCheckpointTrigger == false);
    streamDispatchStreamBlock(pTask);
  } else {
    stError("s-task:%s failed to put checkpoint into outputQ, code:%s", pTask->id.idStr, tstrerror(code));
    streamFreeQitem((SStreamQueueItem*)pBlock);
  }

  return code;
}

int32_t streamProcessCheckpointTriggerBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  SSDataBlock* pDataBlock = taosArrayGet(pBlock->blocks, 0);
  int64_t      checkpointId = pDataBlock->info.version;
  int32_t      transId = pDataBlock->info.window.skey;
  const char*  id = pTask->id.idStr;
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      vgId = pTask->pMeta->vgId;

  stDebug("s-task:%s vgId:%d start to handle the checkpoint-trigger block, checkpointId:%" PRId64 " ver:%" PRId64
          ", transId:%d current checkpointingId:%" PRId64,
          id, vgId, pTask->chkInfo.checkpointId, pTask->chkInfo.checkpointVer, transId, checkpointId);

  // set task status
  if (streamTaskGetStatus(pTask)->state != TASK_STATUS__CK) {
    pTask->chkInfo.checkpointingId = checkpointId;
    pTask->chkInfo.transId = transId;

    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s handle checkpoint-trigger block failed, code:%s", id, tstrerror(code));
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }
  }

  // todo fix race condition: set the status and append checkpoint block
  int32_t taskLevel = pTask->info.taskLevel;
  if (taskLevel == TASK_LEVEL__SOURCE) {
    int8_t type = pTask->outputInfo.type;
    if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      stDebug("s-task:%s set childIdx:%d, and add checkpoint-trigger block into outputQ", id, pTask->info.selfChildId);
      continueDispatchCheckpointTriggerBlock(pBlock, pTask);
    } else {  // only one task exists, no need to dispatch downstream info
      atomic_add_fetch_32(&pTask->chkInfo.numOfNotReady, 1);
      streamProcessCheckpointReadyMsg(pTask);
      streamFreeQitem((SStreamQueueItem*)pBlock);
    }
  } else if (taskLevel == TASK_LEVEL__SINK || taskLevel == TASK_LEVEL__AGG) {
    ASSERT(taosArrayGetSize(pTask->upstreamInfo.pList) > 0);
    if (pTask->chkInfo.startTs == 0) {
      pTask->chkInfo.startTs = taosGetTimestampMs();
      pTask->execInfo.checkpoint += 1;
    }

    // update the child Id for downstream tasks
    streamAddCheckpointReadyMsg(pTask, pBlock->srcTaskId, pTask->info.selfChildId, checkpointId);

    // there are still some upstream tasks not send checkpoint request, do nothing and wait for then
    int32_t notReady = streamAlignCheckpoint(pTask);
    int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
    if (notReady > 0) {
      stDebug("s-task:%s received checkpoint block, idx:%d, %d upstream tasks not send checkpoint info yet, total:%d",
              id, pTask->info.selfChildId, notReady, num);
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }

    if (taskLevel == TASK_LEVEL__SINK) {
      stDebug("s-task:%s process checkpoint block, all %d upstreams sent checkpoint msgs, send ready msg to upstream",
              id, num);
      streamFreeQitem((SStreamQueueItem*)pBlock);
      streamTaskBuildCheckpoint(pTask);
    } else {  // source & agg tasks need to forward the checkpoint msg downwards
      stDebug("s-task:%s process checkpoint block, all %d upstreams sent checkpoint msgs, continue forwards msg", id,
              num);

      // set the needed checked downstream tasks, only when all downstream tasks do checkpoint complete, this task
      // can start local checkpoint procedure
      pTask->chkInfo.numOfNotReady = streamTaskGetNumOfDownstream(pTask);

      // Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
      // already. And then, dispatch check point msg to all downstream tasks
      code = continueDispatchCheckpointTriggerBlock(pBlock, pTask);
    }
  }

  return code;
}

/**
 * All down stream tasks have successfully completed the check point task.
 * Current stream task is allowed to start to do checkpoint things in ASYNC model.
 */
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__AGG);

  // only when all downstream tasks are send checkpoint rsp, we can start the checkpoint procedure for the agg task
  int32_t notReady = atomic_sub_fetch_32(&pTask->chkInfo.numOfNotReady, 1);
  ASSERT(notReady >= 0);

  if (notReady == 0) {
    stDebug("s-task:%s all downstream tasks have completed the checkpoint, start to do checkpoint for current task",
            pTask->id.idStr);
    appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT);
  } else {
    int32_t total = streamTaskGetNumOfDownstream(pTask);
    stDebug("s-task:%s %d/%d downstream tasks are not ready, wait", pTask->id.idStr, notReady, total);
  }

  return 0;
}

void streamTaskClearCheckInfo(SStreamTask* pTask, bool clearChkpReadyMsg) {
  pTask->chkInfo.checkpointingId = 0;  // clear the checkpoint id
  pTask->chkInfo.failedId = 0;
  pTask->chkInfo.startTs = 0;  // clear the recorded start time
  pTask->chkInfo.numOfNotReady = 0;
  pTask->chkInfo.transId = 0;
  pTask->chkInfo.dispatchCheckpointTrigger = false;
  pTask->chkInfo.downstreamAlignNum = 0;

  streamTaskOpenAllUpstreamInput(pTask);  // open inputQ for all upstream tasks
  if (clearChkpReadyMsg) {
    streamClearChkptReadyMsg(pTask);
  }
}

int32_t streamTaskUpdateTaskCheckpointInfo(SStreamTask* pTask, SVUpdateCheckpointInfoReq* pReq) {
  SStreamMeta*     pMeta = pTask->pMeta;
  int32_t          vgId = pMeta->vgId;
  int32_t          code = 0;
  const char*      id = pTask->id.idStr;
  SCheckpointInfo* pInfo = &pTask->chkInfo;

  taosThreadMutexLock(&pTask->lock);

  if (pReq->checkpointId <= pInfo->checkpointId) {
    stDebug("s-task:%s vgId:%d latest checkpointId:%" PRId64 " checkpointVer:%" PRId64
            " no need to update the checkpoint info, updated checkpointId:%" PRId64 " checkpointVer:%" PRId64 " ignored",
            id, vgId, pInfo->checkpointId, pInfo->checkpointVer, pReq->checkpointId, pReq->checkpointVer);
    taosThreadMutexUnlock(&pTask->lock);
    return TSDB_CODE_SUCCESS;
  }

  SStreamTaskState* pStatus = streamTaskGetStatus(pTask);

  stDebug("s-task:%s vgId:%d status:%s start to update the checkpoint info, checkpointId:%" PRId64 "->%" PRId64
          " checkpointVer:%" PRId64 "->%" PRId64 " checkpointTs:%" PRId64 "->%" PRId64,
          id, vgId, pStatus->name, pInfo->checkpointId, pReq->checkpointId, pInfo->checkpointVer, pReq->checkpointVer,
          pInfo->checkpointTime, pReq->checkpointTs);

  if (pStatus->state != TASK_STATUS__DROPPING) {
    ASSERT(pInfo->checkpointId <= pReq->checkpointId && pInfo->checkpointVer <= pReq->checkpointVer);

    pInfo->checkpointId = pReq->checkpointId;
    pInfo->checkpointVer = pReq->checkpointVer;
    pInfo->checkpointTime = pReq->checkpointTs;

    streamTaskClearCheckInfo(pTask, false);

    // todo handle error
    if (pStatus->state == TASK_STATUS__CK) {
      code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    } else {
      stDebug("s-task:0x%x vgId:%d not handle checkpoint-done event, status:%s", pReq->taskId, vgId, pStatus->name);
    }
  } else {
    stDebug("s-task:0x%x vgId:%d status:%s not update checkpoint info, checkpointId:%" PRId64 "->%" PRId64 " failed",
            pReq->taskId, vgId, pStatus->name, pInfo->checkpointId, pReq->checkpointId);
    taosThreadMutexUnlock(&pTask->lock);

    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  if (pReq->dropRelHTask) {
    stDebug("s-task:0x%x vgId:%d drop the related fill-history task:0x%" PRIx64 " after update checkpoint",
            pReq->taskId, vgId, pReq->hTaskId);
    CLEAR_RELATED_FILLHISTORY_TASK(pTask);
  }

  stDebug("s-task:0x%x set the persistent status attr to be ready, prev:%s, status in sm:%s", pReq->taskId,
          streamTaskGetStatusStr(pTask->status.taskStatus), streamTaskGetStatus(pTask)->name);

  pTask->status.taskStatus = TASK_STATUS__READY;

  code = streamMetaSaveTask(pMeta, pTask);
  if (code != TSDB_CODE_SUCCESS) {
    stError("s-task:%s vgId:%d failed to save task info after do checkpoint, checkpointId:%" PRId64 ", since %s", id,
            vgId, pReq->checkpointId, terrstr());
    return code;
  }

  taosThreadMutexUnlock(&pTask->lock);
  streamMetaWUnLock(pMeta);

  // drop task should not in the meta-lock, and drop the related fill-history task now
  if (pReq->dropRelHTask) {
    streamMetaUnregisterTask(pMeta, pReq->hStreamId, pReq->hTaskId);

    // commit the update
    int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
    stDebug("s-task:%s vgId:%d related fill-history task:0x%x dropped, remain tasks:%d", id, vgId, pReq->taskId, numOfTasks);
  }

  streamMetaWLock(pMeta);

  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }

  return TSDB_CODE_SUCCESS;
}

void streamTaskSetFailedCheckpointId(SStreamTask* pTask) {
  pTask->chkInfo.failedId = pTask->chkInfo.checkpointingId;
  stDebug("s-task:%s mark the checkpointId:%" PRId64 " (transId:%d) failed", pTask->id.idStr,
          pTask->chkInfo.checkpointingId, pTask->chkInfo.transId);
}

static int32_t getCheckpointDataMeta(const char* id, const char* path, SArray* list) {
  char* file = taosMemoryCalloc(1, strlen(path) + 32);
  sprintf(file, "%s%s%s", path, TD_DIRSEP, "META_TMP");

  int32_t code = downloadCheckpointDataByName(id, "META", file);
  if (code != 0) {
    stDebug("%s chkp failed to download meta file:%s", id, file);
    taosMemoryFree(file);
    return code;
  }

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  char      buf[128] = {0};
  if (taosReadFile(pFile, buf, sizeof(buf)) <= 0) {
    stError("chkp failed to read meta file:%s", file);
    code = -1;
  } else {
    int32_t len = strlen(buf);
    for (int i = 0; i < len; i++) {
      if (buf[i] == '\n') {
        char* item = taosMemoryCalloc(1, i + 1);
        memcpy(item, buf, i);
        taosArrayPush(list, &item);

        item = taosMemoryCalloc(1, len - i);
        memcpy(item, buf + i + 1, len - i - 1);
        taosArrayPush(list, &item);
      }
    }
  }

  taosCloseFile(&pFile);
  taosRemoveFile(file);
  taosMemoryFree(file);
  return code;
}

int32_t uploadCheckpointData(void* param) {
  SAsyncUploadArg* pParam = param;
  char*            path = NULL;
  int32_t          code = 0;
  SArray*          toDelFiles = taosArrayInit(4, sizeof(void*));
  char*            taskStr = pParam->taskId ? pParam->taskId : "NULL";

  void* pBackend = taskAcquireDb(pParam->dbRefId);
  if (pBackend == NULL) {
    stError("s-task:%s failed to acquire db", taskStr);
    taosMemoryFree(pParam->taskId);
    taosMemoryFree(pParam);
    return -1;
  }

  if ((code = taskDbGenChkpUploadData(pParam->pTask->pBackend, ((SStreamMeta*)pParam->pMeta)->bkdChkptMgt,
                                      pParam->chkpId, (int8_t)(pParam->type), &path, toDelFiles)) != 0) {
    stError("s-task:%s failed to gen upload checkpoint:%" PRId64, taskStr, pParam->chkpId);
  }

  if (pParam->type == DATA_UPLOAD_S3) {
    if (code == 0 && (code = getCheckpointDataMeta(pParam->taskId, path, toDelFiles)) != 0) {
      stError("s-task:%s failed to get checkpointData for checkpointId:%" PRId64 " meta", taskStr, pParam->chkpId);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = streamTaskUploadCheckpoint(pParam->taskId, path);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to upload checkpoint data:%s, checkpointId:%" PRId64, taskStr, path, pParam->chkpId);
    } else {
      stDebug("s-task:%s backup checkpointId:%"PRId64" to remote succ", taskStr, pParam->chkpId);
    }
  }

  taskReleaseDb(pParam->dbRefId);

  if (code == 0) {
    int32_t size = taosArrayGetSize(toDelFiles);
    stDebug("s-task:%s remove redundant %d files", taskStr, size);

    for (int i = 0; i < size; i++) {
      char* pName = taosArrayGetP(toDelFiles, i);
      code = deleteCheckpointFile(pParam->taskId, pName);
      stDebug("s-task:%s try to del file: %s", taskStr, pName);
      if (code != 0) {
        break;
      }
    }
  }

  taosArrayDestroyP(toDelFiles, taosMemoryFree);

  stDebug("s-task:%s remove local checkpoint dir:%s", taskStr, path);
  taosRemoveDir(path);
  taosMemoryFree(path);

  taosMemoryFree(pParam->taskId);
  taosMemoryFree(pParam);

  return code;
}

int32_t streamTaskRemoteBackupCheckpoint(SStreamTask* pTask, int64_t checkpointId, char* taskId) {
  ECHECKPOINT_BACKUP_TYPE type = streamGetCheckpointBackupType();
  if (type == DATA_UPLOAD_DISABLE) {
    return 0;
  }

  if (pTask == NULL || pTask->pBackend == NULL) {
    return 0;
  }

  SAsyncUploadArg* arg = taosMemoryCalloc(1, sizeof(SAsyncUploadArg));
  arg->type = type;
  arg->taskId = taosStrdup(taskId);
  arg->chkpId = checkpointId;
  arg->pTask = pTask;
  arg->dbRefId = taskGetDBRef(pTask->pBackend);
  arg->pMeta = pTask->pMeta;

  return streamMetaAsyncExec(pTask->pMeta, uploadCheckpointData, arg, NULL);
}

int32_t streamTaskBuildCheckpoint(SStreamTask* pTask) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int64_t      startTs = pTask->chkInfo.startTs;
  int64_t      ckId = pTask->chkInfo.checkpointingId;
  const char*  id = pTask->id.idStr;
  bool         dropRelHTask = (streamTaskGetPrevStatus(pTask) == TASK_STATUS__HALT);
  SStreamMeta* pMeta = pTask->pMeta;

  // sink task does not need to save the status, and generated the checkpoint
  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    stDebug("s-task:%s level:%d start gen checkpoint, checkpointId:%" PRId64, id, pTask->info.taskLevel, ckId);

    code = streamBackendDoCheckpoint(pTask->pBackend, ckId);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s gen checkpoint:%" PRId64 " failed, code:%s", id, ckId, tstrerror(terrno));
    }
  }

  // send check point response to upstream task
  if (code == TSDB_CODE_SUCCESS) {
    if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
      code = streamTaskSendCheckpointSourceRsp(pTask);
    } else {
      code = streamTaskSendCheckpointReadyMsg(pTask);
    }

    if (code != TSDB_CODE_SUCCESS) {
      // todo: let's retry send rsp to upstream/mnode
      stError("s-task:%s failed to send checkpoint rsp to upstream, checkpointId:%" PRId64 ", code:%s", id, ckId,
              tstrerror(code));
    }
  }

  // update the latest checkpoint info if all works are done successfully, for rsma, the pMsgCb is null.
  if (code == TSDB_CODE_SUCCESS && (pTask->pMsgCb != NULL)) {
    STaskId* pHTaskId = &pTask->hTaskInfo.id;
    code = streamBuildAndSendCheckpointUpdateMsg(pTask->pMsgCb, pMeta->vgId, &pTask->id, pHTaskId, &pTask->chkInfo,
                                                 dropRelHTask);
    if (code == TSDB_CODE_SUCCESS) {
      code = streamTaskRemoteBackupCheckpoint(pTask, ckId, (char*)id);
      if (code != TSDB_CODE_SUCCESS) {
        stError("s-task:%s failed to upload checkpoint:%" PRId64 " failed", id, ckId);
      }
    } else {
      stError("s-task:%s commit taskInfo failed, checkpoint:%" PRId64 " failed, code:%s", id, ckId, tstrerror(code));
    }
  }

  // clear the checkpoint info if failed
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexLock(&pTask->lock);
    streamTaskClearCheckInfo(pTask, false);
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    taosThreadMutexUnlock(&pTask->lock);

    streamTaskSetFailedCheckpointId(pTask);
    stDebug("s-task:%s clear checkpoint flag since gen checkpoint failed, checkpointId:%" PRId64, id, ckId);
  }

  double el = (taosGetTimestampMs() - startTs) / 1000.0;
  stInfo("s-task:%s vgId:%d level:%d, checkpointId:%" PRId64 " ver:%" PRId64 " elapsed time:%.2f Sec, %s ", id,
         pMeta->vgId, pTask->info.taskLevel, ckId, pTask->chkInfo.checkpointVer, el,
         (code == TSDB_CODE_SUCCESS) ? "succ" : "failed");

  return code;
}

static int32_t uploadCheckpointToS3(const char* id, const char* path) {
  TdDirPtr pDir = taosOpenDir(path);
  if (pDir == NULL) return -1;

  TdDirEntryPtr de = NULL;
  s3Init();
  while ((de = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || taosDirEntryIsDir(de)) continue;

    char filename[PATH_MAX] = {0};
    if (path[strlen(path) - 1] == TD_DIRSEP_CHAR) {
      snprintf(filename, sizeof(filename), "%s%s", path, name);
    } else {
      snprintf(filename, sizeof(filename), "%s%s%s", path, TD_DIRSEP, name);
    }

    char object[PATH_MAX] = {0};
    snprintf(object, sizeof(object), "%s%s%s", id, TD_DIRSEP, name);

    if (s3PutObjectFromFile2(filename, object, 0) != 0) {
      taosCloseDir(&pDir);
      return -1;
    }
    stDebug("[s3] upload checkpoint:%s", filename);
    // break;
  }

  taosCloseDir(&pDir);
  return 0;
}

int32_t downloadCheckpointByNameS3(const char* id, const char* fname, const char* dstName) {
  int32_t code = 0;
  char*   buf = taosMemoryCalloc(1, strlen(id) + strlen(dstName) + 4);
  if (buf == NULL) {
    code = terrno = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  sprintf(buf, "%s/%s", id, fname);
  if (s3GetObjectToFile(buf, dstName) != 0) {
    code = errno;
  }

  taosMemoryFree(buf);
  return code;
}

ECHECKPOINT_BACKUP_TYPE streamGetCheckpointBackupType() {
  if (strlen(tsSnodeAddress) != 0) {
    return DATA_UPLOAD_RSYNC;
  } else if (tsS3StreamEnabled) {
    return DATA_UPLOAD_S3;
  } else {
    return DATA_UPLOAD_DISABLE;
  }
}

int32_t streamTaskUploadCheckpoint(const char* id, const char* path) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("invalid parameters in upload checkpoint, %s", id);
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return uploadRsync(id, path);
  } else if (tsS3StreamEnabled) {
    return uploadCheckpointToS3(id, path);
  }

  return 0;
}

// fileName:  CURRENT
int32_t downloadCheckpointDataByName(const char* id, const char* fname, const char* dstName) {
  if (id == NULL || fname == NULL || strlen(id) == 0 || strlen(fname) == 0 || strlen(fname) >= PATH_MAX) {
    stError("down load checkpoint data parameters invalid");
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return 0;
  } else if (tsS3StreamEnabled) {
    return downloadCheckpointByNameS3(id, fname, dstName);
  }

  return 0;
}

int32_t streamTaskDownloadCheckpointData(const char* id, char* path) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("down checkpoint data parameters invalid");
    return -1;
  }

  if (strlen(tsSnodeAddress) != 0) {
    return downloadRsync(id, path);
  } else if (tsS3StreamEnabled) {
    return s3GetObjectsByPrefix(id, path);
  }

  return 0;
}

int32_t deleteCheckpoint(const char* id) {
  if (id == NULL || strlen(id) == 0) {
    stError("deleteCheckpoint parameters invalid");
    return -1;
  }
  if (strlen(tsSnodeAddress) != 0) {
    return deleteRsync(id);
  } else if (tsS3StreamEnabled) {
    s3DeleteObjectsByPrefix(id);
  }
  return 0;
}

int32_t deleteCheckpointFile(const char* id, const char* name) {
  char object[128] = {0};
  snprintf(object, sizeof(object), "%s/%s", id, name);

  char* tmp = object;
  s3DeleteObjects((const char**)&tmp, 1);
  return 0;
}
