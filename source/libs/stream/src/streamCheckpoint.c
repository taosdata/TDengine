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
  UPLOAD_TYPE type;
  char*       taskId;
  int64_t     chkpId;

  SStreamTask* pTask;
} SAsyncUploadArg;

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pReq->mgmtEps) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->mnodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->expireTime) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->transId) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->mndTrigger) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pReq->mgmtEps) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->mnodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->expireTime) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->transId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->mndTrigger) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->nodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->expireTime) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->success) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tEncodeStreamCheckpointReadyMsg(SEncoder* pEncoder, const SStreamCheckpointReadyMsg* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointReadyMsg(SDecoder* pDecoder, SStreamCheckpointReadyMsg* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->childId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

static int32_t streamAlignCheckpoint(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  int64_t old = atomic_val_compare_exchange_32(&pTask->chkInfo.downstreamAlignNum, 0, num);
  if (old == 0) {
    stDebug("s-task:%s set initial align upstream num:%d", pTask->id.idStr, num);
  }

  return atomic_sub_fetch_32(&pTask->chkInfo.downstreamAlignNum, 1);
}

static int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType) {
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

  streamSchedExec(pTask);
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

static int32_t continueDispatchCheckpointBlock(SStreamDataBlock* pBlock, SStreamTask* pTask) {
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

int32_t streamProcessCheckpointBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  SSDataBlock* pDataBlock = taosArrayGet(pBlock->blocks, 0);
  int64_t      checkpointId = pDataBlock->info.version;
  int32_t      transId = pDataBlock->info.window.skey;
  const char*  id = pTask->id.idStr;
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      vgId = pTask->pMeta->vgId;

  stDebug("s-task:%s vgId:%d start to handle the checkpoint block, checkpointId:%" PRId64 " ver:%" PRId64
          ", transId:%d current checkpointingId:%" PRId64,
          id, vgId, pTask->chkInfo.checkpointId, pTask->chkInfo.checkpointVer, transId, checkpointId);

  // set task status
  if (streamTaskGetStatus(pTask)->state != TASK_STATUS__CK) {
    pTask->chkInfo.checkpointingId = checkpointId;
    pTask->chkInfo.transId = transId;

    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s handle checkpoint-trigger block failed, code:%s", id, tstrerror(code));
      return code;
    }
  }

  // todo fix race condition: set the status and append checkpoint block
  int32_t taskLevel = pTask->info.taskLevel;
  if (taskLevel == TASK_LEVEL__SOURCE) {
    int8_t type = pTask->outputInfo.type;
    if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      stDebug("s-task:%s set childIdx:%d, and add checkpoint-trigger block into outputQ", id, pTask->info.selfChildId);
      continueDispatchCheckpointBlock(pBlock, pTask);
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
      code = continueDispatchCheckpointBlock(pBlock, pTask);
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

  streamTaskOpenAllUpstreamInput(pTask);  // open inputQ for all upstream tasks
  if (clearChkpReadyMsg) {
    streamClearChkptReadyMsg(pTask);
  }
}

int32_t streamSaveTaskCheckpointInfo(SStreamTask* p, int64_t checkpointId) {
  SStreamMeta*     pMeta = p->pMeta;
  int32_t          vgId = pMeta->vgId;
  const char*      id = p->id.idStr;
  int32_t          code = 0;
  SCheckpointInfo* pCKInfo = &p->chkInfo;

  // fill-history task, rsma task, and sink task will not generate the checkpoint
  if ((p->info.fillHistory == 1) || (p->info.taskLevel > TASK_LEVEL__SINK)) {
    return code;
  }

  taosThreadMutexLock(&p->lock);

  SStreamTaskState* pStatus = streamTaskGetStatus(p);
  if (pStatus->state == TASK_STATUS__CK) {
    ASSERT(pCKInfo->checkpointId <= pCKInfo->checkpointingId && pCKInfo->checkpointingId == checkpointId &&
           pCKInfo->checkpointVer <= pCKInfo->processedVer);

    pCKInfo->checkpointId = pCKInfo->checkpointingId;
    pCKInfo->checkpointVer = pCKInfo->processedVer;

    streamTaskClearCheckInfo(p, false);
    taosThreadMutexUnlock(&p->lock);

    code = streamTaskHandleEvent(p->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
  } else {
    stDebug("s-task:%s vgId:%d status:%s not keep the checkpoint metaInfo, checkpoint:%" PRId64 " failed", id, vgId,
            pStatus->name, pCKInfo->checkpointingId);
    taosThreadMutexUnlock(&p->lock);

    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  if (code != TSDB_CODE_SUCCESS) {
    stDebug("s-task:%s vgId:%d handle event:checkpoint-done failed", id, vgId);
    return code;
  }

  stDebug("vgId:%d s-task:%s level:%d open upstream inputQ, save status after checkpoint, checkpointId:%" PRId64
          ", Ver(saved):%" PRId64 " currentVer:%" PRId64 ", status: normal, prev:%s",
          vgId, id, p->info.taskLevel, checkpointId, pCKInfo->checkpointVer, pCKInfo->nextProcessVer, pStatus->name);

  // save the task if not sink task
  if (p->info.taskLevel <= TASK_LEVEL__SINK) {
    streamMetaWLock(pMeta);

    code = streamMetaSaveTask(pMeta, p);
    if (code != TSDB_CODE_SUCCESS) {
      streamMetaWUnLock(pMeta);
      stError("s-task:%s vgId:%d failed to save task info after do checkpoint, checkpointId:%" PRId64 ", since %s", id,
              vgId, checkpointId, terrstr());
      return code;
    }

    code = streamMetaCommit(pMeta);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s vgId:%d failed to commit stream meta after do checkpoint, checkpointId:%" PRId64 ", since %s",
              id, vgId, checkpointId, terrstr());
    }

    streamMetaWUnLock(pMeta);
  }

  return code;
}

void streamTaskSetCheckpointFailedId(SStreamTask* pTask) {
  pTask->chkInfo.failedId = pTask->chkInfo.checkpointingId;
  stDebug("s-task:%s mark the checkpointId:%" PRId64 " (transId:%d) failed", pTask->id.idStr,
          pTask->chkInfo.checkpointingId, pTask->chkInfo.transId);
}

int32_t getChkpMeta(char* id, char* path, SArray* list) {
  char* file = taosMemoryCalloc(1, strlen(path) + 32);
  sprintf(file, "%s%s%s", path, TD_DIRSEP, "META_TMP");
  int32_t code = downloadCheckpointByName(id, "META", file);
  if (code != 0) {
    stDebug("chkp failed to download meta file:%s", file);
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
int32_t doUploadChkp(void* param) {
  SAsyncUploadArg* arg = param;
  char*            path = NULL;
  int32_t          code = 0;
  SArray*          toDelFiles = taosArrayInit(4, sizeof(void*));

  if ((code = taskDbGenChkpUploadData(arg->pTask->pBackend, arg->pTask->pMeta->bkdChkptMgt, arg->chkpId,
                                      (int8_t)(arg->type), &path, toDelFiles)) != 0) {
    stError("s-task:%s failed to gen upload checkpoint:%" PRId64 "", arg->pTask->id.idStr, arg->chkpId);
  }
  if (arg->type == UPLOAD_S3) {
    if (code == 0 && (code = getChkpMeta(arg->taskId, path, toDelFiles)) != 0) {
      stError("s-task:%s failed to get  checkpoint:%" PRId64 " meta", arg->pTask->id.idStr, arg->chkpId);
    }
  }

  if (code == 0 && (code = uploadCheckpoint(arg->taskId, path)) != 0) {
    stError("s-task:%s failed to upload checkpoint:%" PRId64, arg->pTask->id.idStr, arg->chkpId);
  }

  if (code == 0) {
    for (int i = 0; i < taosArrayGetSize(toDelFiles); i++) {
      char* p = taosArrayGetP(toDelFiles, i);
      code = deleteCheckpointFile(arg->taskId, p);
      stDebug("s-task:%s try to del file: %s", arg->pTask->id.idStr, p);
      if (code != 0) {
        break;
      }
    }
  }

  taosArrayDestroyP(toDelFiles, taosMemoryFree);

  taosRemoveDir(path);
  taosMemoryFree(path);

  taosMemoryFree(arg->taskId);
  taosMemoryFree(arg);
  return code;
}
int32_t streamTaskUploadChkp(SStreamTask* pTask, int64_t chkpId, char* taskId) {
  // async upload
  UPLOAD_TYPE type = getUploadType();
  if (type == UPLOAD_DISABLE) {
    return 0;
  }
  if (pTask == NULL || pTask->pBackend == NULL) {
    return 0;
  }
  SAsyncUploadArg* arg = taosMemoryCalloc(1, sizeof(SAsyncUploadArg));
  arg->type = type;
  arg->taskId = taosStrdup(taskId);
  arg->chkpId = chkpId;
  arg->pTask = pTask;

  return streamMetaAsyncExec(pTask->pMeta, doUploadChkp, arg, NULL);
}
int32_t streamTaskBuildCheckpoint(SStreamTask* pTask) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int64_t     startTs = pTask->chkInfo.startTs;
  int64_t     ckId = pTask->chkInfo.checkpointingId;
  const char* id = pTask->id.idStr;
  bool        dropRelHTask = (streamTaskGetPrevStatus(pTask) == TASK_STATUS__HALT);

  // sink task do not need to save the status, and generated the checkpoint
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

  // clear the checkpoint info, and commit the newest checkpoint info if all works are done successfully
  if (code == TSDB_CODE_SUCCESS) {
    code = streamSaveTaskCheckpointInfo(pTask, ckId);
    if (code == TSDB_CODE_SUCCESS) {
      code = streamTaskUploadChkp(pTask, ckId, (char*)id);
      if (code != TSDB_CODE_SUCCESS) {
        stError("s-task:%s failed to upload checkpoint:%" PRId64 " failed", id, ckId);
      }
    } else {
      stError("s-task:%s commit taskInfo failed, checkpoint:%" PRId64 " failed, code:%s", id, ckId, tstrerror(code));
    }
  }

  if ((code == TSDB_CODE_SUCCESS) && dropRelHTask) {
    // transferred from the halt status, it is done the fill-history procedure and finish with the checkpoint
    // free it and remove fill-history task from disk meta-store
    taosThreadMutexLock(&pTask->lock);
    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      SStreamTaskId hTaskId = {.streamId = pTask->hTaskInfo.id.streamId, .taskId = pTask->hTaskInfo.id.taskId};

      stDebug("s-task:%s fill-history finish checkpoint done, drop related fill-history task:0x%x", id, hTaskId.taskId);
      streamBuildAndSendDropTaskMsg(pTask->pMsgCb, pTask->pMeta->vgId, &hTaskId, 1);
    } else {
      stWarn("s-task:%s related fill-history task:0x%x is erased", id, (int32_t)pTask->hTaskInfo.id.taskId);
    }
    taosThreadMutexUnlock(&pTask->lock);
  }

  // clear the checkpoint info if failed
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexLock(&pTask->lock);
    streamTaskClearCheckInfo(pTask, false);
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    taosThreadMutexUnlock(&pTask->lock);

    streamTaskSetCheckpointFailedId(pTask);
    stDebug("s-task:%s clear checkpoint flag since gen checkpoint failed, checkpointId:%" PRId64, id, ckId);
  }

  double el = (taosGetTimestampMs() - startTs) / 1000.0;
  stInfo("s-task:%s vgId:%d level:%d, checkpointId:%" PRId64 " ver:%" PRId64 " elapsed time:%.2f Sec, %s ", id,
         pTask->pMeta->vgId, pTask->info.taskLevel, ckId, pTask->chkInfo.checkpointVer, el,
         (code == TSDB_CODE_SUCCESS) ? "succ" : "failed");

  return code;
}

static int uploadCheckpointToS3(char* id, char* path) {
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

static int downloadCheckpointByNameS3(char* id, char* fname, char* dstName) {
  int   code = 0;
  char* buf = taosMemoryCalloc(1, strlen(id) + strlen(dstName) + 4);
  sprintf(buf, "%s/%s", id, fname);
  if (s3GetObjectToFile(buf, dstName) != 0) {
    code = -1;
  }
  taosMemoryFree(buf);
  return code;
}

UPLOAD_TYPE getUploadType() {
  if (strlen(tsSnodeAddress) != 0) {
    return UPLOAD_RSYNC;
  } else if (tsS3StreamEnabled) {
    return UPLOAD_S3;
  } else {
    return UPLOAD_DISABLE;
  }
}

int uploadCheckpoint(char* id, char* path) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("uploadCheckpoint parameters invalid");
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
int downloadCheckpointByName(char* id, char* fname, char* dstName) {
  if (id == NULL || fname == NULL || strlen(id) == 0 || strlen(fname) == 0 || strlen(fname) >= PATH_MAX) {
    stError("uploadCheckpointByName parameters invalid");
    return -1;
  }
  if (strlen(tsSnodeAddress) != 0) {
    return 0;
  } else if (tsS3StreamEnabled) {
    return downloadCheckpointByNameS3(id, fname, dstName);
  }
  return 0;
}

int downloadCheckpoint(char* id, char* path) {
  if (id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX) {
    stError("downloadCheckpoint parameters invalid");
    return -1;
  }
  if (strlen(tsSnodeAddress) != 0) {
    return downloadRsync(id, path);
  } else if (tsS3StreamEnabled) {
    return s3GetObjectsByPrefix(id, path);
  }
  return 0;
}

int deleteCheckpoint(char* id) {
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

int deleteCheckpointFile(char* id, char* name) {
  char object[128] = {0};
  snprintf(object, sizeof(object), "%s/%s", id, name);
  char* tmp = object;
  s3DeleteObjects((const char**)&tmp, 1);
  return 0;
}
