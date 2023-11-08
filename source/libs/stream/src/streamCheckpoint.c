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
#include "rsync.h"

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pReq->mgmtEps) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->mnodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->expireTime) < 0) return -1;
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

int32_t tDecodeStreamCheckpointSourceRsp(SDecoder* pDecoder, SStreamCheckpointSourceRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->nodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->expireTime) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->success) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
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
  streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);

  pTask->chkInfo.checkpointingId = pReq->checkpointId;
  pTask->chkInfo.checkpointNotReadyTasks = streamTaskGetNumOfDownstream(pTask);
  pTask->chkInfo.startTs = taosGetTimestampMs();
  pTask->execInfo.checkpoint += 1;

  // 2. Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
  int32_t code = appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER);
  return code;
}

static int32_t continueDispatchCheckpointBlock(SStreamDataBlock* pBlock, SStreamTask* pTask) {
  pBlock->srcTaskId = pTask->id.taskId;
  pBlock->srcVgId = pTask->pMeta->vgId;

  int32_t code = taosWriteQitem(pTask->outputq.queue->pQueue, pBlock);
  if (code == 0) {
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
  const char*  id = pTask->id.idStr;
  int32_t      code = TSDB_CODE_SUCCESS;

  // set task status
  if (streamTaskGetStatus(pTask, NULL) != TASK_STATUS__CK) {
    pTask->chkInfo.checkpointingId = checkpointId;
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
        atomic_add_fetch_32(&pTask->chkInfo.checkpointNotReadyTasks, 1);
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
    } else { // source & agg tasks need to forward the checkpoint msg downwards
      stDebug("s-task:%s process checkpoint block, all %d upstreams sent checkpoint msgs, continue forwards msg", id,
              num);

      // set the needed checked downstream tasks, only when all downstream tasks do checkpoint complete, this task
      // can start local checkpoint procedure
      pTask->chkInfo.checkpointNotReadyTasks = streamTaskGetNumOfDownstream(pTask);

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
  int32_t notReady = atomic_sub_fetch_32(&pTask->chkInfo.checkpointNotReadyTasks, 1);
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

void streamTaskClearCheckInfo(SStreamTask* pTask) {
  pTask->chkInfo.checkpointingId = 0;  // clear the checkpoint id
  pTask->chkInfo.failedId = 0;
  pTask->chkInfo.startTs = 0;  // clear the recorded start time
  pTask->chkInfo.downstreamAlignNum = 0;
  pTask->chkInfo.checkpointNotReadyTasks = 0;
  streamTaskOpenAllUpstreamInput(pTask);  // open inputQ for all upstream tasks
}

int32_t streamSaveTaskCheckpointInfo(SStreamTask* p, int64_t checkpointId) {
  SStreamMeta* pMeta = p->pMeta;
  int32_t      vgId = pMeta->vgId;
  int32_t      code = 0;

  if (p->info.fillHistory == 1) {
    return code;
  }

  streamMetaWLock(pMeta);
  ASSERT(p->chkInfo.checkpointId < p->chkInfo.checkpointingId && p->chkInfo.checkpointingId == checkpointId);

  p->chkInfo.checkpointId = p->chkInfo.checkpointingId;
  streamTaskClearCheckInfo(p);

  char* str = NULL;
  streamTaskGetStatus(p, &str);

  code = streamTaskHandleEvent(p->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
  if (code != TSDB_CODE_SUCCESS) {
    stDebug("s-task:%s vgId:%d save task status failed, since handle event failed", p->id.idStr, vgId);
    streamMetaWUnLock(pMeta);
    return -1;
  } else {  // save the task
    streamMetaSaveTask(pMeta, p);
  }

  stDebug(
      "vgId:%d s-task:%s level:%d open upstream inputQ, commit task status after checkpoint completed, "
      "checkpointId:%" PRId64 ", Ver(saved):%" PRId64 " currentVer:%" PRId64 ", status to be normal, prev:%s",
      vgId, p->id.idStr, p->info.taskLevel, checkpointId, p->chkInfo.checkpointVer, p->chkInfo.nextProcessVer, str);

  code = streamMetaCommit(pMeta);
  if (code < 0) {
    stError("vgId:%d failed to commit stream meta after do checkpoint, checkpointId:%" PRId64 ", since %s", pMeta->vgId,
            checkpointId, terrstr());
  } else {
    stInfo("vgId:%d commit stream meta after do checkpoint, checkpointId:%" PRId64 " DONE", pMeta->vgId, checkpointId);
  }

  streamMetaWUnLock(pMeta);
  return code;
}

int32_t streamTaskBuildCheckpoint(SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;

  // check for all tasks, and do generate the vnode-wide checkpoint data.
  int64_t checkpointStartTs = pTask->chkInfo.startTs;

  // sink task do not need to save the status, and generated the checkpoint
  if (pTask->info.taskLevel != TASK_LEVEL__SINK) {
    stDebug("s-task:%s level:%d start gen checkpoint", pTask->id.idStr, pTask->info.taskLevel);
    streamBackendDoCheckpoint(pTask->pBackend, pTask->chkInfo.checkpointingId);
    streamSaveTaskCheckpointInfo(pTask, pTask->chkInfo.checkpointingId);
  }

  double el = (taosGetTimestampMs() - checkpointStartTs) / 1000.0;
  stInfo("s-task:%s vgId:%d checkpointId:%" PRId64 " save all tasks status, level:%d elapsed time:%.2f Sec ",
         pTask->id.idStr, pTask->pMeta->vgId, pTask->chkInfo.checkpointingId, pTask->info.taskLevel, el);

  // send check point response to upstream task
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    code = streamTaskSendCheckpointSourceRsp(pTask);
  } else {
    code = streamTaskSendCheckpointReadyMsg(pTask);
  }

  if (code != TSDB_CODE_SUCCESS) {
    // record the failure checkpoint id
    pTask->chkInfo.failedId = pTask->chkInfo.checkpointingId;

    // todo: let's retry send rsp to upstream/mnode
    stError("s-task:%s failed to send checkpoint rsp to upstream, checkpointId:%" PRId64 ", code:%s", pTask->id.idStr,
            pTask->chkInfo.checkpointingId, tstrerror(code));
  }

  return code;
}

//static int64_t kBlockSize = 64 * 1024;
//static int sendCheckpointToS3(char* id, SArray* fileList){
//  code = s3PutObjectFromFile2(from->fname, object_name);
//  return 0;
//}
//static int sendCheckpointToSnode(char* id, SArray* fileList){
//  if(strlen(id) >= CHECKPOINT_PATH_LEN){
//    tqError("uploadCheckpoint id name too long, name:%s", id);
//    return -1;
//  }
//  uint8_t* buf = taosMemoryCalloc(1, sizeof(SChekpointDataHeader) + kBlockSize);
//  if(buf == NULL){
//    tqError("uploadCheckpoint malloc failed");
//    return -1;
//  }
//
//  SChekpointDataHeader* pHdr = (SChekpointDataHeader*)buf;
//  strcpy(pHdr->id, id);
//
//  TdFilePtr fd = NULL;
//  for(int i = 0; i < taosArrayGetSize(fileList); i++){
//    char* name = (char*)taosArrayGetP(fileList, i);
//    if(strlen(name) >= CHECKPOINT_PATH_LEN){
//      tqError("uploadCheckpoint file name too long, name:%s", name);
//      return -1;
//    }
//    int64_t  offset = 0;
//
//    fd = taosOpenFile(name, TD_FILE_READ);
//    tqDebug("uploadCheckpoint open file %s, file index: %d", name, i);
//
//    while(1){
//      int64_t  nread = taosPReadFile(fd, buf + sizeof(SChekpointDataHeader), kBlockSize, offset);
//      if (nread == -1) {
//        taosCloseFile(&fd);
//        taosMemoryFree(buf);
//        tqError("uploadCheckpoint failed to read file name:%s,reason:%d", name, errno);
//        return -1;
//      } else if (nread == 0){
//        tqDebug("uploadCheckpoint no data read, close file:%s, move to next file, open and read", name);
//        taosCloseFile(&fd);
//        break;
//      } else if (nread == kBlockSize){
//        offset += nread;
//      } else {
//        taosCloseFile(&fd);
//        offset = 0;
//      }
//      tqDebug("uploadCheckpoint read file %s, size:%" PRId64 ", current offset:%" PRId64, name, nread, offset);
//
//
//      pHdr->size = nread;
//      strcpy(pHdr->name, name);
//
//      SRpcMsg rpcMsg = {0};
//      int32_t bytes = sizeof(SChekpointDataHeader) + nread;
//      rpcMsg.pCont = rpcMallocCont(bytes);
//      rpcMsg.msgType = TDMT_SYNC_SNAPSHOT_SEND;
//      rpcMsg.contLen = bytes;
//      if (rpcMsg.pCont == NULL) {
//        tqError("uploadCheckpoint malloc failed");
//        taosCloseFile(&fd);
//        taosMemoryFree(buf);
//        return -1;
//      }
//      memcpy(rpcMsg.pCont, buf, bytes);
//      int try = 3;
//      int32_t code = 0;
//      while(try-- > 0){
//        code = tmsgSendReq(pEpSet, &rpcMsg);
//        if(code == 0)
//          break;
//        taosMsleep(10);
//      }
//      if(code != 0){
//        tqError("uploadCheckpoint send request failed code:%d", code);
//        taosCloseFile(&fd);
//        taosMemoryFree(buf);
//        return -1;
//      }
//
//      if(offset == 0){
//        break;
//      }
//    }
//  }
//
//  taosMemoryFree(buf);

//}

int uploadCheckpoint(char* id, char* path){
  if(id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX){
    stError("uploadCheckpoint parameters invalid");
    return -1;
  }
  if(strlen(tsSnodeIp) != 0){
    uploadRsync(id, path);
//  }else if(tsS3StreamEnabled){

  }
  return 0;
}

int downloadCheckpoint(char* id, char* path){
  if(id == NULL || path == NULL || strlen(id) == 0 || strlen(path) == 0 || strlen(path) >= PATH_MAX){
    stError("downloadCheckpoint parameters invalid");
    return -1;
  }
  if(strlen(tsSnodeIp) != 0){
    downloadRsync(id, path);
//  }else if(tsS3StreamEnabled){

  }
  return 0;
}

int deleteCheckpoint(char* id){
  if(id == NULL || strlen(id) == 0){
    stError("deleteCheckpoint parameters invalid");
    return -1;
  }
  if(strlen(tsSnodeIp) != 0){
    deleteRsync(id);
//  }else if(tsS3StreamEnabled){

  }
  return 0;
}
