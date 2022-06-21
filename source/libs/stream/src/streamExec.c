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

#include "executor.h"
#include "tstream.h"

static int32_t streamTaskExecImpl(SStreamTask* pTask, void* data, SArray* pRes) {
  void* exec = pTask->exec.executor;

  // set input
  SStreamQueueItem* pItem = (SStreamQueueItem*)data;
  if (pItem->type == STREAM_INPUT__TRIGGER) {
    SStreamTrigger* pTrigger = (SStreamTrigger*)data;
    qSetMultiStreamInput(exec, pTrigger->pBlock, 1, STREAM_DATA_TYPE_SSDATA_BLOCK, false);
  } else if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)data;
    ASSERT(pTask->inputType == STREAM_INPUT__DATA_SUBMIT);
    qSetStreamInput(exec, pSubmit->data, STREAM_DATA_TYPE_SUBMIT_BLOCK, false);
  } else if (pItem->type == STREAM_INPUT__DATA_BLOCK) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)data;
    ASSERT(pTask->inputType == STREAM_INPUT__DATA_BLOCK);
    SArray* blocks = pBlock->blocks;
    qSetMultiStreamInput(exec, blocks->pData, blocks->size, STREAM_DATA_TYPE_SSDATA_BLOCK, false);
  } else if (pItem->type == STREAM_INPUT__DROP) {
    // TODO exec drop
    return 0;
  }

  // exec
  while (1) {
    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if (qExecTask(exec, &output, &ts) < 0) {
      ASSERT(false);
    }
    if (output == NULL) break;
    // TODO: do we need free memory?
    SSDataBlock* outputCopy = createOneDataBlock(output, true);
    outputCopy->info.childId = pTask->childId;
    taosArrayPush(pRes, outputCopy);
  }
  return 0;
}

static SArray* streamExecForQall(SStreamTask* pTask, SArray* pRes) {
  while (1) {
    void* data = streamQueueNextItem(pTask->inputQueue);
    if (data == NULL) break;

    streamTaskExecImpl(pTask, data, pRes);

    if (pTask->taskStatus == TASK_STATUS__DROPPING) {
      return NULL;
    }

    if (taosArrayGetSize(pRes) != 0) {
      SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
      if (qRes == NULL) {
        streamQueueProcessFail(pTask->inputQueue);
        taosArrayDestroy(pRes);
        return NULL;
      }
      qRes->type = STREAM_INPUT__DATA_BLOCK;
      qRes->blocks = pRes;
      /*qRes->sourceVg = pTask->nodeId;*/
      if (streamTaskOutput(pTask, qRes) < 0) {
        streamQueueProcessFail(pTask->inputQueue);
        taosArrayDestroy(pRes);
        taosFreeQitem(qRes);
        return NULL;
      }

      if (((SStreamQueueItem*)data)->type == STREAM_INPUT__TRIGGER) {
        blockDataDestroy(((SStreamTrigger*)data)->pBlock);
        taosFreeQitem(data);
      } else {
        if (pTask->inputType == STREAM_INPUT__DATA_SUBMIT) {
          streamDataSubmitRefDec((SStreamDataSubmit*)data);
          taosFreeQitem(data);
        } else {
          taosArrayDestroyEx(((SStreamDataBlock*)data)->blocks, (FDelete)tDeleteSSDataBlock);
          taosFreeQitem(data);
        }
      }
      streamQueueProcessSuccess(pTask->inputQueue);
      return taosArrayInit(0, sizeof(SSDataBlock));
    }
  }
  return pRes;
}

// TODO: handle version
int32_t streamExec(SStreamTask* pTask, SMsgCb* pMsgCb) {
  SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
  if (pRes == NULL) return -1;
  while (1) {
    int8_t execStatus =
        atomic_val_compare_exchange_8(&pTask->execStatus, TASK_EXEC_STATUS__IDLE, TASK_EXEC_STATUS__EXECUTING);
    if (execStatus == TASK_EXEC_STATUS__IDLE) {
      // first run
      pRes = streamExecForQall(pTask, pRes);
      if (pRes == NULL) goto FAIL;

      // set status closing
      atomic_store_8(&pTask->execStatus, TASK_EXEC_STATUS__CLOSING);

      // second run, make sure inputQ and qall are cleared
      pRes = streamExecForQall(pTask, pRes);
      if (pRes == NULL) goto FAIL;

      taosArrayDestroy(pRes);
      atomic_store_8(&pTask->execStatus, TASK_EXEC_STATUS__IDLE);
      return 0;
    } else if (execStatus == TASK_EXEC_STATUS__CLOSING) {
      continue;
    } else if (execStatus == TASK_EXEC_STATUS__EXECUTING) {
      ASSERT(taosArrayGetSize(pRes) == 0);
      taosArrayDestroy(pRes);
      return 0;
    } else {
      ASSERT(0);
    }
  }
FAIL:
  if (pRes) taosArrayDestroy(pRes);
  atomic_store_8(&pTask->execStatus, TASK_EXEC_STATUS__IDLE);
  return -1;
}

