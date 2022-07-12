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

#include "streamInc.h"

static int32_t streamTaskExecImpl(SStreamTask* pTask, void* data, SArray* pRes) {
  void* exec = pTask->exec.executor;

  // set input
  SStreamQueueItem* pItem = (SStreamQueueItem*)data;
  if (pItem->type == STREAM_INPUT__TRIGGER) {
    SStreamTrigger* pTrigger = (SStreamTrigger*)data;
    qSetMultiStreamInput(exec, pTrigger->pBlock, 1, STREAM_INPUT__DATA_BLOCK, false);
  } else if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    ASSERT(pTask->isDataScan);
    SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)data;
    qInfo("task %d %p set submit input %p %p %d", pTask->taskId, pTask, pSubmit, pSubmit->data, *pSubmit->dataRef);
    qSetStreamInput(exec, pSubmit->data, STREAM_INPUT__DATA_SUBMIT, false);
  } else if (pItem->type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)data;
    SArray*           blocks = pBlock->blocks;
    qInfo("task %d %p set ssdata input", pTask->taskId, pTask);
    qSetMultiStreamInput(exec, blocks->pData, blocks->size, STREAM_INPUT__DATA_BLOCK, false);
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
    if (output == NULL) {
      if (pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
        SSDataBlock       block = {0};
        SStreamDataBlock* pRetrieveBlock = (SStreamDataBlock*)data;
        ASSERT(taosArrayGetSize(pRetrieveBlock->blocks) == 1);
        assignOneDataBlock(&block, taosArrayGet(pRetrieveBlock->blocks, 0));
        block.info.type = STREAM_PULL_OVER;
        block.info.childId = pTask->selfChildId;
        taosArrayPush(pRes, &block);
      }
      break;
    }

    if (output->info.type == STREAM_RETRIEVE) {
      if (streamBroadcastToChildren(pTask, output) < 0) {
        // TODO
      }
      continue;
    }

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->selfChildId;
    taosArrayPush(pRes, &block);
  }
  return 0;
}

static SArray* streamExecForQall(SStreamTask* pTask, SArray* pRes) {
  while (1) {
    void* data = streamQueueNextItem(pTask->inputQueue);
    if (data == NULL) break;

    streamTaskExecImpl(pTask, data, pRes);

    if (pTask->taskStatus == TASK_STATUS__DROPPING) {
      taosArrayDestroyEx(pRes, (FDelete)tDeleteSSDataBlock);
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
      if (streamTaskOutput(pTask, qRes) < 0) {
        streamQueueProcessFail(pTask->inputQueue);
        taosArrayDestroyEx(pRes, (FDelete)tDeleteSSDataBlock);
        taosFreeQitem(qRes);
        return NULL;
      }
      streamQueueProcessSuccess(pTask->inputQueue);
      pRes = taosArrayInit(0, sizeof(SSDataBlock));
    }

    int8_t type = ((SStreamQueueItem*)data)->type;
    if (type == STREAM_INPUT__TRIGGER) {
      blockDataDestroy(((SStreamTrigger*)data)->pBlock);
      taosFreeQitem(data);
    } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE) {
      taosArrayDestroyEx(((SStreamDataBlock*)data)->blocks, (FDelete)tDeleteSSDataBlock);
      taosFreeQitem(data);
    } else if (type == STREAM_INPUT__DATA_SUBMIT) {
      ASSERT(pTask->isDataScan);
      streamDataSubmitRefDec((SStreamDataSubmit*)data);
      taosFreeQitem(data);
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

      taosArrayDestroyEx(pRes, (FDelete)tDeleteSSDataBlock);
      atomic_store_8(&pTask->execStatus, TASK_EXEC_STATUS__IDLE);
      return 0;
    } else if (execStatus == TASK_EXEC_STATUS__CLOSING) {
      continue;
    } else if (execStatus == TASK_EXEC_STATUS__EXECUTING) {
      ASSERT(taosArrayGetSize(pRes) == 0);
      taosArrayDestroyEx(pRes, (FDelete)tDeleteSSDataBlock);
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
