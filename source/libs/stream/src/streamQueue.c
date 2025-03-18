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

#define MAX_STREAM_EXEC_BATCH_NUM 32
#define MAX_SMOOTH_BURST_RATIO    5  // 5 sec

// todo refactor:
// read data from input queue
typedef struct SQueueReader {
  SStreamQueue* pQueue;
  int32_t       taskLevel;
  int32_t       maxBlocks;     // maximum block in one batch
  int32_t       waitDuration;  // maximum wait time to format several block into a batch to process, unit: ms
} SQueueReader;

#define streamQueueCurItem(_q) ((_q)->qItem)

static bool streamTaskExtractAvailableToken(STokenBucket* pBucket, const char* id);
static void streamTaskPutbackToken(STokenBucket* pBucket);
static void streamTaskConsumeQuota(STokenBucket* pBucket, int32_t bytes);
static void streamQueueNextItemInSourceQ(SStreamQueue* pQueue, SStreamQueueItem** pItem, ETaskStatus status, const char* id);

static void streamQueueCleanup(SStreamQueue* pQueue) {
  SStreamQueueItem* qItem = NULL;
  while (1) {
    streamQueueNextItemInSourceQ(pQueue, &qItem, TASK_STATUS__READY, "");
    if (qItem == NULL) {
      break;
    }
    streamFreeQitem(qItem);
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
}

int32_t streamQueueOpen(int64_t cap, SStreamQueue** pQ) {
  *pQ = NULL;

  int32_t code = 0;
  int32_t lino = 0;

  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) {
    return terrno;
  }

  code = taosOpenQueue(&pQueue->pQueue);
  TSDB_CHECK_CODE(code, lino, _error);

  code = taosAllocateQall(&pQueue->qall);
  TSDB_CHECK_CODE(code, lino, _error);

  code = taosOpenQueue(&pQueue->pChkptQueue);
  TSDB_CHECK_CODE(code, lino, _error);

  pQueue->status = STREAM_QUEUE__SUCESS;

  taosSetQueueCapacity(pQueue->pQueue, cap);
  taosSetQueueMemoryCapacity(pQueue->pQueue, cap * 1024);

  *pQ = pQueue;
  return code;

_error:
  streamQueueClose(pQueue, 0);
  stError("failed to open stream queue at line:%d, code:%s", lino, tstrerror(code));
  return code;
}

void streamQueueClose(SStreamQueue* pQueue, int32_t taskId) {
  stDebug("s-task:0x%x free the queue:%p, items in queue:%d", taskId, pQueue->pQueue,
          taosQueueItemSize(pQueue->pQueue));
  streamQueueCleanup(pQueue);

  taosFreeQall(pQueue->qall);
  taosCloseQueue(pQueue->pQueue);
  pQueue->pQueue = NULL;

  taosCloseQueue(pQueue->pChkptQueue);
  pQueue->pChkptQueue = NULL;

  taosMemoryFree(pQueue);
}

void streamQueueNextItem(SStreamQueue* pQueue, SStreamQueueItem** pItem) {
  *pItem = NULL;
  int8_t flag = atomic_exchange_8(&pQueue->status, STREAM_QUEUE__PROCESSING);

  if (flag == STREAM_QUEUE__FAILED) {
    *pItem = streamQueueCurItem(pQueue);
  } else {
    pQueue->qItem = NULL;
    (void) taosGetQitem(pQueue->qall, &pQueue->qItem);

    if (pQueue->qItem == NULL) {
      (void) taosReadAllQitems(pQueue->pQueue, pQueue->qall);
      (void) taosGetQitem(pQueue->qall, &pQueue->qItem);
    }

    *pItem = streamQueueCurItem(pQueue);
  }
}

void streamQueueNextItemInSourceQ(SStreamQueue* pQueue, SStreamQueueItem** pItem, ETaskStatus status, const char* id) {
  *pItem = NULL;
  int8_t flag = atomic_exchange_8(&pQueue->status, STREAM_QUEUE__PROCESSING);

  if (flag == STREAM_QUEUE__CHKPTFAILED) {
    *pItem = pQueue->qChkptItem;
    return;
  }

  if (flag == STREAM_QUEUE__FAILED) {
    *pItem = pQueue->qItem;
    return;
  }

  pQueue->qChkptItem = NULL;
  taosReadQitem(pQueue->pChkptQueue, (void**)&pQueue->qChkptItem);
  if (pQueue->qChkptItem != NULL) {
    stDebug("s-task:%s read data from checkpoint queue, status:%d", id, status);
    *pItem = pQueue->qChkptItem;
    return;
  }

  // if in checkpoint status, not read data from ordinary input q.
  if (status == TASK_STATUS__CK) {
    stDebug("s-task:%s in checkpoint status, not read data in block queue, status:%d", id, status);
    return;
  }

  // let's try the ordinary input q
  pQueue->qItem = NULL;
  int32_t num = taosGetQitem(pQueue->qall, &pQueue->qItem);
  TAOS_UNUSED(num);

  if (pQueue->qItem == NULL) {
    num = taosReadAllQitems(pQueue->pQueue, pQueue->qall);
    num = taosGetQitem(pQueue->qall, &pQueue->qItem);
    TAOS_UNUSED(num);
  }

  *pItem = streamQueueCurItem(pQueue);
}

void streamQueueProcessSuccess(SStreamQueue* queue) {
  if (atomic_load_8(&queue->status) != STREAM_QUEUE__PROCESSING) {
    stError("invalid queue status:%d, expect:%d", atomic_load_8(&queue->status), STREAM_QUEUE__PROCESSING);
    return;
  }

  queue->qItem = NULL;
  queue->qChkptItem = NULL;
  atomic_store_8(&queue->status, STREAM_QUEUE__SUCESS);
}

void streamQueueProcessFail(SStreamQueue* queue) {
  if (atomic_load_8(&queue->status) != STREAM_QUEUE__PROCESSING) {
    stError("invalid queue status:%d, expect:%d", atomic_load_8(&queue->status), STREAM_QUEUE__PROCESSING);
    return;
  }
  atomic_store_8(&queue->status, STREAM_QUEUE__FAILED);
}

void streamQueueGetSourceChkptFailed(SStreamQueue* pQueue) {
  if (atomic_load_8(&pQueue->status) != STREAM_QUEUE__PROCESSING) {
    stError("invalid queue status:%d, expect:%d", atomic_load_8(&pQueue->status), STREAM_QUEUE__PROCESSING);
    return;
  }
  atomic_store_8(&pQueue->status, STREAM_QUEUE__CHKPTFAILED);
}

bool streamQueueIsFull(const SStreamQueue* pQueue) {
  int32_t numOfItems = streamQueueGetNumOfItems(pQueue);
  if (numOfItems >= STREAM_TASK_QUEUE_CAPACITY) {
    return true;
  }

  return (SIZE_IN_MiB(taosQueueMemorySize(pQueue->pQueue)) >= STREAM_TASK_QUEUE_CAPACITY_IN_SIZE);
}

int32_t streamQueueGetNumOfItems(const SStreamQueue* pQueue) {
  int32_t numOfItems1 = taosQueueItemSize(pQueue->pQueue);
  int32_t numOfItems2 = taosQallItemSize(pQueue->qall);

  return numOfItems1 + numOfItems2;
}

int32_t streamQueueGetNumOfUnAccessedItems(const SStreamQueue* pQueue) {
  int32_t numOfItems1 = taosQueueItemSize(pQueue->pQueue);
  int32_t numOfItems2 = taosQallUnAccessedItemSize(pQueue->qall);

  return numOfItems1 + numOfItems2;
}

int32_t streamQueueGetItemSize(const SStreamQueue* pQueue) {
  return taosQueueMemorySize(pQueue->pQueue) + taosQallUnAccessedMemSize(pQueue->qall);
}

int32_t streamQueueItemGetSize(const SStreamQueueItem* pItem) {
  STaosQnode* p = (STaosQnode*)((char*)pItem - sizeof(STaosQnode));
  return p->dataSize;
}

void streamQueueItemIncSize(const SStreamQueueItem* pItem, int32_t size) {
  STaosQnode* p = (STaosQnode*)((char*)pItem - sizeof(STaosQnode));
  p->dataSize += size;
}

const char* streamQueueItemGetTypeStr(int32_t type) {
  switch (type) {
    case STREAM_INPUT__CHECKPOINT:
      return "checkpoint";
    case STREAM_INPUT__CHECKPOINT_TRIGGER:
      return "checkpoint-trigger";
    case STREAM_INPUT__TRANS_STATE:
      return "trans-state";
    case STREAM_INPUT__REF_DATA_BLOCK:
      return "ref-block";
    case STREAM_INPUT__RECALCULATE:
      return "recalculate";
    default:
      return "datablock";
  }
}

EExtractDataCode streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks,
                                             int32_t* blockSize) {
  const char*   id = pTask->id.idStr;
  int32_t       taskLevel = pTask->info.taskLevel;
  SStreamQueue* pQueue = pTask->inputq.queue;

  *pInput = NULL;
  *numOfBlocks = 0;
  *blockSize = 0;

  // no available token in bucket for sink task, let's wait for a little bit
  if (taskLevel == TASK_LEVEL__SINK && (!streamTaskExtractAvailableToken(pTask->outputInfo.pTokenBucket, id))) {
    stDebug("s-task:%s no available token in bucket for sink data, wait for 10ms", id);
    return EXEC_AFTER_IDLE;
  }

  while (1) {
    ETaskStatus status = streamTaskGetStatus(pTask).state;
    if (status == TASK_STATUS__PAUSE || status == TASK_STATUS__STOP) {
      stDebug("s-task:%s task should pause/stop, extract input blocks:%d", id, *numOfBlocks);
      return EXEC_CONTINUE;
    }

    SStreamQueueItem* qItem = NULL;
    if (taskLevel == TASK_LEVEL__SOURCE) {
      streamQueueNextItemInSourceQ(pQueue, &qItem, status, id);
    } else {
      streamQueueNextItem(pQueue, &qItem);
    }

    if (qItem == NULL) {
      // restore the token to bucket
      if (*numOfBlocks > 0) {
        *blockSize = streamQueueItemGetSize(*pInput);
        if (taskLevel == TASK_LEVEL__SINK) {
          streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
        }
      } else {
        streamTaskPutbackToken(pTask->outputInfo.pTokenBucket);
      }

      return EXEC_CONTINUE;
    }

    // do not merge blocks for sink node and check point data block
    int8_t type = qItem->type;
    if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
        type == STREAM_INPUT__TRANS_STATE || type == STREAM_INPUT__REF_DATA_BLOCK || type == STREAM_INPUT__RECALCULATE) {
      const char* p = streamQueueItemGetTypeStr(type);

      if (*pInput == NULL) {
        stDebug("s-task:%s %s msg extracted, start to process immediately", id, p);

        // restore the token to bucket in case of checkpoint/trans-state msg
        streamTaskPutbackToken(pTask->outputInfo.pTokenBucket);
        *blockSize = 0;
        *numOfBlocks = 1;
        *pInput = qItem;
        return EXEC_CONTINUE;
      } else {  // previous existed blocks needs to be handled, before handle the checkpoint msg block
        stDebug("s-task:%s %s msg extracted, handle previous blocks, numOfBlocks:%d", id, p, *numOfBlocks);
        *blockSize = streamQueueItemGetSize(*pInput);
        if (taskLevel == TASK_LEVEL__SINK) {
          streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
        }

        if ((type == STREAM_INPUT__CHECKPOINT_TRIGGER || type == STREAM_INPUT__CHECKPOINT) &&
            (taskLevel == TASK_LEVEL__SOURCE)) {
          streamQueueGetSourceChkptFailed(pQueue);
        } else {
          streamQueueProcessFail(pQueue);
        }
        return EXEC_CONTINUE;
      }
    } else {
      if (*pInput == NULL) {
        *pInput = qItem;
      } else { // merge current block failed, let's handle the already merged blocks.
        void*   newRet = NULL;
        int32_t code = streamQueueMergeQueueItem(*pInput, qItem, (SStreamQueueItem**)&newRet);
        if (newRet == NULL) {
          if (code != -1) {
            stError("s-task:%s failed to merge blocks from inputQ, numOfBlocks:%d, code:%s", id, *numOfBlocks,
                    tstrerror(code));
          }

          *blockSize = streamQueueItemGetSize(*pInput);
          if (taskLevel == TASK_LEVEL__SINK) {
            streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
          }

          streamQueueProcessFail(pQueue);
          return EXEC_CONTINUE;
        }

        *pInput = newRet;
      }

      *numOfBlocks += 1;
      streamQueueProcessSuccess(pQueue);

      if (*numOfBlocks >= MAX_STREAM_EXEC_BATCH_NUM) {
        stDebug("s-task:%s batch size limit:%d reached, start to process blocks", id, MAX_STREAM_EXEC_BATCH_NUM);

        *blockSize = streamQueueItemGetSize(*pInput);
        if (taskLevel == TASK_LEVEL__SINK) {
          streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
        }

        return EXEC_CONTINUE;
      }
    }
  }
}

int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t      type = pItem->type;
  STaosQueue* pQueue = pTask->inputq.queue->pQueue;
  int32_t     level = pTask->info.taskLevel;
  int32_t     total = streamQueueGetNumOfItems(pTask->inputq.queue) + 1;

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* px = (SStreamDataSubmit*)pItem;
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && streamQueueIsFull(pTask->inputq.queue)) {
      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
      stTrace(
          "s-task:%s inputQ is full, capacity(size:%d num:%dMiB), current(blocks:%d, size:%.2fMiB) stop to push data",
          pTask->id.idStr, STREAM_TASK_QUEUE_CAPACITY, STREAM_TASK_QUEUE_CAPACITY_IN_SIZE, total, size);
      streamDataSubmitDestroy(px);
      return TSDB_CODE_STREAM_INPUTQ_FULL;
    }

    int32_t msgLen = px->submit.msgLen;
    int64_t ver = px->submit.ver;

    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamDataSubmitDestroy(px);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));

    // use the local variable to avoid the pItem be freed by other threads, since it has been put into queue already.
    stDebug("s-task:%s submit enqueue msgLen:%d ver:%" PRId64 ", total in queue:%d, size:%.2fMiB", pTask->id.idStr,
            msgLen, ver, total, size + SIZE_IN_MiB(msgLen));
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__REF_DATA_BLOCK) {
    if (streamQueueIsFull(pTask->inputq.queue)) {
      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));

      stTrace("s-task:%s input queue is full, capacity:%d size:%d MiB, current(blocks:%d, size:%.2fMiB) abort",
              pTask->id.idStr, STREAM_TASK_QUEUE_CAPACITY, STREAM_TASK_QUEUE_CAPACITY_IN_SIZE, total, size);
      streamFreeQitem(pItem);
      return TSDB_CODE_STREAM_INPUTQ_FULL;
    }

    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamFreeQitem(pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    stDebug("s-task:%s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr, total, size);
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
             type == STREAM_INPUT__TRANS_STATE || type == STREAM_INPUT__DATA_RETRIEVE || type == STREAM_INPUT__RECALCULATE) {

    int32_t code = 0;
    if ((type == STREAM_INPUT__CHECKPOINT_TRIGGER || type == STREAM_INPUT__CHECKPOINT) && (level == TASK_LEVEL__SOURCE)) {
      STaosQueue* pChkptQ = pTask->inputq.queue->pChkptQueue;
      code = taosWriteQitem(pChkptQ, pItem);

      double  size = SIZE_IN_MiB(taosQueueMemorySize(pChkptQ));
      int32_t num = taosQueueItemSize(pChkptQ);

      stDebug("s-task:%s level:%d %s checkpoint enqueue ctrl queue, total in queue:%d, size:%.2fMiB, data queue:%d",
              pTask->id.idStr, pTask->info.taskLevel, streamQueueItemGetTypeStr(type), num, size, (total - 1));
    } else {
      code = taosWriteQitem(pQueue, pItem);
      if (code != TSDB_CODE_SUCCESS) {
        streamFreeQitem(pItem);
        return code;
      }

      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
      stDebug("s-task:%s level:%d %s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr,
              pTask->info.taskLevel, streamQueueItemGetTypeStr(type), total, size);
    }
  } else if (type == STREAM_INPUT__GET_RES) {
    // use the default memory limit, refactor later.
    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamFreeQitem(pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    stDebug("s-task:%s data res enqueue, current(blocks:%d, size:%.2fMiB)", pTask->id.idStr, total, size);
  } else {
    stError("s-task:%s invalid type:%d to put in inputQ", pTask->id.idStr, type);
    return TSDB_CODE_INVALID_PARA;
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && type != STREAM_INPUT__CHECKPOINT_TRIGGER &&
      type != STREAM_INPUT__RECALCULATE && (pTask->info.delaySchedParam != 0)) {
    (void)atomic_val_compare_exchange_8(&pTask->schedInfo.status, TASK_TRIGGER_STATUS__INACTIVE,
                                        TASK_TRIGGER_STATUS__MAY_ACTIVE);
    stDebug("s-task:%s new data arrived, active the sched-trigger, triggerStatus:%d", pTask->id.idStr,
            pTask->schedInfo.status);
  }

  return 0;
}

int32_t streamTaskPutTranstateIntoInputQ(SStreamTask* pTask) {
  int32_t           code = 0;
  SStreamDataBlock* pTranstate = NULL;
  SSDataBlock*      pBlock = NULL;

  code = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock), (void**)&pTranstate);
  if (code) {
    return code;
  }

  pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    code = terrno;
    goto _err;
  }

  pTranstate->type = STREAM_INPUT__TRANS_STATE;

  pBlock->info.type = STREAM_TRANS_STATE;
  pBlock->info.rows = 1;
  pBlock->info.childId = pTask->info.selfChildId;

  pTranstate->blocks = taosArrayInit(4, sizeof(SSDataBlock));  // pBlock;
  if (pTranstate->blocks == NULL) {
    code = terrno;
    goto _err;
  }

  void* p = taosArrayPush(pTranstate->blocks, pBlock);
  if (p == NULL) {
    code = terrno;
    goto _err;
  }

  taosMemoryFree(pBlock);
  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pTranstate) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pTask->status.appendTranstateBlock = true;
  return TSDB_CODE_SUCCESS;

_err:
  taosMemoryFree(pBlock);
  taosFreeQitem(pTranstate);
  return code;
}

// the result should be put into the outputQ in any cases, the result may be lost otherwise.
int32_t streamTaskPutDataIntoOutputQ(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  STaosQueue* pQueue = pTask->outputq.queue->pQueue;
  int32_t     code = taosWriteQitem(pQueue, pBlock);

  int32_t total = streamQueueGetNumOfItems(pTask->outputq.queue);
  double  size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
  if (code != 0) {
    stError("s-task:%s failed to put res into outputQ, outputQ items:%d, size:%.2fMiB code:%s, result lost",
            pTask->id.idStr, total + 1, size, tstrerror(code));
  } else {
    if (streamQueueIsFull(pTask->outputq.queue)) {
      stWarn(
          "s-task:%s outputQ is full(outputQ items:%d, size:%.2fMiB), set the output status BLOCKING, wait for 500ms "
          "after handle this batch of blocks",
          pTask->id.idStr, total, size);
    } else {
      stDebug("s-task:%s data put into outputQ, outputQ items:%d, size:%.2fMiB", pTask->id.idStr, total, size);
    }
  }

  return code;
}

int32_t streamTaskInitTokenBucket(STokenBucket* pBucket, int32_t numCap, int32_t numRate, float quotaRate,
                                  const char* id) {
  if (numCap < 10 || numRate < 10 || pBucket == NULL) {
    stError("failed to init sink task bucket, cap:%d, rate:%d", numCap, numRate);
    return TSDB_CODE_INVALID_PARA;
  }

  pBucket->numCapacity = numCap;
  pBucket->numOfToken = numCap;
  pBucket->numRate = numRate;

  pBucket->quotaRate = quotaRate;
  pBucket->quotaCapacity = quotaRate * MAX_SMOOTH_BURST_RATIO;
  pBucket->quotaRemain = pBucket->quotaCapacity;

  pBucket->tokenFillTimestamp = taosGetTimestampMs();
  pBucket->quotaFillTimestamp = taosGetTimestampMs();
  stDebug("s-task:%s sink quotaRate:%.2fMiB, numRate:%d", id, quotaRate, numRate);
  return TSDB_CODE_SUCCESS;
}

static void fillTokenBucket(STokenBucket* pBucket, const char* id) {
  int64_t now = taosGetTimestampMs();

  int64_t deltaToken = now - pBucket->tokenFillTimestamp;
  if (pBucket->numOfToken < 0) {
    return;
  }

  int32_t incNum = (deltaToken / 1000.0) * pBucket->numRate;
  if (incNum > 0) {
    pBucket->numOfToken = TMIN(pBucket->numOfToken + incNum, pBucket->numCapacity);
    pBucket->tokenFillTimestamp = now;
  }

  // increase the new available quota as time goes on
  int64_t deltaQuota = now - pBucket->quotaFillTimestamp;
  double  incSize = (deltaQuota / 1000.0) * pBucket->quotaRate;
  if (incSize > 0) {
    pBucket->quotaRemain = TMIN(pBucket->quotaRemain + incSize, pBucket->quotaCapacity);
    pBucket->quotaFillTimestamp = now;
  }

  if (incNum > 0 || incSize > 0) {
    stTrace("token/quota available, token:%d inc:%d, token_TsDelta:%" PRId64
            ", quota:%.2fMiB inc:%.3fMiB quotaTs:%" PRId64 " now:%" PRId64 "ms, %s",
            pBucket->numOfToken, incNum, deltaToken, pBucket->quotaRemain, incSize, deltaQuota, now, id);
  }
}

bool streamTaskExtractAvailableToken(STokenBucket* pBucket, const char* id) {
  fillTokenBucket(pBucket, id);

  if (pBucket->numOfToken > 0) {
    if (pBucket->quotaRemain > 0) {
      pBucket->numOfToken -= 1;
      return true;
    } else {  // no available size quota now
      return false;
    }
  } else {
    return false;
  }
}

void streamTaskPutbackToken(STokenBucket* pBucket) {
  pBucket->numOfToken = TMIN(pBucket->numOfToken + 1, pBucket->numCapacity);
}

// size in KB
void streamTaskConsumeQuota(STokenBucket* pBucket, int32_t bytes) { pBucket->quotaRemain -= SIZE_IN_MiB(bytes); }

void streamTaskInputFail(SStreamTask* pTask) { atomic_store_8(&pTask->inputq.status, TASK_INPUT_STATUS__FAILED); }
