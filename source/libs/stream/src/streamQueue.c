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

#define MAX_STREAM_EXEC_BATCH_NUM                 32
#define MAX_SMOOTH_BURST_RATIO                    5     // 5 sec
#define WAIT_FOR_DURATION                         10

// todo refactor:
// read data from input queue
typedef struct SQueueReader {
  SStreamQueue* pQueue;
  int32_t       taskLevel;
  int32_t       maxBlocks;     // maximum block in one batch
  int32_t       waitDuration;  // maximum wait time to format several block into a batch to process, unit: ms
} SQueueReader;

static bool streamTaskExtractAvailableToken(STokenBucket* pBucket, const char* id);
static void streamTaskPutbackToken(STokenBucket* pBucket);
static void streamTaskConsumeQuota(STokenBucket* pBucket, int32_t bytes);

static void streamQueueCleanup(SStreamQueue* pQueue) {
  void* qItem = NULL;
  while ((qItem = streamQueueNextItem(pQueue)) != NULL) {
    streamFreeQitem(qItem);
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
}

static void* streamQueueCurItem(SStreamQueue* queue) { return queue->qItem; }

SStreamQueue* streamQueueOpen(int64_t cap) {
  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) {
    return NULL;
  }

  pQueue->pQueue = taosOpenQueue();
  pQueue->qall = taosAllocateQall();

  if (pQueue->pQueue == NULL || pQueue->qall == NULL) {
    if (pQueue->pQueue) taosCloseQueue(pQueue->pQueue);
    if (pQueue->qall) taosFreeQall(pQueue->qall);
    taosMemoryFree(pQueue);
    return NULL;
  }

  pQueue->status = STREAM_QUEUE__SUCESS;
  taosSetQueueCapacity(pQueue->pQueue, cap);
  taosSetQueueMemoryCapacity(pQueue->pQueue, cap * 1024);
  return pQueue;
}

void streamQueueClose(SStreamQueue* pQueue, int32_t taskId) {
  stDebug("s-task:0x%x free the queue:%p, items in queue:%d", taskId, pQueue->pQueue, taosQueueItemSize(pQueue->pQueue));
  streamQueueCleanup(pQueue);

  taosFreeQall(pQueue->qall);
  taosCloseQueue(pQueue->pQueue);
  taosMemoryFree(pQueue);
}

void* streamQueueNextItem(SStreamQueue* pQueue) {
  int8_t flag = atomic_exchange_8(&pQueue->status, STREAM_QUEUE__PROCESSING);

  if (flag == STREAM_QUEUE__FAILED) {
    ASSERT(pQueue->qItem != NULL);
    return streamQueueCurItem(pQueue);
  } else {
    pQueue->qItem = NULL;
    taosGetQitem(pQueue->qall, &pQueue->qItem);
    if (pQueue->qItem == NULL) {
      taosReadAllQitems(pQueue->pQueue, pQueue->qall);
      taosGetQitem(pQueue->qall, &pQueue->qItem);
    }

    return streamQueueCurItem(pQueue);
  }
}

void streamQueueProcessSuccess(SStreamQueue* queue) {
  ASSERT(atomic_load_8(&queue->status) == STREAM_QUEUE__PROCESSING);
  queue->qItem = NULL;
  atomic_store_8(&queue->status, STREAM_QUEUE__SUCESS);
}

void streamQueueProcessFail(SStreamQueue* queue) {
  ASSERT(atomic_load_8(&queue->status) == STREAM_QUEUE__PROCESSING);
  atomic_store_8(&queue->status, STREAM_QUEUE__FAILED);
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

int32_t streamQueueGetItemSize(const SStreamQueue* pQueue) {
  return taosQueueMemorySize(pQueue->pQueue) + taosQallUnAccessedMemSize(pQueue->qall);
}

int32_t streamQueueItemGetSize(const SStreamQueueItem* pItem) {
  STaosQnode* p = (STaosQnode*)((char*) pItem - sizeof(STaosQnode));
  return p->dataSize;
}

void streamQueueItemIncSize(const SStreamQueueItem* pItem, int32_t size) {
  STaosQnode* p = (STaosQnode*)((char*) pItem - sizeof(STaosQnode));
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
    default:
      return "datablock";
  }
}

int32_t streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks,
                                    int32_t* blockSize) {
  const char* id = pTask->id.idStr;
  int32_t     taskLevel = pTask->info.taskLevel;

  *pInput = NULL;
  *numOfBlocks = 0;
  *blockSize = 0;

  // no available token in bucket for sink task, let's wait for a little bit
  if (taskLevel == TASK_LEVEL__SINK && (!streamTaskExtractAvailableToken(pTask->outputInfo.pTokenBucket, id))) {
    stDebug("s-task:%s no available token in bucket for sink data, wait for 10ms", id);
    return TSDB_CODE_SUCCESS;
  }

  while (1) {
    if (streamTaskShouldPause(pTask) || streamTaskShouldStop(pTask)) {
      stDebug("s-task:%s task should pause, extract input blocks:%d", id, *numOfBlocks);
      return TSDB_CODE_SUCCESS;
    }

    SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputq.queue);
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

      return TSDB_CODE_SUCCESS;
    }

    // do not merge blocks for sink node and check point data block
    int8_t type = qItem->type;
    if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
        type == STREAM_INPUT__TRANS_STATE) {
      const char* p = streamQueueItemGetTypeStr(type);

      if (*pInput == NULL) {
        stDebug("s-task:%s %s msg extracted, start to process immediately", id, p);

        // restore the token to bucket in case of checkpoint/trans-state msg
        streamTaskPutbackToken(pTask->outputInfo.pTokenBucket);
        *blockSize = 0;
        *numOfBlocks = 1;
        *pInput = qItem;
        return TSDB_CODE_SUCCESS;
      } else { // previous existed blocks needs to be handle, before handle the checkpoint msg block
        stDebug("s-task:%s %s msg extracted, handle previous blocks, numOfBlocks:%d", id, p, *numOfBlocks);
        *blockSize = streamQueueItemGetSize(*pInput);
        if (taskLevel == TASK_LEVEL__SINK) {
          streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
        }

        streamQueueProcessFail(pTask->inputq.queue);
        return TSDB_CODE_SUCCESS;
      }
    } else {
      if (*pInput == NULL) {
        ASSERT((*numOfBlocks) == 0);
        *pInput = qItem;
      } else {
        // merge current block failed, let's handle the already merged blocks.
        void* newRet = streamQueueMergeQueueItem(*pInput, qItem);
        if (newRet == NULL) {
          if (terrno != 0) {
            stError("s-task:%s failed to merge blocks from inputQ, numOfBlocks:%d, code:%s", id, *numOfBlocks,
                   tstrerror(terrno));
          }

          *blockSize = streamQueueItemGetSize(*pInput);
          if (taskLevel == TASK_LEVEL__SINK) {
            streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
          }

          streamQueueProcessFail(pTask->inputq.queue);
          return TSDB_CODE_SUCCESS;
        }

        *pInput = newRet;
      }

      *numOfBlocks += 1;
      streamQueueProcessSuccess(pTask->inputq.queue);

      if (*numOfBlocks >= MAX_STREAM_EXEC_BATCH_NUM) {
        stDebug("s-task:%s batch size limit:%d reached, start to process blocks", id, MAX_STREAM_EXEC_BATCH_NUM);

        *blockSize = streamQueueItemGetSize(*pInput);
        if (taskLevel == TASK_LEVEL__SINK) {
          streamTaskConsumeQuota(pTask->outputInfo.pTokenBucket, *blockSize);
        }

        return TSDB_CODE_SUCCESS;
      }
    }
  }
}

int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t      type = pItem->type;
  STaosQueue* pQueue = pTask->inputq.queue->pQueue;
  int32_t     total = streamQueueGetNumOfItems(pTask->inputq.queue) + 1;

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* px = (SStreamDataSubmit*)pItem;
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && streamQueueIsFull(pTask->inputq.queue)) {
      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
      stTrace(
          "s-task:%s inputQ is full, capacity(size:%d num:%dMiB), current(blocks:%d, size:%.2fMiB) stop to push data",
          pTask->id.idStr, STREAM_TASK_QUEUE_CAPACITY, STREAM_TASK_QUEUE_CAPACITY_IN_SIZE, total, size);
      streamDataSubmitDestroy(px);
      return -1;
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
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    if (streamQueueIsFull(pTask->inputq.queue)) {
      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));

      stTrace("s-task:%s input queue is full, capacity:%d size:%d MiB, current(blocks:%d, size:%.2fMiB) abort",
             pTask->id.idStr, STREAM_TASK_QUEUE_CAPACITY, STREAM_TASK_QUEUE_CAPACITY_IN_SIZE, total, size);
      streamFreeQitem(pItem);
      return -1;
    }

    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamFreeQitem(pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    stDebug("s-task:%s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr, total, size);
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
             type == STREAM_INPUT__TRANS_STATE) {
    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamFreeQitem(pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    stDebug("s-task:%s level:%d %s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr,
           pTask->info.taskLevel, streamQueueItemGetTypeStr(type), total, size);
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
    ASSERT(0);
  }

  if (type != STREAM_INPUT__GET_RES && type != STREAM_INPUT__CHECKPOINT && pTask->info.triggerParam != 0) {
    atomic_val_compare_exchange_8(&pTask->schedInfo.status, TASK_TRIGGER_STATUS__INACTIVE, TASK_TRIGGER_STATUS__ACTIVE);
    stDebug("s-task:%s new data arrived, active the trigger, triggerStatus:%d", pTask->id.idStr, pTask->schedInfo.status);
  }

  return 0;
}

// the result should be put into the outputQ in any cases, the result may be lost otherwise.
int32_t streamTaskPutDataIntoOutputQ(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  STaosQueue* pQueue = pTask->outputq.queue->pQueue;
  int32_t code = taosWriteQitem(pQueue, pBlock);

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

  return TSDB_CODE_SUCCESS;
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
  ASSERT(pBucket->numOfToken >= 0);

  int32_t incNum = (deltaToken / 1000.0) * pBucket->numRate;
  if (incNum > 0) {
    pBucket->numOfToken = TMIN(pBucket->numOfToken + incNum, pBucket->numCapacity);
    pBucket->tokenFillTimestamp = now;
  }

  // increase the new available quota as time goes on
  int64_t deltaQuota = now - pBucket->quotaFillTimestamp;
  double incSize = (deltaQuota / 1000.0) * pBucket->quotaRate;
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
    } else { // no available size quota now
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
void streamTaskConsumeQuota(STokenBucket* pBucket, int32_t bytes) {
  pBucket->quotaRemain -= SIZE_IN_MiB(bytes);
}