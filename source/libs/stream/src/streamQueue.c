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
#define MIN_STREAM_EXEC_BATCH_NUM                 4
#define STREAM_TASK_QUEUE_CAPACITY                20480
#define STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE  (30)
#define STREAM_TASK_OUTPUT_QUEUE_CAPACITY_IN_SIZE (50)

// todo refactor:
// read data from input queue
typedef struct SQueueReader {
  SStreamQueue* pQueue;
  int32_t       taskLevel;
  int32_t       maxBlocks;     // maximum block in one batch
  int32_t       waitDuration;  // maximum wait time to format several block into a batch to process, unit: ms
} SQueueReader;

static bool streamTaskHasAvailableToken(STokenBucket* pBucket);

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

bool streamQueueIsFull(const STaosQueue* pQueue, bool inputQ) {
  bool isFull = taosQueueItemSize((STaosQueue*)pQueue) >= STREAM_TASK_QUEUE_CAPACITY;
  if (isFull) {
    return true;
  }

  int32_t threahold = (inputQ) ? STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE : STREAM_TASK_OUTPUT_QUEUE_CAPACITY_IN_SIZE;
  double  size = SIZE_IN_MiB(taosQueueMemorySize((STaosQueue*)pQueue));
  return (size >= threahold);
}

int32_t streamQueueGetNumOfItems(const SStreamQueue* pQueue) {
  int32_t numOfItems1 = taosQueueItemSize(pQueue->pQueue);
  int32_t numOfItems2 = taosQallItemSize(pQueue->qall);

  return numOfItems1 + numOfItems2;
}

int32_t streamQueueGetNumOfItemsInQueue(const SStreamQueue* pQueue) {
  return taosQueueItemSize(pQueue->pQueue);
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

int32_t streamTaskGetDataFromInputQ(SStreamTask* pTask, SStreamQueueItem** pInput, int32_t* numOfBlocks) {
  int32_t     retryTimes = 0;
  int32_t     MAX_RETRY_TIMES = 5;
  const char* id = pTask->id.idStr;
  int32_t     taskLevel = pTask->info.taskLevel;
  *numOfBlocks = 0;

  //  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {  // extract block from inputQ, one-by-one
  //    while (1) {
  //      if (streamTaskShouldPause(&pTask->status) || streamTaskShouldStop(&pTask->status)) {
  //        stDebug("s-task:%s task should pause, extract input blocks:%d", pTask->id.idStr, *numOfBlocks);
  //        return TSDB_CODE_SUCCESS;
  //      }
  //
  //      STokenBucket* pBucket = pTask->pTokenBucket;
  //      //      if (!streamTaskHasAvailableToken(pBucket)) {  // no available token in th bucket, ignore this
  //      execution
  //      //        stInfo("s-task:%s no available token for sink, capacity:%d, rate:%d token/sec, quit",
  //      pTask->id.idStr,
  //      //               pBucket->capacity, pBucket->rate);
  //      //        return TSDB_CODE_SUCCESS;
  //      //      }
  //
  //      SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputInfo.queue);
  //      if (qItem == NULL) {
  //        if (++retryTimes < MAX_RETRY_TIMES) {
  //          taosMsleep(10);
  //          continue;
  //        }
  //
  //        return TSDB_CODE_SUCCESS;
  //      }
  //
  //      stDebug("s-task:%s sink task handle block, type:%s", id, streamQueueItemGetTypeStr(qItem->type));
  //      pTask->sinkRecorder.bytes += streamQueueItemGetSize(qItem);
  //
  //      *numOfBlocks = 1;
  //      *pInput = qItem;
  //      return TSDB_CODE_SUCCESS;
  //    }
  //  }

  while (1) {
    if (streamTaskShouldPause(&pTask->status) || streamTaskShouldStop(&pTask->status)) {
      stDebug("s-task:%s task should pause, extract input blocks:%d", pTask->id.idStr, *numOfBlocks);
      return TSDB_CODE_SUCCESS;
    }

    SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputInfo.queue);
    if (qItem == NULL) {
      if ((taskLevel == TASK_LEVEL__SOURCE  || taskLevel == TASK_LEVEL__SINK) && (++retryTimes) < MAX_RETRY_TIMES) {
        taosMsleep(10);
        continue;
      }
      return TSDB_CODE_SUCCESS;
    }

    // do not merge blocks for sink node and check point data block
    int8_t type = qItem->type;
    if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
        type == STREAM_INPUT__TRANS_STATE) {
      const char* p = streamQueueItemGetTypeStr(qItem->type);

      if (*pInput == NULL) {
        stDebug("s-task:%s %s msg extracted, start to process immediately", id, p);

        *numOfBlocks = 1;
        *pInput = qItem;
        return TSDB_CODE_SUCCESS;
      } else {
        // previous existed blocks needs to be handle, before handle the checkpoint msg block
        stDebug("s-task:%s %s msg extracted, handle previous blocks, numOfBlocks:%d", id, p, *numOfBlocks);
        streamQueueProcessFail(pTask->inputInfo.queue);
        return TSDB_CODE_SUCCESS;
      }
    } else {
      if (*pInput == NULL) {
        ASSERT((*numOfBlocks) == 0);
        *pInput = qItem;
      } else {
        // todo we need to sort the data block, instead of just appending into the array list.
        void* newRet = streamMergeQueueItem(*pInput, qItem);
        if (newRet == NULL) {
          if (terrno != 0) {
            stError("s-task:%s failed to merge blocks from inputQ, numOfBlocks:%d, code:%s", id, *numOfBlocks,
                   tstrerror(terrno));
          }

          streamQueueProcessFail(pTask->inputInfo.queue);
          return TSDB_CODE_SUCCESS;
        }

        *pInput = newRet;
      }

      *numOfBlocks += 1;
      streamQueueProcessSuccess(pTask->inputInfo.queue);

      if (*numOfBlocks >= MAX_STREAM_EXEC_BATCH_NUM) {
        stDebug("s-task:%s batch size limit:%d reached, start to process blocks", id, MAX_STREAM_EXEC_BATCH_NUM);
        return TSDB_CODE_SUCCESS;
      }
    }
  }
}

int32_t streamTaskPutDataIntoInputQ(SStreamTask* pTask, SStreamQueueItem* pItem) {
  int8_t      type = pItem->type;
  STaosQueue* pQueue = pTask->inputInfo.queue->pQueue;
  int32_t     total = streamQueueGetNumOfItems(pTask->inputInfo.queue) + 1;

  if (type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* px = (SStreamDataSubmit*)pItem;
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && streamQueueIsFull(pQueue, true)) {
      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
      stTrace(
          "s-task:%s inputQ is full, capacity(size:%d num:%dMiB), current(blocks:%d, size:%.2fMiB) stop to push data",
          pTask->id.idStr, STREAM_TASK_QUEUE_CAPACITY, STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE, total, size);
      streamDataSubmitDestroy(px);
      taosFreeQitem(pItem);
      return -1;
    }

    int32_t msgLen = px->submit.msgLen;
    int64_t ver = px->submit.ver;

    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      streamDataSubmitDestroy(px);
      taosFreeQitem(pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));

    // use the local variable to avoid the pItem be freed by other threads, since it has been put into queue already.
    stDebug("s-task:%s submit enqueue msgLen:%d ver:%" PRId64 ", total in queue:%d, size:%.2fMiB", pTask->id.idStr,
           msgLen, ver, total, size + SIZE_IN_MiB(msgLen));
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE ||
             type == STREAM_INPUT__REF_DATA_BLOCK) {
    if (streamQueueIsFull(pQueue, true)) {
      double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));

      stTrace("s-task:%s input queue is full, capacity:%d size:%d MiB, current(blocks:%d, size:%.2fMiB) abort",
             pTask->id.idStr, STREAM_TASK_QUEUE_CAPACITY, STREAM_TASK_INPUT_QUEUE_CAPACITY_IN_SIZE, total, size);
      destroyStreamDataBlock((SStreamDataBlock*)pItem);
      return -1;
    }

    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      destroyStreamDataBlock((SStreamDataBlock*)pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    stDebug("s-task:%s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr, total, size);
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
             type == STREAM_INPUT__TRANS_STATE) {
    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      taosFreeQitem(pItem);
      return code;
    }

    double size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    stDebug("s-task:%s level:%d %s blockdata enqueue, total in queue:%d, size:%.2fMiB", pTask->id.idStr,
           pTask->info.taskLevel, streamQueueItemGetTypeStr(type), total, size);
  } else if (type == STREAM_INPUT__GET_RES) {
    // use the default memory limit, refactor later.
    int32_t code = taosWriteQitem(pQueue, pItem);
    if (code != TSDB_CODE_SUCCESS) {
      taosFreeQitem(pItem);
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

// the result should be put into the outputQ in any cases, otherwise, the result may be lost
int32_t streamTaskPutDataIntoOutputQ(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  STaosQueue* pQueue = pTask->outputInfo.queue->pQueue;

  while (streamQueueIsFull(pQueue, false)) {
    if (streamTaskShouldStop(&pTask->status)) {
      stInfo("s-task:%s discard result block due to task stop", pTask->id.idStr);
      return TSDB_CODE_STREAM_EXEC_CANCELLED;
    }

    int32_t total = streamQueueGetNumOfItems(pTask->outputInfo.queue);
    double  size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
    // let's wait for there are enough space to hold this result pBlock
    stDebug("s-task:%s outputQ is full, wait for 500ms and retry, outputQ items:%d, size:%.2fMiB", pTask->id.idStr,
           total, size);
    taosMsleep(500);
  }

  int32_t code = taosWriteQitem(pQueue, pBlock);

  int32_t total = streamQueueGetNumOfItems(pTask->outputInfo.queue);
  double  size = SIZE_IN_MiB(taosQueueMemorySize(pQueue));
  if (code != 0) {
    stError("s-task:%s failed to put res into outputQ, outputQ items:%d, size:%.2fMiB code:%s, result lost",
           pTask->id.idStr, total + 1, size, tstrerror(code));
  } else {
    stDebug("s-task:%s data put into outputQ, outputQ items:%d, size:%.2fMiB", pTask->id.idStr, total, size);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskInitTokenBucket(STokenBucket* pBucket, int32_t cap, int32_t rate) {
  if (cap < 50 || rate < 50 || pBucket == NULL) {
    stError("failed to init sink task bucket, cap:%d, rate:%d", cap, rate);
    return TSDB_CODE_INVALID_PARA;
  }

  pBucket->capacity = cap;
  pBucket->rate = rate;
  pBucket->numOfToken = cap;
  pBucket->fillTimestamp = taosGetTimestampMs();
  return TSDB_CODE_SUCCESS;
}

static void fillBucket(STokenBucket* pBucket) {
  int64_t now = taosGetTimestampMs();
  int64_t delta = now - pBucket->fillTimestamp;
  ASSERT(pBucket->numOfToken >= 0);

  int32_t inc = (delta / 1000.0) * pBucket->rate;
  if (inc > 0) {
    if ((pBucket->numOfToken + inc) < pBucket->capacity) {
      pBucket->numOfToken += inc;
    } else {
      pBucket->numOfToken = pBucket->capacity;
    }

    pBucket->fillTimestamp = now;
    stDebug("new token available, current:%d, inc:%d ts:%"PRId64, pBucket->numOfToken, inc, now);
  }
}

bool streamTaskHasAvailableToken(STokenBucket* pBucket) {
  fillBucket(pBucket);
  if (pBucket->numOfToken > 0) {
    --pBucket->numOfToken;
    return true;
  } else {
    return false;
  }
}