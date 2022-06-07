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

int32_t streamSink1(SStreamTask* pTask, SMsgCb* pMsgCb) {
  SStreamQueue* queue;
  if (pTask->execType == TASK_EXEC__NONE) {
    queue = pTask->inputQueue;
  } else {
    queue = pTask->outputQueue;
  }

  /*if (streamDequeueBegin(queue) == true) {*/
  /*return -1;*/
  /*}*/

  if (pTask->sinkType == TASK_SINK__TABLE || pTask->sinkType == TASK_SINK__SMA ||
      pTask->dispatchType != TASK_DISPATCH__NONE) {
    while (1) {
      SStreamDataBlock* pBlock = streamQueueNextItem(queue);
      if (pBlock == NULL) break;
      ASSERT(pBlock->type == STREAM_DATA_TYPE_SSDATA_BLOCK);

      // local sink
      if (pTask->sinkType == TASK_SINK__TABLE) {
        ASSERT(pTask->dispatchType == TASK_DISPATCH__NONE);
        pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pBlock->blocks);
      } else if (pTask->sinkType == TASK_SINK__SMA) {
        ASSERT(pTask->dispatchType == TASK_DISPATCH__NONE);
        pTask->smaSink.smaSink(pTask->ahandle, pTask->smaSink.smaId, pBlock->blocks);
      }

      // TODO: sink and dispatch should be only one
      if (pTask->dispatchType != TASK_DISPATCH__NONE) {
        ASSERT(queue == pTask->outputQueue);
        ASSERT(pTask->sinkType == TASK_SINK__NONE);

        streamDispatch(pTask, pMsgCb, pBlock);
      }

      streamQueueProcessSuccess(queue);
    }
  }

  return 0;
}

#if 0
int32_t streamSink(SStreamTask* pTask, SMsgCb* pMsgCb) {
  bool firstRun = 1;
  while (1) {
    SStreamDataBlock* pBlock = NULL;
    if (!firstRun) {
      taosReadAllQitems(pTask->outputQ, pTask->outputQAll);
    }
    taosGetQitem(pTask->outputQAll, (void**)&pBlock);
    if (pBlock == NULL) {
      if (firstRun) {
        firstRun = 0;
        continue;
      } else {
        break;
      }
    }

    SArray* pRes = pBlock->blocks;

    // sink
    if (pTask->sinkType == TASK_SINK__TABLE) {
      // blockDebugShowData(pRes);
      pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, 0, pRes);
    } else if (pTask->sinkType == TASK_SINK__SMA) {
      pTask->smaSink.smaSink(pTask->ahandle, pTask->smaSink.smaId, pRes);
      //
    } else if (pTask->sinkType == TASK_SINK__FETCH) {
      //
    } else {
      ASSERT(pTask->sinkType == TASK_SINK__NONE);
    }

    // dispatch
    // TODO dispatch guard
    int8_t outputStatus = atomic_load_8(&pTask->outputStatus);
    if (outputStatus == TASK_OUTPUT_STATUS__NORMAL) {
      if (pTask->dispatchType == TASK_DISPATCH__INPLACE) {
        SRpcMsg dispatchMsg = {0};
        if (streamBuildExecMsg(pTask, pRes, &dispatchMsg, NULL) < 0) {
          ASSERT(0);
          return -1;
        }

        int32_t qType;
        if (pTask->dispatchMsgType == TDMT_VND_TASK_DISPATCH || pTask->dispatchMsgType == TDMT_SND_TASK_DISPATCH) {
          qType = FETCH_QUEUE;
          /*} else if (pTask->dispatchMsgType == TDMT_VND_TASK_MERGE_EXEC ||*/
          /*pTask->dispatchMsgType == TDMT_SND_TASK_MERGE_EXEC) {*/
          /*qType = MERGE_QUEUE;*/
          /*} else if (pTask->dispatchMsgType == TDMT_VND_TASK_WRITE_EXEC) {*/
          /*qType = WRITE_QUEUE;*/
        } else {
          ASSERT(0);
        }
        tmsgPutToQueue(pMsgCb, qType, &dispatchMsg);

      } else if (pTask->dispatchType == TASK_DISPATCH__FIXED) {
        SRpcMsg dispatchMsg = {0};
        SEpSet* pEpSet = NULL;
        if (streamBuildExecMsg(pTask, pRes, &dispatchMsg, &pEpSet) < 0) {
          ASSERT(0);
          return -1;
        }

        tmsgSendReq(pEpSet, &dispatchMsg);

      } else if (pTask->dispatchType == TASK_DISPATCH__SHUFFLE) {
        SHashObj* pShuffleRes = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
        if (pShuffleRes == NULL) {
          return -1;
        }

        int32_t sz = taosArrayGetSize(pRes);
        for (int32_t i = 0; i < sz; i++) {
          SSDataBlock* pDataBlock = taosArrayGet(pRes, i);
          SArray*      pArray = taosHashGet(pShuffleRes, &pDataBlock->info.groupId, sizeof(int64_t));
          if (pArray == NULL) {
            pArray = taosArrayInit(0, sizeof(SSDataBlock));
            if (pArray == NULL) {
              return -1;
            }
            taosHashPut(pShuffleRes, &pDataBlock->info.groupId, sizeof(int64_t), &pArray, sizeof(void*));
          }
          taosArrayPush(pArray, pDataBlock);
        }

        if (streamShuffleDispatch(pTask, pMsgCb, pShuffleRes) < 0) {
          return -1;
        }

      } else {
        ASSERT(pTask->dispatchType == TASK_DISPATCH__NONE);
      }
    }
  }
  return 0;
}
#endif
