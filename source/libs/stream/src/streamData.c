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

SStreamDataBlock* createStreamBlockFromDispatchMsg(const SStreamDispatchReq* pReq, int32_t blockType, int32_t srcVg) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, pReq->totalLen);
  if (pData == NULL) {
    return NULL;
  }

  pData->type = blockType;
  pData->srcVgId = srcVg;
  pData->srcTaskId = pReq->upstreamTaskId;

  int32_t blockNum = pReq->blockNum;
  SArray* pArray = taosArrayInit_s(sizeof(SSDataBlock), blockNum);
  if (pArray == NULL) {
    taosFreeQitem(pData);
    return NULL;
  }

  ASSERT((pReq->blockNum == taosArrayGetSize(pReq->data)) && (pReq->blockNum == taosArrayGetSize(pReq->dataLen)));

  for (int32_t i = 0; i < blockNum; i++) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*) taosArrayGetP(pReq->data, i);
    SSDataBlock*       pDataBlock = taosArrayGet(pArray, i);
    blockDecode(pDataBlock, pRetrieve->data);

    // TODO: refactor
    pDataBlock->info.window.skey = be64toh(pRetrieve->skey);
    pDataBlock->info.window.ekey = be64toh(pRetrieve->ekey);
    pDataBlock->info.version = be64toh(pRetrieve->version);
    pDataBlock->info.watermark = be64toh(pRetrieve->watermark);
    memcpy(pDataBlock->info.parTbName, pRetrieve->parTbName, TSDB_TABLE_NAME_LEN);

    pDataBlock->info.type = pRetrieve->streamBlockType;
    pDataBlock->info.childId = pReq->upstreamChildId;
  }

  pData->blocks = pArray;
  return pData;
}

SStreamDataBlock* createStreamBlockFromResults(SStreamQueueItem* pItem, SStreamTask* pTask, int64_t resultSize, SArray* pRes) {
  SStreamDataBlock* pStreamBlocks = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, resultSize);
  if (pStreamBlocks == NULL) {
    taosArrayClearEx(pRes, (FDelete)blockDataFreeRes);
    return NULL;
  }

  pStreamBlocks->srcTaskId = pTask->id.taskId;
  pStreamBlocks->type = STREAM_INPUT__DATA_BLOCK;
  pStreamBlocks->blocks = pRes;

  if (pItem == NULL) {
    return pStreamBlocks;
  }

  if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)pItem;
    pStreamBlocks->sourceVer = pSubmit->ver;
  } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)pItem;
    pStreamBlocks->sourceVer = pMerged->ver;
  }

  return pStreamBlocks;
}

void destroyStreamDataBlock(SStreamDataBlock* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
  taosFreeQitem(pBlock);
}

int32_t streamRetrieveReqToData(const SStreamRetrieveReq* pReq, SStreamDataBlock* pData) {
  SArray* pArray = taosArrayInit(1, sizeof(SSDataBlock));
  if (pArray == NULL) {
    return -1;
  }

  taosArrayPush(pArray, &(SSDataBlock){0});
  SRetrieveTableRsp* pRetrieve = pReq->pRetrieve;
  SSDataBlock*       pDataBlock = taosArrayGet(pArray, 0);
  blockDecode(pDataBlock, pRetrieve->data);

  // TODO: refactor
  pDataBlock->info.window.skey = be64toh(pRetrieve->skey);
  pDataBlock->info.window.ekey = be64toh(pRetrieve->ekey);
  pDataBlock->info.version = be64toh(pRetrieve->version);

  pDataBlock->info.type = pRetrieve->streamBlockType;

  pData->reqId = pReq->reqId;
  pData->blocks = pArray;

  return 0;
}

SStreamDataSubmit* streamDataSubmitNew(SPackedData* pData, int32_t type) {
  SStreamDataSubmit* pDataSubmit = (SStreamDataSubmit*)taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM, pData->msgLen);
  if (pDataSubmit == NULL) {
    return NULL;
  }

  pDataSubmit->ver = pData->ver;
  pDataSubmit->submit = *pData;
  pDataSubmit->type = type;

  return pDataSubmit;
}

void streamDataSubmitDestroy(SStreamDataSubmit* pDataSubmit) {
  ASSERT(pDataSubmit->type == STREAM_INPUT__DATA_SUBMIT);
  taosMemoryFree(pDataSubmit->submit.msgStr);
  taosFreeQitem(pDataSubmit);
}

SStreamMergedSubmit* streamMergedSubmitNew() {
  SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)taosAllocateQitem(sizeof(SStreamMergedSubmit), DEF_QITEM, 0);
  if (pMerged == NULL) {
    return NULL;
  }

  pMerged->submits = taosArrayInit(0, sizeof(SPackedData));
  if (pMerged->submits == NULL) {
    taosArrayDestroy(pMerged->submits);
    taosFreeQitem(pMerged);
    return NULL;
  }

  pMerged->type = STREAM_INPUT__MERGED_SUBMIT;
  return pMerged;
}

int32_t streamMergeSubmit(SStreamMergedSubmit* pMerged, SStreamDataSubmit* pSubmit) {
  taosArrayPush(pMerged->submits, &pSubmit->submit);
  if (pSubmit->ver > pMerged->ver) {
    pMerged->ver = pSubmit->ver;
  }
  return 0;
}

// todo handle memory error
SStreamQueueItem* streamQueueMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem) {
  terrno = 0;
  
  if (dst->type == STREAM_INPUT__DATA_BLOCK && pElem->type == STREAM_INPUT__DATA_BLOCK) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)dst;
    SStreamDataBlock* pBlockSrc = (SStreamDataBlock*)pElem;
    taosArrayAddAll(pBlock->blocks, pBlockSrc->blocks);
    taosArrayDestroy(pBlockSrc->blocks);
    streamQueueItemIncSize(dst, streamQueueItemGetSize(pElem));

    taosFreeQitem(pElem);
    return dst;
  } else if (dst->type == STREAM_INPUT__MERGED_SUBMIT && pElem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)dst;
    SStreamDataSubmit*   pBlockSrc = (SStreamDataSubmit*)pElem;
    streamMergeSubmit(pMerged, pBlockSrc);
    streamQueueItemIncSize(dst, streamQueueItemGetSize(pElem));

    taosFreeQitem(pElem);
    return dst;
  } else if (dst->type == STREAM_INPUT__DATA_SUBMIT && pElem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = streamMergedSubmitNew();
    if (pMerged == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

    streamQueueItemIncSize((SStreamQueueItem*)pMerged, streamQueueItemGetSize(pElem));

    streamMergeSubmit(pMerged, (SStreamDataSubmit*)dst);
    streamMergeSubmit(pMerged, (SStreamDataSubmit*)pElem);

    taosFreeQitem(dst);
    taosFreeQitem(pElem);
    return (SStreamQueueItem*)pMerged;
  } else {
    stDebug("block type:%s not merged with existed blocks list, type:%d", streamQueueItemGetTypeStr(pElem->type), dst->type);
    return NULL;
  }
}

static void freeItems(void* param) {
  SSDataBlock* pBlock = param;
  taosArrayDestroy(pBlock->pDataBlock);
}

void streamFreeQitem(SStreamQueueItem* data) {
  int8_t type = data->type;
  if (type == STREAM_INPUT__GET_RES) {
    blockDataDestroy(((SStreamTrigger*)data)->pBlock);
    taosFreeQitem(data);
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE) {
    destroyStreamDataBlock((SStreamDataBlock*)data);
  } else if (type == STREAM_INPUT__DATA_SUBMIT) {
    streamDataSubmitDestroy((SStreamDataSubmit*)data);
  } else if (type == STREAM_INPUT__MERGED_SUBMIT) {
    SStreamMergedSubmit* pMerge = (SStreamMergedSubmit*)data;

    int32_t sz = taosArrayGetSize(pMerge->submits);
    for (int32_t i = 0; i < sz; i++) {
      SPackedData* pSubmit = (SPackedData*)taosArrayGet(pMerge->submits, i);
      taosMemoryFree(pSubmit->msgStr);
    }
    taosArrayDestroy(pMerge->submits);
    taosFreeQitem(pMerge);
  } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {
    SStreamRefDataBlock* pRefBlock = (SStreamRefDataBlock*)data;
    blockDataDestroy(pRefBlock->pBlock);
    taosFreeQitem(pRefBlock);
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER || type == STREAM_INPUT__TRANS_STATE) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*) data;
    taosArrayDestroyEx(pBlock->blocks, freeItems);
    taosFreeQitem(pBlock);
  }
}
