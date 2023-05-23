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

SStreamDataBlock* createStreamDataFromDispatchMsg(const SStreamDispatchReq* pReq, int32_t blockType, int32_t srcVg) {
  SStreamDataBlock* pData = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, pReq->totalLen);
  if (pData == NULL) {
    return NULL;
  }

  pData->type = blockType;
  pData->srcVgId = srcVg;

  int32_t blockNum = pReq->blockNum;
  SArray* pArray = taosArrayInit_s(sizeof(SSDataBlock), blockNum);
  if (pArray == NULL) {
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

  pStreamBlocks->type = STREAM_INPUT__DATA_BLOCK;
  pStreamBlocks->blocks = pRes;

  if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)pItem;
    pStreamBlocks->childId = pTask->selfChildId;
    pStreamBlocks->sourceVer = pSubmit->ver;
  } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)pItem;
    pStreamBlocks->childId = pTask->selfChildId;
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

  pDataSubmit->dataRef = (int32_t*)taosMemoryMalloc(sizeof(int32_t));
  if (pDataSubmit->dataRef == NULL) {
    taosFreeQitem(pDataSubmit);
    return NULL;
  }

  pDataSubmit->submit = *pData;
  *pDataSubmit->dataRef = 1;   // initialize the reference count to be 1
  pDataSubmit->type = type;

  return pDataSubmit;
}

void streamDataSubmitDestroy(SStreamDataSubmit* pDataSubmit) {
  int32_t ref = atomic_sub_fetch_32(pDataSubmit->dataRef, 1);
  ASSERT(ref >= 0 && pDataSubmit->type == STREAM_INPUT__DATA_SUBMIT);

  if (ref == 0) {
    taosMemoryFree(pDataSubmit->submit.msgStr);
    taosMemoryFree(pDataSubmit->dataRef);
  }
}

SStreamMergedSubmit* streamMergedSubmitNew() {
  SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)taosAllocateQitem(sizeof(SStreamMergedSubmit), DEF_QITEM, 0);
  if (pMerged == NULL) {
    return NULL;
  }

  pMerged->submits = taosArrayInit(0, sizeof(SPackedData));
  pMerged->dataRefs = taosArrayInit(0, sizeof(void*));

  if (pMerged->dataRefs == NULL || pMerged->submits == NULL) {
    taosArrayDestroy(pMerged->submits);
    taosArrayDestroy(pMerged->dataRefs);
    taosFreeQitem(pMerged);
    return NULL;
  }

  pMerged->type = STREAM_INPUT__MERGED_SUBMIT;
  return pMerged;
}

int32_t streamMergeSubmit(SStreamMergedSubmit* pMerged, SStreamDataSubmit* pSubmit) {
  taosArrayPush(pMerged->dataRefs, &pSubmit->dataRef);
  taosArrayPush(pMerged->submits, &pSubmit->submit);
  pMerged->ver = pSubmit->ver;
  return 0;
}

static FORCE_INLINE void streamDataSubmitRefInc(SStreamDataSubmit* pDataSubmit) {
  atomic_add_fetch_32(pDataSubmit->dataRef, 1);
}

SStreamDataSubmit* streamSubmitBlockClone(SStreamDataSubmit* pSubmit) {
  int32_t len = 0;
  if (pSubmit->type == STREAM_INPUT__DATA_SUBMIT) {
    len = pSubmit->submit.msgLen;
  }

  SStreamDataSubmit* pSubmitClone = taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM, len);
  if (pSubmitClone == NULL) {
    return NULL;
  }

  streamDataSubmitRefInc(pSubmit);
  memcpy(pSubmitClone, pSubmit, sizeof(SStreamDataSubmit));
  return pSubmitClone;
}

SStreamQueueItem* streamMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem) {
  if (dst->type == STREAM_INPUT__DATA_BLOCK && pElem->type == STREAM_INPUT__DATA_BLOCK) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)dst;
    SStreamDataBlock* pBlockSrc = (SStreamDataBlock*)pElem;
    taosArrayAddAll(pBlock->blocks, pBlockSrc->blocks);
    taosArrayDestroy(pBlockSrc->blocks);
    taosFreeQitem(pElem);
    return dst;
  } else if (dst->type == STREAM_INPUT__MERGED_SUBMIT && pElem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)dst;
    SStreamDataSubmit*   pBlockSrc = (SStreamDataSubmit*)pElem;
    streamMergeSubmit(pMerged, pBlockSrc);
    taosFreeQitem(pElem);
    return dst;
  } else if (dst->type == STREAM_INPUT__DATA_SUBMIT && pElem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = streamMergedSubmitNew();
    // todo handle error

    streamMergeSubmit(pMerged, (SStreamDataSubmit*)dst);
    streamMergeSubmit(pMerged, (SStreamDataSubmit*)pElem);
    taosFreeQitem(dst);
    taosFreeQitem(pElem);
    return (SStreamQueueItem*)pMerged;
  } else {
    return NULL;
  }
}

void streamFreeQitem(SStreamQueueItem* data) {
  int8_t type = data->type;
  if (type == STREAM_INPUT__GET_RES) {
    blockDataDestroy(((SStreamTrigger*)data)->pBlock);
    taosFreeQitem(data);
  } else if (type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__DATA_RETRIEVE) {
    taosArrayDestroyEx(((SStreamDataBlock*)data)->blocks, (FDelete)blockDataFreeRes);
    taosFreeQitem(data);
  } else if (type == STREAM_INPUT__DATA_SUBMIT) {
    streamDataSubmitDestroy((SStreamDataSubmit*)data);
    taosFreeQitem(data);
  } else if (type == STREAM_INPUT__MERGED_SUBMIT) {
    SStreamMergedSubmit* pMerge = (SStreamMergedSubmit*)data;

    int32_t sz = taosArrayGetSize(pMerge->submits);
    for (int32_t i = 0; i < sz; i++) {
      int32_t* pRef = taosArrayGetP(pMerge->dataRefs, i);
      int32_t  ref = atomic_sub_fetch_32(pRef, 1);
      ASSERT(ref >= 0);

      if (ref == 0) {
        SPackedData* pSubmit = (SPackedData*)taosArrayGet(pMerge->submits, i);
        taosMemoryFree(pSubmit->msgStr);
        taosMemoryFree(pRef);
      }
    }
    taosArrayDestroy(pMerge->submits);
    taosArrayDestroy(pMerge->dataRefs);
    taosFreeQitem(pMerge);
  } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {
    SStreamRefDataBlock* pRefBlock = (SStreamRefDataBlock*)data;
    blockDataDestroy(pRefBlock->pBlock);
    taosFreeQitem(pRefBlock);
  }
}
