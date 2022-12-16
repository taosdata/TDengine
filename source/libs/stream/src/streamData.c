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

int32_t streamDispatchReqToData(const SStreamDispatchReq* pReq, SStreamDataBlock* pData) {
  int32_t blockNum = pReq->blockNum;
  SArray* pArray = taosArrayInit(blockNum, sizeof(SSDataBlock));
  if (pArray == NULL) {
    return -1;
  }
  taosArraySetSize(pArray, blockNum);

  ASSERT(pReq->blockNum == taosArrayGetSize(pReq->data));
  ASSERT(pReq->blockNum == taosArrayGetSize(pReq->dataLen));

  for (int32_t i = 0; i < blockNum; i++) {
    SRetrieveTableRsp* pRetrieve = taosArrayGetP(pReq->data, i);
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
  return 0;
}

int32_t streamRetrieveReqToData(const SStreamRetrieveReq* pReq, SStreamDataBlock* pData) {
  SArray* pArray = taosArrayInit(1, sizeof(SSDataBlock));
  if (pArray == NULL) {
    return -1;
  }
  taosArraySetSize(pArray, 1);
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

SStreamDataSubmit* streamDataSubmitNew(SSubmitReq* pReq) {
  SStreamDataSubmit* pDataSubmit = (SStreamDataSubmit*)taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM, 0);
  if (pDataSubmit == NULL) return NULL;
  pDataSubmit->dataRef = (int32_t*)taosMemoryMalloc(sizeof(int32_t));
  if (pDataSubmit->dataRef == NULL) goto FAIL;
  pDataSubmit->data = pReq;
  *pDataSubmit->dataRef = 1;
  pDataSubmit->type = STREAM_INPUT__DATA_SUBMIT;
  return pDataSubmit;
FAIL:
  taosFreeQitem(pDataSubmit);
  return NULL;
}

SStreamMergedSubmit* streamMergedSubmitNew() {
  SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)taosAllocateQitem(sizeof(SStreamMergedSubmit), DEF_QITEM, 0);
  if (pMerged == NULL) return NULL;
  pMerged->reqs = taosArrayInit(0, sizeof(void*));
  pMerged->dataRefs = taosArrayInit(0, sizeof(void*));
  if (pMerged->dataRefs == NULL || pMerged->reqs == NULL) goto FAIL;
  pMerged->type = STREAM_INPUT__MERGED_SUBMIT;
  return pMerged;
FAIL:
  if (pMerged->reqs) taosArrayDestroy(pMerged->reqs);
  if (pMerged->dataRefs) taosArrayDestroy(pMerged->dataRefs);
  taosFreeQitem(pMerged);
  return NULL;
}

int32_t streamMergeSubmit(SStreamMergedSubmit* pMerged, SStreamDataSubmit* pSubmit) {
  taosArrayPush(pMerged->dataRefs, &pSubmit->dataRef);
  taosArrayPush(pMerged->reqs, &pSubmit->data);
  pMerged->ver = pSubmit->ver;
  return 0;
}

static FORCE_INLINE void streamDataSubmitRefInc(SStreamDataSubmit* pDataSubmit) {
  atomic_add_fetch_32(pDataSubmit->dataRef, 1);
}

SStreamDataSubmit* streamSubmitRefClone(SStreamDataSubmit* pSubmit) {
  SStreamDataSubmit* pSubmitClone = taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM, 0);
  if (pSubmitClone == NULL) {
    return NULL;
  }
  streamDataSubmitRefInc(pSubmit);
  memcpy(pSubmitClone, pSubmit, sizeof(SStreamDataSubmit));
  return pSubmitClone;
}

void streamDataSubmitRefDec(SStreamDataSubmit* pDataSubmit) {
  int32_t ref = atomic_sub_fetch_32(pDataSubmit->dataRef, 1);
  ASSERT(ref >= 0);
  if (ref == 0) {
    taosMemoryFree(pDataSubmit->data);
    taosMemoryFree(pDataSubmit->dataRef);
  }
}

SStreamQueueItem* streamMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* elem) {
  ASSERT(elem);
  if (dst->type == STREAM_INPUT__DATA_BLOCK && elem->type == STREAM_INPUT__DATA_BLOCK) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)dst;
    SStreamDataBlock* pBlockSrc = (SStreamDataBlock*)elem;
    taosArrayAddAll(pBlock->blocks, pBlockSrc->blocks);
    taosArrayDestroy(pBlockSrc->blocks);
    taosFreeQitem(elem);
    return dst;
  } else if (dst->type == STREAM_INPUT__MERGED_SUBMIT && elem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)dst;
    SStreamDataSubmit*   pBlockSrc = (SStreamDataSubmit*)elem;
    streamMergeSubmit(pMerged, pBlockSrc);
    taosFreeQitem(elem);
    return dst;
  } else if (dst->type == STREAM_INPUT__DATA_SUBMIT && elem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = streamMergedSubmitNew();
    ASSERT(pMerged);
    streamMergeSubmit(pMerged, (SStreamDataSubmit*)dst);
    streamMergeSubmit(pMerged, (SStreamDataSubmit*)elem);
    taosFreeQitem(dst);
    taosFreeQitem(elem);
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
    streamDataSubmitRefDec((SStreamDataSubmit*)data);
    taosFreeQitem(data);
  } else if (type == STREAM_INPUT__MERGED_SUBMIT) {
    SStreamMergedSubmit* pMerge = (SStreamMergedSubmit*)data;
    int32_t              sz = taosArrayGetSize(pMerge->reqs);
    for (int32_t i = 0; i < sz; i++) {
      int32_t* pRef = taosArrayGetP(pMerge->dataRefs, i);
      int32_t  ref = atomic_sub_fetch_32(pRef, 1);
      ASSERT(ref >= 0);
      if (ref == 0) {
        void* dataStr = taosArrayGetP(pMerge->reqs, i);
        taosMemoryFree(dataStr);
        taosMemoryFree(pRef);
      }
    }
    taosArrayDestroy(pMerge->reqs);
    taosArrayDestroy(pMerge->dataRefs);
    taosFreeQitem(pMerge);
  } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {
    SStreamRefDataBlock* pRefBlock = (SStreamRefDataBlock*)data;

    int32_t ref = atomic_sub_fetch_32(pRefBlock->dataRef, 1);
    ASSERT(ref >= 0);
    if (ref == 0) {
      blockDataDestroy(pRefBlock->pBlock);
      taosMemoryFree(pRefBlock->dataRef);
    }
    taosFreeQitem(pRefBlock);
  }
}
