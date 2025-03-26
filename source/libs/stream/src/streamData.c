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
#include "ttime.h"

static int32_t streamMergedSubmitNew(SStreamMergedSubmit** pSubmit) {
  *pSubmit = NULL;

  int32_t code = taosAllocateQitem(sizeof(SStreamMergedSubmit), DEF_QITEM, 0, (void**)pSubmit);
  if (code) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pSubmit)->submits = taosArrayInit(0, sizeof(SPackedData));
  if ((*pSubmit)->submits == NULL) {
    taosFreeQitem(*pSubmit);
    *pSubmit = NULL;
    return terrno;
  }

  (*pSubmit)->type = STREAM_INPUT__MERGED_SUBMIT;
  return TSDB_CODE_SUCCESS;
}

static int32_t streamMergeSubmit(SStreamMergedSubmit* pMerged, SStreamDataSubmit* pSubmit) {
  void* p = taosArrayPush(pMerged->submits, &pSubmit->submit);
  if (p == NULL) {
    return terrno;
  }

  if (pSubmit->ver > pMerged->ver) {
    pMerged->ver = pSubmit->ver;
  }
  return 0;
}

static void freeItems(void* param) {
  SSDataBlock* pBlock = param;
  taosArrayDestroy(pBlock->pDataBlock);
}

int32_t createStreamBlockFromDispatchMsg(const SStreamDispatchReq* pReq, int32_t blockType, int32_t srcVg, SStreamDataBlock** pRes) {
  SStreamDataBlock* pData = NULL;
  int32_t code = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, pReq->totalLen, (void**)&pData);
  if (code) {
    return terrno = code;
  }

  pData->type = blockType;
  pData->srcVgId = srcVg;
  pData->srcTaskId = pReq->upstreamTaskId;

  int32_t blockNum = pReq->blockNum;
  SArray* pArray = taosArrayInit_s(sizeof(SSDataBlock), blockNum);
  if (pArray == NULL) {
    taosFreeQitem(pData);
    return code;
  }

  for (int32_t i = 0; i < blockNum; i++) {
    SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)taosArrayGetP(pReq->data, i);
    SSDataBlock* pDataBlock = taosArrayGet(pArray, i);
    if (pDataBlock == NULL || pRetrieve == NULL) {
      return terrno;
    }

    int32_t compLen = *(int32_t*)pRetrieve->data;
    int32_t fullLen = *(int32_t*)(pRetrieve->data + sizeof(int32_t));

    char* pInput = pRetrieve->data + PAYLOAD_PREFIX_LEN;
    if (pRetrieve->compressed && compLen < fullLen) {
      char* p = taosMemoryMalloc(fullLen);
      if (p == NULL) {
        return terrno;
      }

      int32_t len = tsDecompressString(pInput, compLen, 1, p, fullLen, ONE_STAGE_COMP, NULL, 0);
      pInput = p;
    }

    const char* pDummy = NULL;
    code = blockDecode(pDataBlock, pInput, &pDummy);
    if (code) {
      return code;
    }

    if (pRetrieve->compressed && compLen < fullLen) {
      taosMemoryFree(pInput);
    }

    // TODO: refactor
    pDataBlock->info.window.skey = be64toh(pRetrieve->skey);
    pDataBlock->info.window.ekey = be64toh(pRetrieve->ekey);
    pDataBlock->info.version = be64toh(pRetrieve->version);
    pDataBlock->info.watermark = be64toh(pRetrieve->watermark);
    memcpy(pDataBlock->info.parTbName, pRetrieve->parTbName, TSDB_TABLE_NAME_LEN);

    pDataBlock->info.type = pRetrieve->streamBlockType;
    pDataBlock->info.childId = pReq->upstreamChildId;
    pDataBlock->info.id.uid = be64toh(pRetrieve->useconds);
  }

  pData->blocks = pArray;
  *pRes = pData;

  return code;
}

int32_t createStreamBlockFromResults(SStreamQueueItem* pItem, SStreamTask* pTask, int64_t resultSize, SArray* pRes,
                                     SStreamDataBlock** pBlock) {
  int32_t code = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, resultSize, (void**)pBlock);
  if (code) {
    taosArrayClearEx(pRes, (FDelete)blockDataFreeRes);
    return terrno = code;
  }

  (*pBlock)->srcTaskId = pTask->id.taskId;
  (*pBlock)->type = STREAM_INPUT__DATA_BLOCK;
  (*pBlock)->blocks = pRes;

  if (pItem == NULL) {
    return code;
  }

  if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)pItem;
    (*pBlock)->sourceVer = pSubmit->ver;
  } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)pItem;
    (*pBlock)->sourceVer = pMerged->ver;
  }

  return code;
}

void destroyStreamDataBlock(SStreamDataBlock* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  taosArrayDestroyEx(pBlock->blocks, (FDelete)blockDataFreeRes);
  taosFreeQitem(pBlock);
}

int32_t streamRetrieveReqToData(const SStreamRetrieveReq* pReq, SStreamDataBlock* pData, const char* id) {
  const char*        pDummy = NULL;
  SRetrieveTableRsp* pRetrieve = pReq->pRetrieve;
  SArray*            pArray = taosArrayInit(1, sizeof(SSDataBlock));
  if (pArray == NULL) {
    stError("failed to prepare retrieve block, %s", id);
    return terrno;
  }

  void* px = taosArrayPush(pArray, &(SSDataBlock){0});
  if (px == NULL) {
    taosArrayDestroy(pArray);
    return terrno;
  }

  SSDataBlock* pDataBlock = taosArrayGet(pArray, 0);
  if (pDataBlock == NULL) {
    taosArrayDestroy(pArray);
    return terrno;
  }

  int32_t code = blockDecode(pDataBlock, pRetrieve->data + PAYLOAD_PREFIX_LEN, &pDummy);
  if (code) {
    taosArrayDestroy(pArray);
    return code;
  }

  // TODO: refactor
  pDataBlock->info.window.skey = be64toh(pRetrieve->skey);
  pDataBlock->info.window.ekey = be64toh(pRetrieve->ekey);
  pDataBlock->info.version = be64toh(pRetrieve->version);

  pDataBlock->info.type = pRetrieve->streamBlockType;

  pData->reqId = pReq->reqId;
  pData->blocks = pArray;

  return code;
}

int32_t streamDataSubmitNew(SPackedData* pData, int32_t type, SStreamDataSubmit** pSubmit) {
  SStreamDataSubmit* pDataSubmit = NULL;
  int32_t code = taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM, pData->msgLen, (void**)&pDataSubmit);
  if (code) {
    return code;
  }

  pDataSubmit->ver = pData->ver;
  pDataSubmit->submit = *pData;
  pDataSubmit->type = type;

  *pSubmit = pDataSubmit;
  return TSDB_CODE_SUCCESS;
}

void streamDataSubmitDestroy(SStreamDataSubmit* pDataSubmit) {
  if (pDataSubmit != NULL && pDataSubmit->type == STREAM_INPUT__DATA_SUBMIT) {
    taosMemoryFree(pDataSubmit->submit.msgStr);
    taosFreeQitem(pDataSubmit);
  }
}

// todo handle memory error
int32_t streamQueueMergeQueueItem(SStreamQueueItem* dst, SStreamQueueItem* pElem, SStreamQueueItem** pRes) {
  *pRes = NULL;
  int32_t code = 0;

  if (dst->type == STREAM_INPUT__DATA_BLOCK && pElem->type == STREAM_INPUT__DATA_BLOCK) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)dst;
    SStreamDataBlock* pBlockSrc = (SStreamDataBlock*)pElem;
    void* px = taosArrayAddAll(pBlock->blocks, pBlockSrc->blocks);
    if (px == NULL) {
      return terrno;
    }

    taosArrayDestroy(pBlockSrc->blocks);
    streamQueueItemIncSize(dst, streamQueueItemGetSize(pElem));

    taosFreeQitem(pElem);
    *pRes = dst;
    return code;
  } else if (dst->type == STREAM_INPUT__MERGED_SUBMIT && pElem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)dst;
    SStreamDataSubmit*   pBlockSrc = (SStreamDataSubmit*)pElem;

    code = streamMergeSubmit(pMerged, pBlockSrc);
    streamQueueItemIncSize(dst, streamQueueItemGetSize(pElem));

    taosFreeQitem(pElem);
    *pRes = dst;
    *pRes = dst;
    return code;
  } else if (dst->type == STREAM_INPUT__DATA_SUBMIT && pElem->type == STREAM_INPUT__DATA_SUBMIT) {
    SStreamMergedSubmit* pMerged = NULL;
    code = streamMergedSubmitNew(&pMerged);
    if (code != 0) {
      return code;
    }

    streamQueueItemIncSize((SStreamQueueItem*)pMerged, streamQueueItemGetSize(pElem));

    code = streamMergeSubmit(pMerged, (SStreamDataSubmit*)dst);
    if (code == 0) {
      code = streamMergeSubmit(pMerged, (SStreamDataSubmit*)pElem);
    }

    taosFreeQitem(dst);
    taosFreeQitem(pElem);

    *pRes = (SStreamQueueItem*)pMerged;
    return code;
  } else {
    code = TSDB_CODE_FAILED;
    stDebug("block type:%s not merged with existed blocks list, type:%d", streamQueueItemGetTypeStr(pElem->type),
            dst->type);
    return code;
  }
}

void streamFreeQitem(SStreamQueueItem* data) {
  if (data == NULL) {
    return;
  }

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
      if (pSubmit == NULL) {
        continue;
      }
      taosMemoryFree(pSubmit->msgStr);
    }

    taosArrayDestroy(pMerge->submits);
    taosFreeQitem(pMerge);
  } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {
    SStreamRefDataBlock* pRefBlock = (SStreamRefDataBlock*)data;
    blockDataDestroy(pRefBlock->pBlock);
    taosFreeQitem(pRefBlock);
  } else if (type == STREAM_INPUT__CHECKPOINT || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
             type == STREAM_INPUT__TRANS_STATE || type == STREAM_INPUT__RECALCULATE) {
    SStreamDataBlock* pBlock = (SStreamDataBlock*)data;
    taosArrayDestroyEx(pBlock->blocks, freeItems);
    taosFreeQitem(pBlock);
  }
}

int32_t streamCreateForcewindowTrigger(SStreamTrigger** pTrigger, int32_t interval, SInterval* pInterval,
                                       STimeWindow* pLatestWindow, const char* id) {
  QRY_PARAM_CHECK(pTrigger);

  SStreamTrigger* p = NULL;
  int64_t         ts = taosGetTimestamp(pInterval->precision);
  int64_t         skey = pLatestWindow->skey + pInterval->sliding;

  int32_t code = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM, 0, (void**)&p);
  if (code) {
    stError("s-task:%s failed to create force_window trigger, code:%s", id, tstrerror(code));
    return code;
  }

  p->type = STREAM_INPUT__GET_RES;
  p->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (p->pBlock == NULL) {
    taosFreeQitem(p);
    return terrno;
  }

  p->pBlock->info.window.skey = skey;
  p->pBlock->info.window.ekey = TMAX(ts, skey + pInterval->interval);
  p->pBlock->info.type = STREAM_GET_RESULT;

  stDebug("s-task:%s force_window_close trigger block generated, window range:%" PRId64 "-%" PRId64, id,
          p->pBlock->info.window.skey, p->pBlock->info.window.ekey);

  *pTrigger = p;
  return code;
}

int32_t streamCreateTriggerBlock(SStreamTrigger** pTrigger, int32_t type, int32_t blockType) {
  QRY_PARAM_CHECK(pTrigger);

  SStreamTrigger* p = NULL;
  int32_t         code = taosAllocateQitem(sizeof(SStreamTrigger), DEF_QITEM, 0, (void**)&p);
  if (code) {
    return code;
  }

  p->type = (int8_t) type;
  p->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (p->pBlock == NULL) {
    taosFreeQitem(p);
    return terrno;
  }

  p->pBlock->info.type = blockType;
  *pTrigger = p;
  return code;
}

int32_t streamCreateRecalculateBlock(SStreamTask* pTask, SStreamDataBlock** pBlock, int32_t type) {
  int32_t           code = 0;
  SSDataBlock*      p = NULL;
  SStreamDataBlock* pRecalc = NULL;

  if (pBlock != NULL) {
    *pBlock = NULL;
  }

  code = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock), (void**)&pRecalc);
  if (code) {
    return code;
  }

  p = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (p == NULL) {
    code = terrno;
    goto _err;
  }

  pRecalc->type = STREAM_INPUT__RECALCULATE;

  p->info.type = type;
  p->info.rows = 1;
  p->info.childId = pTask->info.selfChildId;

  pRecalc->blocks = taosArrayInit(4, sizeof(SSDataBlock));  // pBlock;
  if (pRecalc->blocks == NULL) {
    code = terrno;
    goto _err;
  }

  void* px = taosArrayPush(pRecalc->blocks, p);
  if (px == NULL) {
    code = terrno;
    goto _err;
  }

  taosMemoryFree(p);
  *pBlock = pRecalc;

  return code;

_err:
  taosMemoryFree(p);
  taosFreeQitem(pRecalc);
  return code;
}
