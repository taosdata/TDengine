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

#if 0
int32_t streamDataBlockEncode(void** buf, const SStreamDataBlock* pOutput) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pOutput->type);
  tlen += taosEncodeFixedI32(buf, pOutput->sourceVg);
  tlen += taosEncodeFixedI64(buf, pOutput->sourceVer);
  ASSERT(pOutput->type == STREAM_INPUT__DATA_BLOCK);
  tlen += tEncodeDataBlocks(buf, pOutput->blocks);
  return tlen;
}

void* streamDataBlockDecode(const void* buf, SStreamDataBlock* pInput) {
  buf = taosDecodeFixedI8(buf, &pInput->type);
  buf = taosDecodeFixedI32(buf, &pInput->sourceVg);
  buf = taosDecodeFixedI64(buf, &pInput->sourceVer);
  ASSERT(pInput->type == STREAM_INPUT__DATA_BLOCK);
  buf = tDecodeDataBlocks(buf, &pInput->blocks);
  return (void*)buf;
}
#endif

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
    int32_t            len = *(int32_t*)taosArrayGet(pReq->dataLen, i);
    SRetrieveTableRsp* pRetrieve = taosArrayGetP(pReq->data, i);
    SSDataBlock*       pDataBlock = taosArrayGet(pArray, i);
    blockCompressDecode(pDataBlock, htonl(pRetrieve->numOfCols), htonl(pRetrieve->numOfRows), pRetrieve->data);
    // TODO: refactor
    pDataBlock->info.childId = pReq->sourceChildId;
  }
  pData->blocks = pArray;
  return 0;
}

SStreamDataSubmit* streamDataSubmitNew(SSubmitReq* pReq) {
  SStreamDataSubmit* pDataSubmit = (SStreamDataSubmit*)taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM);
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

static FORCE_INLINE void streamDataSubmitRefInc(SStreamDataSubmit* pDataSubmit) {
  atomic_add_fetch_32(pDataSubmit->dataRef, 1);
}

SStreamDataSubmit* streamSubmitRefClone(SStreamDataSubmit* pSubmit) {
  SStreamDataSubmit* pSubmitClone = taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM);
  if (pSubmitClone == NULL) {
    return NULL;
  }
  streamDataSubmitRefInc(pSubmit);
  memcpy(pSubmitClone, pSubmit, sizeof(SStreamDataSubmit));
  return pSubmitClone;
}
