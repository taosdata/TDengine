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

#include "tstream.h"

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
  //
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

#if 0
int32_t tEncodeSStreamTaskExecReq(void** buf, const SStreamTaskExecReq* pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->streamId);
  tlen += taosEncodeFixedI32(buf, pReq->taskId);
  tlen += tEncodeDataBlocks(buf, pReq->data);
  return tlen;
}

void* tDecodeSStreamTaskExecReq(const void* buf, SStreamTaskExecReq* pReq) {
  buf = taosDecodeFixedI64(buf, &pReq->streamId);
  buf = taosDecodeFixedI32(buf, &pReq->taskId);
  buf = tDecodeDataBlocks(buf, &pReq->data);
  return (void*)buf;
}

void tFreeSStreamTaskExecReq(SStreamTaskExecReq* pReq) { taosArrayDestroy(pReq->data); }
#endif
