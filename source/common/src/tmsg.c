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

#include "tmsg.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#define TD_MSG_INFO_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_INFO_
#define TD_MSG_DICT_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

int tInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  pIter->totalLen = pMsg->length;
  pIter->len = 0;
  pIter->pMsg = pMsg;
  if (pMsg->length <= sizeof(SSubmitMsg)) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  return 0;
}

int tGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock) {
  if (pIter->len == 0) {
    pIter->len += sizeof(SSubmitMsg);
  } else {
    SSubmitBlk *pSubmitBlk = (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);
    pIter->len += (sizeof(SSubmitBlk) + pSubmitBlk->dataLen + pSubmitBlk->schemaLen);
  }

  if (pIter->len > pIter->totalLen) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    *pPBlock = NULL;
    return -1;
  }

  *pPBlock = (pIter->len == pIter->totalLen) ? NULL : (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);

  return 0;
}

int tInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pBlock->dataLen <= 0) return -1;
  pIter->totalLen = pBlock->dataLen;
  pIter->len = 0;
  pIter->row = (SMemRow)(pBlock->data + pBlock->schemaLen);
  return 0;
}

SMemRow tGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  SMemRow row = pIter->row;

  if (pIter->len >= pIter->totalLen) {
    return NULL;
  } else {
    pIter->len += memRowTLen(row);
    if (pIter->len < pIter->totalLen) {
      pIter->row = POINTER_SHIFT(row, memRowTLen(row));
    }
    return row;
  }
}

int tSerializeSClientHbReq(void **buf, const SClientHbReq *pReq) {
  int tlen = 0;
  tlen += taosEncodeSClientHbKey(buf, &pReq->connKey);

  int32_t kvNum = taosHashGetSize(pReq->info);
  tlen += taosEncodeFixedI32(buf, kvNum);
  SKv *kv;
  void* pIter = taosHashIterate(pReq->info, NULL);
  while (pIter != NULL) {
    kv = pIter;
    tlen += taosEncodeSKv(buf, kv);

    pIter = taosHashIterate(pReq->info, pIter);
  }
  return tlen;
}

void *tDeserializeSClientHbReq(void *buf, SClientHbReq *pReq) {
  buf = taosDecodeSClientHbKey(buf, &pReq->connKey);

  // TODO: error handling
  int32_t kvNum;
  buf = taosDecodeFixedI32(buf, &kvNum);
  if (pReq->info == NULL) {
    pReq->info = taosHashInit(kvNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  for(int i = 0; i < kvNum; i++) {
    SKv kv;
    buf = taosDecodeSKv(buf, &kv);
    taosHashPut(pReq->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  }

  return buf;
}

int   tSerializeSClientHbRsp(void** buf, const SClientHbRsp* pRsp) {
  int tlen = 0;
  int32_t kvNum = taosArrayGetSize(pRsp->info);
  tlen += taosEncodeSClientHbKey(buf, &pRsp->connKey);
  tlen += taosEncodeFixedI32(buf, pRsp->status);
  tlen += taosEncodeFixedI32(buf, kvNum);
  for (int i = 0; i < kvNum; i++) {
    SKv *kv = (SKv *)taosArrayGet(pRsp->info, i);
    tlen += taosEncodeSKv(buf, kv);
  }
  return tlen;
}
void* tDeserializeSClientHbRsp(void* buf, SClientHbRsp* pRsp) {
  int32_t kvNum = 0;
  buf = taosDecodeSClientHbKey(buf, &pRsp->connKey);
  buf = taosDecodeFixedI32(buf, &pRsp->status);
  buf = taosDecodeFixedI32(buf, &kvNum);
  pRsp->info = taosArrayInit(kvNum, sizeof(SKv));
  for (int i = 0; i < kvNum; i++) {
    SKv kv = {0};
    buf = taosDecodeSKv(buf, &kv);
    taosArrayPush(pRsp->info, &kv);
  }
  
  return buf;
}

int   tSerializeSClientHbBatchReq(void** buf, const SClientHbBatchReq* pBatchReq) {
  int tlen = 0;
  tlen += taosEncodeFixedI64(buf, pBatchReq->reqId);
  int32_t reqNum = taosArrayGetSize(pBatchReq->reqs);
  tlen += taosEncodeFixedI32(buf, reqNum); 
  for (int i = 0; i < reqNum; i++) {
    SClientHbReq* pReq = taosArrayGet(pBatchReq->reqs, i);
    tlen += tSerializeSClientHbReq(buf, pReq);
  }
  return tlen;
}

void* tDeserializeSClientHbBatchReq(void* buf, SClientHbBatchReq* pBatchReq) {
  buf = taosDecodeFixedI64(buf, &pBatchReq->reqId);
  if (pBatchReq->reqs == NULL) {
    pBatchReq->reqs = taosArrayInit(0, sizeof(SClientHbReq));
  }
 
  int32_t reqNum;
  buf = taosDecodeFixedI32(buf, &reqNum);
  for (int i = 0; i < reqNum; i++) {
    SClientHbReq req = {0};
    buf = tDeserializeSClientHbReq(buf, &req);
    taosArrayPush(pBatchReq->reqs, &req);
  }
  return buf;
}

int   tSerializeSClientHbBatchRsp(void** buf, const SClientHbBatchRsp* pBatchRsp) {
  int tlen = 0;
  int32_t sz = taosArrayGetSize(pBatchRsp->rsps);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int i = 0; i < sz; i++) {
    SClientHbRsp* pRsp = taosArrayGet(pBatchRsp->rsps, i);
    tlen += tSerializeSClientHbRsp(buf, pRsp);
  }
  return tlen;
}

void* tDeserializeSClientHbBatchRsp(void* buf, SClientHbBatchRsp* pBatchRsp) {
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pBatchRsp->rsps = taosArrayInit(sz, sizeof(SClientHbRsp));
  for (int i = 0; i < sz; i++) {
    SClientHbRsp rsp = {0};
    buf = tDeserializeSClientHbRsp(buf, &rsp); 
    taosArrayPush(pBatchRsp->rsps, &rsp);
  }
  return buf;
}

int tSerializeSVCreateTbReq(void **buf, SVCreateTbReq *pReq) {
  int tlen = 0;

  tlen += taosEncodeFixedU64(buf, pReq->ver);
  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedU32(buf, pReq->ttl);
  tlen += taosEncodeFixedU32(buf, pReq->keep);
  tlen += taosEncodeFixedU8(buf, pReq->type);

  switch (pReq->type) {
    case TD_SUPER_TABLE:
      tlen += taosEncodeFixedU64(buf, pReq->stbCfg.suid);
      tlen += taosEncodeFixedU32(buf, pReq->stbCfg.nCols);
      for (uint32_t i = 0; i < pReq->stbCfg.nCols; i++) {
        tlen += taosEncodeFixedI8(buf, pReq->stbCfg.pSchema[i].type);
        tlen += taosEncodeFixedI32(buf, pReq->stbCfg.pSchema[i].colId);
        tlen += taosEncodeFixedI32(buf, pReq->stbCfg.pSchema[i].bytes);
        tlen += taosEncodeString(buf, pReq->stbCfg.pSchema[i].name);
      }
      tlen += taosEncodeFixedU32(buf, pReq->stbCfg.nTagCols);
      for (uint32_t i = 0; i < pReq->stbCfg.nTagCols; i++) {
        tlen += taosEncodeFixedI8(buf, pReq->stbCfg.pTagSchema[i].type);
        tlen += taosEncodeFixedI32(buf, pReq->stbCfg.pTagSchema[i].colId);
        tlen += taosEncodeFixedI32(buf, pReq->stbCfg.pTagSchema[i].bytes);
        tlen += taosEncodeString(buf, pReq->stbCfg.pTagSchema[i].name);
      }
      break;
    case TD_CHILD_TABLE:
      tlen += taosEncodeFixedU64(buf, pReq->ctbCfg.suid);
      tlen += tdEncodeKVRow(buf, pReq->ctbCfg.pTag);
      break;
    case TD_NORMAL_TABLE:
      tlen += taosEncodeFixedU32(buf, pReq->ntbCfg.nCols);
      for (uint32_t i = 0; i < pReq->ntbCfg.nCols; i++) {
        tlen += taosEncodeFixedI8(buf, pReq->ntbCfg.pSchema[i].type);
        tlen += taosEncodeFixedI32(buf, pReq->ntbCfg.pSchema[i].colId);
        tlen += taosEncodeFixedI32(buf, pReq->ntbCfg.pSchema[i].bytes);
        tlen += taosEncodeString(buf, pReq->ntbCfg.pSchema[i].name);
      }
      break;
    default:
      ASSERT(0);
  }

  return tlen;
}

void *tDeserializeSVCreateTbReq(void *buf, SVCreateTbReq *pReq) {
  buf = taosDecodeFixedU64(buf, &(pReq->ver));
  buf = taosDecodeString(buf, &(pReq->name));
  buf = taosDecodeFixedU32(buf, &(pReq->ttl));
  buf = taosDecodeFixedU32(buf, &(pReq->keep));
  buf = taosDecodeFixedU8(buf, &(pReq->type));

  switch (pReq->type) {
    case TD_SUPER_TABLE:
      buf = taosDecodeFixedU64(buf, &(pReq->stbCfg.suid));
      buf = taosDecodeFixedU32(buf, &(pReq->stbCfg.nCols));
      pReq->stbCfg.pSchema = (SSchema *)malloc(pReq->stbCfg.nCols * sizeof(SSchema));
      for (uint32_t i = 0; i < pReq->stbCfg.nCols; i++) {
        buf = taosDecodeFixedI8(buf, &(pReq->stbCfg.pSchema[i].type));
        buf = taosDecodeFixedI32(buf, &(pReq->stbCfg.pSchema[i].colId));
        buf = taosDecodeFixedI32(buf, &(pReq->stbCfg.pSchema[i].bytes));
        buf = taosDecodeStringTo(buf, pReq->stbCfg.pSchema[i].name);
      }
      buf = taosDecodeFixedU32(buf, &pReq->stbCfg.nTagCols);
      pReq->stbCfg.pTagSchema = (SSchema *)malloc(pReq->stbCfg.nTagCols * sizeof(SSchema));
      for (uint32_t i = 0; i < pReq->stbCfg.nTagCols; i++) {
        buf = taosDecodeFixedI8(buf, &(pReq->stbCfg.pTagSchema[i].type));
        buf = taosDecodeFixedI32(buf, &pReq->stbCfg.pTagSchema[i].colId);
        buf = taosDecodeFixedI32(buf, &pReq->stbCfg.pTagSchema[i].bytes);
        buf = taosDecodeStringTo(buf, pReq->stbCfg.pTagSchema[i].name);
      }
      break;
    case TD_CHILD_TABLE:
      buf = taosDecodeFixedU64(buf, &pReq->ctbCfg.suid);
      buf = tdDecodeKVRow(buf, &pReq->ctbCfg.pTag);
      break;
    case TD_NORMAL_TABLE:
      buf = taosDecodeFixedU32(buf, &pReq->ntbCfg.nCols);
      pReq->ntbCfg.pSchema = (SSchema *)malloc(pReq->ntbCfg.nCols * sizeof(SSchema));
      for (uint32_t i = 0; i < pReq->ntbCfg.nCols; i++) {
        buf = taosDecodeFixedI8(buf, &pReq->ntbCfg.pSchema[i].type);
        buf = taosDecodeFixedI32(buf, &pReq->ntbCfg.pSchema[i].colId);
        buf = taosDecodeFixedI32(buf, &pReq->ntbCfg.pSchema[i].bytes);
        buf = taosDecodeStringTo(buf, pReq->ntbCfg.pSchema[i].name);
      }
      break;
    default:
      ASSERT(0);
  }

  return buf;
}

int tSVCreateTbBatchReqSerialize(void **buf, SVCreateTbBatchReq *pReq) {
  int tlen = 0;

  tlen += taosEncodeFixedU64(buf, pReq->ver);
  tlen += taosEncodeFixedU32(buf, taosArrayGetSize(pReq->pArray));
  for (size_t i = 0; i < taosArrayGetSize(pReq->pArray); i++) {
    SVCreateTbReq *pCreateTbReq = taosArrayGet(pReq->pArray, i);
    tlen += tSerializeSVCreateTbReq(buf, pCreateTbReq);
  }

  return tlen;
}

void *tSVCreateTbBatchReqDeserialize(void *buf, SVCreateTbBatchReq *pReq) {
  uint32_t nsize = 0;

  buf = taosDecodeFixedU64(buf, &pReq->ver);
  buf = taosDecodeFixedU32(buf, &nsize);
  pReq->pArray = taosArrayInit(nsize, sizeof(SVCreateTbReq));
  for (size_t i = 0; i < nsize; i++) {
    SVCreateTbReq req;
    buf = tDeserializeSVCreateTbReq(buf, &req);
    taosArrayPush(pReq->pArray, &req);
  }

  return buf;
}
