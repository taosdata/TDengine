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

#define _DEFAULT_SOURCE
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

int32_t tInitSubmitMsgIter(SSubmitReq *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  pIter->totalLen = htonl(pMsg->length);
  ASSERT(pIter->totalLen > 0);
  pIter->len = 0;
  pIter->pMsg = pMsg;
  if (pIter->totalLen <= sizeof(SSubmitReq)) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  return 0;
}

int32_t tGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock) {
  ASSERT(pIter->len >= 0);

  if (pIter->len == 0) {
    pIter->len += sizeof(SSubmitReq);
  } else {
    if (pIter->len >= pIter->totalLen) {
      ASSERT(0);
    }

    pIter->len += (sizeof(SSubmitBlk) + pIter->dataLen + pIter->schemaLen);
    ASSERT(pIter->len > 0);
  }

  if (pIter->len > pIter->totalLen) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    *pPBlock = NULL;
    return -1;
  }

  if (pIter->len == pIter->totalLen) {
    *pPBlock = NULL;
  } else {
    *pPBlock = (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);
    pIter->uid = htobe64((*pPBlock)->uid);
    pIter->suid = htobe64((*pPBlock)->suid);
    pIter->sversion = htonl((*pPBlock)->sversion);
    pIter->dataLen = htonl((*pPBlock)->dataLen);
    pIter->schemaLen = htonl((*pPBlock)->schemaLen);
    pIter->numOfRows = htons((*pPBlock)->numOfRows);
  }
  return 0;
}

int32_t tInitSubmitBlkIter(SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pMsgIter->dataLen <= 0) return -1;
  pIter->totalLen = pMsgIter->dataLen;
  pIter->len = 0;
  pIter->row = (STSRow *)(pBlock->data + pMsgIter->schemaLen);
  return 0;
}

STSRow *tGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  STSRow *row = pIter->row;

  if (pIter->len >= pIter->totalLen) {
    return NULL;
  } else {
    pIter->len += TD_ROW_LEN(row);
    if (pIter->len < pIter->totalLen) {
      pIter->row = POINTER_SHIFT(row, TD_ROW_LEN(row));
    }
    return row;
  }
}

int32_t tPrintFixedSchemaSubmitReq(SSubmitReq *pReq, STSchema *pTschema) {
  SSubmitMsgIter msgIter = {0};
  if (tInitSubmitMsgIter(pReq, &msgIter) < 0) return -1;
  while (true) {
    SSubmitBlk *pBlock = NULL;
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;
    if (pBlock == NULL) break;
    SSubmitBlkIter blkIter = {0};
    tInitSubmitBlkIter(&msgIter, pBlock, &blkIter);
    STSRowIter rowIter = {0};
    tdSTSRowIterInit(&rowIter, pTschema);
    STSRow *row;
    while ((row = tGetSubmitBlkNext(&blkIter)) != NULL) {
      tdSRowPrint(row, pTschema, "stream");
    }
  }
  return 0;
}

int32_t tEncodeSEpSet(SEncoder *pEncoder, const SEpSet *pEp) {
  if (tEncodeI8(pEncoder, pEp->inUse) < 0) return -1;
  if (tEncodeI8(pEncoder, pEp->numOfEps) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    if (tEncodeU16(pEncoder, pEp->eps[i].port) < 0) return -1;
    if (tEncodeCStr(pEncoder, pEp->eps[i].fqdn) < 0) return -1;
  }
  return 0;
}

int32_t tDecodeSEpSet(SDecoder *pDecoder, SEpSet *pEp) {
  if (tDecodeI8(pDecoder, &pEp->inUse) < 0) return -1;
  if (tDecodeI8(pDecoder, &pEp->numOfEps) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    if (tDecodeU16(pDecoder, &pEp->eps[i].port) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pEp->eps[i].fqdn) < 0) return -1;
  }
  return 0;
}

int32_t tEncodeSQueryNodeAddr(SEncoder *pEncoder, SQueryNodeAddr *pAddr) {
  if (tEncodeI32(pEncoder, pAddr->nodeId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pAddr->epSet) < 0) return -1;
  return 0;
}

int32_t tDecodeSQueryNodeAddr(SDecoder *pDecoder, SQueryNodeAddr *pAddr) {
  if (tDecodeI32(pDecoder, &pAddr->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pAddr->epSet) < 0) return -1;
  return 0;
}

int32_t taosEncodeSEpSet(void **buf, const SEpSet *pEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pEp->inUse);
  tlen += taosEncodeFixedI8(buf, pEp->numOfEps);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    tlen += taosEncodeFixedU16(buf, pEp->eps[i].port);
    tlen += taosEncodeString(buf, pEp->eps[i].fqdn);
  }
  return tlen;
}

void *taosDecodeSEpSet(const void *buf, SEpSet *pEp) {
  buf = taosDecodeFixedI8(buf, &pEp->inUse);
  buf = taosDecodeFixedI8(buf, &pEp->numOfEps);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    buf = taosDecodeFixedU16(buf, &pEp->eps[i].port);
    buf = taosDecodeStringTo(buf, pEp->eps[i].fqdn);
  }
  return (void *)buf;
}

static int32_t tSerializeSClientHbReq(SEncoder *pEncoder, const SClientHbReq *pReq) {
  if (tEncodeSClientHbKey(pEncoder, &pReq->connKey) < 0) return -1;

  if (pReq->connKey.connType == CONN_TYPE__QUERY) {
    int32_t queryNum = 0;
    if (pReq->query) {
      queryNum = 1;
      if (tEncodeI32(pEncoder, queryNum) < 0) return -1;
      if (tEncodeU32(pEncoder, pReq->query->connId) < 0) return -1;
      if (tEncodeI32(pEncoder, pReq->query->pid) < 0) return -1;
      if (tEncodeCStr(pEncoder, pReq->query->app) < 0) return -1;

      int32_t num = taosArrayGetSize(pReq->query->queryDesc);
      if (tEncodeI32(pEncoder, num) < 0) return -1;

      for (int32_t i = 0; i < num; ++i) {
        SQueryDesc *desc = taosArrayGet(pReq->query->queryDesc, i);
        if (tEncodeCStr(pEncoder, desc->sql) < 0) return -1;
        if (tEncodeU64(pEncoder, desc->queryId) < 0) return -1;
        if (tEncodeI64(pEncoder, desc->useconds) < 0) return -1;
        if (tEncodeI64(pEncoder, desc->stime) < 0) return -1;
        if (tEncodeI64(pEncoder, desc->reqRid) < 0) return -1;
        if (tEncodeI32(pEncoder, desc->pid) < 0) return -1;
        if (tEncodeCStr(pEncoder, desc->fqdn) < 0) return -1;
        if (tEncodeI32(pEncoder, desc->subPlanNum) < 0) return -1;

        int32_t snum = desc->subDesc ? taosArrayGetSize(desc->subDesc) : 0;
        if (tEncodeI32(pEncoder, snum) < 0) return -1;
        for (int32_t m = 0; m < snum; ++m) {
          SQuerySubDesc *sDesc = taosArrayGet(desc->subDesc, m);
          if (tEncodeI64(pEncoder, sDesc->tid) < 0) return -1;
          if (tEncodeI32(pEncoder, sDesc->status) < 0) return -1;
        }
      }
    } else {
      if (tEncodeI32(pEncoder, queryNum) < 0) return -1;
    }
  }

  int32_t kvNum = taosHashGetSize(pReq->info);
  if (tEncodeI32(pEncoder, kvNum) < 0) return -1;
  void *pIter = taosHashIterate(pReq->info, NULL);
  while (pIter != NULL) {
    SKv *kv = pIter;
    if (tEncodeSKv(pEncoder, kv) < 0) return -1;
    pIter = taosHashIterate(pReq->info, pIter);
  }

  return 0;
}

static int32_t tDeserializeSClientHbReq(SDecoder *pDecoder, SClientHbReq *pReq) {
  if (tDecodeSClientHbKey(pDecoder, &pReq->connKey) < 0) return -1;

  if (pReq->connKey.connType == CONN_TYPE__QUERY) {
    int32_t queryNum = 0;
    if (tDecodeI32(pDecoder, &queryNum) < 0) return -1;
    if (queryNum) {
      pReq->query = taosMemoryCalloc(1, sizeof(*pReq->query));
      if (NULL == pReq->query) return -1;
      if (tDecodeU32(pDecoder, &pReq->query->connId) < 0) return -1;
      if (tDecodeI32(pDecoder, &pReq->query->pid) < 0) return -1;
      if (tDecodeCStrTo(pDecoder, pReq->query->app) < 0) return -1;

      int32_t num = 0;
      if (tDecodeI32(pDecoder, &num) < 0) return -1;
      if (num > 0) {
        pReq->query->queryDesc = taosArrayInit(num, sizeof(SQueryDesc));
        if (NULL == pReq->query->queryDesc) return -1;

        for (int32_t i = 0; i < num; ++i) {
          SQueryDesc desc = {0};
          if (tDecodeCStrTo(pDecoder, desc.sql) < 0) return -1;
          if (tDecodeU64(pDecoder, &desc.queryId) < 0) return -1;
          if (tDecodeI64(pDecoder, &desc.useconds) < 0) return -1;
          if (tDecodeI64(pDecoder, &desc.stime) < 0) return -1;
          if (tDecodeI64(pDecoder, &desc.reqRid) < 0) return -1;
          if (tDecodeI32(pDecoder, &desc.pid) < 0) return -1;
          if (tDecodeCStrTo(pDecoder, desc.fqdn) < 0) return -1;
          if (tDecodeI32(pDecoder, &desc.subPlanNum) < 0) return -1;

          int32_t snum = 0;
          if (tDecodeI32(pDecoder, &snum) < 0) return -1;
          if (snum > 0) {
            desc.subDesc = taosArrayInit(snum, sizeof(SQuerySubDesc));
            if (NULL == desc.subDesc) return -1;

            for (int32_t m = 0; m < snum; ++m) {
              SQuerySubDesc sDesc = {0};
              if (tDecodeI64(pDecoder, &sDesc.tid) < 0) return -1;
              if (tDecodeI32(pDecoder, &sDesc.status) < 0) return -1;
              taosArrayPush(desc.subDesc, &sDesc);
            }
          }

          taosArrayPush(pReq->query->queryDesc, &desc);
        }
      }
    }
  }

  int32_t kvNum = 0;
  if (tDecodeI32(pDecoder, &kvNum) < 0) return -1;
  if (pReq->info == NULL) {
    pReq->info = taosHashInit(kvNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  if (pReq->info == NULL) return -1;
  for (int32_t i = 0; i < kvNum; i++) {
    SKv kv = {0};
    if (tDecodeSKv(pDecoder, &kv) < 0) return -1;
    taosHashPut(pReq->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  }

  return 0;
}

static int32_t tSerializeSClientHbRsp(SEncoder *pEncoder, const SClientHbRsp *pRsp) {
  if (tEncodeSClientHbKey(pEncoder, &pRsp->connKey) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->status) < 0) return -1;

  int32_t queryNum = 0;
  if (pRsp->query) {
    queryNum = 1;
    if (tEncodeI32(pEncoder, queryNum) < 0) return -1;
    if (tEncodeU32(pEncoder, pRsp->query->connId) < 0) return -1;
    if (tEncodeU64(pEncoder, pRsp->query->killRid) < 0) return -1;
    if (tEncodeI32(pEncoder, pRsp->query->totalDnodes) < 0) return -1;
    if (tEncodeI32(pEncoder, pRsp->query->onlineDnodes) < 0) return -1;
    if (tEncodeI8(pEncoder, pRsp->query->killConnection) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pRsp->query->epSet) < 0) return -1;
  } else {
    if (tEncodeI32(pEncoder, queryNum) < 0) return -1;
  }

  int32_t kvNum = taosArrayGetSize(pRsp->info);
  if (tEncodeI32(pEncoder, kvNum) < 0) return -1;
  for (int32_t i = 0; i < kvNum; i++) {
    SKv *kv = taosArrayGet(pRsp->info, i);
    if (tEncodeSKv(pEncoder, kv) < 0) return -1;
  }

  return 0;
}

static int32_t tDeserializeSClientHbRsp(SDecoder *pDecoder, SClientHbRsp *pRsp) {
  if (tDecodeSClientHbKey(pDecoder, &pRsp->connKey) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->status) < 0) return -1;

  int32_t queryNum = 0;
  if (tDecodeI32(pDecoder, &queryNum) < 0) return -1;
  if (queryNum) {
    pRsp->query = taosMemoryCalloc(1, sizeof(*pRsp->query));
    if (NULL == pRsp->query) return -1;
    if (tDecodeU32(pDecoder, &pRsp->query->connId) < 0) return -1;
    if (tDecodeU64(pDecoder, &pRsp->query->killRid) < 0) return -1;
    if (tDecodeI32(pDecoder, &pRsp->query->totalDnodes) < 0) return -1;
    if (tDecodeI32(pDecoder, &pRsp->query->onlineDnodes) < 0) return -1;
    if (tDecodeI8(pDecoder, &pRsp->query->killConnection) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &pRsp->query->epSet) < 0) return -1;
  }

  int32_t kvNum = 0;
  if (tDecodeI32(pDecoder, &kvNum) < 0) return -1;
  pRsp->info = taosArrayInit(kvNum, sizeof(SKv));
  if (pRsp->info == NULL) return -1;
  for (int32_t i = 0; i < kvNum; i++) {
    SKv kv = {0};
    tDecodeSKv(pDecoder, &kv);
    taosArrayPush(pRsp->info, &kv);
  }

  return 0;
}

int32_t tSerializeSClientHbBatchReq(void *buf, int32_t bufLen, const SClientHbBatchReq *pBatchReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pBatchReq->reqId) < 0) return -1;

  int32_t reqNum = taosArrayGetSize(pBatchReq->reqs);
  if (tEncodeI32(&encoder, reqNum) < 0) return -1;
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq *pReq = taosArrayGet(pBatchReq->reqs, i);
    if (tSerializeSClientHbReq(&encoder, pReq) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSClientHbBatchReq(void *buf, int32_t bufLen, SClientHbBatchReq *pBatchReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pBatchReq->reqId) < 0) return -1;

  int32_t reqNum = 0;
  if (tDecodeI32(&decoder, &reqNum) < 0) return -1;
  if (reqNum > 0) {
    pBatchReq->reqs = taosArrayInit(reqNum, sizeof(SClientHbReq));
    if (NULL == pBatchReq->reqs) return -1;
  }
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq req = {0};
    tDeserializeSClientHbReq(&decoder, &req);
    taosArrayPush(pBatchReq->reqs, &req);
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSClientHbBatchRsp(void *buf, int32_t bufLen, const SClientHbBatchRsp *pBatchRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pBatchRsp->reqId) < 0) return -1;
  if (tEncodeI64(&encoder, pBatchRsp->rspId) < 0) return -1;

  int32_t rspNum = taosArrayGetSize(pBatchRsp->rsps);
  if (tEncodeI32(&encoder, rspNum) < 0) return -1;
  for (int32_t i = 0; i < rspNum; i++) {
    SClientHbRsp *pRsp = taosArrayGet(pBatchRsp->rsps, i);
    if (tSerializeSClientHbRsp(&encoder, pRsp) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSClientHbBatchRsp(void *buf, int32_t bufLen, SClientHbBatchRsp *pBatchRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pBatchRsp->reqId) < 0) return -1;
  if (tDecodeI64(&decoder, &pBatchRsp->rspId) < 0) return -1;

  int32_t rspNum = 0;
  if (tDecodeI32(&decoder, &rspNum) < 0) return -1;
  if (pBatchRsp->rsps == NULL) {
    pBatchRsp->rsps = taosArrayInit(rspNum, sizeof(SClientHbRsp));
  }
  for (int32_t i = 0; i < rspNum; i++) {
    SClientHbRsp rsp = {0};
    tDeserializeSClientHbRsp(&decoder, &rsp);
    taosArrayPush(pBatchRsp->rsps, &rsp);
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMCreateStbReq(void *buf, int32_t bufLen, SMCreateStbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeFloat(&encoder, pReq->xFilesFactor) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->delay) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ttl) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfColumns) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfTags) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commentLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ast1Len) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ast2Len) < 0) return -1;

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField *pField = taosArrayGet(pReq->pColumns, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
    if (tEncodeI8(&encoder, pField->flags) < 0) return -1;
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField *pField = taosArrayGet(pReq->pTags, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
    if (tEncodeI8(&encoder, pField->flags) < 0) return -1;
  }

  if (pReq->commentLen > 0) {
    if (tEncodeBinary(&encoder, pReq->comment, pReq->commentLen) < 0) return -1;
  }
  if (pReq->ast1Len > 0) {
    if (tEncodeBinary(&encoder, pReq->pAst1, pReq->ast1Len) < 0) return -1;
  }
  if (pReq->ast2Len > 0) {
    if (tEncodeBinary(&encoder, pReq->pAst2, pReq->ast2Len) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateStbReq(void *buf, int32_t bufLen, SMCreateStbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeFloat(&decoder, &pReq->xFilesFactor) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->delay) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfColumns) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfTags) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commentLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ast1Len) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ast2Len) < 0) return -1;

  pReq->pColumns = taosArrayInit(pReq->numOfColumns, sizeof(SField));
  pReq->pTags = taosArrayInit(pReq->numOfTags, sizeof(SField));
  if (pReq->pColumns == NULL || pReq->pTags == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (tDecodeI8(&decoder, &field.flags) < 0) return -1;
    if (taosArrayPush(pReq->pColumns, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (tDecodeI8(&decoder, &field.flags) < 0) return -1;
    if (taosArrayPush(pReq->pTags, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (pReq->commentLen > 0) {
    pReq->comment = taosMemoryMalloc(pReq->commentLen);
    if (pReq->comment == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->comment) < 0) return -1;
  }

  if (pReq->ast1Len > 0) {
    pReq->pAst1 = taosMemoryMalloc(pReq->ast1Len);
    if (pReq->pAst1 == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->pAst1) < 0) return -1;
  }

  if (pReq->ast2Len > 0) {
    pReq->pAst2 = taosMemoryMalloc(pReq->ast2Len);
    if (pReq->pAst2 == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->pAst2) < 0) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMCreateStbReq(SMCreateStbReq *pReq) {
  taosArrayDestroy(pReq->pColumns);
  taosArrayDestroy(pReq->pTags);
  taosMemoryFreeClear(pReq->comment);
  taosMemoryFreeClear(pReq->pAst1);
  taosMemoryFreeClear(pReq->pAst2);
  pReq->pColumns = NULL;
  pReq->pTags = NULL;
}

int32_t tSerializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMAlterStbReq(void *buf, int32_t bufLen, SMAlterStbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->alterType) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->tagVer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->colVer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfFields) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfFields; ++i) {
    SField *pField = taosArrayGet(pReq->pFields, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->ttl) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commentLen) < 0) return -1;
  if (pReq->commentLen > 0) {
    if (tEncodeCStr(&encoder, pReq->comment) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMAlterStbReq(void *buf, int32_t bufLen, SMAlterStbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->alterType) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->tagVer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->colVer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfFields) < 0) return -1;
  pReq->pFields = taosArrayInit(pReq->numOfFields, sizeof(SField));
  if (pReq->pFields == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfFields; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (taosArrayPush(pReq->pFields, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (tDecodeI32(&decoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commentLen) < 0) return -1;
  if (pReq->commentLen > 0) {
    pReq->comment = taosMemoryMalloc(pReq->commentLen);
    if (pReq->comment == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->comment) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMAltertbReq(SMAlterStbReq *pReq) {
  taosArrayDestroy(pReq->pFields);
  pReq->pFields = NULL;
}
int32_t tSerializeSMEpSet(void *buf, int32_t bufLen, SMEpSet *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSEpSet(&encoder, &pReq->epSet) < 0) return -1;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMEpSet(void *buf, int32_t bufLen, SMEpSet *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSEpSet(&decoder, &pReq->epSet) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMCreateSmaReq(void *buf, int32_t bufLen, SMCreateSmaReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->stb) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->intervalUnit) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->slidingUnit) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->timezone) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dstVgId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->interval) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->offset) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->sliding) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->exprLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->tagsFilterLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->sqlLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->astLen) < 0) return -1;
  if (pReq->exprLen > 0) {
    if (tEncodeBinary(&encoder, pReq->expr, pReq->exprLen) < 0) return -1;
  }
  if (pReq->tagsFilterLen > 0) {
    if (tEncodeBinary(&encoder, pReq->tagsFilter, pReq->tagsFilterLen) < 0) return -1;
  }
  if (pReq->sqlLen > 0) {
    if (tEncodeBinary(&encoder, pReq->sql, pReq->sqlLen) < 0) return -1;
  }
  if (pReq->astLen > 0) {
    if (tEncodeBinary(&encoder, pReq->ast, pReq->astLen) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateSmaReq(void *buf, int32_t bufLen, SMCreateSmaReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->stb) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->intervalUnit) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->slidingUnit) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->timezone) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dstVgId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->interval) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->offset) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->sliding) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->exprLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->tagsFilterLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->sqlLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->astLen) < 0) return -1;
  if (pReq->exprLen > 0) {
    pReq->expr = taosMemoryMalloc(pReq->exprLen);
    if (pReq->expr == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->expr) < 0) return -1;
  }
  if (pReq->tagsFilterLen > 0) {
    pReq->tagsFilter = taosMemoryMalloc(pReq->tagsFilterLen);
    if (pReq->tagsFilter == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->tagsFilter) < 0) return -1;
  }
  if (pReq->sqlLen > 0) {
    pReq->sql = taosMemoryMalloc(pReq->sqlLen);
    if (pReq->sql == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->sql) < 0) return -1;
  }
  if (pReq->astLen > 0) {
    pReq->ast = taosMemoryMalloc(pReq->astLen);
    if (pReq->ast == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->ast) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMCreateSmaReq(SMCreateSmaReq *pReq) {
  taosMemoryFreeClear(pReq->expr);
  taosMemoryFreeClear(pReq->tagsFilter);
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
}

int32_t tSerializeSMDropSmaReq(void *buf, int32_t bufLen, SMDropSmaReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;

  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropSmaReq(void *buf, int32_t bufLen, SMDropSmaReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}
int32_t tSerializeSMCreateFullTextReq(void *buf, int32_t bufLen, SMCreateFullTextReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMCreateFullTextReq(void *buf, int32_t bufLen, SMCreateFullTextReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}
void tFreeSMCreateFullTextReq(SMCreateFullTextReq *pReq) {
  // impl later
  return;
}
int32_t tSerializeSMDropFullTextReq(void *buf, int32_t bufLen, SMDropFullTextReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;

  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMDropFullTextReq(void *buf, int32_t bufLen, SMDropFullTextReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSStatusReq(void *buf, int32_t bufLen, SStatusReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  // status
  if (tEncodeI32(&encoder, pReq->sver) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dnodeVer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->clusterId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->rebootTime) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->updateTime) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfCores) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfSupportVnodes) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dnodeEp) < 0) return -1;

  // cluster cfg
  if (tEncodeI32(&encoder, pReq->clusterCfg.statusInterval) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->clusterCfg.checkTime) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->clusterCfg.timezone) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->clusterCfg.locale) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->clusterCfg.charset) < 0) return -1;

  // vnode loads
  int32_t vlen = (int32_t)taosArrayGetSize(pReq->pVloads);
  if (tEncodeI32(&encoder, vlen) < 0) return -1;
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    if (tEncodeI32(&encoder, pload->vgId) < 0) return -1;
    if (tEncodeI32(&encoder, pload->syncState) < 0) return -1;
    if (tEncodeI64(&encoder, pload->numOfTables) < 0) return -1;
    if (tEncodeI64(&encoder, pload->numOfTimeSeries) < 0) return -1;
    if (tEncodeI64(&encoder, pload->totalStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->compStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->pointsWritten) < 0) return -1;
  }

  // mnode loads
  if (tEncodeI32(&encoder, pReq->mload.syncState) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatusReq(void *buf, int32_t bufLen, SStatusReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  // status
  if (tDecodeI32(&decoder, &pReq->sver) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dnodeVer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->clusterId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->rebootTime) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->updateTime) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfCores) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfSupportVnodes) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dnodeEp) < 0) return -1;

  // cluster cfg
  if (tDecodeI32(&decoder, &pReq->clusterCfg.statusInterval) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->clusterCfg.checkTime) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->clusterCfg.timezone) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->clusterCfg.locale) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->clusterCfg.charset) < 0) return -1;

  // vnode loads
  int32_t vlen = 0;
  if (tDecodeI32(&decoder, &vlen) < 0) return -1;
  pReq->pVloads = taosArrayInit(vlen, sizeof(SVnodeLoad));
  if (pReq->pVloads == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad vload = {0};
    if (tDecodeI32(&decoder, &vload.vgId) < 0) return -1;
    if (tDecodeI32(&decoder, &vload.syncState) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.numOfTables) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.numOfTimeSeries) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.totalStorage) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.compStorage) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.pointsWritten) < 0) return -1;
    if (taosArrayPush(pReq->pVloads, &vload) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (tDecodeI32(&decoder, &pReq->mload.syncState) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSStatusReq(SStatusReq *pReq) { taosArrayDestroy(pReq->pVloads); }

int32_t tSerializeSStatusRsp(void *buf, int32_t bufLen, SStatusRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  // status
  if (tEncodeI64(&encoder, pRsp->dnodeVer) < 0) return -1;

  // dnode cfg
  if (tEncodeI32(&encoder, pRsp->dnodeCfg.dnodeId) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->dnodeCfg.clusterId) < 0) return -1;

  // dnode eps
  int32_t dlen = (int32_t)taosArrayGetSize(pRsp->pDnodeEps);
  if (tEncodeI32(&encoder, dlen) < 0) return -1;
  for (int32_t i = 0; i < dlen; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pRsp->pDnodeEps, i);
    if (tEncodeI32(&encoder, pDnodeEp->id) < 0) return -1;
    if (tEncodeI8(&encoder, pDnodeEp->isMnode) < 0) return -1;
    if (tEncodeCStr(&encoder, pDnodeEp->ep.fqdn) < 0) return -1;
    if (tEncodeU16(&encoder, pDnodeEp->ep.port) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatusRsp(void *buf, int32_t bufLen, SStatusRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  // status
  if (tDecodeI64(&decoder, &pRsp->dnodeVer) < 0) return -1;

  // cluster cfg
  if (tDecodeI32(&decoder, &pRsp->dnodeCfg.dnodeId) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->dnodeCfg.clusterId) < 0) return -1;

  // dnode eps
  int32_t dlen = 0;
  if (tDecodeI32(&decoder, &dlen) < 0) return -1;
  pRsp->pDnodeEps = taosArrayInit(dlen, sizeof(SDnodeEp));
  if (pRsp->pDnodeEps == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < dlen; ++i) {
    SDnodeEp dnodeEp = {0};
    if (tDecodeI32(&decoder, &dnodeEp.id) < 0) return -1;
    if (tDecodeI8(&decoder, &dnodeEp.isMnode) < 0) return -1;
    if (tDecodeCStrTo(&decoder, dnodeEp.ep.fqdn) < 0) return -1;
    if (tDecodeU16(&decoder, &dnodeEp.ep.port) < 0) return -1;
    if (taosArrayPush(pRsp->pDnodeEps, &dnodeEp) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSStatusRsp(SStatusRsp *pRsp) { taosArrayDestroy(pRsp->pDnodeEps); }

int32_t tSerializeSCreateAcctReq(void *buf, int32_t bufLen, SCreateAcctReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxUsers) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxDbs) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxTimeSeries) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxStreams) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->accessState) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->maxStorage) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateAcctReq(void *buf, int32_t bufLen, SCreateAcctReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxUsers) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxDbs) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxTimeSeries) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxStreams) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->accessState) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->maxStorage) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropUserReq(void *buf, int32_t bufLen, SDropUserReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropUserReq(void *buf, int32_t bufLen, SDropUserReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->createType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->superUser) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->createType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->superUser) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->alterType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->superUser) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbname) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->alterType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->superUser) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dbname) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSGetUserAuthReq(void *buf, int32_t bufLen, SGetUserAuthReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserAuthReq(void *buf, int32_t bufLen, SGetUserAuthReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSGetUserAuthRspImpl(SEncoder *pEncoder, SGetUserAuthRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->user) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->superAuth) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->version) < 0) return -1;

  int32_t numOfCreatedDbs = taosHashGetSize(pRsp->createdDbs);
  int32_t numOfReadDbs = taosHashGetSize(pRsp->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pRsp->writeDbs);
  if (tEncodeI32(pEncoder, numOfCreatedDbs) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfReadDbs) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfWriteDbs) < 0) return -1;

  char *db = taosHashIterate(pRsp->createdDbs, NULL);
  while (db != NULL) {
    if (tEncodeCStr(pEncoder, db) < 0) return -1;
    db = taosHashIterate(pRsp->createdDbs, db);
  }

  db = taosHashIterate(pRsp->readDbs, NULL);
  while (db != NULL) {
    if (tEncodeCStr(pEncoder, db) < 0) return -1;
    db = taosHashIterate(pRsp->readDbs, db);
  }

  db = taosHashIterate(pRsp->writeDbs, NULL);
  while (db != NULL) {
    if (tEncodeCStr(pEncoder, db) < 0) return -1;
    db = taosHashIterate(pRsp->writeDbs, db);
  }

  return 0;
}

int32_t tSerializeSGetUserAuthRsp(void *buf, int32_t bufLen, SGetUserAuthRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tSerializeSGetUserAuthRspImpl(&encoder, pRsp) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserAuthRspImpl(SDecoder *pDecoder, SGetUserAuthRsp *pRsp) {
  pRsp->createdDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pRsp->readDbs == NULL || pRsp->writeDbs == NULL) {
    return -1;
  }

  if (tDecodeCStrTo(pDecoder, pRsp->user) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->superAuth) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->version) < 0) return -1;

  int32_t numOfCreatedDbs = 0;
  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  if (tDecodeI32(pDecoder, &numOfCreatedDbs) < 0) return -1;
  if (tDecodeI32(pDecoder, &numOfReadDbs) < 0) return -1;
  if (tDecodeI32(pDecoder, &numOfWriteDbs) < 0) return -1;

  for (int32_t i = 0; i < numOfCreatedDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) return -1;
    int32_t len = strlen(db);
    taosHashPut(pRsp->createdDbs, db, len, db, len);
  }

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) return -1;
    int32_t len = strlen(db);
    taosHashPut(pRsp->readDbs, db, len, db, len);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) return -1;
    int32_t len = strlen(db);
    taosHashPut(pRsp->writeDbs, db, len, db, len);
  }

  return 0;
}

int32_t tDeserializeSGetUserAuthRsp(void *buf, int32_t bufLen, SGetUserAuthRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDeserializeSGetUserAuthRspImpl(&decoder, pRsp) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSGetUserAuthRsp(SGetUserAuthRsp *pRsp) {
  taosHashCleanup(pRsp->createdDbs);
  taosHashCleanup(pRsp->readDbs);
  taosHashCleanup(pRsp->writeDbs);
}

int32_t tSerializeSCreateDropMQSBNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDropMQSBNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fqdn) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->port) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fqdn) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->port) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->config) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->value) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->config) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->value) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateDnodeReq(void *buf, int32_t bufLen, SCreateDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fqdn) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->port) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDnodeReq(void *buf, int32_t bufLen, SCreateDnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fqdn) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->port) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateFuncReq(void *buf, int32_t bufLen, SCreateFuncReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->funcType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->scriptType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->outputType) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->outputLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->bufSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->codeLen) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->signature) < 0) return -1;

  if (pReq->pCode != NULL) {
    if (tEncodeBinary(&encoder, pReq->pCode, pReq->codeLen) < 0) return -1;
  }

  int32_t commentSize = 0;
  if (pReq->pComment != NULL) {
    commentSize = strlen(pReq->pComment) + 1;
  }
  if (tEncodeI32(&encoder, commentSize) < 0) return -1;
  if (pReq->pComment != NULL) {
    if (tEncodeCStr(&encoder, pReq->pComment) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateFuncReq(void *buf, int32_t bufLen, SCreateFuncReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->funcType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->scriptType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->outputType) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->outputLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->bufSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->codeLen) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->signature) < 0) return -1;

  if (pReq->codeLen > 0) {
    pReq->pCode = taosMemoryCalloc(1, pReq->codeLen);
    if (pReq->pCode == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    if (tDecodeCStrTo(&decoder, pReq->pCode) < 0) return -1;
  }

  int32_t commentSize = 0;
  if (tDecodeI32(&decoder, &commentSize) < 0) return -1;
  if (commentSize > 0) {
    pReq->pComment = taosMemoryCalloc(1, commentSize);
    if (pReq->pComment == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    if (tDecodeCStrTo(&decoder, pReq->pComment) < 0) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCreateFuncReq(SCreateFuncReq *pReq) {
  taosMemoryFree(pReq->pCode);
  taosMemoryFree(pReq->pComment);
}

int32_t tSerializeSDropFuncReq(void *buf, int32_t bufLen, SDropFuncReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropFuncReq(void *buf, int32_t bufLen, SDropFuncReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSRetrieveFuncReq(void *buf, int32_t bufLen, SRetrieveFuncReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfFuncs) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreCodeComment) < 0) return -1;

  if (pReq->numOfFuncs != (int32_t)taosArrayGetSize(pReq->pFuncNames)) return -1;
  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char *fname = taosArrayGet(pReq->pFuncNames, i);
    if (tEncodeCStr(&encoder, fname) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveFuncReq(void *buf, int32_t bufLen, SRetrieveFuncReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfFuncs) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pReq->ignoreCodeComment) < 0) return -1;

  pReq->pFuncNames = taosArrayInit(pReq->numOfFuncs, TSDB_FUNC_NAME_LEN);
  if (pReq->pFuncNames == NULL) return -1;

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char fname[TSDB_FUNC_NAME_LEN] = {0};
    if (tDecodeCStrTo(&decoder, fname) < 0) return -1;
    taosArrayPush(pReq->pFuncNames, fname);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSRetrieveFuncReq(SRetrieveFuncReq *pReq) { taosArrayDestroy(pReq->pFuncNames); }

int32_t tSerializeSRetrieveFuncRsp(void *buf, int32_t bufLen, SRetrieveFuncRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfFuncs) < 0) return -1;

  if (pRsp->numOfFuncs != (int32_t)taosArrayGetSize(pRsp->pFuncInfos)) return -1;
  for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
    SFuncInfo *pInfo = taosArrayGet(pRsp->pFuncInfos, i);

    if (tEncodeCStr(&encoder, pInfo->name) < 0) return -1;
    if (tEncodeI8(&encoder, pInfo->funcType) < 0) return -1;
    if (tEncodeI8(&encoder, pInfo->scriptType) < 0) return -1;
    if (tEncodeI8(&encoder, pInfo->outputType) < 0) return -1;
    if (tEncodeI32(&encoder, pInfo->outputLen) < 0) return -1;
    if (tEncodeI32(&encoder, pInfo->bufSize) < 0) return -1;
    if (tEncodeI64(&encoder, pInfo->signature) < 0) return -1;
    if (tEncodeI32(&encoder, pInfo->codeSize) < 0) return -1;
    if (tEncodeI32(&encoder, pInfo->commentSize) < 0) return -1;
    if (pInfo->codeSize) {
      if (tEncodeBinary(&encoder, pInfo->pCode, pInfo->codeSize) < 0) return -1;
    }
    if (pInfo->commentSize) {
      if (tEncodeCStr(&encoder, pInfo->pComment) < 0) return -1;
    }
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveFuncRsp(void *buf, int32_t bufLen, SRetrieveFuncRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfFuncs) < 0) return -1;

  pRsp->pFuncInfos = taosArrayInit(pRsp->numOfFuncs, sizeof(SFuncInfo));
  if (pRsp->pFuncInfos == NULL) return -1;

  for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
    SFuncInfo fInfo = {0};
    if (tDecodeCStrTo(&decoder, fInfo.name) < 0) return -1;
    if (tDecodeI8(&decoder, &fInfo.funcType) < 0) return -1;
    if (tDecodeI8(&decoder, &fInfo.scriptType) < 0) return -1;
    if (tDecodeI8(&decoder, &fInfo.outputType) < 0) return -1;
    if (tDecodeI32(&decoder, &fInfo.outputLen) < 0) return -1;
    if (tDecodeI32(&decoder, &fInfo.bufSize) < 0) return -1;
    if (tDecodeI64(&decoder, &fInfo.signature) < 0) return -1;
    if (tDecodeI32(&decoder, &fInfo.codeSize) < 0) return -1;
    if (tDecodeI32(&decoder, &fInfo.commentSize) < 0) return -1;
    if (fInfo.codeSize) {
      fInfo.pCode = taosMemoryCalloc(1, fInfo.codeSize);
      if (fInfo.pCode == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      if (tDecodeCStrTo(&decoder, fInfo.pCode) < 0) return -1;
    }
    if (fInfo.commentSize) {
      fInfo.pComment = taosMemoryCalloc(1, fInfo.commentSize);
      if (fInfo.pComment == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      if (tDecodeCStrTo(&decoder, fInfo.pComment) < 0) return -1;
    }

    taosArrayPush(pRsp->pFuncInfos, &fInfo);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSFuncInfo(SFuncInfo *pInfo) {
  if (NULL == pInfo) {
    return;
  }

  taosMemoryFree(pInfo->pCode);
  taosMemoryFree(pInfo->pComment);
}

void tFreeSRetrieveFuncRsp(SRetrieveFuncRsp *pRsp) {
  int32_t size = taosArrayGetSize(pRsp->pFuncInfos);
  for (int32_t i = 0; i < size; ++i) {
    SFuncInfo *pInfo = taosArrayGet(pRsp->pFuncInfos, i);
    tFreeSFuncInfo(pInfo);
  }
  taosArrayDestroy(pRsp->pFuncInfos);
}

int32_t tSerializeSCreateDbReq(void *buf, int32_t bufLen, SCreateDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfVgroups) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfStables) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->schemaless) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreExist) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    if (tEncodeI64(&encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI64(&encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->keepUnit) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDbReq(void *buf, int32_t bufLen, SCreateDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfVgroups) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfStables) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->schemaless) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->ignoreExist) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfRetensions) < 0) return -1;
  pReq->pRetensions = taosArrayInit(pReq->numOfRetensions, sizeof(SRetention));
  if (pReq->pRetensions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention rentension = {0};
    if (tDecodeI64(&decoder, &rentension.freq) < 0) return -1;
    if (tDecodeI64(&decoder, &rentension.keep) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.freqUnit) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.keepUnit) < 0) return -1;
    if (taosArrayPush(pReq->pRetensions, &rentension) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCreateDbReq(SCreateDbReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
}

int32_t tSerializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->ignoreNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDbRsp(void *buf, int32_t bufLen, SDropDbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->db) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->uid) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDbRsp(void *buf, int32_t bufLen, SDropDbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->uid) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfTable) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfTable) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQnodeListReq(void *buf, int32_t bufLen, SQnodeListReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->rowNum) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeListReq(void *buf, int32_t bufLen, SQnodeListReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->rowNum) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  int32_t num = taosArrayGetSize(pRsp->addrsList);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SQueryNodeAddr *addr = taosArrayGet(pRsp->addrsList, i);
    if (tEncodeSQueryNodeAddr(&encoder, addr) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (NULL == pRsp->addrsList) {
    pRsp->addrsList = taosArrayInit(num, sizeof(SQueryNodeAddr));
    if (NULL == pRsp->addrsList) return -1;
  }

  for (int32_t i = 0; i < num; ++i) {
    SQueryNodeAddr addr = {0};
    if (tDecodeSQueryNodeAddr(&decoder, &addr) < 0) return -1;
    taosArrayPush(pRsp->addrsList, &addr);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSQnodeListRsp(SQnodeListRsp *pRsp) { taosArrayDestroy(pRsp->addrsList); }

int32_t tSerializeSCompactDbReq(void *buf, int32_t bufLen, SCompactDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactDbReq(void *buf, int32_t bufLen, SCompactDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUseDbRspImp(SEncoder *pEncoder, const SUseDbRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->db) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->uid) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgVersion) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgNum) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->hashMethod) < 0) return -1;

  for (int32_t i = 0; i < pRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(pRsp->pVgroupInfos, i);
    if (tEncodeI32(pEncoder, pVgInfo->vgId) < 0) return -1;
    if (tEncodeU32(pEncoder, pVgInfo->hashBegin) < 0) return -1;
    if (tEncodeU32(pEncoder, pVgInfo->hashEnd) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pVgInfo->epSet) < 0) return -1;
    if (tEncodeI32(pEncoder, pVgInfo->numOfTable) < 0) return -1;
  }

  return 0;
}

int32_t tSerializeSUseDbRsp(void *buf, int32_t bufLen, const SUseDbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tSerializeSUseDbRspImp(&encoder, pRsp) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSUseDbBatchRsp(void *buf, int32_t bufLen, SUseDbBatchRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tEncodeI32(&encoder, numOfBatch) < 0) return -1;
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SUseDbRsp *pUsedbRsp = taosArrayGet(pRsp->pArray, i);
    if (tSerializeSUseDbRspImp(&encoder, pUsedbRsp) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUseDbRspImp(SDecoder *pDecoder, SUseDbRsp *pRsp) {
  if (tDecodeCStrTo(pDecoder, pRsp->db) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->uid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->vgVersion) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->vgNum) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->hashMethod) < 0) return -1;

  if (pRsp->vgNum <= 0) {
    return 0;
  }

  pRsp->pVgroupInfos = taosArrayInit(pRsp->vgNum, sizeof(SVgroupInfo));
  if (pRsp->pVgroupInfos == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pRsp->vgNum; ++i) {
    SVgroupInfo vgInfo = {0};
    if (tDecodeI32(pDecoder, &vgInfo.vgId) < 0) return -1;
    if (tDecodeU32(pDecoder, &vgInfo.hashBegin) < 0) return -1;
    if (tDecodeU32(pDecoder, &vgInfo.hashEnd) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &vgInfo.epSet) < 0) return -1;
    if (tDecodeI32(pDecoder, &vgInfo.numOfTable) < 0) return -1;
    taosArrayPush(pRsp->pVgroupInfos, &vgInfo);
  }

  return 0;
}

int32_t tDeserializeSUseDbRsp(void *buf, int32_t bufLen, SUseDbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDeserializeSUseDbRspImp(&decoder, pRsp) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tDeserializeSUseDbBatchRsp(void *buf, int32_t bufLen, SUseDbBatchRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tDecodeI32(&decoder, &numOfBatch) < 0) return -1;

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(SUseDbRsp));
  if (pRsp->pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfBatch; ++i) {
    SUseDbRsp usedbRsp = {0};
    if (tDeserializeSUseDbRspImp(&decoder, &usedbRsp) < 0) return -1;
    taosArrayPush(pRsp->pArray, &usedbRsp);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSUsedbRsp(SUseDbRsp *pRsp) { taosArrayDestroy(pRsp->pVgroupInfos); }

void tFreeSUseDbBatchRsp(SUseDbBatchRsp *pRsp) {
  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SUseDbRsp *pUsedbRsp = taosArrayGet(pRsp->pArray, i);
    tFreeSUsedbRsp(pUsedbRsp);
  }

  taosArrayDestroy(pRsp->pArray);
}

int32_t tSerializeSUserAuthBatchRsp(void *buf, int32_t bufLen, SUserAuthBatchRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tEncodeI32(&encoder, numOfBatch) < 0) return -1;
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SGetUserAuthRsp *pUserAuthRsp = taosArrayGet(pRsp->pArray, i);
    if (tSerializeSGetUserAuthRspImpl(&encoder, pUserAuthRsp) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserAuthBatchRsp(void *buf, int32_t bufLen, SUserAuthBatchRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tDecodeI32(&decoder, &numOfBatch) < 0) return -1;

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(SGetUserAuthRsp));
  if (pRsp->pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfBatch; ++i) {
    SGetUserAuthRsp rsp = {0};
    if (tDeserializeSGetUserAuthRspImpl(&decoder, &rsp) < 0) return -1;
    taosArrayPush(pRsp->pArray, &rsp);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSUserAuthBatchRsp(SUserAuthBatchRsp *pRsp) {
  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SGetUserAuthRsp *pUserAuthRsp = taosArrayGet(pRsp->pArray, i);
    tFreeSGetUserAuthRsp(pUserAuthRsp);
  }

  taosArrayDestroy(pRsp->pArray);
}

int32_t tSerializeSDbCfgReq(void *buf, int32_t bufLen, SDbCfgReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgReq(void *buf, int32_t bufLen, SDbCfgReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDbCfgRsp(void *buf, int32_t bufLen, const SDbCfgRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfVgroups) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfStables) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->fsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->replications) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->cacheLastRow) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pRsp->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pRsp->pRetensions, i);
    if (tEncodeI64(&encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI64(&encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->keepUnit) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgRsp(void *buf, int32_t bufLen, SDbCfgRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfVgroups) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfStables) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->fsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->replications) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->cacheLastRow) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfRetensions) < 0) return -1;
  pRsp->pRetensions = taosArrayInit(pRsp->numOfRetensions, sizeof(SRetention));
  if (pRsp->pRetensions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pRsp->numOfRetensions; ++i) {
    SRetention rentension = {0};
    if (tDecodeI64(&decoder, &rentension.freq) < 0) return -1;
    if (tDecodeI64(&decoder, &rentension.keep) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.freqUnit) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.keepUnit) < 0) return -1;
    if (taosArrayPush(pRsp->pRetensions, &rentension) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUserIndexReq(void *buf, int32_t bufLen, SUserIndexReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->indexFName) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserIndexReq(void *buf, int32_t bufLen, SUserIndexReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->indexFName) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUserIndexRsp(void *buf, int32_t bufLen, const SUserIndexRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->tblFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->colName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->indexType) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->indexExts) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserIndexRsp(void *buf, int32_t bufLen, SUserIndexRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->tblFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->colName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->indexType) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->indexExts) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->type) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->payloadLen) < 0) return -1;
  if (pReq->payloadLen > 0) {
    if (tEncodeBinary(&encoder, pReq->payload, pReq->payloadLen) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->type) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->payloadLen) < 0) return -1;
  if (pReq->payloadLen > 0) {
    pReq->payload = taosMemoryMalloc(pReq->payloadLen);
    if (pReq->payload == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->payload) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSShowReq(SShowReq *pReq) { taosMemoryFreeClear(pReq->payload); }

int32_t tSerializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->showId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->tb) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->showId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->tb) < 0) return -1;
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

static int32_t tEncodeSTableMetaRsp(SEncoder *pEncoder, STableMetaRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->tbName) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->stbName) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->dbId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->numOfTags) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->numOfColumns) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->precision) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->tableType) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->sversion) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->tversion) < 0) return -1;
  if (tEncodeU64(pEncoder, pRsp->suid) < 0) return -1;
  if (tEncodeU64(pEncoder, pRsp->tuid) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgId) < 0) return -1;
  for (int32_t i = 0; i < pRsp->numOfColumns + pRsp->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    if (tEncodeSSchema(pEncoder, pSchema) < 0) return -1;
  }

  return 0;
}

static int32_t tDecodeSTableMetaRsp(SDecoder *pDecoder, STableMetaRsp *pRsp) {
  if (tDecodeCStrTo(pDecoder, pRsp->tbName) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pRsp->stbName) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->dbId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->numOfTags) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->numOfColumns) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->precision) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->tableType) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->sversion) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->tversion) < 0) return -1;
  if (tDecodeU64(pDecoder, &pRsp->suid) < 0) return -1;
  if (tDecodeU64(pDecoder, &pRsp->tuid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->vgId) < 0) return -1;

  int32_t totalCols = pRsp->numOfTags + pRsp->numOfColumns;
  pRsp->pSchemas = taosMemoryMalloc(sizeof(SSchema) * totalCols);
  if (pRsp->pSchemas == NULL) return -1;

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    if (tDecodeSSchema(pDecoder, pSchema) < 0) return -1;
  }

  return 0;
}

int32_t tSerializeSTableMetaRsp(void *buf, int32_t bufLen, STableMetaRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSTableMetaRsp(&encoder, pRsp) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSTableMetaBatchRsp(void *buf, int32_t bufLen, STableMetaBatchRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tEncodeI32(&encoder, numOfBatch) < 0) return -1;
  for (int32_t i = 0; i < numOfBatch; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pArray, i);
    if (tEncodeSTableMetaRsp(&encoder, pMetaRsp) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableMetaRsp(void *buf, int32_t bufLen, STableMetaRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSTableMetaRsp(&decoder, pRsp) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tDeserializeSTableMetaBatchRsp(void *buf, int32_t bufLen, STableMetaBatchRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tDecodeI32(&decoder, &numOfBatch) < 0) return -1;

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(STableMetaRsp));
  if (pRsp->pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfBatch; ++i) {
    STableMetaRsp tableMetaRsp = {0};
    if (tDecodeSTableMetaRsp(&decoder, &tableMetaRsp) < 0) return -1;
    taosArrayPush(pRsp->pArray, &tableMetaRsp);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSTableMetaRsp(STableMetaRsp *pRsp) { taosMemoryFreeClear(pRsp->pSchemas); }

void tFreeSTableMetaBatchRsp(STableMetaBatchRsp *pRsp) {
  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  for (int32_t i = 0; i < numOfBatch; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pArray, i);
    tFreeSTableMetaRsp(pMetaRsp);
  }

  taosArrayDestroy(pRsp->pArray);
}

int32_t tSerializeSShowRsp(void *buf, int32_t bufLen, SShowRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->showId) < 0) return -1;
  if (tEncodeSTableMetaRsp(&encoder, &pRsp->tableMeta) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowRsp(void *buf, int32_t bufLen, SShowRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->showId) < 0) return -1;
  if (tDecodeSTableMetaRsp(&decoder, &pRsp->tableMeta) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSShowRsp(SShowRsp *pRsp) { tFreeSTableMetaRsp(&pRsp->tableMeta); }

int32_t tSerializeSTableInfoReq(void *buf, int32_t bufLen, STableInfoReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->tbName) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  if (buf != NULL) {
    SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
    pHead->vgId = htonl(pReq->header.vgId);
    pHead->contLen = htonl(tlen + headLen);
  }

  return tlen + headLen;
}

int32_t tDeserializeSTableInfoReq(void *buf, int32_t bufLen, STableInfoReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dbFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->tbName) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMDropTopicReq(void *buf, int32_t bufLen, SMDropTopicReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropTopicReq(void *buf, int32_t bufLen, SMDropTopicReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMDropCgroupReq(void *buf, int32_t bufLen, SMDropCgroupReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->topic) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->cgroup) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropCgroupReq(void *buf, int32_t bufLen, SMDropCgroupReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->topic) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->cgroup) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCMCreateTopicReq(void *buf, int32_t bufLen, const SCMCreateTopicReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;
  if (pReq->sql != NULL) sqlLen = (int32_t)strlen(pReq->sql);
  if (pReq->ast != NULL) astLen = (int32_t)strlen(pReq->ast);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->withTbName) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->withSchema) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->withTag) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->subscribeDbName) < 0) return -1;
  if (tEncodeI32(&encoder, sqlLen) < 0) return -1;
  if (tEncodeI32(&encoder, astLen) < 0) return -1;
  if (sqlLen > 0 && tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (astLen > 0 && tEncodeCStr(&encoder, pReq->ast) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateTopicReq(void *buf, int32_t bufLen, SCMCreateTopicReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->withTbName) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->withSchema) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->withTag) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->subscribeDbName) < 0) return -1;
  if (tDecodeI32(&decoder, &sqlLen) < 0) return -1;
  if (tDecodeI32(&decoder, &astLen) < 0) return -1;

  if (sqlLen > 0) {
    pReq->sql = taosMemoryCalloc(1, sqlLen + 1);
    if (pReq->sql == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->sql) < 0) return -1;
  }

  if (astLen > 0) {
    pReq->ast = taosMemoryCalloc(1, astLen + 1);
    if (pReq->ast == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->ast) < 0) return -1;
  } else {
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCMCreateTopicReq(SCMCreateTopicReq *pReq) {
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
}

int32_t tSerializeSCMCreateTopicRsp(void *buf, int32_t bufLen, const SCMCreateTopicRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->topicId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateTopicRsp(void *buf, int32_t bufLen, SCMCreateTopicRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->topicId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSConnectReq(void *buf, int32_t bufLen, SConnectReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->connType) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pid) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->app) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->passwd) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->startTime) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConnectReq(void *buf, int32_t bufLen, SConnectReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->connType) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pid) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->app) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->passwd) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->startTime) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSConnectRsp(void *buf, int32_t bufLen, SConnectRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->acctId) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->clusterId) < 0) return -1;
  if (tEncodeU32(&encoder, pRsp->connId) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->dnodeNum) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->superUser) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->connType) < 0) return -1;
  if (tEncodeSEpSet(&encoder, &pRsp->epSet) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->sVersion) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConnectRsp(void *buf, int32_t bufLen, SConnectRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->acctId) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->clusterId) < 0) return -1;
  if (tDecodeU32(&decoder, &pRsp->connId) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->dnodeNum) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->superUser) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->connType) < 0) return -1;
  if (tDecodeSEpSet(&decoder, &pRsp->epSet) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->sVersion) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMTimerMsg(void *buf, int32_t bufLen, SMTimerReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->reserved) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMTimerMsg(void *buf, int32_t bufLen, SMTimerReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->reserved) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tEncodeSReplica(SEncoder *pEncoder, SReplica *pReplica) {
  if (tEncodeI32(pEncoder, pReplica->id) < 0) return -1;
  if (tEncodeU16(pEncoder, pReplica->port) < 0) return -1;
  if (tEncodeCStr(pEncoder, pReplica->fqdn) < 0) return -1;
  return 0;
}

int32_t tDecodeSReplica(SDecoder *pDecoder, SReplica *pReplica) {
  if (tDecodeI32(pDecoder, &pReplica->id) < 0) return -1;
  if (tDecodeU16(pDecoder, &pReplica->port) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pReplica->fqdn) < 0) return -1;
  return 0;
}

int32_t tSerializeSCreateVnodeReq(void *buf, int32_t bufLen, SCreateVnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfStables) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->hashBegin) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->hashEnd) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->hashMethod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->selfIndex) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    if (tEncodeI64(&encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI64(&encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->keepUnit) < 0) return -1;
  }

  if (tEncodeI8(&encoder, pReq->isTsma) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateVnodeReq(void *buf, int32_t bufLen, SCreateVnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfStables) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->hashBegin) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->hashEnd) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->hashMethod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replica) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->selfIndex) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
  }

  if (tDecodeI32(&decoder, &pReq->numOfRetensions) < 0) return -1;
  pReq->pRetensions = taosArrayInit(pReq->numOfRetensions, sizeof(SRetention));
  if (pReq->pRetensions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention rentension = {0};
    if (tDecodeI64(&decoder, &rentension.freq) < 0) return -1;
    if (tDecodeI64(&decoder, &rentension.keep) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.freqUnit) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.keepUnit) < 0) return -1;
    if (taosArrayPush(pReq->pRetensions, &rentension) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (tDecodeI8(&decoder, &pReq->isTsma) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tFreeSCreateVnodeReq(SCreateVnodeReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
  return 0;
}

int32_t tSerializeSDropVnodeReq(void *buf, int32_t bufLen, SDropVnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropVnodeReq(void *buf, int32_t bufLen, SDropVnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCompactVnodeReq(void *buf, int32_t bufLen, SCompactVnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactVnodeReq(void *buf, int32_t bufLen, SCompactVnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAlterVnodeReq(void *buf, int32_t bufLen, SAlterVnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->selfIndex) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeReq(void *buf, int32_t bufLen, SAlterVnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replica) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->selfIndex) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->connId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->queryId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->connId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->queryId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->connId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->connId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillTransReq(void *buf, int32_t bufLen, SKillTransReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->transId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillTransReq(void *buf, int32_t bufLen, SKillTransReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->transId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAuthReq(void *buf, int32_t bufLen, SAuthReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->spi) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->encrypt) < 0) return -1;
  if (tEncodeBinary(&encoder, pReq->secret, TSDB_PASSWORD_LEN) < 0) return -1;
  if (tEncodeBinary(&encoder, pReq->ckey, TSDB_PASSWORD_LEN) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAuthReq(void *buf, int32_t bufLen, SAuthReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->spi) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->encrypt) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->secret) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->ckey) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSServerStatusRsp(void *buf, int32_t bufLen, SServerStatusRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->statusCode) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->details) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSServerStatusRsp(void *buf, int32_t bufLen, SServerStatusRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->statusCode) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->details) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tEncodeSMqOffset(SEncoder *encoder, const SMqOffset *pOffset) {
  if (tEncodeI32(encoder, pOffset->vgId) < 0) return -1;
  if (tEncodeI64(encoder, pOffset->offset) < 0) return -1;
  if (tEncodeCStr(encoder, pOffset->topicName) < 0) return -1;
  if (tEncodeCStr(encoder, pOffset->cgroup) < 0) return -1;
  return encoder->pos;
}

int32_t tDecodeSMqOffset(SDecoder *decoder, SMqOffset *pOffset) {
  if (tDecodeI32(decoder, &pOffset->vgId) < 0) return -1;
  if (tDecodeI64(decoder, &pOffset->offset) < 0) return -1;
  if (tDecodeCStrTo(decoder, pOffset->topicName) < 0) return -1;
  if (tDecodeCStrTo(decoder, pOffset->cgroup) < 0) return -1;
  return 0;
}

int32_t tEncodeSMqCMCommitOffsetReq(SEncoder *encoder, const SMqCMCommitOffsetReq *pReq) {
  if (tStartEncode(encoder) < 0) return -1;
  if (tEncodeI32(encoder, pReq->num) < 0) return -1;
  for (int32_t i = 0; i < pReq->num; i++) {
    tEncodeSMqOffset(encoder, &pReq->offsets[i]);
  }
  tEndEncode(encoder);
  return encoder->pos;
}

int32_t tDecodeSMqCMCommitOffsetReq(SDecoder *decoder, SMqCMCommitOffsetReq *pReq) {
  if (tStartDecode(decoder) < 0) return -1;
  if (tDecodeI32(decoder, &pReq->num) < 0) return -1;
  pReq->offsets = (SMqOffset *)tDecoderMalloc(decoder, sizeof(SMqOffset) * pReq->num);
  if (pReq->offsets == NULL) return -1;
  for (int32_t i = 0; i < pReq->num; i++) {
    tDecodeSMqOffset(decoder, &pReq->offsets[i]);
  }
  tEndDecode(decoder);
  return 0;
}

int32_t tSerializeSExplainRsp(void *buf, int32_t bufLen, SExplainRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfPlans) < 0) return -1;
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    SExplainExecInfo *info = &pRsp->subplanInfo[i];
    if (tEncodeDouble(&encoder, info->startupCost) < 0) return -1;
    if (tEncodeDouble(&encoder, info->totalCost) < 0) return -1;
    if (tEncodeU64(&encoder, info->numOfRows) < 0) return -1;
    if (tEncodeU32(&encoder, info->verboseLen) < 0) return -1;
    if (tEncodeBinary(&encoder, info->verboseInfo, info->verboseLen) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSExplainRsp(void *buf, int32_t bufLen, SExplainRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfPlans) < 0) return -1;
  if (pRsp->numOfPlans > 0) {
    pRsp->subplanInfo = taosMemoryMalloc(pRsp->numOfPlans * sizeof(SExplainExecInfo));
    if (pRsp->subplanInfo == NULL) return -1;
  }
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    if (tDecodeDouble(&decoder, &pRsp->subplanInfo[i].startupCost) < 0) return -1;
    if (tDecodeDouble(&decoder, &pRsp->subplanInfo[i].totalCost) < 0) return -1;
    if (tDecodeU64(&decoder, &pRsp->subplanInfo[i].numOfRows) < 0) return -1;
    if (tDecodeU32(&decoder, &pRsp->subplanInfo[i].verboseLen) < 0) return -1;
    if (tDecodeBinary(&decoder, (uint8_t **)&pRsp->subplanInfo[i].verboseInfo, &pRsp->subplanInfo[i].verboseLen) < 0)
      return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSSchedulerHbReq(void *buf, int32_t bufLen, SSchedulerHbReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->sId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->epId.nodeId) < 0) return -1;
  if (tEncodeU16(&encoder, pReq->epId.ep.port) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->epId.ep.fqdn) < 0) return -1;
  if (pReq->taskAction) {
    int32_t num = taosArrayGetSize(pReq->taskAction);
    if (tEncodeI32(&encoder, num) < 0) return -1;
    for (int32_t i = 0; i < num; ++i) {
      STaskAction *action = taosArrayGet(pReq->taskAction, i);
      if (tEncodeU64(&encoder, action->queryId) < 0) return -1;
      if (tEncodeU64(&encoder, action->taskId) < 0) return -1;
      if (tEncodeI8(&encoder, action->action) < 0) return -1;
    }
  } else {
    if (tEncodeI32(&encoder, 0) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  if (buf != NULL) {
    SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
    pHead->vgId = htonl(pReq->header.vgId);
    pHead->contLen = htonl(tlen + headLen);
  }

  return tlen + headLen;
}

int32_t tDeserializeSSchedulerHbReq(void *buf, int32_t bufLen, SSchedulerHbReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->sId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->epId.nodeId) < 0) return -1;
  if (tDecodeU16(&decoder, &pReq->epId.ep.port) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->epId.ep.fqdn) < 0) return -1;
  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (num > 0) {
    pReq->taskAction = taosArrayInit(num, sizeof(STaskStatus));
    if (NULL == pReq->taskAction) return -1;
    for (int32_t i = 0; i < num; ++i) {
      STaskAction action = {0};
      if (tDecodeU64(&decoder, &action.queryId) < 0) return -1;
      if (tDecodeU64(&decoder, &action.taskId) < 0) return -1;
      if (tDecodeI8(&decoder, &action.action) < 0) return -1;
      taosArrayPush(pReq->taskAction, &action);
    }
  } else {
    pReq->taskAction = NULL;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSSchedulerHbReq(SSchedulerHbReq *pReq) { taosArrayDestroy(pReq->taskAction); }

int32_t tSerializeSSchedulerHbRsp(void *buf, int32_t bufLen, SSchedulerHbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->epId.nodeId) < 0) return -1;
  if (tEncodeU16(&encoder, pRsp->epId.ep.port) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->epId.ep.fqdn) < 0) return -1;
  if (pRsp->taskStatus) {
    int32_t num = taosArrayGetSize(pRsp->taskStatus);
    if (tEncodeI32(&encoder, num) < 0) return -1;
    for (int32_t i = 0; i < num; ++i) {
      STaskStatus *status = taosArrayGet(pRsp->taskStatus, i);
      if (tEncodeU64(&encoder, status->queryId) < 0) return -1;
      if (tEncodeU64(&encoder, status->taskId) < 0) return -1;
      if (tEncodeI64(&encoder, status->refId) < 0) return -1;
      if (tEncodeI8(&encoder, status->status) < 0) return -1;
    }
  } else {
    if (tEncodeI32(&encoder, 0) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSchedulerHbRsp(void *buf, int32_t bufLen, SSchedulerHbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->epId.nodeId) < 0) return -1;
  if (tDecodeU16(&decoder, &pRsp->epId.ep.port) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->epId.ep.fqdn) < 0) return -1;
  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (num > 0) {
    pRsp->taskStatus = taosArrayInit(num, sizeof(STaskStatus));
    if (NULL == pRsp->taskStatus) return -1;
    for (int32_t i = 0; i < num; ++i) {
      STaskStatus status = {0};
      if (tDecodeU64(&decoder, &status.queryId) < 0) return -1;
      if (tDecodeU64(&decoder, &status.taskId) < 0) return -1;
      if (tDecodeI64(&decoder, &status.refId) < 0) return -1;
      if (tDecodeI8(&decoder, &status.status) < 0) return -1;
      taosArrayPush(pRsp->taskStatus, &status);
    }
  } else {
    pRsp->taskStatus = NULL;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSSchedulerHbRsp(SSchedulerHbRsp *pRsp) { taosArrayDestroy(pRsp->taskStatus); }

int32_t tSerializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->code) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->code) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVCreateTbBatchRsp(void *buf, int32_t bufLen, SVCreateTbBatchRsp *pRsp) {
  // SEncoder encoder = {0};
  // tEncoderInit(&encoder, buf, bufLen);

  // if (tStartEncode(&encoder) < 0) return -1;
  // if (pRsp->rspList) {
  //   int32_t num = taosArrayGetSize(pRsp->rspList);
  //   if (tEncodeI32(&encoder, num) < 0) return -1;
  //   for (int32_t i = 0; i < num; ++i) {
  //     SVCreateTbRsp *rsp = taosArrayGet(pRsp->rspList, i);
  //     if (tEncodeI32(&encoder, rsp->code) < 0) return -1;
  //   }
  // } else {
  //   if (tEncodeI32(&encoder, 0) < 0) return -1;
  // }
  // tEndEncode(&encoder);

  // int32_t tlen = encoder.pos;
  // tEncoderClear(&encoder);
  // reture tlen;
  return 0;
}

int32_t tDeserializeSVCreateTbBatchRsp(void *buf, int32_t bufLen, SVCreateTbBatchRsp *pRsp) {
  // SDecoder  decoder = {0};
  // int32_t num = 0;
  // tDecoderInit(&decoder, buf, bufLen);

  // if (tStartDecode(&decoder) < 0) return -1;
  // if (tDecodeI32(&decoder, &num) < 0) return -1;
  // if (num > 0) {
  //   pRsp->rspList = taosArrayInit(num, sizeof(SVCreateTbRsp));
  //   if (NULL == pRsp->rspList) return -1;
  //   for (int32_t i = 0; i < num; ++i) {
  //     SVCreateTbRsp rsp = {0};
  //     if (tDecodeI32(&decoder, &rsp.code) < 0) return -1;
  //     if (NULL == taosArrayPush(pRsp->rspList, &rsp)) return -1;
  //   }
  // } else {
  //   pRsp->rspList = NULL;
  // }
  // tEndDecode(&decoder);

  // tDecoderClear(&decoder);
  return 0;
}

int tEncodeSVCreateTbBatchRsp(SEncoder *pCoder, const SVCreateTbBatchRsp *pRsp) {
  int32_t        nRsps = taosArrayGetSize(pRsp->pArray);
  SVCreateTbRsp *pCreateRsp;

  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, nRsps) < 0) return -1;
  for (int32_t i = 0; i < nRsps; i++) {
    pCreateRsp = taosArrayGet(pRsp->pArray, i);
    if (tEncodeSVCreateTbRsp(pCoder, pCreateRsp) < 0) return -1;
  }

  tEndEncode(pCoder);

  return 0;
}

int tDecodeSVCreateTbBatchRsp(SDecoder *pCoder, SVCreateTbBatchRsp *pRsp) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pRsp->nRsps) < 0) return -1;
  pRsp->pRsps = (SVCreateTbRsp *)tDecoderMalloc(pCoder, sizeof(*pRsp->pRsps) * pRsp->nRsps);
  for (int32_t i = 0; i < pRsp->nRsps; i++) {
    if (tDecodeSVCreateTbRsp(pCoder, pRsp->pRsps + i) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeTSma(SEncoder *pCoder, const STSma *pSma) {
  if (tEncodeI8(pCoder, pSma->version) < 0) return -1;
  if (tEncodeI8(pCoder, pSma->intervalUnit) < 0) return -1;
  if (tEncodeI8(pCoder, pSma->slidingUnit) < 0) return -1;
  if (tEncodeI8(pCoder, pSma->timezoneInt) < 0) return -1;
  if (tEncodeCStr(pCoder, pSma->indexName) < 0) return -1;
  if (tEncodeI32(pCoder, pSma->exprLen) < 0) return -1;
  if (tEncodeI32(pCoder, pSma->tagsFilterLen) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->indexUid) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->tableUid) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->interval) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->offset) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->sliding) < 0) return -1;
  if (pSma->exprLen > 0) {
    if (tEncodeCStr(pCoder, pSma->expr) < 0) return -1;
  }
  if (pSma->tagsFilterLen > 0) {
    if (tEncodeCStr(pCoder, pSma->tagsFilter) < 0) return -1;
  }

  return 0;
}

int32_t tDecodeTSma(SDecoder *pCoder, STSma *pSma) {
  if (tDecodeI8(pCoder, &pSma->version) < 0) return -1;
  if (tDecodeI8(pCoder, &pSma->intervalUnit) < 0) return -1;
  if (tDecodeI8(pCoder, &pSma->slidingUnit) < 0) return -1;
  if (tDecodeI8(pCoder, &pSma->timezoneInt) < 0) return -1;
  if (tDecodeCStrTo(pCoder, pSma->indexName) < 0) return -1;
  if (tDecodeI32(pCoder, &pSma->exprLen) < 0) return -1;
  if (tDecodeI32(pCoder, &pSma->tagsFilterLen) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->indexUid) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->tableUid) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->interval) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->offset) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->sliding) < 0) return -1;
  if (pSma->exprLen > 0) {
    if (tDecodeCStr(pCoder, &pSma->expr) < 0) return -1;
  } else {
    pSma->expr = NULL;
  }
  if (pSma->tagsFilterLen > 0) {
    if (tDecodeCStr(pCoder, &pSma->tagsFilter) < 0) return -1;
  } else {
    pSma->tagsFilter = NULL;
  }

  return 0;
}

int32_t tEncodeSVCreateTSmaReq(SEncoder *pCoder, const SVCreateTSmaReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  tEncodeTSma(pCoder, pReq);

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVCreateTSmaReq(SDecoder *pCoder, SVCreateTSmaReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  tDecodeTSma(pCoder, pReq);

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropTSmaReq(SEncoder *pCoder, const SVDropTSmaReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI64(pCoder, pReq->indexUid) < 0) return -1;
  if (tEncodeCStr(pCoder, pReq->indexName) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropTSmaReq(SDecoder *pCoder, SVDropTSmaReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pReq->indexUid) < 0) return -1;
  if (tDecodeCStrTo(pCoder, pReq->indexName) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

int32_t tSerializeSCMCreateStreamReq(void *buf, int32_t bufLen, const SCMCreateStreamReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;
  if (pReq->sql != NULL) sqlLen = (int32_t)strlen(pReq->sql);
  if (pReq->ast != NULL) astLen = (int32_t)strlen(pReq->ast);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sourceDB) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->targetStbFullName) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI32(&encoder, sqlLen) < 0) return -1;
  if (tEncodeI32(&encoder, astLen) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->triggerType) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->watermark) < 0) return -1;
  if (sqlLen > 0 && tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (astLen > 0 && tEncodeCStr(&encoder, pReq->ast) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateStreamReq(void *buf, int32_t bufLen, SCMCreateStreamReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->sourceDB) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->targetStbFullName) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeI32(&decoder, &sqlLen) < 0) return -1;
  if (tDecodeI32(&decoder, &astLen) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->triggerType) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->watermark) < 0) return -1;

  if (sqlLen > 0) {
    pReq->sql = taosMemoryCalloc(1, sqlLen + 1);
    if (pReq->sql == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->sql) < 0) return -1;
  }

  if (astLen > 0) {
    pReq->ast = taosMemoryCalloc(1, astLen + 1);
    if (pReq->ast == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->ast) < 0) return -1;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCMCreateStreamReq(SCMCreateStreamReq *pReq) {
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
}

int32_t tEncodeSRSmaParam(SEncoder *pCoder, const SRSmaParam *pRSmaParam) {
  if (tEncodeFloat(pCoder, pRSmaParam->xFilesFactor) < 0) return -1;
  if (tEncodeI32v(pCoder, pRSmaParam->delay) < 0) return -1;
  if (tEncodeI32v(pCoder, pRSmaParam->qmsg1Len) < 0) return -1;
  if (tEncodeI32v(pCoder, pRSmaParam->qmsg2Len) < 0) return -1;
  if (pRSmaParam->qmsg1Len > 0) {
    if (tEncodeBinary(pCoder, pRSmaParam->qmsg1, (uint64_t)pRSmaParam->qmsg1Len) < 0)  // qmsg1Len contains len of '\0'
      return -1;
  }
  if (pRSmaParam->qmsg2Len > 0) {
    if (tEncodeBinary(pCoder, pRSmaParam->qmsg2, (uint64_t)pRSmaParam->qmsg2Len) < 0)  // qmsg2Len contains len of '\0'
      return -1;
  }

  return 0;
}

int32_t tDecodeSRSmaParam(SDecoder *pCoder, SRSmaParam *pRSmaParam) {
  if (tDecodeFloat(pCoder, &pRSmaParam->xFilesFactor) < 0) return -1;
  if (tDecodeI32v(pCoder, &pRSmaParam->delay) < 0) return -1;
  if (tDecodeI32v(pCoder, &pRSmaParam->qmsg1Len) < 0) return -1;
  if (tDecodeI32v(pCoder, &pRSmaParam->qmsg2Len) < 0) return -1;
  if (pRSmaParam->qmsg1Len > 0) {
    uint64_t len;
    if (tDecodeBinaryAlloc(pCoder, (void **)&pRSmaParam->qmsg1, &len) < 0) return -1;  // qmsg1Len contains len of '\0'
  } else {
    pRSmaParam->qmsg1 = NULL;
  }
  if (pRSmaParam->qmsg2Len > 0) {
    uint64_t len;
    if (tDecodeBinaryAlloc(pCoder, (void **)&pRSmaParam->qmsg2, &len) < 0) return -1;  // qmsg2Len contains len of '\0'
  } else {
    pRSmaParam->qmsg2 = NULL;
  }
  return 0;
}

int tEncodeSVCreateStbReq(SEncoder *pCoder, const SVCreateStbReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeCStr(pCoder, pReq->name) < 0) return -1;
  if (tEncodeI64(pCoder, pReq->suid) < 0) return -1;
  if (tEncodeI8(pCoder, pReq->rollup) < 0) return -1;
  if (tEncodeSSchemaWrapper(pCoder, &pReq->schemaRow) < 0) return -1;
  if (tEncodeSSchemaWrapper(pCoder, &pReq->schemaTag) < 0) return -1;
  if (pReq->rollup) {
    if (tEncodeSRSmaParam(pCoder, &pReq->pRSmaParam) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateStbReq(SDecoder *pCoder, SVCreateStbReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->suid) < 0) return -1;
  if (tDecodeI8(pCoder, &pReq->rollup) < 0) return -1;
  if (tDecodeSSchemaWrapper(pCoder, &pReq->schemaRow) < 0) return -1;
  if (tDecodeSSchemaWrapper(pCoder, &pReq->schemaTag) < 0) return -1;
  if (pReq->rollup) {
    if (tDecodeSRSmaParam(pCoder, &pReq->pRSmaParam) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

STSchema *tdGetSTSChemaFromSSChema(SSchema **pSchema, int32_t nCols) {
  STSchemaBuilder schemaBuilder = {0};
  if (tdInitTSchemaBuilder(&schemaBuilder, 1) < 0) {
    return NULL;
  }

  for (int i = 0; i < nCols; i++) {
    SSchema *schema = *pSchema + i;
    if (tdAddColToSchema(&schemaBuilder, schema->type, schema->flags, schema->colId, schema->bytes) < 0) {
      tdDestroyTSchemaBuilder(&schemaBuilder);
      return NULL;
    }
  }

  STSchema *pNSchema = tdGetSchemaFromBuilder(&schemaBuilder);
  if (pNSchema == NULL) {
    tdDestroyTSchemaBuilder(&schemaBuilder);
    return NULL;
  }

  tdDestroyTSchemaBuilder(&schemaBuilder);
  return pNSchema;
}

int tEncodeSVCreateTbReq(SEncoder *pCoder, const SVCreateTbReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, pReq->flags) < 0) return -1;
  if (tEncodeI64(pCoder, pReq->uid) < 0) return -1;
  if (tEncodeI64(pCoder, pReq->ctime) < 0) return -1;

  if (tEncodeCStr(pCoder, pReq->name) < 0) return -1;
  if (tEncodeI32(pCoder, pReq->ttl) < 0) return -1;
  if (tEncodeI8(pCoder, pReq->type) < 0) return -1;

  if (pReq->type == TSDB_CHILD_TABLE) {
    if (tEncodeI64(pCoder, pReq->ctb.suid) < 0) return -1;
    if (tEncodeBinary(pCoder, pReq->ctb.pTag, kvRowLen(pReq->ctb.pTag)) < 0) return -1;
  } else if (pReq->type == TSDB_NORMAL_TABLE) {
    if (tEncodeSSchemaWrapper(pCoder, &pReq->ntb.schemaRow) < 0) return -1;
  } else {
    ASSERT(0);
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbReq(SDecoder *pCoder, SVCreateTbReq *pReq) {
  uint32_t len;

  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pReq->flags) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->uid) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->ctime) < 0) return -1;

  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeI32(pCoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI8(pCoder, &pReq->type) < 0) return -1;

  if (pReq->type == TSDB_CHILD_TABLE) {
    if (tDecodeI64(pCoder, &pReq->ctb.suid) < 0) return -1;
    if (tDecodeBinary(pCoder, &pReq->ctb.pTag, &len) < 0) return -1;
  } else if (pReq->type == TSDB_NORMAL_TABLE) {
    if (tDecodeSSchemaWrapper(pCoder, &pReq->ntb.schemaRow) < 0) return -1;
  } else {
    ASSERT(0);
  }

  tEndDecode(pCoder);
  return 0;
}

int tEncodeSVCreateTbBatchReq(SEncoder *pCoder, const SVCreateTbBatchReq *pReq) {
  int32_t nReq = taosArrayGetSize(pReq->pArray);

  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, nReq) < 0) return -1;
  for (int iReq = 0; iReq < nReq; iReq++) {
    if (tEncodeSVCreateTbReq(pCoder, (SVCreateTbReq *)taosArrayGet(pReq->pArray, iReq)) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbBatchReq(SDecoder *pCoder, SVCreateTbBatchReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pReq->nReqs) < 0) return -1;
  pReq->pReqs = (SVCreateTbReq *)tDecoderMalloc(pCoder, sizeof(SVCreateTbReq) * pReq->nReqs);
  if (pReq->pReqs == NULL) return -1;
  for (int iReq = 0; iReq < pReq->nReqs; iReq++) {
    if (tDecodeSVCreateTbReq(pCoder, pReq->pReqs + iReq) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int tEncodeSVCreateTbRsp(SEncoder *pCoder, const SVCreateTbRsp *pRsp) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32(pCoder, pRsp->code) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbRsp(SDecoder *pCoder, SVCreateTbRsp *pRsp) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32(pCoder, &pRsp->code) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

// TDMT_VND_DROP_TABLE =================
static int32_t tEncodeSVDropTbReq(SEncoder *pCoder, const SVDropTbReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeCStr(pCoder, pReq->name) < 0) return -1;
  if (tEncodeI8(pCoder, pReq->igNotExists) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSVDropTbReq(SDecoder *pCoder, SVDropTbReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeI8(pCoder, &pReq->igNotExists) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

static int32_t tEncodeSVDropTbRsp(SEncoder *pCoder, const SVDropTbRsp *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32(pCoder, pReq->code) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSVDropTbRsp(SDecoder *pCoder, SVDropTbRsp *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32(pCoder, &pReq->code) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropTbBatchReq(SEncoder *pCoder, const SVDropTbBatchReq *pReq) {
  int32_t      nReqs = taosArrayGetSize(pReq->pArray);
  SVDropTbReq *pDropTbReq;

  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, nReqs) < 0) return -1;
  for (int iReq = 0; iReq < nReqs; iReq++) {
    pDropTbReq = (SVDropTbReq *)taosArrayGet(pReq->pArray, iReq);
    if (tEncodeSVDropTbReq(pCoder, pDropTbReq) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropTbBatchReq(SDecoder *pCoder, SVDropTbBatchReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pReq->nReqs) < 0) return -1;
  pReq->pReqs = (SVDropTbReq *)tDecoderMalloc(pCoder, sizeof(SVDropTbReq) * pReq->nReqs);
  if (pReq->pReqs == NULL) return -1;
  for (int iReq = 0; iReq < pReq->nReqs; iReq++) {
    if (tDecodeSVDropTbReq(pCoder, pReq->pReqs + iReq) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropTbBatchRsp(SEncoder *pCoder, const SVDropTbBatchRsp *pRsp) {
  int32_t nRsps = taosArrayGetSize(pRsp->pArray);
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, nRsps) < 0) return -1;
  for (int iRsp = 0; iRsp < nRsps; iRsp++) {
    if (tEncodeSVDropTbRsp(pCoder, (SVDropTbRsp *)taosArrayGet(pRsp->pArray, iRsp)) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropTbBatchRsp(SDecoder *pCoder, SVDropTbBatchRsp *pRsp) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pRsp->nRsps) < 0) return -1;
  pRsp->pRsps = (SVDropTbRsp *)tDecoderMalloc(pCoder, sizeof(SVDropTbRsp) * pRsp->nRsps);
  if (pRsp->pRsps == NULL) return -1;
  for (int iRsp = 0; iRsp < pRsp->nRsps; iRsp++) {
    if (tDecodeSVDropTbRsp(pCoder, pRsp->pRsps + iRsp) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropStbReq(SEncoder *pCoder, const SVDropStbReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeCStr(pCoder, pReq->name) < 0) return -1;
  if (tEncodeI64(pCoder, pReq->suid) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropStbReq(SDecoder *pCoder, SVDropStbReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->suid) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

static int32_t tEncodeSVSubmitBlk(SEncoder *pCoder, const SVSubmitBlk *pBlock, int32_t flags) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI64(pCoder, pBlock->suid) < 0) return -1;
  if (tEncodeI64(pCoder, pBlock->uid) < 0) return -1;
  if (tEncodeI32v(pCoder, pBlock->sver) < 0) return -1;
  if (tEncodeBinary(pCoder, pBlock->pData, pBlock->nData) < 0) return -1;

  if (flags & TD_AUTO_CREATE_TABLE) {
    if (tEncodeSVCreateTbReq(pCoder, &pBlock->cTbReq) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSVSubmitBlk(SDecoder *pCoder, SVSubmitBlk *pBlock, int32_t flags) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pBlock->suid) < 0) return -1;
  if (tDecodeI64(pCoder, &pBlock->uid) < 0) return -1;
  if (tDecodeI32v(pCoder, &pBlock->sver) < 0) return -1;
  if (tDecodeBinary(pCoder, &pBlock->pData, &pBlock->nData) < 0) return -1;

  if (flags & TD_AUTO_CREATE_TABLE) {
    if (tDecodeSVCreateTbReq(pCoder, &pBlock->cTbReq) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVSubmitReq(SEncoder *pCoder, const SVSubmitReq *pReq) {
  int32_t nBlocks = taosArrayGetSize(pReq->pArray);

  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, pReq->flags) < 0) return -1;
  if (tEncodeI32v(pCoder, nBlocks) < 0) return -1;
  for (int32_t iBlock = 0; iBlock < nBlocks; iBlock++) {
    if (tEncodeSVSubmitBlk(pCoder, (SVSubmitBlk *)taosArrayGet(pReq->pArray, iBlock), pReq->flags) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVSubmitReq(SDecoder *pCoder, SVSubmitReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pReq->flags) < 0) return -1;
  if (tDecodeI32v(pCoder, &pReq->nBlocks) < 0) return -1;
  pReq->pBlocks = tDecoderMalloc(pCoder, sizeof(SVSubmitBlk) * pReq->nBlocks);
  if (pReq->pBlocks == NULL) return -1;
  for (int32_t iBlock = 0; iBlock < pReq->nBlocks; iBlock++) {
    if (tDecodeSVSubmitBlk(pCoder, pReq->pBlocks + iBlock, pReq->flags) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

static int32_t tEncodeSSubmitBlkRsp(SEncoder *pEncoder, const SSubmitBlkRsp *pBlock) {
  if (tStartEncode(pEncoder) < 0) return -1;

  if (tEncodeI32(pEncoder, pBlock->code) < 0) return -1;
  if (tEncodeI8(pEncoder, pBlock->hashMeta) < 0) return -1;
  if (tEncodeI64(pEncoder, pBlock->uid) < 0) return -1;
  if (tEncodeCStr(pEncoder, pBlock->tblFName) < 0) return -1;
  if (tEncodeI32v(pEncoder, pBlock->numOfRows) < 0) return -1;
  if (tEncodeI32v(pEncoder, pBlock->affectedRows) < 0) return -1;
  if (tEncodeI64v(pEncoder, pBlock->sver) < 0) return -1;

  tEndEncode(pEncoder);
  return 0;
}

static int32_t tDecodeSSubmitBlkRsp(SDecoder *pDecoder, SSubmitBlkRsp *pBlock) {
  if (tStartDecode(pDecoder) < 0) return -1;

  if (tDecodeI32(pDecoder, &pBlock->code) < 0) return -1;
  if (tDecodeI8(pDecoder, &pBlock->hashMeta) < 0) return -1;
  if (tDecodeI64(pDecoder, &pBlock->uid) < 0) return -1;
  pBlock->tblFName = taosMemoryCalloc(TSDB_TABLE_FNAME_LEN, 1);
  if (NULL == pBlock->tblFName) return -1;
  if (tDecodeCStrTo(pDecoder, pBlock->tblFName) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pBlock->numOfRows) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pBlock->affectedRows) < 0) return -1;
  if (tDecodeI64v(pDecoder, &pBlock->sver) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSSubmitRsp(SEncoder *pEncoder, const SSubmitRsp *pRsp) {
  int32_t nBlocks = taosArrayGetSize(pRsp->pArray);

  if (tStartEncode(pEncoder) < 0) return -1;

  if (tEncodeI32v(pEncoder, pRsp->numOfRows) < 0) return -1;
  if (tEncodeI32v(pEncoder, pRsp->affectedRows) < 0) return -1;
  if (tEncodeI32v(pEncoder, nBlocks) < 0) return -1;
  for (int32_t iBlock = 0; iBlock < nBlocks; iBlock++) {
    if (tEncodeSSubmitBlkRsp(pEncoder, (SSubmitBlkRsp *)taosArrayGet(pRsp->pArray, iBlock)) < 0) return -1;
  }

  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeSSubmitRsp(SDecoder *pDecoder, SSubmitRsp *pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;

  if (tDecodeI32v(pDecoder, &pRsp->numOfRows) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pRsp->affectedRows) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pRsp->nBlocks) < 0) return -1;
  pRsp->pBlocks = taosMemoryCalloc(pRsp->nBlocks, sizeof(*pRsp->pBlocks));
  if (pRsp->pBlocks == NULL) return -1;
  for (int32_t iBlock = 0; iBlock < pRsp->nBlocks; iBlock++) {
    if (tDecodeSSubmitBlkRsp(pDecoder, pRsp->pBlocks + iBlock) < 0) return -1;
  }

  tEndDecode(pDecoder);
  tDecoderClear(pDecoder);
  return 0;
}

void tFreeSSubmitRsp(SSubmitRsp *pRsp) {
  if (NULL == pRsp) return;

  if (pRsp->pBlocks) {
    for (int32_t i = 0; i < pRsp->nBlocks; ++i) {
      SSubmitBlkRsp *sRsp = pRsp->pBlocks + i;
      taosMemoryFree(sRsp->tblFName);
    }

    taosMemoryFree(pRsp->pBlocks);
  }

  taosMemoryFree(pRsp);
}

int32_t tEncodeSVAlterTbReq(SEncoder *pEncoder, const SVAlterTbReq *pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;

  if (tEncodeCStr(pEncoder, pReq->tbName) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->action) < 0) return -1;
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      if (tEncodeCStr(pEncoder, pReq->colName) < 0) return -1;
      if (tEncodeI8(pEncoder, pReq->type) < 0) return -1;
      if (tEncodeI8(pEncoder, pReq->flags) < 0) return -1;
      if (tEncodeI32v(pEncoder, pReq->bytes) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      if (tEncodeCStr(pEncoder, pReq->colName) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      if (tEncodeCStr(pEncoder, pReq->colName) < 0) return -1;
      if (tEncodeI32v(pEncoder, pReq->colModBytes) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (tEncodeCStr(pEncoder, pReq->colName) < 0) return -1;
      if (tEncodeCStr(pEncoder, pReq->colNewName) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      if (tEncodeCStr(pEncoder, pReq->tagName) < 0) return -1;
      if (tEncodeI8(pEncoder, pReq->isNull) < 0) return -1;
      if (!pReq->isNull) {
        if (tEncodeBinary(pEncoder, pReq->pTagVal, pReq->nTagVal) < 0) return -1;
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      if (tEncodeI8(pEncoder, pReq->updateTTL) < 0) return -1;
      if (pReq->updateTTL) {
        if (tEncodeI32v(pEncoder, pReq->newTTL) < 0) return -1;
      }
      if (tEncodeI8(pEncoder, pReq->updateComment) < 0) return -1;
      if (pReq->updateComment) {
        if (tEncodeCStr(pEncoder, pReq->newComment) < 0) return -1;
      }
      break;
    default:
      break;
  }

  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeSVAlterTbReq(SDecoder *pDecoder, SVAlterTbReq *pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;

  if (tDecodeCStr(pDecoder, &pReq->tbName) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->action) < 0) return -1;
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      if (tDecodeCStr(pDecoder, &pReq->colName) < 0) return -1;
      if (tDecodeI8(pDecoder, &pReq->type) < 0) return -1;
      if (tDecodeI8(pDecoder, &pReq->flags) < 0) return -1;
      if (tDecodeI32v(pDecoder, &pReq->bytes) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      if (tDecodeCStr(pDecoder, &pReq->colName) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      if (tDecodeCStr(pDecoder, &pReq->colName) < 0) return -1;
      if (tDecodeI32v(pDecoder, &pReq->colModBytes) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (tDecodeCStr(pDecoder, &pReq->colName) < 0) return -1;
      if (tDecodeCStr(pDecoder, &pReq->colNewName) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      if (tDecodeCStr(pDecoder, &pReq->tagName) < 0) return -1;
      if (tDecodeI8(pDecoder, &pReq->isNull) < 0) return -1;
      if (!pReq->isNull) {
        if (tDecodeBinary(pDecoder, &pReq->pTagVal, &pReq->nTagVal) < 0) return -1;
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      if (tDecodeI8(pDecoder, &pReq->updateTTL) < 0) return -1;
      if (pReq->updateTTL) {
        if (tDecodeI32v(pDecoder, &pReq->newTTL) < 0) return -1;
      }
      if (tDecodeI8(pDecoder, &pReq->updateComment) < 0) return -1;
      if (pReq->updateComment) {
        if (tDecodeCStr(pDecoder, &pReq->newComment) < 0) return -1;
      }
      break;
    default:
      break;
  }

  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSVAlterTbRsp(SEncoder *pEncoder, const SVAlterTbRsp *pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->code) < 0) return -1;
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeSVAlterTbRsp(SDecoder *pDecoder, SVAlterTbRsp *pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->code) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}
