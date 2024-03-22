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
#undef TD_MSG_RANGE_CODE_
#define TD_MSG_INFO_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_INFO_
#undef TD_MSG_RANGE_CODE_
#define TD_MSG_DICT_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_INFO_
#undef TD_MSG_DICT_
#undef TD_MSG_SEG_CODE_
#define TD_MSG_RANGE_CODE_
#include "tmsgdef.h"

#include "tlog.h"

#define DECODESQL()                                                                 \
  do {                                                                              \
    if (!tDecodeIsEnd(&decoder)) {                                                  \
      if (tDecodeI32(&decoder, &pReq->sqlLen) < 0) return -1;                       \
      if (pReq->sqlLen > 0) {                                                       \
        if (tDecodeBinaryAlloc(&decoder, (void **)&pReq->sql, NULL) < 0) return -1; \
      }                                                                             \
    }                                                                               \
  } while (0)

#define ENCODESQL()                                                        \
  do {                                                                     \
    if (pReq->sqlLen > 0 && pReq->sql != NULL) {                           \
      if (tEncodeI32(&encoder, pReq->sqlLen) < 0) return -1;               \
      if (tEncodeBinary(&encoder, pReq->sql, pReq->sqlLen) < 0) return -1; \
    }                                                                      \
  } while (0)

#define FREESQL()                \
  do {                           \
    if (pReq->sql != NULL) {     \
      taosMemoryFree(pReq->sql); \
    }                            \
    pReq->sql = NULL;            \
  } while (0)

static int32_t tDecodeSVAlterTbReqCommon(SDecoder *pDecoder, SVAlterTbReq *pReq);
static int32_t tDecodeSBatchDeleteReqCommon(SDecoder *pDecoder, SBatchDeleteReq *pReq);

int32_t tInitSubmitMsgIter(const SSubmitReq *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  pIter->totalLen = htonl(pMsg->length);
  pIter->numOfBlocks = htonl(pMsg->numOfBlocks);
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
    pIter->numOfRows = htonl((*pPBlock)->numOfRows);
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

#ifdef BUILD_NO_CALL
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
#endif

int32_t tEncodeSEpSet(SEncoder *pEncoder, const SEpSet *pEp) {
  if (tEncodeI8(pEncoder, pEp->inUse) < 0) return -1;
  if (tEncodeI8(pEncoder, pEp->numOfEps) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    if (tEncodeU16(pEncoder, pEp->eps[i].port) < 0) return -1;
    if (tEncodeCStrWithLen(pEncoder, pEp->eps[i].fqdn, TSDB_FQDN_LEN) < 0) return -1;
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

int32_t tEncodeSQueryNodeLoad(SEncoder *pEncoder, SQueryNodeLoad *pLoad) {
  if (tEncodeSQueryNodeAddr(pEncoder, &pLoad->addr) < 0) return -1;
  if (tEncodeU64(pEncoder, pLoad->load) < 0) return -1;
  return 0;
}

int32_t tDecodeSQueryNodeAddr(SDecoder *pDecoder, SQueryNodeAddr *pAddr) {
  if (tDecodeI32(pDecoder, &pAddr->nodeId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pAddr->epSet) < 0) return -1;
  return 0;
}

int32_t tDecodeSQueryNodeLoad(SDecoder *pDecoder, SQueryNodeLoad *pLoad) {
  if (tDecodeSQueryNodeAddr(pDecoder, &pLoad->addr) < 0) return -1;
  if (tDecodeU64(pDecoder, &pLoad->load) < 0) return -1;
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
    if (tEncodeI64(pEncoder, pReq->app.appId) < 0) return -1;
    if (tEncodeI32(pEncoder, pReq->app.pid) < 0) return -1;
    if (tEncodeCStr(pEncoder, pReq->app.name) < 0) return -1;
    if (tEncodeI64(pEncoder, pReq->app.startTime) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.numOfInsertsReq) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.numOfInsertRows) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.insertElapsedTime) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.insertBytes) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.fetchBytes) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.queryElapsedTime) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.numOfSlowQueries) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.totalRequests) < 0) return -1;
    if (tEncodeU64(pEncoder, pReq->app.summary.currentRequests) < 0) return -1;

    int32_t queryNum = 0;
    if (pReq->query) {
      queryNum = 1;
      if (tEncodeI32(pEncoder, queryNum) < 0) return -1;
      if (tEncodeU32(pEncoder, pReq->query->connId) < 0) return -1;

      int32_t num = taosArrayGetSize(pReq->query->queryDesc);
      if (tEncodeI32(pEncoder, num) < 0) return -1;

      for (int32_t i = 0; i < num; ++i) {
        SQueryDesc *desc = taosArrayGet(pReq->query->queryDesc, i);
        if (tEncodeCStr(pEncoder, desc->sql) < 0) return -1;
        if (tEncodeU64(pEncoder, desc->queryId) < 0) return -1;
        if (tEncodeI64(pEncoder, desc->useconds) < 0) return -1;
        if (tEncodeI64(pEncoder, desc->stime) < 0) return -1;
        if (tEncodeI64(pEncoder, desc->reqRid) < 0) return -1;
        if (tEncodeI8(pEncoder, desc->stableQuery) < 0) return -1;
        if (tEncodeI8(pEncoder, desc->isSubQuery) < 0) return -1;
        if (tEncodeCStr(pEncoder, desc->fqdn) < 0) return -1;
        if (tEncodeI32(pEncoder, desc->subPlanNum) < 0) return -1;

        int32_t snum = desc->subDesc ? taosArrayGetSize(desc->subDesc) : 0;
        if (tEncodeI32(pEncoder, snum) < 0) return -1;
        for (int32_t m = 0; m < snum; ++m) {
          SQuerySubDesc *sDesc = taosArrayGet(desc->subDesc, m);
          if (tEncodeI64(pEncoder, sDesc->tid) < 0) return -1;
          if (tEncodeCStr(pEncoder, sDesc->status) < 0) return -1;
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
    if (tDecodeI64(pDecoder, &pReq->app.appId) < 0) return -1;
    if (tDecodeI32(pDecoder, &pReq->app.pid) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pReq->app.name) < 0) return -1;
    if (tDecodeI64(pDecoder, &pReq->app.startTime) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.numOfInsertsReq) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.numOfInsertRows) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.insertElapsedTime) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.insertBytes) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.fetchBytes) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.queryElapsedTime) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.numOfSlowQueries) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.totalRequests) < 0) return -1;
    if (tDecodeU64(pDecoder, &pReq->app.summary.currentRequests) < 0) return -1;

    int32_t queryNum = 0;
    if (tDecodeI32(pDecoder, &queryNum) < 0) return -1;
    if (queryNum) {
      pReq->query = taosMemoryCalloc(1, sizeof(*pReq->query));
      if (NULL == pReq->query) return -1;
      if (tDecodeU32(pDecoder, &pReq->query->connId) < 0) return -1;

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
          if (tDecodeI8(pDecoder, (int8_t *)&desc.stableQuery) < 0) return -1;
          if (tDecodeI8(pDecoder, (int8_t *)&desc.isSubQuery) < 0) return -1;
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
              if (tDecodeCStrTo(pDecoder, sDesc.status) < 0) return -1;
              taosArrayPush(desc.subDesc, &sDesc);
            }
          }

          ASSERT(desc.subPlanNum == taosArrayGetSize(desc.subDesc));

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
    int32_t num = taosArrayGetSize(pRsp->query->pQnodeList);
    if (tEncodeI32(pEncoder, num) < 0) return -1;
    for (int32_t i = 0; i < num; ++i) {
      SQueryNodeLoad *pLoad = taosArrayGet(pRsp->query->pQnodeList, i);
      if (tEncodeSQueryNodeLoad(pEncoder, pLoad) < 0) return -1;
    }
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
    int32_t pQnodeNum = 0;
    if (tDecodeI32(pDecoder, &pQnodeNum) < 0) return -1;
    if (pQnodeNum > 0) {
      pRsp->query->pQnodeList = taosArrayInit(pQnodeNum, sizeof(SQueryNodeLoad));
      if (NULL == pRsp->query->pQnodeList) return -1;
      for (int32_t i = 0; i < pQnodeNum; ++i) {
        SQueryNodeLoad load = {0};
        if (tDecodeSQueryNodeLoad(pDecoder, &load) < 0) return -1;
        taosArrayPush(pRsp->query->pQnodeList, &load);
      }
    }
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
  if (tEncodeI32(&encoder, pBatchRsp->svrTimestamp) < 0) return -1;

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
  if (tDecodeI32(&decoder, &pBatchRsp->svrTimestamp) < 0) return -1;

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
  if (tEncodeI8(&encoder, pReq->source) < 0) return -1;
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    if (tEncodeI8(&encoder, pReq->reserved[i]) < 0) return -1;
  }
  if (tEncodeI64(&encoder, pReq->suid) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->delay1) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->delay2) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->watermark1) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->watermark2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ttl) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->colVer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->tagVer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfColumns) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfTags) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfFuncs) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commentLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ast1Len) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ast2Len) < 0) return -1;

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField *pField = taosArrayGet(pReq->pColumns, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI8(&encoder, pField->flags) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField *pField = taosArrayGet(pReq->pTags, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI8(&encoder, pField->flags) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    const char *pFunc = taosArrayGet(pReq->pFuncs, i);
    if (tEncodeCStr(&encoder, pFunc) < 0) return -1;
  }

  if (pReq->commentLen > 0) {
    if (tEncodeCStr(&encoder, pReq->pComment) < 0) return -1;
  }
  if (pReq->ast1Len > 0) {
    if (tEncodeBinary(&encoder, pReq->pAst1, pReq->ast1Len) < 0) return -1;
  }
  if (pReq->ast2Len > 0) {
    if (tEncodeBinary(&encoder, pReq->pAst2, pReq->ast2Len) < 0) return -1;
  }
  if (tEncodeI64(&encoder, pReq->deleteMark1) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->deleteMark2) < 0) return -1;

  ENCODESQL();

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
  if (tDecodeI8(&decoder, &pReq->source) < 0) return -1;
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    if (tDecodeI8(&decoder, &pReq->reserved[i]) < 0) return -1;
  }
  if (tDecodeI64(&decoder, &pReq->suid) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->delay1) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->delay2) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->watermark1) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->watermark2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->colVer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->tagVer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfColumns) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfTags) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfFuncs) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commentLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ast1Len) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ast2Len) < 0) return -1;

  pReq->pColumns = taosArrayInit(pReq->numOfColumns, sizeof(SField));
  pReq->pTags = taosArrayInit(pReq->numOfTags, sizeof(SField));
  pReq->pFuncs = taosArrayInit(pReq->numOfFuncs, TSDB_FUNC_NAME_LEN);
  if (pReq->pColumns == NULL || pReq->pTags == NULL || pReq->pFuncs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
    if (tDecodeI8(&decoder, &field.flags) < 0) return -1;
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (taosArrayPush(pReq->pColumns, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
    if (tDecodeI8(&decoder, &field.flags) < 0) return -1;
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (taosArrayPush(pReq->pTags, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char pFunc[TSDB_FUNC_NAME_LEN] = {0};
    if (tDecodeCStrTo(&decoder, pFunc) < 0) return -1;
    if (taosArrayPush(pReq->pFuncs, pFunc) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (pReq->commentLen > 0) {
    pReq->pComment = taosMemoryMalloc(pReq->commentLen + 1);
    if (pReq->pComment == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->pComment) < 0) return -1;
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

  if (tDecodeI64(&decoder, &pReq->deleteMark1) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->deleteMark2) < 0) return -1;

  DECODESQL();

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMCreateStbReq(SMCreateStbReq *pReq) {
  taosArrayDestroy(pReq->pColumns);
  taosArrayDestroy(pReq->pTags);
  taosArrayDestroy(pReq->pFuncs);
  taosMemoryFreeClear(pReq->pComment);
  taosMemoryFreeClear(pReq->pAst1);
  taosMemoryFreeClear(pReq->pAst2);
  FREESQL();
}

int32_t tSerializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->source) < 0) return -1;
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    if (tEncodeI8(&encoder, pReq->reserved[i]) < 0) return -1;
  }
  if (tEncodeI64(&encoder, pReq->suid) < 0) return -1;
  ENCODESQL();
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
  if (tDecodeI8(&decoder, &pReq->source) < 0) return -1;
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    if (tDecodeI8(&decoder, &pReq->reserved[i]) < 0) return -1;
  }
  if (tDecodeI64(&decoder, &pReq->suid) < 0) return -1;

  DECODESQL();

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMDropStbReq(SMDropStbReq *pReq) { FREESQL(); }

int32_t tSerializeSMAlterStbReq(void *buf, int32_t bufLen, SMAlterStbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->alterType) < 0) return -1;
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
  ENCODESQL();
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
    pReq->comment = taosMemoryMalloc(pReq->commentLen + 1);
    if (pReq->comment == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->comment) < 0) return -1;
  }

  DECODESQL();

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMAltertbReq(SMAlterStbReq *pReq) {
  taosArrayDestroy(pReq->pFields);
  pReq->pFields = NULL;
  taosMemoryFreeClear(pReq->comment);
  FREESQL();
}

int32_t tSerializeSEpSet(void *buf, int32_t bufLen, const SEpSet *pEpset) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSEpSet(&encoder, pEpset) < 0) return -1;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSEpSet(void *buf, int32_t bufLen, SEpSet *pEpset) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSEpSet(&decoder, pEpset) < 0) return -1;

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
  if (tEncodeI64(&encoder, pReq->watermark) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->maxDelay) < 0) return -1;
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
  if (tEncodeI64(&encoder, pReq->deleteMark) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->lastTs) < 0) return -1;
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
  if (tDecodeI64(&decoder, &pReq->watermark) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->maxDelay) < 0) return -1;
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
  if (tDecodeI64(&decoder, &pReq->deleteMark) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->lastTs) < 0) return -1;
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

int32_t tSerializeSCreateTagIdxReq(void *buf, int32_t bufLen, SCreateTagIndexReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->stbName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->colName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->idxName) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->idxType) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSCreateTagIdxReq(void *buf, int32_t bufLen, SCreateTagIndexReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeCStrTo(&decoder, pReq->dbFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->stbName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->colName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->idxName) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->idxType) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}
int32_t tSerializeSDropTagIdxReq(void *buf, int32_t bufLen, SDropTagIndexReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  tEndEncode(&encoder);

  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSDropTagIdxReq(void *buf, int32_t bufLen, SDropTagIndexReq *pReq) {
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

int32_t tSerializeSNotifyReq(void *buf, int32_t bufLen, SNotifyReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->clusterId) < 0) return -1;

  int32_t nVgroup = taosArrayGetSize(pReq->pVloads);
  if (tEncodeI32(&encoder, nVgroup) < 0) return -1;
  for (int32_t i = 0; i < nVgroup; ++i) {
    SVnodeLoadLite *vload = TARRAY_GET_ELEM(pReq->pVloads, i);
    if (tEncodeI32(&encoder, vload->vgId) < 0) return -1;
    if (tEncodeI64(&encoder, vload->nTimeSeries) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSNotifyReq(void *buf, int32_t bufLen, SNotifyReq *pReq) {
  int32_t  code = TSDB_CODE_INVALID_MSG;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) goto _exit;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) goto _exit;
  if (tDecodeI64(&decoder, &pReq->clusterId) < 0) goto _exit;
  int32_t nVgroup = 0;
  if (tDecodeI32(&decoder, &nVgroup) < 0) goto _exit;
  if (nVgroup > 0) {
    pReq->pVloads = taosArrayInit(nVgroup, sizeof(SVnodeLoadLite));
    if (!pReq->pVloads) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    for (int32_t i = 0; i < nVgroup; ++i) {
      SVnodeLoadLite vload;
      if (tDecodeI32(&decoder, &(vload.vgId)) < 0) goto _exit;
      if (tDecodeI64(&decoder, &(vload.nTimeSeries)) < 0) goto _exit;
      taosArrayPush(pReq->pVloads, &vload);
    }
  }

  code = 0;

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

void tFreeSNotifyReq(SNotifyReq *pReq) {
  if (pReq) {
    taosArrayDestroy(pReq->pVloads);
  }
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
  if (tEncodeFloat(&encoder, pReq->numOfCores) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfSupportVnodes) < 0) return -1;
  if (tEncodeI32v(&encoder, pReq->numOfDiskCfg) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->memTotal) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->memAvail) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dnodeEp) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->machineId) < 0) return -1;

  // cluster cfg
  if (tEncodeI32(&encoder, pReq->clusterCfg.statusInterval) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->clusterCfg.checkTime) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->clusterCfg.timezone) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->clusterCfg.locale) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->clusterCfg.charset) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->clusterCfg.enableWhiteList) < 0) return -1;

  // vnode loads
  int32_t vlen = (int32_t)taosArrayGetSize(pReq->pVloads);
  if (tEncodeI32(&encoder, vlen) < 0) return -1;
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    int64_t     reserved = 0;
    if (tEncodeI32(&encoder, pload->vgId) < 0) return -1;
    if (tEncodeI8(&encoder, pload->syncState) < 0) return -1;
    if (tEncodeI8(&encoder, pload->syncRestore) < 0) return -1;
    if (tEncodeI8(&encoder, pload->syncCanRead) < 0) return -1;
    if (tEncodeI64(&encoder, pload->cacheUsage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->numOfTables) < 0) return -1;
    if (tEncodeI64(&encoder, pload->numOfTimeSeries) < 0) return -1;
    if (tEncodeI64(&encoder, pload->totalStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->compStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->pointsWritten) < 0) return -1;
    if (tEncodeI32(&encoder, pload->numOfCachedTables) < 0) return -1;
    if (tEncodeI32(&encoder, pload->learnerProgress) < 0) return -1;
    if (tEncodeI64(&encoder, pload->roleTimeMs) < 0) return -1;
    if (tEncodeI64(&encoder, pload->startTimeMs) < 0) return -1;
  }

  // mnode loads
  if (tEncodeI8(&encoder, pReq->mload.syncState) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->mload.syncRestore) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->qload.dnodeId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedQuery) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedCQuery) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedFetch) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedDrop) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedNotify) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedHb) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfProcessedDelete) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.cacheDataSize) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfQueryInQueue) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.numOfFetchInQueue) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.timeInQueryQueue) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->qload.timeInFetchQueue) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->statusSeq) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->mload.syncTerm) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->mload.roleTimeMs) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->clusterCfg.ttlChangeOnWrite) < 0) return -1;

  // vnode extra
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    int64_t     reserved = 0;
    if (tEncodeI64(&encoder, pload->syncTerm) < 0) return -1;
    if (tEncodeI64(&encoder, reserved) < 0) return -1;
    if (tEncodeI64(&encoder, reserved) < 0) return -1;
    if (tEncodeI64(&encoder, reserved) < 0) return -1;
  }

  if (tEncodeI64(&encoder, pReq->ipWhiteVer) < 0) return -1;
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
  if (tDecodeFloat(&decoder, &pReq->numOfCores) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfSupportVnodes) < 0) return -1;
  if (tDecodeI32v(&decoder, &pReq->numOfDiskCfg) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->memTotal) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->memAvail) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dnodeEp) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->machineId) < 0) return -1;

  // cluster cfg
  if (tDecodeI32(&decoder, &pReq->clusterCfg.statusInterval) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->clusterCfg.checkTime) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->clusterCfg.timezone) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->clusterCfg.locale) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->clusterCfg.charset) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->clusterCfg.enableWhiteList) < 0) return -1;

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
    vload.syncTerm = -1;

    if (tDecodeI32(&decoder, &vload.vgId) < 0) return -1;
    if (tDecodeI8(&decoder, &vload.syncState) < 0) return -1;
    if (tDecodeI8(&decoder, &vload.syncRestore) < 0) return -1;
    if (tDecodeI8(&decoder, &vload.syncCanRead) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.cacheUsage) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.numOfTables) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.numOfTimeSeries) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.totalStorage) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.compStorage) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.pointsWritten) < 0) return -1;
    if (tDecodeI32(&decoder, &vload.numOfCachedTables) < 0) return -1;
    if (tDecodeI32(&decoder, &vload.learnerProgress) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.roleTimeMs) < 0) return -1;
    if (tDecodeI64(&decoder, &vload.startTimeMs) < 0) return -1;
    if (taosArrayPush(pReq->pVloads, &vload) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  // mnode loads
  if (tDecodeI8(&decoder, &pReq->mload.syncState) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->mload.syncRestore) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->qload.dnodeId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedQuery) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedCQuery) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedFetch) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedDrop) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedNotify) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedHb) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfProcessedDelete) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.cacheDataSize) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfQueryInQueue) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.numOfFetchInQueue) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.timeInQueryQueue) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->qload.timeInFetchQueue) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->statusSeq) < 0) return -1;

  pReq->mload.syncTerm = -1;
  pReq->mload.roleTimeMs = 0;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI64(&decoder, &pReq->mload.syncTerm) < 0) return -1;
    if (tDecodeI64(&decoder, &pReq->mload.roleTimeMs) < 0) return -1;
  }

  pReq->clusterCfg.ttlChangeOnWrite = false;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->clusterCfg.ttlChangeOnWrite) < 0) return -1;
  }

  // vnode extra
  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < vlen; ++i) {
      SVnodeLoad *pLoad = taosArrayGet(pReq->pVloads, i);
      int64_t     reserved = 0;
      if (tDecodeI64(&decoder, &pLoad->syncTerm) < 0) return -1;
      if (tDecodeI64(&decoder, &reserved) < 0) return -1;
      if (tDecodeI64(&decoder, &reserved) < 0) return -1;
      if (tDecodeI64(&decoder, &reserved) < 0) return -1;
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI64(&decoder, &pReq->ipWhiteVer) < 0) return -1;
  }

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

  if (tEncodeI32(&encoder, pRsp->statusSeq) < 0) return -1;

  if (tEncodeI64(&encoder, pRsp->ipWhiteVer) < 0) return -1;
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

  if (tDecodeI32(&decoder, &pRsp->statusSeq) < 0) return -1;

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI64(&decoder, &pRsp->ipWhiteVer) < 0) return -1;
  }
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSStatusRsp(SStatusRsp *pRsp) { taosArrayDestroy(pRsp->pDnodeEps); }

int32_t tSerializeSStatisReq(void *buf, int32_t bufLen, SStatisReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->contLen) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pCont) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatisReq(void *buf, int32_t bufLen, SStatisReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->contLen) < 0) return -1;
  if (pReq->contLen > 0) {
    pReq->pCont = taosMemoryMalloc(pReq->contLen + 1);
    if (pReq->pCont == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->pCont) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSStatisReq(SStatisReq *pReq) {
  taosMemoryFreeClear(pReq->pCont);
}

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
  ENCODESQL();
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
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSDropUserReq(SDropUserReq *pReq) { FREESQL(); }

SIpWhiteList *cloneIpWhiteList(SIpWhiteList *pIpWhiteList) {
  if (pIpWhiteList == NULL) return NULL;

  int32_t       sz = sizeof(SIpWhiteList) + pIpWhiteList->num * sizeof(SIpV4Range);
  SIpWhiteList *pNew = taosMemoryCalloc(1, sz);
  memcpy(pNew, pIpWhiteList, sz);
  return pNew;
}

int32_t tSerializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->createType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->superUser) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->sysInfo) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->enable) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numIpRanges) < 0) return -1;
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    if (tEncodeU32(&encoder, pReq->pIpRanges[i].ip) < 0) return -1;
    if (tEncodeU32(&encoder, pReq->pIpRanges[i].mask) < 0) return -1;
  }

  ENCODESQL();
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
  if (tDecodeI8(&decoder, &pReq->sysInfo) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->enable) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numIpRanges) < 0) return -1;
  pReq->pIpRanges = taosMemoryMalloc(pReq->numIpRanges * sizeof(SIpV4Range));
  if (pReq->pIpRanges == NULL) return -1;
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    if (tDecodeU32(&decoder, &(pReq->pIpRanges[i].ip)) < 0) return -1;
    if (tDecodeU32(&decoder, &(pReq->pIpRanges[i].mask)) < 0) return -1;
  }

  DECODESQL();
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUpdateIpWhite(void *buf, int32_t bufLen, SUpdateIpWhite *pReq) {
  // impl later
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->ver) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfUser) < 0) return -1;
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUser = &(pReq->pUserIpWhite[i]);

    if (tEncodeI64(&encoder, pUser->ver) < 0) return -1;
    if (tEncodeCStr(&encoder, pUser->user) < 0) return -1;
    if (tEncodeI32(&encoder, pUser->numOfRange) < 0) return -1;
    for (int j = 0; j < pUser->numOfRange; j++) {
      SIpV4Range *pRange = &pUser->pIpRanges[j];
      if (tEncodeU32(&encoder, pRange->ip) < 0) return -1;
      if (tEncodeU32(&encoder, pRange->mask) < 0) return -1;
    }
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSUpdateIpWhite(void *buf, int32_t bufLen, SUpdateIpWhite *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  // impl later
  if (tDecodeI64(&decoder, &pReq->ver) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfUser) < 0) return -1;

  pReq->pUserIpWhite = taosMemoryCalloc(1, sizeof(SUpdateUserIpWhite) * pReq->numOfUser);
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUserWhite = &pReq->pUserIpWhite[i];
    if (tDecodeI64(&decoder, &pUserWhite->ver) < 0) return -1;
    if (tDecodeCStrTo(&decoder, pUserWhite->user) < 0) return -1;
    if (tDecodeI32(&decoder, &pUserWhite->numOfRange) < 0) return -1;

    pUserWhite->pIpRanges = taosMemoryCalloc(1, pUserWhite->numOfRange * sizeof(SIpV4Range));
    for (int j = 0; j < pUserWhite->numOfRange; j++) {
      SIpV4Range *pRange = &pUserWhite->pIpRanges[j];
      if (tDecodeU32(&decoder, &pRange->ip) < 0) return -1;
      if (tDecodeU32(&decoder, &pRange->mask) < 0) return -1;
    }
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}
void tFreeSUpdateIpWhiteReq(SUpdateIpWhite *pReq) {
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUserWhite = &pReq->pUserIpWhite[i];
    taosMemoryFree(pUserWhite->pIpRanges);
  }
  taosMemoryFree(pReq->pUserIpWhite);
  // impl later
  return;
}
SUpdateIpWhite *cloneSUpdateIpWhiteReq(SUpdateIpWhite *pReq) {
  SUpdateIpWhite *pClone = taosMemoryCalloc(1, sizeof(SUpdateIpWhite));

  pClone->numOfUser = pReq->numOfUser;
  pClone->ver = pReq->ver;
  pClone->pUserIpWhite = taosMemoryCalloc(1, sizeof(SUpdateUserIpWhite) * pReq->numOfUser);

  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pNew = &pClone->pUserIpWhite[i];
    SUpdateUserIpWhite *pOld = &pReq->pUserIpWhite[i];

    pNew->ver = pOld->ver;
    memcpy(pNew->user, pOld->user, strlen(pOld->user));
    pNew->numOfRange = pOld->numOfRange;

    int32_t sz = pOld->numOfRange * sizeof(SIpV4Range);
    pNew->pIpRanges = taosMemoryCalloc(1, sz);
    memcpy(pNew->pIpRanges, pOld->pIpRanges, sz);
  }
  return pClone;
}
int32_t tSerializeRetrieveIpWhite(void *buf, int32_t bufLen, SRetrieveIpWhiteReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI64(&encoder, pReq->ipWhiteVer) < 0) {
    return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeRetrieveIpWhite(void *buf, int32_t bufLen, SRetrieveIpWhiteReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  // impl later
  if (tDecodeI64(&decoder, &pReq->ipWhiteVer) < 0) return -1;
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCreateUserReq(SCreateUserReq *pReq) {
  FREESQL();
  taosMemoryFreeClear(pReq->pIpRanges);
}

int32_t tSerializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->alterType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->superUser) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->sysInfo) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->enable) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->isView) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->objname) < 0) return -1;
  int32_t len = strlen(pReq->tabName);
  if (tEncodeI32(&encoder, len) < 0) return -1;
  if (len > 0) {
    if (tEncodeCStr(&encoder, pReq->tabName) < 0) return -1;
  }
  if (tEncodeBinary(&encoder, pReq->tagCond, pReq->tagCondLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numIpRanges) < 0) return -1;
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    if (tEncodeU32(&encoder, pReq->pIpRanges[i].ip) < 0) return -1;
    if (tEncodeU32(&encoder, pReq->pIpRanges[i].mask) < 0) return -1;
  }
  if (tEncodeI64(&encoder, pReq->privileges) < 0) return -1;
  ENCODESQL();
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
  if (tDecodeI8(&decoder, &pReq->sysInfo) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->enable) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->isView) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->objname) < 0) return -1;
  if (!tDecodeIsEnd(&decoder)) {
    int32_t len = 0;
    if (tDecodeI32(&decoder, &len) < 0) return -1;
    if (len > 0) {
      if (tDecodeCStrTo(&decoder, pReq->tabName) < 0) return -1;
    }
    uint64_t tagCondLen = 0;
    if (tDecodeBinaryAlloc(&decoder, (void **)&pReq->tagCond, &tagCondLen) < 0) return -1;
    pReq->tagCondLen = tagCondLen;
  }
  if (tDecodeI32(&decoder, &pReq->numIpRanges) < 0) return -1;
  pReq->pIpRanges = taosMemoryMalloc(pReq->numIpRanges * sizeof(SIpV4Range));
  if (pReq->pIpRanges == NULL) return -1;
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    if (tDecodeU32(&decoder, &(pReq->pIpRanges[i].ip)) < 0) return -1;
    if (tDecodeU32(&decoder, &(pReq->pIpRanges[i].mask)) < 0) return -1;
  }
  if (tDecodeI64(&decoder, &pReq->privileges) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSAlterUserReq(SAlterUserReq *pReq) {
  taosMemoryFreeClear(pReq->tagCond);
  taosMemoryFree(pReq->pIpRanges);
  FREESQL();
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
  if (tEncodeI8(pEncoder, pRsp->sysInfo) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->enable) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->dropped) < 0) return -1;
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

  int32_t numOfReadTbs = taosHashGetSize(pRsp->readTbs);
  int32_t numOfWriteTbs = taosHashGetSize(pRsp->writeTbs);
  int32_t numOfAlterTbs = taosHashGetSize(pRsp->alterTbs);
  int32_t numOfReadViews = taosHashGetSize(pRsp->readViews);
  int32_t numOfWriteViews = taosHashGetSize(pRsp->writeViews);
  int32_t numOfAlterViews = taosHashGetSize(pRsp->alterViews);
  int32_t numOfUseDbs = taosHashGetSize(pRsp->useDbs);
  if (tEncodeI32(pEncoder, numOfReadTbs) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfWriteTbs) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfAlterTbs) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfReadViews) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfWriteViews) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfAlterViews) < 0) return -1;
  if (tEncodeI32(pEncoder, numOfUseDbs) < 0) return -1;

  char *tb = taosHashIterate(pRsp->readTbs, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(tb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    size_t valueLen = 0;
    valueLen = strlen(tb);
    if (tEncodeI32(pEncoder, valueLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, tb) < 0) return -1;

    tb = taosHashIterate(pRsp->readTbs, tb);
  }

  tb = taosHashIterate(pRsp->writeTbs, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(tb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    size_t valueLen = 0;
    valueLen = strlen(tb);
    if (tEncodeI32(pEncoder, valueLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, tb) < 0) return -1;

    tb = taosHashIterate(pRsp->writeTbs, tb);
  }

  tb = taosHashIterate(pRsp->alterTbs, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(tb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    size_t valueLen = 0;
    valueLen = strlen(tb);
    if (tEncodeI32(pEncoder, valueLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, tb) < 0) return -1;

    tb = taosHashIterate(pRsp->alterTbs, tb);
  }

  tb = taosHashIterate(pRsp->readViews, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(tb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    size_t valueLen = 0;
    valueLen = strlen(tb);
    if (tEncodeI32(pEncoder, valueLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, tb) < 0) return -1;

    tb = taosHashIterate(pRsp->readViews, tb);
  }

  tb = taosHashIterate(pRsp->writeViews, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(tb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    size_t valueLen = 0;
    valueLen = strlen(tb);
    if (tEncodeI32(pEncoder, valueLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, tb) < 0) return -1;

    tb = taosHashIterate(pRsp->writeViews, tb);
  }

  tb = taosHashIterate(pRsp->alterViews, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(tb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    size_t valueLen = 0;
    valueLen = strlen(tb);
    if (tEncodeI32(pEncoder, valueLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, tb) < 0) return -1;

    tb = taosHashIterate(pRsp->alterViews, tb);
  }

  int32_t *useDb = taosHashIterate(pRsp->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void * key = taosHashGetKey(useDb, &keyLen);
    if (tEncodeI32(pEncoder, keyLen) < 0) return -1;
    if (tEncodeCStr(pEncoder, key) < 0) return -1;

    if (tEncodeI32(pEncoder, *useDb) < 0) return -1;
    useDb = taosHashIterate(pRsp->useDbs, useDb);
  }

  // since 3.0.7.0
  if (tEncodeI32(pEncoder, pRsp->passVer) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->whiteListVer) < 0) return -1;
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
  char *key = NULL, *value = NULL;
  pRsp->createdDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readTbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeTbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->alterTbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readViews = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeViews = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->alterViews = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->useDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pRsp->createdDbs == NULL || pRsp->readDbs == NULL || pRsp->writeDbs == NULL || pRsp->readTbs == NULL ||
      pRsp->writeTbs == NULL || pRsp->alterTbs == NULL || pRsp->readViews == NULL || pRsp->writeViews == NULL ||
      pRsp->alterViews == NULL || pRsp->useDbs == NULL) {
    goto _err;
  }

  if (tDecodeCStrTo(pDecoder, pRsp->user) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->superAuth) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->sysInfo) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->enable) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->dropped) < 0) goto _err;
  if (tDecodeI32(pDecoder, &pRsp->version) < 0) goto _err;

  int32_t numOfCreatedDbs = 0;
  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  if (tDecodeI32(pDecoder, &numOfCreatedDbs) < 0) goto _err;
  if (tDecodeI32(pDecoder, &numOfReadDbs) < 0) goto _err;
  if (tDecodeI32(pDecoder, &numOfWriteDbs) < 0) goto _err;

  for (int32_t i = 0; i < numOfCreatedDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) goto _err;
    int32_t len = strlen(db);
    taosHashPut(pRsp->createdDbs, db, len + 1, db, len + 1);
  }

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) goto _err;
    int32_t len = strlen(db);
    taosHashPut(pRsp->readDbs, db, len + 1, db, len + 1);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) goto _err;
    int32_t len = strlen(db);
    taosHashPut(pRsp->writeDbs, db, len + 1, db, len + 1);
  }

  if (!tDecodeIsEnd(pDecoder)) {
    int32_t numOfReadTbs = 0;
    int32_t numOfWriteTbs = 0;
    int32_t numOfAlterTbs = 0;
    int32_t numOfReadViews = 0;
    int32_t numOfWriteViews = 0;
    int32_t numOfAlterViews = 0;
    int32_t numOfUseDbs = 0;
    if (tDecodeI32(pDecoder, &numOfReadTbs) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfWriteTbs) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfAlterTbs) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfReadViews) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfWriteViews) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfAlterViews) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfUseDbs) < 0) goto _err;

    for (int32_t i = 0; i < numOfReadTbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      value = taosMemoryCalloc(valuelen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      taosHashPut(pRsp->readTbs, key, keyLen, value, valuelen + 1);

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfWriteTbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      value = taosMemoryCalloc(valuelen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      taosHashPut(pRsp->writeTbs, key, keyLen, value, valuelen + 1);

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfAlterTbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      value = taosMemoryCalloc(valuelen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      taosHashPut(pRsp->alterTbs, key, keyLen, value, valuelen + 1);

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfReadViews; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      value = taosMemoryCalloc(valuelen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      taosHashPut(pRsp->readViews, key, keyLen, value, valuelen + 1);

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfWriteViews; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      value = taosMemoryCalloc(valuelen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      taosHashPut(pRsp->writeViews, key, keyLen, value, valuelen + 1);

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfAlterViews; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      value = taosMemoryCalloc(valuelen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      taosHashPut(pRsp->alterViews, key, keyLen, value, valuelen + 1);

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfUseDbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      key = taosMemoryCalloc(keyLen + 1, sizeof(char));
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t ref = 0;
      if (tDecodeI32(pDecoder, &ref) < 0) goto _err;

      taosHashPut(pRsp->useDbs, key, keyLen, &ref, sizeof(ref));
      taosMemoryFreeClear(key);
    }
    // since 3.0.7.0
    if (!tDecodeIsEnd(pDecoder)) {
      if (tDecodeI32(pDecoder, &pRsp->passVer) < 0) goto _err;
    } else {
      pRsp->passVer = 0;
    }
    if (!tDecodeIsEnd(pDecoder)) {
      if (tDecodeI64(pDecoder, &pRsp->whiteListVer) < 0) goto _err;
    } else {
      pRsp->whiteListVer = 0;
    }
  }
  return 0;
_err:
  taosHashCleanup(pRsp->createdDbs);
  taosHashCleanup(pRsp->readDbs);
  taosHashCleanup(pRsp->writeDbs);
  taosHashCleanup(pRsp->readTbs);
  taosHashCleanup(pRsp->writeTbs);
  taosHashCleanup(pRsp->alterTbs);
  taosHashCleanup(pRsp->readViews);
  taosHashCleanup(pRsp->writeViews);
  taosHashCleanup(pRsp->alterViews);
  taosHashCleanup(pRsp->useDbs);

  taosMemoryFreeClear(key);
  taosMemoryFreeClear(value);
  return -1;
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
  taosHashCleanup(pRsp->readTbs);
  taosHashCleanup(pRsp->writeTbs);
  taosHashCleanup(pRsp->alterTbs);
  taosHashCleanup(pRsp->readViews);
  taosHashCleanup(pRsp->writeViews);
  taosHashCleanup(pRsp->alterViews);
  taosHashCleanup(pRsp->useDbs);
}

int32_t tSerializeSGetUserWhiteListReq(void *buf, int32_t bufLen, SGetUserWhiteListReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserWhiteListReq(void *buf, int32_t bufLen, SGetUserWhiteListReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSGetUserWhiteListRsp(void *buf, int32_t bufLen, SGetUserWhiteListRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->user) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numWhiteLists) < 0) return -1;
  for (int i = 0; i < pRsp->numWhiteLists; ++i) {
    if (tEncodeU32(&encoder, pRsp->pWhiteLists[i].ip) < 0) return -1;
    if (tEncodeU32(&encoder, pRsp->pWhiteLists[i].mask) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserWhiteListRsp(void *buf, int32_t bufLen, SGetUserWhiteListRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->user) < 0) return -1;

  if (tDecodeI32(&decoder, &pRsp->numWhiteLists) < 0) return -1;
  pRsp->pWhiteLists = taosMemoryMalloc(pRsp->numWhiteLists * sizeof(SIpV4Range));
  if (pRsp->pWhiteLists == NULL) return -1;
  for (int32_t i = 0; i < pRsp->numWhiteLists; ++i) {
    if (tDecodeU32(&decoder, &(pRsp->pWhiteLists[i].ip)) < 0) return -1;
    if (tDecodeU32(&decoder, &(pRsp->pWhiteLists[i].mask)) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSGetUserWhiteListRsp(SGetUserWhiteListRsp *pRsp) { taosMemoryFree(pRsp->pWhiteLists); }

int32_t tSerializeSMCfgClusterReq(void *buf, int32_t bufLen, SMCfgClusterReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->config) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->value) < 0) return -1;
  ENCODESQL();
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCfgClusterReq(void *buf, int32_t bufLen, SMCfgClusterReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->config) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->value) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMCfgClusterReq(SMCfgClusterReq *pReq) { FREESQL(); }

int32_t tSerializeSCreateDropMQSNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  ENCODESQL();
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDropMQSNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMCreateQnodeReq(SMCreateQnodeReq *pReq) { FREESQL(); }

void tFreeSDDropQnodeReq(SDDropQnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fqdn) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->port) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->force) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->unsafe) < 0) return -1;
  ENCODESQL();
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
  if (tDecodeI8(&decoder, &pReq->force) < 0) return -1;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->unsafe) < 0) return -1;
  } else {
    pReq->unsafe = false;
  }

  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSDropDnodeReq(SDropDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSRestoreDnodeReq(void *buf, int32_t bufLen, SRestoreDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->restoreType) < 0) return -1;
  ENCODESQL();
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRestoreDnodeReq(void *buf, int32_t bufLen, SRestoreDnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->restoreType) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSRestoreDnodeReq(SRestoreDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->config) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->value) < 0) return -1;
  ENCODESQL();
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
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMCfgDnodeReq(SMCfgDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSDCfgDnodeReq(void *buf, int32_t bufLen, SDCfgDnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->config) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->value) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDCfgDnodeReq(void *buf, int32_t bufLen, SDCfgDnodeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
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
  ENCODESQL();
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
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCreateDnodeReq(SCreateDnodeReq *pReq) { FREESQL(); }

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

  if (tEncodeI8(&encoder, pReq->orReplace) < 0) return -1;

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

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->orReplace) < 0) return -1;
  } else {
    pReq->orReplace = false;
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

  if (pRsp->numOfFuncs != (int32_t)taosArrayGetSize(pRsp->pFuncExtraInfos)) return -1;
  for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
    SFuncExtraInfo *extraInfo = taosArrayGet(pRsp->pFuncExtraInfos, i);
    if (tEncodeI32(&encoder, extraInfo->funcVersion) < 0) return -1;
    if (tEncodeI64(&encoder, extraInfo->funcCreatedTime) < 0) return -1;
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

  pRsp->pFuncExtraInfos = taosArrayInit(pRsp->numOfFuncs, sizeof(SFuncExtraInfo));
  if (pRsp->pFuncExtraInfos == NULL) return -1;
  if (tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
      SFuncExtraInfo extraInfo = {0};
      taosArrayPush(pRsp->pFuncExtraInfos, &extraInfo);
    }
  } else {
    for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
      SFuncExtraInfo extraInfo = {0};
      if (tDecodeI32(&decoder, &extraInfo.funcVersion) < 0) return -1;
      if (tDecodeI64(&decoder, &extraInfo.funcCreatedTime) < 0) return -1;
      taosArrayPush(pRsp->pFuncExtraInfos, &extraInfo);
    }
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
  taosArrayDestroy(pRsp->pFuncExtraInfos);
}

int32_t tSerializeSTableCfgReq(void *buf, int32_t bufLen, STableCfgReq *pReq) {
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

int32_t tDeserializeSTableCfgReq(void *buf, int32_t bufLen, STableCfgReq *pReq) {
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

int32_t tSerializeSTableCfgRsp(void *buf, int32_t bufLen, STableCfgRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->tbName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->stbName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfTags) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfColumns) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->tableType) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->delay1) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->delay2) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->watermark1) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->watermark2) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->ttl) < 0) return -1;

  int32_t numOfFuncs = taosArrayGetSize(pRsp->pFuncs);
  if (tEncodeI32(&encoder, numOfFuncs) < 0) return -1;
  for (int32_t i = 0; i < numOfFuncs; ++i) {
    const char *pFunc = taosArrayGet(pRsp->pFuncs, i);
    if (tEncodeCStr(&encoder, pFunc) < 0) return -1;
  }

  if (tEncodeI32(&encoder, pRsp->commentLen) < 0) return -1;
  if (pRsp->commentLen > 0) {
    if (tEncodeCStr(&encoder, pRsp->pComment) < 0) return -1;
  }

  for (int32_t i = 0; i < pRsp->numOfColumns + pRsp->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    if (tEncodeSSchema(&encoder, pSchema) < 0) return -1;
  }

  if (tEncodeI32(&encoder, pRsp->tagsLen) < 0) return -1;
  if (tEncodeBinary(&encoder, pRsp->pTags, pRsp->tagsLen) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableCfgRsp(void *buf, int32_t bufLen, STableCfgRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->tbName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->stbName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfTags) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfColumns) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->tableType) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->delay1) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->delay2) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->watermark1) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->watermark2) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->ttl) < 0) return -1;

  int32_t numOfFuncs = 0;
  if (tDecodeI32(&decoder, &numOfFuncs) < 0) return -1;
  if (numOfFuncs > 0) {
    pRsp->pFuncs = taosArrayInit(numOfFuncs, TSDB_FUNC_NAME_LEN);
    if (NULL == pRsp->pFuncs) return -1;
  }
  for (int32_t i = 0; i < numOfFuncs; ++i) {
    char pFunc[TSDB_FUNC_NAME_LEN];
    if (tDecodeCStrTo(&decoder, pFunc) < 0) return -1;
    if (taosArrayPush(pRsp->pFuncs, pFunc) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (tDecodeI32(&decoder, &pRsp->commentLen) < 0) return -1;
  if (pRsp->commentLen > 0) {
    if (tDecodeCStrAlloc(&decoder, &pRsp->pComment) < 0) return -1;
  } else {
    pRsp->pComment = NULL;
  }

  int32_t totalCols = pRsp->numOfTags + pRsp->numOfColumns;
  pRsp->pSchemas = taosMemoryMalloc(sizeof(SSchema) * totalCols);
  if (pRsp->pSchemas == NULL) return -1;

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    if (tDecodeSSchema(&decoder, pSchema) < 0) return -1;
  }

  if (tDecodeI32(&decoder, &pRsp->tagsLen) < 0) return -1;
  if (tDecodeBinaryAlloc(&decoder, (void **)&pRsp->pTags, NULL) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSTableCfgRsp(STableCfgRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosMemoryFreeClear(pRsp->pComment);
  taosMemoryFreeClear(pRsp->pSchemas);
  taosMemoryFreeClear(pRsp->pTags);

  taosArrayDestroy(pRsp->pFuncs);
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
  if (tEncodeI32(&encoder, pReq->cacheLastSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walFsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLast) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->schemaless) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walRetentionPeriod) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->walRetentionSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walRollPeriod) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->walSegmentSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->sstTrigger) < 0) return -1;
  if (tEncodeI16(&encoder, pReq->hashPrefix) < 0) return -1;
  if (tEncodeI16(&encoder, pReq->hashSuffix) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreExist) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    if (tEncodeI64(&encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI64(&encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->keepUnit) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->tsdbPageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->keepTimeOffset) < 0) return -1;

  ENCODESQL();
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
  if (tDecodeI32(&decoder, &pReq->cacheLastSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walFsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLast) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->schemaless) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walRetentionPeriod) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->walRetentionSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walRollPeriod) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->walSegmentSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->sstTrigger) < 0) return -1;
  if (tDecodeI16(&decoder, &pReq->hashPrefix) < 0) return -1;
  if (tDecodeI16(&decoder, &pReq->hashSuffix) < 0) return -1;
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

  if (tDecodeI32(&decoder, &pReq->tsdbPageSize) < 0) return -1;

  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->keepTimeOffset) < 0) return -1;
  }

  DECODESQL();

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCreateDbReq(SCreateDbReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
  FREESQL();
}

int32_t tSerializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->cacheLastSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walFsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLast) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->sstTrigger) < 0) return -1;

  // 1st modification
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  // 2nd modification
  if (tEncodeI32(&encoder, pReq->walRetentionPeriod) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walRetentionSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->keepTimeOffset) < 0) return -1;
  ENCODESQL();
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
  if (tDecodeI32(&decoder, &pReq->cacheLastSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walFsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLast) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->sstTrigger) < 0) return -1;

  // 1st modification
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  } else {
    pReq->minRows = -1;
  }

  // 2nd modification
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->walRetentionPeriod) < 0) return -1;
    if (tDecodeI32(&decoder, &pReq->walRetentionSize) < 0) return -1;
  } else {
    pReq->walRetentionPeriod = -1;
    pReq->walRetentionSize = -1;
  }
  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->keepTimeOffset) < 0) return -1;
  }

  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSAlterDbReq(SAlterDbReq *pReq) { FREESQL(); }

int32_t tSerializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreNotExists) < 0) return -1;
  ENCODESQL();
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
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSDropDbReq(SDropDbReq *pReq) { FREESQL(); }

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
  if (tEncodeI64(&encoder, pReq->stateTs) < 0) return -1;
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
  if (tDecodeI64(&decoder, &pReq->stateTs) < 0) return -1;
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

int32_t tSerializeSDnodeListReq(void *buf, int32_t bufLen, SDnodeListReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->rowNum) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSServerVerReq(void *buf, int32_t bufLen, SServerVerReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->useless) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

// int32_t tDeserializeSServerVerReq(void *buf, int32_t bufLen, SServerVerReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &pReq->useless) < 0) return -1;

//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

int32_t tSerializeSServerVerRsp(void *buf, int32_t bufLen, SServerVerRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->ver) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSServerVerRsp(void *buf, int32_t bufLen, SServerVerRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->ver) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  int32_t num = taosArrayGetSize(pRsp->qnodeList);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SQueryNodeLoad *pLoad = taosArrayGet(pRsp->qnodeList, i);
    if (tEncodeSQueryNodeLoad(&encoder, pLoad) < 0) return -1;
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
  if (NULL == pRsp->qnodeList) {
    pRsp->qnodeList = taosArrayInit(num, sizeof(SQueryNodeLoad));
    if (NULL == pRsp->qnodeList) return -1;
  }

  for (int32_t i = 0; i < num; ++i) {
    SQueryNodeLoad load = {0};
    if (tDecodeSQueryNodeLoad(&decoder, &load) < 0) return -1;
    taosArrayPush(pRsp->qnodeList, &load);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSQnodeListRsp(SQnodeListRsp *pRsp) { taosArrayDestroy(pRsp->qnodeList); }

int32_t tSerializeSDnodeListRsp(void *buf, int32_t bufLen, SDnodeListRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  int32_t num = taosArrayGetSize(pRsp->dnodeList);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SEpSet *pEpSet = taosArrayGet(pRsp->dnodeList, i);
    if (tEncodeSEpSet(&encoder, pEpSet) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDnodeListRsp(void *buf, int32_t bufLen, SDnodeListRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (NULL == pRsp->dnodeList) {
    pRsp->dnodeList = taosArrayInit(num, sizeof(SEpSet));
    if (NULL == pRsp->dnodeList) return -1;
  }

  for (int32_t i = 0; i < num; ++i) {
    SEpSet epSet = {0};
    if (tDecodeSEpSet(&decoder, &epSet) < 0) return -1;
    taosArrayPush(pRsp->dnodeList, &epSet);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSDnodeListRsp(SDnodeListRsp *pRsp) { taosArrayDestroy(pRsp->dnodeList); }

int32_t tSerializeSCompactDbReq(void *buf, int32_t bufLen, SCompactDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->timeRange.skey) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->timeRange.ekey) < 0) return -1;
  ENCODESQL();
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
  if (tDecodeI64(&decoder, &pReq->timeRange.skey) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->timeRange.ekey) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCompactDbReq(SCompactDbReq *pReq) { FREESQL(); }

int32_t tSerializeSCompactDbRsp(void *buf, int32_t bufLen, SCompactDbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->compactId) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->bAccepted) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactDbRsp(void *buf, int32_t bufLen, SCompactDbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->compactId) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->bAccepted) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillCompactReq(void *buf, int32_t bufLen, SKillCompactReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->compactId) < 0) return -1;
  ENCODESQL();

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillCompactReq(void *buf, int32_t bufLen, SKillCompactReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->compactId) < 0) return -1;
  DECODESQL();

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSKillCompactReq(SKillCompactReq *pReq) { FREESQL(); }

int32_t tSerializeSUseDbRspImp(SEncoder *pEncoder, const SUseDbRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->db) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->uid) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgVersion) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgNum) < 0) return -1;
  if (tEncodeI16(pEncoder, pRsp->hashPrefix) < 0) return -1;
  if (tEncodeI16(pEncoder, pRsp->hashSuffix) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->hashMethod) < 0) return -1;

  for (int32_t i = 0; i < pRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(pRsp->pVgroupInfos, i);
    if (tEncodeI32(pEncoder, pVgInfo->vgId) < 0) return -1;
    if (tEncodeU32(pEncoder, pVgInfo->hashBegin) < 0) return -1;
    if (tEncodeU32(pEncoder, pVgInfo->hashEnd) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pVgInfo->epSet) < 0) return -1;
    if (tEncodeI32(pEncoder, pVgInfo->numOfTable) < 0) return -1;
  }

  if (tEncodeI32(pEncoder, pRsp->errCode) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->stateTs) < 0) return -1;
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

int32_t tSerializeSDbHbRspImp(SEncoder *pEncoder, const SDbHbRsp *pRsp) {
  if (pRsp->useDbRsp) {
    if (tEncodeI8(pEncoder, 1) < 0) return -1;
    if (tSerializeSUseDbRspImp(pEncoder, pRsp->useDbRsp) < 0) return -1;
  } else {
    if (tEncodeI8(pEncoder, 0) < 0) return -1;
  }

  if (pRsp->cfgRsp) {
    if (tEncodeI8(pEncoder, 1) < 0) return -1;
    if (tSerializeSDbCfgRspImpl(pEncoder, pRsp->cfgRsp) < 0) return -1;
  } else {
    if (tEncodeI8(pEncoder, 0) < 0) return -1;
  }

  return 0;
}

int32_t tSerializeSDbHbBatchRsp(void *buf, int32_t bufLen, SDbHbBatchRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tEncodeI32(&encoder, numOfBatch) < 0) return -1;
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SDbHbRsp *pDbRsp = taosArrayGet(pRsp->pArray, i);
    if (tSerializeSDbHbRspImp(&encoder, pDbRsp) < 0) return -1;
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
  if (tDecodeI16(pDecoder, &pRsp->hashPrefix) < 0) return -1;
  if (tDecodeI16(pDecoder, &pRsp->hashSuffix) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->hashMethod) < 0) return -1;

  if (pRsp->vgNum > 0) {
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
  }

  if (tDecodeI32(pDecoder, &pRsp->errCode) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->stateTs) < 0) return -1;
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

int32_t tDeserializeSDbHbRspImp(SDecoder *decoder, SDbHbRsp *pRsp) {
  int8_t flag = 0;
  if (tDecodeI8(decoder, &flag) < 0) return -1;
  if (flag) {
    pRsp->useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
    if (NULL == pRsp->useDbRsp) return -1;
    if (tDeserializeSUseDbRspImp(decoder, pRsp->useDbRsp) < 0) return -1;
  }
  if (tDecodeI8(decoder, &flag) < 0) return -1;
  if (flag) {
    pRsp->cfgRsp = taosMemoryCalloc(1, sizeof(SDbCfgRsp));
    if (NULL == pRsp->cfgRsp) return -1;
    if (tDeserializeSDbCfgRspImpl(decoder, pRsp->cfgRsp) < 0) return -1;
  }

  return 0;
}

int32_t tDeserializeSDbHbBatchRsp(void *buf, int32_t bufLen, SDbHbBatchRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tDecodeI32(&decoder, &numOfBatch) < 0) return -1;

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(SDbHbRsp));
  if (pRsp->pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfBatch; ++i) {
    SDbHbRsp rsp = {0};
    if (tDeserializeSDbHbRspImp(&decoder, &rsp) < 0) {
      tDecoderClear(&decoder);
      return -1;
    }
    taosArrayPush(pRsp->pArray, &rsp);
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSUsedbRsp(SUseDbRsp *pRsp) { taosArrayDestroy(pRsp->pVgroupInfos); }

void tFreeSDbHbRsp(SDbHbRsp *pDbRsp) {
  if (NULL == pDbRsp) {
    return;
  }

  if (pDbRsp->useDbRsp) {
    tFreeSUsedbRsp(pDbRsp->useDbRsp);
    taosMemoryFree(pDbRsp->useDbRsp);
  }

  if (pDbRsp->cfgRsp) {
    tFreeSDbCfgRsp(pDbRsp->cfgRsp);
    taosMemoryFree(pDbRsp->cfgRsp);
  }
}

void tFreeSDbHbBatchRsp(SDbHbBatchRsp *pRsp) {
  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SDbHbRsp *pDbRsp = taosArrayGet(pRsp->pArray, i);
    tFreeSDbHbRsp(pDbRsp);
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

int32_t tSerializeSTrimDbReq(void *buf, int32_t bufLen, STrimDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxSpeed) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTrimDbReq(void *buf, int32_t bufLen, STrimDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxSpeed) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVTrimDbReq(void *buf, int32_t bufLen, SVTrimDbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->timestamp) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVTrimDbReq(void *buf, int32_t bufLen, SVTrimDbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->timestamp) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVDropTtlTableReq(void *buf, int32_t bufLen, SVDropTtlTableReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->timestampSec) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ttlDropMaxCount) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->nUids) < 0) return -1;
  for (int32_t i = 0; i < pReq->nUids; ++i) {
    tb_uid_t *pTbUid = taosArrayGet(pReq->pTbUids, i);
    if (tEncodeI64(&encoder, *pTbUid) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVDropTtlTableReq(void *buf, int32_t bufLen, SVDropTtlTableReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->timestampSec) < 0) return -1;
  pReq->ttlDropMaxCount = INT32_MAX;
  pReq->nUids = 0;
  pReq->pTbUids = NULL;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->ttlDropMaxCount) < 0) return -1;
    if (tDecodeI32(&decoder, &pReq->nUids) < 0) return -1;

    if (pReq->nUids > 0) {
      pReq->pTbUids = taosArrayInit(pReq->nUids, sizeof(tb_uid_t));
      if (pReq->pTbUids == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
    }

    tb_uid_t tbUid = 0;
    for (int32_t i = 0; i < pReq->nUids; ++i) {
      if (tDecodeI64(&decoder, &tbUid) < 0) return -1;
      if (taosArrayPush(pReq->pTbUids, &tbUid) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
    }
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDbCfgRspImpl(SEncoder *encoder, const SDbCfgRsp *pRsp) {
  if (tEncodeCStr(encoder, pRsp->db) < 0) return -1;
  if (tEncodeI64(encoder, pRsp->dbId) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->cfgVersion) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->numOfVgroups) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->numOfStables) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->buffer) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->cacheSize) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->pageSize) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->pages) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->daysPerFile) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->daysToKeep0) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->daysToKeep1) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->daysToKeep2) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->minRows) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->maxRows) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->walFsyncPeriod) < 0) return -1;
  if (tEncodeI16(encoder, pRsp->hashPrefix) < 0) return -1;
  if (tEncodeI16(encoder, pRsp->hashSuffix) < 0) return -1;
  if (tEncodeI8(encoder, pRsp->walLevel) < 0) return -1;
  if (tEncodeI8(encoder, pRsp->precision) < 0) return -1;
  if (tEncodeI8(encoder, pRsp->compression) < 0) return -1;
  if (tEncodeI8(encoder, pRsp->replications) < 0) return -1;
  if (tEncodeI8(encoder, pRsp->strict) < 0) return -1;
  if (tEncodeI8(encoder, pRsp->cacheLast) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->tsdbPageSize) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->walRetentionPeriod) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->walRollPeriod) < 0) return -1;
  if (tEncodeI64(encoder, pRsp->walRetentionSize) < 0) return -1;
  if (tEncodeI64(encoder, pRsp->walSegmentSize) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pRsp->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pRsp->pRetensions, i);
    if (tEncodeI64(encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI64(encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(encoder, pRetension->keepUnit) < 0) return -1;
  }
  if (tEncodeI8(encoder, pRsp->schemaless) < 0) return -1;
  if (tEncodeI16(encoder, pRsp->sstTrigger) < 0) return -1;
  if (tEncodeI32(encoder, pRsp->keepTimeOffset) < 0) return -1;

  return 0;
}

int32_t tSerializeSDbCfgRsp(void *buf, int32_t bufLen, const SDbCfgRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  tSerializeSDbCfgRspImpl(&encoder, pRsp);
  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgRspImpl(SDecoder *decoder, SDbCfgRsp *pRsp) {
  if (tDecodeCStrTo(decoder, pRsp->db) < 0) return -1;
  if (tDecodeI64(decoder, &pRsp->dbId) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->cfgVersion) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->numOfVgroups) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->numOfStables) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->buffer) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->cacheSize) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->pageSize) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->pages) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->daysPerFile) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->daysToKeep0) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->daysToKeep1) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->daysToKeep2) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->minRows) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->maxRows) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->walFsyncPeriod) < 0) return -1;
  if (tDecodeI16(decoder, &pRsp->hashPrefix) < 0) return -1;
  if (tDecodeI16(decoder, &pRsp->hashSuffix) < 0) return -1;
  if (tDecodeI8(decoder, &pRsp->walLevel) < 0) return -1;
  if (tDecodeI8(decoder, &pRsp->precision) < 0) return -1;
  if (tDecodeI8(decoder, &pRsp->compression) < 0) return -1;
  if (tDecodeI8(decoder, &pRsp->replications) < 0) return -1;
  if (tDecodeI8(decoder, &pRsp->strict) < 0) return -1;
  if (tDecodeI8(decoder, &pRsp->cacheLast) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->tsdbPageSize) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->walRetentionPeriod) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->walRollPeriod) < 0) return -1;
  if (tDecodeI64(decoder, &pRsp->walRetentionSize) < 0) return -1;
  if (tDecodeI64(decoder, &pRsp->walSegmentSize) < 0) return -1;
  if (tDecodeI32(decoder, &pRsp->numOfRetensions) < 0) return -1;
  if (pRsp->numOfRetensions > 0) {
    pRsp->pRetensions = taosArrayInit(pRsp->numOfRetensions, sizeof(SRetention));
    if (pRsp->pRetensions == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int32_t i = 0; i < pRsp->numOfRetensions; ++i) {
    SRetention rentension = {0};
    if (tDecodeI64(decoder, &rentension.freq) < 0) return -1;
    if (tDecodeI64(decoder, &rentension.keep) < 0) return -1;
    if (tDecodeI8(decoder, &rentension.freqUnit) < 0) return -1;
    if (tDecodeI8(decoder, &rentension.keepUnit) < 0) return -1;
    if (taosArrayPush(pRsp->pRetensions, &rentension) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }
  if (tDecodeI8(decoder, &pRsp->schemaless) < 0) return -1;
  if (tDecodeI16(decoder, &pRsp->sstTrigger) < 0) return -1;
  pRsp->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(decoder)) {
    if (tDecodeI32(decoder, &pRsp->keepTimeOffset) < 0) return -1;
  }

  return 0;
}

int32_t tDeserializeSDbCfgRsp(void *buf, int32_t bufLen, SDbCfgRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDeserializeSDbCfgRspImpl(&decoder, pRsp) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSDbCfgRsp(SDbCfgRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosArrayDestroy(pRsp->pRetensions);
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

int32_t tSerializeSTableIndexReq(void *buf, int32_t bufLen, STableIndexReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->tbFName) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableIndexReq(void *buf, int32_t bufLen, STableIndexReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->tbFName) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSTableIndexInfo(SEncoder *pEncoder, STableIndexInfo *pInfo) {
  if (tEncodeI8(pEncoder, pInfo->intervalUnit) < 0) return -1;
  if (tEncodeI8(pEncoder, pInfo->slidingUnit) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->interval) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->offset) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->sliding) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->dstTbUid) < 0) return -1;
  if (tEncodeI32(pEncoder, pInfo->dstVgId) < 0) return -1;
  if (tEncodeSEpSet(pEncoder, &pInfo->epSet) < 0) return -1;
  if (tEncodeCStr(pEncoder, pInfo->expr) < 0) return -1;
  return 0;
}

int32_t tSerializeSTableIndexRsp(void *buf, int32_t bufLen, const STableIndexRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->tbName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeU64(&encoder, pRsp->suid) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->version) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->indexSize) < 0) return -1;
  int32_t num = taosArrayGetSize(pRsp->pIndex);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  if (num > 0) {
    for (int32_t i = 0; i < num; ++i) {
      STableIndexInfo *pInfo = (STableIndexInfo *)taosArrayGet(pRsp->pIndex, i);
      if (tSerializeSTableIndexInfo(&encoder, pInfo) < 0) return -1;
    }
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

void tFreeSerializeSTableIndexRsp(STableIndexRsp *pRsp) {
  if (pRsp->pIndex != NULL) {
    tFreeSTableIndexRsp(pRsp);
    pRsp->pIndex = NULL;
  }
}

int32_t tDeserializeSTableIndexInfo(SDecoder *pDecoder, STableIndexInfo *pInfo) {
  if (tDecodeI8(pDecoder, &pInfo->intervalUnit) < 0) return -1;
  if (tDecodeI8(pDecoder, &pInfo->slidingUnit) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->interval) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->offset) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->sliding) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->dstTbUid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pInfo->dstVgId) < 0) return -1;
  if (tDecodeSEpSet(pDecoder, &pInfo->epSet) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pInfo->expr) < 0) return -1;

  return 0;
}

int32_t tDeserializeSTableIndexRsp(void *buf, int32_t bufLen, STableIndexRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->tbName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeU64(&decoder, &pRsp->suid) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->version) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->indexSize) < 0) return -1;
  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (num > 0) {
    pRsp->pIndex = taosArrayInit(num, sizeof(STableIndexInfo));
    if (NULL == pRsp->pIndex) return -1;
    STableIndexInfo info;
    for (int32_t i = 0; i < num; ++i) {
      if (tDeserializeSTableIndexInfo(&decoder, &info) < 0) return -1;
      if (NULL == taosArrayPush(pRsp->pIndex, &info)) {
        taosMemoryFree(info.expr);
        return -1;
      }
    }
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSTableIndexInfo(void *info) {
  if (NULL == info) {
    return;
  }

  STableIndexInfo *pInfo = (STableIndexInfo *)info;

  taosMemoryFree(pInfo->expr);
}

int32_t tSerializeSShowVariablesReq(void *buf, int32_t bufLen, SShowVariablesReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->useless) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

// int32_t tDeserializeSShowVariablesReq(void *buf, int32_t bufLen, SShowVariablesReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &pReq->useless) < 0) return -1;

//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

int32_t tEncodeSVariablesInfo(SEncoder *pEncoder, SVariablesInfo *pInfo) {
  if (tEncodeCStr(pEncoder, pInfo->name) < 0) return -1;
  if (tEncodeCStr(pEncoder, pInfo->value) < 0) return -1;
  if (tEncodeCStr(pEncoder, pInfo->scope) < 0) return -1;
  return 0;
}

int32_t tDecodeSVariablesInfo(SDecoder *pDecoder, SVariablesInfo *pInfo) {
  if (tDecodeCStrTo(pDecoder, pInfo->name) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pInfo->value) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pInfo->scope) < 0) return -1;
  return 0;
}

int32_t tSerializeSShowVariablesRsp(void *buf, int32_t bufLen, SShowVariablesRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  int32_t varNum = taosArrayGetSize(pRsp->variables);
  if (tEncodeI32(&encoder, varNum) < 0) return -1;
  for (int32_t i = 0; i < varNum; ++i) {
    SVariablesInfo *pInfo = taosArrayGet(pRsp->variables, i);
    if (tEncodeSVariablesInfo(&encoder, pInfo) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowVariablesRsp(void *buf, int32_t bufLen, SShowVariablesRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  int32_t varNum = 0;
  if (tDecodeI32(&decoder, &varNum) < 0) return -1;
  if (varNum > 0) {
    pRsp->variables = taosArrayInit(varNum, sizeof(SVariablesInfo));
    if (NULL == pRsp->variables) return -1;
    for (int32_t i = 0; i < varNum; ++i) {
      SVariablesInfo info = {0};
      if (tDecodeSVariablesInfo(&decoder, &info) < 0) return -1;
      if (NULL == taosArrayPush(pRsp->variables, &info)) return -1;
    }
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSShowVariablesRsp(SShowVariablesRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosArrayDestroy(pRsp->variables);
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

// int32_t tDeserializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &pReq->type) < 0) return -1;
//   if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
//   if (tDecodeI32(&decoder, &pReq->payloadLen) < 0) return -1;
//   if (pReq->payloadLen > 0) {
//     pReq->payload = taosMemoryMalloc(pReq->payloadLen);
//     if (pReq->payload == NULL) return -1;
//     if (tDecodeCStrTo(&decoder, pReq->payload) < 0) return -1;
//   }

//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

void tFreeSShowReq(SShowReq *pReq) { taosMemoryFreeClear(pReq->payload); }

int32_t tSerializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->showId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->tb) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->filterTb) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->compactId) < 0) return -1;
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
  if (tDecodeCStrTo(&decoder, pReq->filterTb) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI64(&decoder, &pReq->compactId) < 0) return -1;
  } else {
    pReq->compactId = -1;
  }

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
  if (totalCols > 0) {
    pRsp->pSchemas = taosMemoryMalloc(sizeof(SSchema) * totalCols);
    if (pRsp->pSchemas == NULL) return -1;

    for (int32_t i = 0; i < totalCols; ++i) {
      SSchema *pSchema = &pRsp->pSchemas[i];
      if (tDecodeSSchema(pDecoder, pSchema) < 0) return -1;
    }
  } else {
    pRsp->pSchemas = NULL;
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

int32_t tSerializeSSTbHbRsp(void *buf, int32_t bufLen, SSTbHbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfMeta = taosArrayGetSize(pRsp->pMetaRsp);
  if (tEncodeI32(&encoder, numOfMeta) < 0) return -1;
  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pMetaRsp, i);
    if (tEncodeSTableMetaRsp(&encoder, pMetaRsp) < 0) return -1;
  }

  int32_t numOfIndex = taosArrayGetSize(pRsp->pIndexRsp);
  if (tEncodeI32(&encoder, numOfIndex) < 0) return -1;
  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp *pIndexRsp = taosArrayGet(pRsp->pIndexRsp, i);
    if (tEncodeCStr(&encoder, pIndexRsp->tbName) < 0) return -1;
    if (tEncodeCStr(&encoder, pIndexRsp->dbFName) < 0) return -1;
    if (tEncodeU64(&encoder, pIndexRsp->suid) < 0) return -1;
    if (tEncodeI32(&encoder, pIndexRsp->version) < 0) return -1;
    if (tEncodeI32(&encoder, pIndexRsp->indexSize) < 0) return -1;
    int32_t num = taosArrayGetSize(pIndexRsp->pIndex);
    if (tEncodeI32(&encoder, num) < 0) return -1;
    for (int32_t j = 0; j < num; ++j) {
      STableIndexInfo *pInfo = (STableIndexInfo *)taosArrayGet(pIndexRsp->pIndex, j);
      if (tSerializeSTableIndexInfo(&encoder, pInfo) < 0) return -1;
    }
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

int32_t tDeserializeSSTbHbRsp(void *buf, int32_t bufLen, SSTbHbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t numOfMeta = 0;
  if (tDecodeI32(&decoder, &numOfMeta) < 0) return -1;
  pRsp->pMetaRsp = taosArrayInit(numOfMeta, sizeof(STableMetaRsp));
  if (pRsp->pMetaRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp tableMetaRsp = {0};
    if (tDecodeSTableMetaRsp(&decoder, &tableMetaRsp) < 0) return -1;
    taosArrayPush(pRsp->pMetaRsp, &tableMetaRsp);
  }

  int32_t numOfIndex = 0;
  if (tDecodeI32(&decoder, &numOfIndex) < 0) return -1;

  pRsp->pIndexRsp = taosArrayInit(numOfIndex, sizeof(STableIndexRsp));
  if (pRsp->pIndexRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp tableIndexRsp = {0};
    if (tDecodeCStrTo(&decoder, tableIndexRsp.tbName) < 0) return -1;
    if (tDecodeCStrTo(&decoder, tableIndexRsp.dbFName) < 0) return -1;
    if (tDecodeU64(&decoder, &tableIndexRsp.suid) < 0) return -1;
    if (tDecodeI32(&decoder, &tableIndexRsp.version) < 0) return -1;
    if (tDecodeI32(&decoder, &tableIndexRsp.indexSize) < 0) return -1;
    int32_t num = 0;
    if (tDecodeI32(&decoder, &num) < 0) return -1;
    if (num > 0) {
      tableIndexRsp.pIndex = taosArrayInit(num, sizeof(STableIndexInfo));
      if (NULL == tableIndexRsp.pIndex) return -1;
      STableIndexInfo info;
      for (int32_t j = 0; j < num; ++j) {
        if (tDeserializeSTableIndexInfo(&decoder, &info) < 0) return -1;
        if (NULL == taosArrayPush(tableIndexRsp.pIndex, &info)) {
          taosMemoryFree(info.expr);
          return -1;
        }
      }
    }
    taosArrayPush(pRsp->pIndexRsp, &tableIndexRsp);
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSTableMetaRsp(void *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosMemoryFreeClear(((STableMetaRsp *)pRsp)->pSchemas);
}

void tFreeSTableIndexRsp(void *info) {
  if (NULL == info) {
    return;
  }

  STableIndexRsp *pInfo = (STableIndexRsp *)info;

  taosArrayDestroyEx(pInfo->pIndex, tFreeSTableIndexInfo);
}

void tFreeSSTbHbRsp(SSTbHbRsp *pRsp) {
  int32_t numOfMeta = taosArrayGetSize(pRsp->pMetaRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pMetaRsp, i);
    tFreeSTableMetaRsp(pMetaRsp);
  }

  taosArrayDestroy(pRsp->pMetaRsp);

  int32_t numOfIndex = taosArrayGetSize(pRsp->pIndexRsp);
  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp *pIndexRsp = taosArrayGet(pRsp->pIndexRsp, i);
    tFreeSTableIndexRsp(pIndexRsp);
  }

  taosArrayDestroy(pRsp->pIndexRsp);
}

// int32_t tSerializeSShowRsp(void *buf, int32_t bufLen, SShowRsp *pRsp) {
//   SEncoder encoder = {0};
//   tEncoderInit(&encoder, buf, bufLen);

//   if (tStartEncode(&encoder) < 0) return -1;
//   if (tEncodeI64(&encoder, pRsp->showId) < 0) return -1;
//   if (tEncodeSTableMetaRsp(&encoder, &pRsp->tableMeta) < 0) return -1;
//   tEndEncode(&encoder);

//   int32_t tlen = encoder.pos;
//   tEncoderClear(&encoder);
//   return tlen;
// }

// int32_t tDeserializeSShowRsp(void *buf, int32_t bufLen, SShowRsp *pRsp) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI64(&decoder, &pRsp->showId) < 0) return -1;
//   if (tDecodeSTableMetaRsp(&decoder, &pRsp->tableMeta) < 0) return -1;

//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

// void tFreeSShowRsp(SShowRsp *pRsp) { tFreeSTableMetaRsp(&pRsp->tableMeta); }

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
  ENCODESQL();
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
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMDropTopicReq(SMDropTopicReq *pReq) { FREESQL(); }

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
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->subType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->withMeta) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->subDbName) < 0) return -1;
  if (TOPIC_SUB_TYPE__DB == pReq->subType) {
  } else {
    if (TOPIC_SUB_TYPE__TABLE == pReq->subType) {
      if (tEncodeCStr(&encoder, pReq->subStbName) < 0) return -1;
    }
    if (pReq->ast && strlen(pReq->ast) > 0) {
      if (tEncodeI32(&encoder, strlen(pReq->ast)) < 0) return -1;
      if (tEncodeCStr(&encoder, pReq->ast) < 0) return -1;
    } else {
      if (tEncodeI32(&encoder, 0) < 0) return -1;
    }
  }
  if (tEncodeI32(&encoder, strlen(pReq->sql)) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sql) < 0) return -1;

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
  if (tDecodeI8(&decoder, &pReq->subType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->withMeta) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->subDbName) < 0) return -1;
  if (TOPIC_SUB_TYPE__DB == pReq->subType) {
  } else {
    if (TOPIC_SUB_TYPE__TABLE == pReq->subType) {
      if (tDecodeCStrTo(&decoder, pReq->subStbName) < 0) return -1;
    }
    if (tDecodeI32(&decoder, &astLen) < 0) return -1;
    if (astLen > 0) {
      pReq->ast = taosMemoryCalloc(1, astLen + 1);
      if (pReq->ast == NULL) return -1;
      if (tDecodeCStrTo(&decoder, pReq->ast) < 0) return -1;
    }
  }
  if (tDecodeI32(&decoder, &sqlLen) < 0) return -1;
  if (sqlLen > 0) {
    pReq->sql = taosMemoryCalloc(1, sqlLen + 1);
    if (pReq->sql == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->sql) < 0) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCMCreateTopicReq(SCMCreateTopicReq *pReq) {
  taosMemoryFreeClear(pReq->sql);
  if (TOPIC_SUB_TYPE__DB != pReq->subType) {
    taosMemoryFreeClear(pReq->ast);
  }
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
  if (tEncodeCStrWithLen(&encoder, pReq->passwd, TSDB_PASSWORD_LEN) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->startTime) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sVer) < 0) return -1;
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
  // Check the client version from version 3.0.3.0
  if (tDecodeIsEnd(&decoder)) {
    tDecoderClear(&decoder);
    return TSDB_CODE_VERSION_NOT_COMPATIBLE;
  }
  if (tDecodeCStrTo(&decoder, pReq->sVer) < 0) return -1;
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
  if (tEncodeI8(&encoder, pRsp->sysInfo) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->connType) < 0) return -1;
  if (tEncodeSEpSet(&encoder, &pRsp->epSet) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->svrTimestamp) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->sVer) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->sDetailVer) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->passVer) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->authVer) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->whiteListVer) < 0) return -1;
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
  if (tDecodeI8(&decoder, &pRsp->sysInfo) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->connType) < 0) return -1;
  if (tDecodeSEpSet(&decoder, &pRsp->epSet) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->svrTimestamp) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->sVer) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->sDetailVer) < 0) return -1;

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pRsp->passVer) < 0) return -1;
  } else {
    pRsp->passVer = 0;
  }
  // since 3.0.7.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pRsp->authVer) < 0) return -1;
  } else {
    pRsp->authVer = 0;
  }

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI64(&decoder, &pRsp->whiteListVer) < 0) return -1;
  } else {
    pRsp->whiteListVer = 0;
  }
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

// int32_t tDeserializeSMTimerMsg(void *buf, int32_t bufLen, SMTimerReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &pReq->reserved) < 0) return -1;
//   tEndDecode(&decoder);

//   tDecoderClear(&decoder);
//   return 0;
// }

int32_t tSerializeSMStreamTickMsg(void *buf, int32_t bufLen, SMStreamTickReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->tick) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

// int32_t tDeserializeSMStreamTickMsg(void *buf, int32_t bufLen, SMStreamTickReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI64(&decoder, &pReq->tick) < 0) return -1;
//   tEndDecode(&decoder);

//   tDecoderClear(&decoder);
//   return 0;
// }

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
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfStables) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->cacheLastSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walFsyncPeriod) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->hashBegin) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->hashEnd) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->hashMethod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLast) < 0) return -1;
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
  if (pReq->isTsma) {
    uint32_t tsmaLen = (uint32_t)(htonl(((SMsgHead *)pReq->pTsma)->contLen));
    if (tEncodeBinary(&encoder, (const uint8_t *)pReq->pTsma, tsmaLen) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->walRetentionPeriod) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->walRetentionSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walRollPeriod) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->walSegmentSize) < 0) return -1;
  if (tEncodeI16(&encoder, pReq->sstTrigger) < 0) return -1;
  if (tEncodeI16(&encoder, pReq->hashPrefix) < 0) return -1;
  if (tEncodeI16(&encoder, pReq->hashSuffix) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->tsdbPageSize) < 0) return -1;
  for (int32_t i = 0; i < 8; ++i) {
    if (tEncodeI64(&encoder, pReq->reserved[i]) < 0) return -1;
  }
  if (tEncodeI8(&encoder, pReq->learnerReplica) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->learnerSelfIndex) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
    SReplica *pReplica = &pReq->learnerReplicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->changeVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->keepTimeOffset) < 0) return -1;

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
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfStables) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->cacheLastSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walFsyncPeriod) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->hashBegin) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->hashEnd) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->hashMethod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLast) < 0) return -1;
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
  if (pReq->isTsma) {
    if (tDecodeBinary(&decoder, (uint8_t **)&pReq->pTsma, NULL) < 0) return -1;
  }

  if (tDecodeI32(&decoder, &pReq->walRetentionPeriod) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->walRetentionSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walRollPeriod) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->walSegmentSize) < 0) return -1;
  if (tDecodeI16(&decoder, &pReq->sstTrigger) < 0) return -1;
  if (tDecodeI16(&decoder, &pReq->hashPrefix) < 0) return -1;
  if (tDecodeI16(&decoder, &pReq->hashSuffix) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->tsdbPageSize) < 0) return -1;
  for (int32_t i = 0; i < 8; ++i) {
    if (tDecodeI64(&decoder, &pReq->reserved[i]) < 0) return -1;
  }
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->learnerReplica) < 0) return -1;
    if (tDecodeI8(&decoder, &pReq->learnerSelfIndex) < 0) return -1;
    for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
      SReplica *pReplica = &pReq->learnerReplicas[i];
      if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->changeVersion) < 0) return -1;
  }
  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->keepTimeOffset) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tFreeSCreateVnodeReq(SCreateVnodeReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
  return 0;
}

int32_t tSerializeSQueryCompactProgressReq(void *buf, int32_t bufLen, SQueryCompactProgressReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->compactId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQueryCompactProgressReq(void *buf, int32_t bufLen, SQueryCompactProgressReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SDecoder decoder = {0};
  tDecoderInit(&decoder, ((uint8_t *)buf) + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->compactId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQueryCompactProgressRsp(void *buf, int32_t bufLen, SQueryCompactProgressRsp *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->compactId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numberFileset) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->finished) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSQueryCompactProgressRsp(void *buf, int32_t bufLen, SQueryCompactProgressRsp *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->compactId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numberFileset) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->finished) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
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
  for (int32_t i = 0; i < 8; ++i) {
    if (tEncodeI64(&encoder, pReq->reserved[i]) < 0) return -1;
  }
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
  for (int32_t i = 0; i < 8; ++i) {
    if (tDecodeI64(&decoder, &pReq->reserved[i]) < 0) return -1;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}
int32_t tSerializeSDropIdxReq(void *buf, int32_t bufLen, SDropIndexReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->colName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->stb) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->stbUid) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  for (int32_t i = 0; i < 8; ++i) {
    if (tEncodeI64(&encoder, pReq->reserved[i]) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
  // TODO
  return 0;
}
int32_t tDeserializeSDropIdxReq(void *buf, int32_t bufLen, SDropIndexReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->colName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->stb) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->stbUid) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  for (int32_t i = 0; i < 8; ++i) {
    if (tDecodeI64(&decoder, &pReq->reserved[i]) < 0) return -1;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  // TODO
  return 0;
}

int32_t tSerializeSCompactVnodeReq(void *buf, int32_t bufLen, SCompactVnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->compactStartTime) < 0) return -1;

  // 1.1 add tw.skey and tw.ekey
  if (tEncodeI64(&encoder, pReq->tw.skey) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->tw.ekey) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->compactId) < 0) return -1;

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
  if (tDecodeI64(&decoder, &pReq->compactStartTime) < 0) return -1;

  // 1.1
  if (tDecodeIsEnd(&decoder)) {
    pReq->tw.skey = TSKEY_MIN;
    pReq->tw.ekey = TSKEY_MAX;
  } else {
    if (tDecodeI64(&decoder, &pReq->tw.skey) < 0) return -1;
    if (tDecodeI64(&decoder, &pReq->tw.ekey) < 0) return -1;
  }

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->compactId) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVKillCompactReq(void *buf, int32_t bufLen, SVKillCompactReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pReq->compactId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVKillCompactReq(void *buf, int32_t bufLen, SVKillCompactReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pReq->compactId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAlterVnodeConfigReq(void *buf, int32_t bufLen, SAlterVnodeConfigReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->buffer) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pageSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pages) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->cacheLastSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walFsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLast) < 0) return -1;
  for (int32_t i = 0; i < 8; ++i) {
    if (tEncodeI64(&encoder, pReq->reserved[i]) < 0) return -1;
  }

  // 1st modification
  if (tEncodeI16(&encoder, pReq->sttTrigger) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  // 2nd modification
  if (tEncodeI32(&encoder, pReq->walRetentionPeriod) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->walRetentionSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->keepTimeOffset) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeConfigReq(void *buf, int32_t bufLen, SAlterVnodeConfigReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->buffer) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pageSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pages) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->cacheLastSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->walFsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLast) < 0) return -1;
  for (int32_t i = 0; i < 8; ++i) {
    if (tDecodeI64(&decoder, &pReq->reserved[i]) < 0) return -1;
  }

  // 1st modification
  if (tDecodeIsEnd(&decoder)) {
    pReq->sttTrigger = -1;
    pReq->minRows = -1;
  } else {
    if (tDecodeI16(&decoder, &pReq->sttTrigger) < 0) return -1;
    if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  }

  // 2n modification
  if (tDecodeIsEnd(&decoder)) {
    pReq->walRetentionPeriod = -1;
    pReq->walRetentionSize = -1;
  } else {
    if (tDecodeI32(&decoder, &pReq->walRetentionPeriod) < 0) return -1;
    if (tDecodeI32(&decoder, &pReq->walRetentionSize) < 0) return -1;
  }
  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->keepTimeOffset) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAlterVnodeReplicaReq(void *buf, int32_t bufLen, SAlterVnodeReplicaReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->strict) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->selfIndex) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  for (int32_t i = 0; i < 8; ++i) {
    if (tEncodeI64(&encoder, pReq->reserved[i]) < 0) return -1;
  }
  if (tEncodeI8(&encoder, pReq->learnerSelfIndex) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->learnerReplica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
    SReplica *pReplica = &pReq->learnerReplicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->changeVersion) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeReplicaReq(void *buf, int32_t bufLen, SAlterVnodeReplicaReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->strict) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->selfIndex) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
  }
  for (int32_t i = 0; i < 8; ++i) {
    if (tDecodeI64(&decoder, &pReq->reserved[i]) < 0) return -1;
  }
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->learnerSelfIndex) < 0) return -1;
    if (tDecodeI8(&decoder, &pReq->learnerReplica) < 0) return -1;
    for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
      SReplica *pReplica = &pReq->learnerReplicas[i];
      if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->changeVersion) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDisableVnodeWriteReq(void *buf, int32_t bufLen, SDisableVnodeWriteReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->disable) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDisableVnodeWriteReq(void *buf, int32_t bufLen, SDisableVnodeWriteReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->disable) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAlterVnodeHashRangeReq(void *buf, int32_t bufLen, SAlterVnodeHashRangeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->srcVgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dstVgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->hashBegin) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->hashEnd) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->changeVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->reserved) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeHashRangeReq(void *buf, int32_t bufLen, SAlterVnodeHashRangeReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->srcVgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dstVgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->hashBegin) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->hashEnd) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->changeVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->reserved) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->queryStrId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->queryStrId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->connId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->connId) < 0) return -1;
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

int32_t tSerializeSBalanceVgroupReq(void *buf, int32_t bufLen, SBalanceVgroupReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->useless) < 0) return -1;
  ENCODESQL();
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSBalanceVgroupReq(void *buf, int32_t bufLen, SBalanceVgroupReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->useless) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSBalanceVgroupReq(SBalanceVgroupReq *pReq) { FREESQL(); }

int32_t tSerializeSBalanceVgroupLeaderReq(void *buf, int32_t bufLen, SBalanceVgroupLeaderReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->useless) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  ENCODESQL();
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSBalanceVgroupLeaderReq(void *buf, int32_t bufLen, SBalanceVgroupLeaderReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->useless) < 0) return -1;
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  }

  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSBalanceVgroupLeaderReq(SBalanceVgroupLeaderReq *pReq) { FREESQL(); }

int32_t tSerializeSMergeVgroupReq(void *buf, int32_t bufLen, SMergeVgroupReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId2) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMergeVgroupReq(void *buf, int32_t bufLen, SMergeVgroupReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId2) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSRedistributeVgroupReq(void *buf, int32_t bufLen, SRedistributeVgroupReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId3) < 0) return -1;
  ENCODESQL();
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRedistributeVgroupReq(void *buf, int32_t bufLen, SRedistributeVgroupReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId3) < 0) return -1;
  DECODESQL();
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSRedistributeVgroupReq(SRedistributeVgroupReq *pReq) { FREESQL(); }

int32_t tSerializeSSplitVgroupReq(void *buf, int32_t bufLen, SSplitVgroupReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSplitVgroupReq(void *buf, int32_t bufLen, SSplitVgroupReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSForceBecomeFollowerReq(void *buf, int32_t bufLen, SForceBecomeFollowerReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

// int32_t tDeserializeSForceBecomeFollowerReq(void *buf, int32_t bufLen, SForceBecomeFollowerReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
//   tEndDecode(&decoder);

//   tDecoderClear(&decoder);
//   return 0;
// }

int32_t tSerializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  if (tEncodeI8(&encoder, pReq->learnerReplica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
    SReplica *pReplica = &pReq->learnerReplicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  if (tEncodeI64(&encoder, pReq->lastIndex) < 0) return -1;
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
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->learnerReplica) < 0) return -1;
    for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
      SReplica *pReplica = &pReq->learnerReplicas[i];
      if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
    }
    if (tDecodeI64(&decoder, &pReq->lastIndex) < 0) return -1;
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

// int32_t tSerializeSAuthReq(void *buf, int32_t bufLen, SAuthReq *pReq) {
//   SEncoder encoder = {0};
//   tEncoderInit(&encoder, buf, bufLen);

//   if (tStartEncode(&encoder) < 0) return -1;
//   if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
//   if (tEncodeI8(&encoder, pReq->spi) < 0) return -1;
//   if (tEncodeI8(&encoder, pReq->encrypt) < 0) return -1;
//   if (tEncodeBinary(&encoder, pReq->secret, TSDB_PASSWORD_LEN) < 0) return -1;
//   if (tEncodeBinary(&encoder, pReq->ckey, TSDB_PASSWORD_LEN) < 0) return -1;
//   tEndEncode(&encoder);

//   int32_t tlen = encoder.pos;
//   tEncoderClear(&encoder);
//   return tlen;
// }

// int32_t tDeserializeSAuthReq(void *buf, int32_t bufLen, SAuthReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
//   if (tDecodeI8(&decoder, &pReq->spi) < 0) return -1;
//   if (tDecodeI8(&decoder, &pReq->encrypt) < 0) return -1;
//   if (tDecodeCStrTo(&decoder, pReq->secret) < 0) return -1;
//   if (tDecodeCStrTo(&decoder, pReq->ckey) < 0) return -1;
//   tEndDecode(&decoder);

//   tDecoderClear(&decoder);
//   return 0;
// }

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
    pRsp->subplanInfo = taosMemoryCalloc(pRsp->numOfPlans, sizeof(SExplainExecInfo));
    if (pRsp->subplanInfo == NULL) return -1;
  }
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    if (tDecodeDouble(&decoder, &pRsp->subplanInfo[i].startupCost) < 0) return -1;
    if (tDecodeDouble(&decoder, &pRsp->subplanInfo[i].totalCost) < 0) return -1;
    if (tDecodeU64(&decoder, &pRsp->subplanInfo[i].numOfRows) < 0) return -1;
    if (tDecodeU32(&decoder, &pRsp->subplanInfo[i].verboseLen) < 0) return -1;
    if (tDecodeBinaryAlloc(&decoder, &pRsp->subplanInfo[i].verboseInfo, NULL) < 0) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSExplainRsp(SExplainRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    SExplainExecInfo *pExec = pRsp->subplanInfo + i;
    taosMemoryFree(pExec->verboseInfo);
  }

  taosMemoryFreeClear(pRsp->subplanInfo);
}

int32_t tSerializeSBatchReq(void *buf, int32_t bufLen, SBatchReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  int32_t num = taosArrayGetSize(pReq->pMsgs);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SBatchMsg *pMsg = taosArrayGet(pReq->pMsgs, i);
    if (tEncodeI32(&encoder, pMsg->msgIdx) < 0) return -1;
    if (tEncodeI32(&encoder, pMsg->msgType) < 0) return -1;
    if (tEncodeI32(&encoder, pMsg->msgLen) < 0) return -1;
    if (tEncodeBinary(&encoder, pMsg->msg, pMsg->msgLen) < 0) return -1;
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

int32_t tDeserializeSBatchReq(void *buf, int32_t bufLen, SBatchReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (num <= 0) {
    pReq->pMsgs = NULL;
    tEndDecode(&decoder);

    tDecoderClear(&decoder);
    return 0;
  }

  pReq->pMsgs = taosArrayInit(num, sizeof(SBatchMsg));
  if (NULL == pReq->pMsgs) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SBatchMsg msg = {0};
    if (tDecodeI32(&decoder, &msg.msgIdx) < 0) return -1;
    if (tDecodeI32(&decoder, &msg.msgType) < 0) return -1;
    if (tDecodeI32(&decoder, &msg.msgLen) < 0) return -1;
    if (tDecodeBinaryAlloc(&decoder, &msg.msg, NULL) < 0) return -1;
    if (NULL == taosArrayPush(pReq->pMsgs, &msg)) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSBatchRsp(void *buf, int32_t bufLen, SBatchRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  int32_t num = taosArrayGetSize(pRsp->pRsps);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SBatchRspMsg *pMsg = taosArrayGet(pRsp->pRsps, i);
    if (tEncodeI32(&encoder, pMsg->reqType) < 0) return -1;
    if (tEncodeI32(&encoder, pMsg->msgIdx) < 0) return -1;
    if (tEncodeI32(&encoder, pMsg->msgLen) < 0) return -1;
    if (tEncodeI32(&encoder, pMsg->rspCode) < 0) return -1;
    if (tEncodeBinary(&encoder, pMsg->msg, pMsg->msgLen) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  return tlen;
}

int32_t tDeserializeSBatchRsp(void *buf, int32_t bufLen, SBatchRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (num <= 0) {
    pRsp->pRsps = NULL;
    tEndDecode(&decoder);

    tDecoderClear(&decoder);
    return 0;
  }

  pRsp->pRsps = taosArrayInit(num, sizeof(SBatchRspMsg));
  if (NULL == pRsp->pRsps) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SBatchRspMsg msg = {0};
    if (tDecodeI32(&decoder, &msg.reqType) < 0) return -1;
    if (tDecodeI32(&decoder, &msg.msgIdx) < 0) return -1;
    if (tDecodeI32(&decoder, &msg.msgLen) < 0) return -1;
    if (tDecodeI32(&decoder, &msg.rspCode) < 0) return -1;
    if (tDecodeBinaryAlloc(&decoder, &msg.msg, NULL) < 0) return -1;
    if (NULL == taosArrayPush(pRsp->pRsps, &msg)) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMqAskEpReq(void *buf, int32_t bufLen, SMqAskEpReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI64(&encoder, pReq->consumerId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->epoch) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->cgroup) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  return tlen;
}

int32_t tDeserializeSMqAskEpReq(void *buf, int32_t bufLen, SMqAskEpReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI64(&decoder, &pReq->consumerId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->epoch) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->cgroup) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tDeatroySMqHbRsp(SMqHbRsp *pRsp) {
  taosArrayDestroy(pRsp->topicPrivileges);
  return 0;
}

int32_t tSerializeSMqHbRsp(void *buf, int32_t bufLen, SMqHbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  int32_t sz = taosArrayGetSize(pRsp->topicPrivileges);
  if (tEncodeI32(&encoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; ++i) {
    STopicPrivilege *privilege = (STopicPrivilege *)taosArrayGet(pRsp->topicPrivileges, i);
    if (tEncodeCStr(&encoder, privilege->topic) < 0) return -1;
    if (tEncodeI8(&encoder, privilege->noPrivilege) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  return tlen;
}

int32_t tDeserializeSMqHbRsp(void *buf, int32_t bufLen, SMqHbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t sz = 0;
  if (tDecodeI32(&decoder, &sz) < 0) return -1;
  if (sz > 0) {
    pRsp->topicPrivileges = taosArrayInit(sz, sizeof(STopicPrivilege));
    if (NULL == pRsp->topicPrivileges) return -1;
    for (int32_t i = 0; i < sz; ++i) {
      STopicPrivilege *data = taosArrayReserve(pRsp->topicPrivileges, 1);
      if (tDecodeCStrTo(&decoder, data->topic) < 0) return -1;
      if (tDecodeI8(&decoder, &data->noPrivilege) < 0) return -1;
    }
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tDeatroySMqHbReq(SMqHbReq *pReq) {
  for (int i = 0; i < taosArrayGetSize(pReq->topics); i++) {
    TopicOffsetRows *vgs = taosArrayGet(pReq->topics, i);
    if (vgs) taosArrayDestroy(vgs->offsetRows);
  }
  taosArrayDestroy(pReq->topics);
  return 0;
}

int32_t tSerializeSMqHbReq(void *buf, int32_t bufLen, SMqHbReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI64(&encoder, pReq->consumerId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->epoch) < 0) return -1;

  int32_t sz = taosArrayGetSize(pReq->topics);
  if (tEncodeI32(&encoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; ++i) {
    TopicOffsetRows *vgs = (TopicOffsetRows *)taosArrayGet(pReq->topics, i);
    if (tEncodeCStr(&encoder, vgs->topicName) < 0) return -1;
    int32_t szVgs = taosArrayGetSize(vgs->offsetRows);
    if (tEncodeI32(&encoder, szVgs) < 0) return -1;
    for (int32_t j = 0; j < szVgs; ++j) {
      OffsetRows *offRows = taosArrayGet(vgs->offsetRows, j);
      if (tEncodeI32(&encoder, offRows->vgId) < 0) return -1;
      if (tEncodeI64(&encoder, offRows->rows) < 0) return -1;
      if (tEncodeSTqOffsetVal(&encoder, &offRows->offset) < 0) return -1;
      if (tEncodeI64(&encoder, offRows->ever) < 0) return -1;
    }
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  return tlen;
}

int32_t tDeserializeSMqHbReq(void *buf, int32_t bufLen, SMqHbReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI64(&decoder, &pReq->consumerId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->epoch) < 0) return -1;
  int32_t sz = 0;
  if (tDecodeI32(&decoder, &sz) < 0) return -1;
  if (sz > 0) {
    pReq->topics = taosArrayInit(sz, sizeof(TopicOffsetRows));
    if (NULL == pReq->topics) return -1;
    for (int32_t i = 0; i < sz; ++i) {
      TopicOffsetRows *data = taosArrayReserve(pReq->topics, 1);
      if (tDecodeCStrTo(&decoder, data->topicName) < 0) return -1;
      int32_t szVgs = 0;
      if (tDecodeI32(&decoder, &szVgs) < 0) return -1;
      if (szVgs > 0) {
        data->offsetRows = taosArrayInit(szVgs, sizeof(OffsetRows));
        if (NULL == data->offsetRows) return -1;
        for (int32_t j = 0; j < szVgs; ++j) {
          OffsetRows *offRows = taosArrayReserve(data->offsetRows, 1);
          if (tDecodeI32(&decoder, &offRows->vgId) < 0) return -1;
          if (tDecodeI64(&decoder, &offRows->rows) < 0) return -1;
          if (tDecodeSTqOffsetVal(&decoder, &offRows->offset) < 0) return -1;
          if (tDecodeI64(&decoder, &offRows->ever) < 0) return -1;
        }
      }
    }
  }
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMqSeekReq(void *buf, int32_t bufLen, SMqSeekReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->consumerId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->subKey) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  if (buf != NULL) {
    SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
    pHead->vgId = htonl(pReq->head.vgId);
    pHead->contLen = htonl(tlen + headLen);
  }

  return tlen + headLen;
}

int32_t tDeserializeSMqSeekReq(void *buf, int32_t bufLen, SMqSeekReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->consumerId) < 0) return -1;
  tDecodeCStrTo(&decoder, pReq->subKey);

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSSubQueryMsg(void *buf, int32_t bufLen, SSubQueryMsg *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeU64(&encoder, pReq->sId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->queryId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->taskId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->refId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->execId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->msgMask) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->taskType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->explain) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->needFetch) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->sqlLen) < 0) return -1;
  if (tEncodeCStrWithLen(&encoder, pReq->sql, pReq->sqlLen) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->msgLen) < 0) return -1;
  if (tEncodeBinary(&encoder, (uint8_t *)pReq->msg, pReq->msgLen) < 0) return -1;

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

int32_t tDeserializeSSubQueryMsg(void *buf, int32_t bufLen, SSubQueryMsg *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeU64(&decoder, &pReq->sId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->queryId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->refId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->execId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->msgMask) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->taskType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->explain) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->needFetch) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->sqlLen) < 0) return -1;
  if (tDecodeCStrAlloc(&decoder, &pReq->sql) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->msgLen) < 0) return -1;
  if (tDecodeBinaryAlloc(&decoder, (void **)&pReq->msg, NULL) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSSubQueryMsg(SSubQueryMsg *pReq) {
  if (NULL == pReq) {
    return;
  }

  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->msg);
}

int32_t tSerializeSOperatorParam(SEncoder *pEncoder, SOperatorParam *pOpParam) {
  if (tEncodeI32(pEncoder, pOpParam->opType) < 0) return -1;
  if (tEncodeI32(pEncoder, pOpParam->downstreamIdx) < 0) return -1;
  switch (pOpParam->opType) {
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN: {
      STableScanOperatorParam *pScan = (STableScanOperatorParam *)pOpParam->value;
      if (tEncodeI8(pEncoder, pScan->tableSeq) < 0) return -1;
      int32_t uidNum = taosArrayGetSize(pScan->pUidList);
      if (tEncodeI32(pEncoder, uidNum) < 0) return -1;
      for (int32_t m = 0; m < uidNum; ++m) {
        int64_t *pUid = taosArrayGet(pScan->pUidList, m);
        if (tEncodeI64(pEncoder, *pUid) < 0) return -1;
      }
      break;
    }
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  int32_t n = taosArrayGetSize(pOpParam->pChildren);
  if (tEncodeI32(pEncoder, n) < 0) return -1;
  for (int32_t i = 0; i < n; ++i) {
    SOperatorParam *pChild = *(SOperatorParam **)taosArrayGet(pOpParam->pChildren, i);
    if (tSerializeSOperatorParam(pEncoder, pChild) < 0) return -1;
  }

  return 0;
}

int32_t tDeserializeSOperatorParam(SDecoder *pDecoder, SOperatorParam *pOpParam) {
  if (tDecodeI32(pDecoder, &pOpParam->opType) < 0) return -1;
  if (tDecodeI32(pDecoder, &pOpParam->downstreamIdx) < 0) return -1;
  switch (pOpParam->opType) {
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN: {
      STableScanOperatorParam *pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
      if (NULL == pScan) return -1;
      if (tDecodeI8(pDecoder, (int8_t *)&pScan->tableSeq) < 0) return -1;
      int32_t uidNum = 0;
      int64_t uid = 0;
      if (tDecodeI32(pDecoder, &uidNum) < 0) return -1;
      if (uidNum > 0) {
        pScan->pUidList = taosArrayInit(uidNum, sizeof(int64_t));
        if (NULL == pScan->pUidList) return -1;

        for (int32_t m = 0; m < uidNum; ++m) {
          if (tDecodeI64(pDecoder, &uid) < 0) return -1;
          taosArrayPush(pScan->pUidList, &uid);
        }
      } else {
        pScan->pUidList = NULL;
      }
      pOpParam->value = pScan;
      break;
    }
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  int32_t childrenNum = 0;
  if (tDecodeI32(pDecoder, &childrenNum) < 0) return -1;

  if (childrenNum > 0) {
    pOpParam->pChildren = taosArrayInit(childrenNum, POINTER_BYTES);
    if (NULL == pOpParam->pChildren) return -1;
    for (int32_t i = 0; i < childrenNum; ++i) {
      SOperatorParam *pChild = taosMemoryCalloc(1, sizeof(SOperatorParam));
      if (NULL == pChild) return -1;
      if (tDeserializeSOperatorParam(pDecoder, pChild) < 0) return -1;
      taosArrayPush(pOpParam->pChildren, &pChild);
    }
  } else {
    pOpParam->pChildren = NULL;
  }

  return 0;
}

int32_t tSerializeSResFetchReq(void *buf, int32_t bufLen, SResFetchReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeU64(&encoder, pReq->sId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->queryId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->execId) < 0) return -1;
  if (pReq->pOpParam) {
    if (tEncodeI32(&encoder, 1) < 0) return -1;
    if (tSerializeSOperatorParam(&encoder, pReq->pOpParam) < 0) return -1;
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

int32_t tDeserializeSResFetchReq(void *buf, int32_t bufLen, SResFetchReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeU64(&decoder, &pReq->sId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->queryId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->execId) < 0) return -1;

  int32_t paramNum = 0;
  if (tDecodeI32(&decoder, &paramNum) < 0) return -1;
  if (paramNum > 0) {
    pReq->pOpParam = taosMemoryMalloc(sizeof(*pReq->pOpParam));
    if (NULL == pReq->pOpParam) return -1;
    if (tDeserializeSOperatorParam(&decoder, pReq->pOpParam) < 0) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSTqOffsetVal(SEncoder *pEncoder, STqOffsetVal *pOffset) {
  if (tEncodeI8(pEncoder, pOffset->type) < 0) return -1;
  if (tEncodeI64(pEncoder, pOffset->uid) < 0) return -1;
  if (tEncodeI64(pEncoder, pOffset->ts) < 0) return -1;

  return 0;
}

int32_t tDerializeSTqOffsetVal(SDecoder *pDecoder, STqOffsetVal *pOffset) {
  if (tDecodeI8(pDecoder, &pOffset->type) < 0) return -1;
  if (tDecodeI64(pDecoder, &pOffset->uid) < 0) return -1;
  if (tDecodeI64(pDecoder, &pOffset->ts) < 0) return -1;

  return 0;
}

int32_t tSerializeSMqPollReq(void *buf, int32_t bufLen, SMqPollReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeCStr(&encoder, pReq->subKey) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->withTbName) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->useSnapshot) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->epoch) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->reqId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->consumerId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->timeout) < 0) return -1;
  if (tSerializeSTqOffsetVal(&encoder, &pReq->reqOffset) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->enableReplay) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->sourceExcluded) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  if (buf != NULL) {
    SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
    pHead->vgId = htonl(pReq->head.vgId);
    pHead->contLen = htonl(tlen + headLen);
  }

  return tlen + headLen;
}

int32_t tDeserializeSMqPollReq(void *buf, int32_t bufLen, SMqPollReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  //  SMsgHead *pHead = buf;
  //  pHead->vgId = pReq->head.vgId;
  //  pHead->contLen = pReq->head.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeCStrTo(&decoder, pReq->subKey) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->withTbName) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->useSnapshot) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->epoch) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->reqId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->consumerId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->timeout) < 0) return -1;
  if (tDerializeSTqOffsetVal(&decoder, &pReq->reqOffset) < 0) return -1;

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->enableReplay) < 0) return -1;
  }

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pReq->sourceExcluded) < 0) return -1;
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSTaskDropReq(void *buf, int32_t bufLen, STaskDropReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeU64(&encoder, pReq->sId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->queryId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->taskId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->refId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->execId) < 0) return -1;

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

int32_t tDeserializeSTaskDropReq(void *buf, int32_t bufLen, STaskDropReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeU64(&decoder, &pReq->sId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->queryId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->refId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->execId) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSTaskNotifyReq(void *buf, int32_t bufLen, STaskNotifyReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeU64(&encoder, pReq->sId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->queryId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->taskId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->refId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->execId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->type) < 0) return -1;

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

int32_t tDeserializeSTaskNotifyReq(void *buf, int32_t bufLen, STaskNotifyReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeU64(&decoder, &pReq->sId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->queryId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->refId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->execId) < 0) return -1;
  if (tDecodeI32(&decoder, (int32_t *)&pReq->type) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeI32(&encoder, pRsp->code) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->affectedRows) < 0) return -1;
  int32_t tbNum = taosArrayGetSize(pRsp->tbVerInfo);
  if (tEncodeI32(&encoder, tbNum) < 0) return -1;
  if (tbNum > 0) {
    for (int32_t i = 0; i < tbNum; ++i) {
      STbVerInfo *pVer = taosArrayGet(pRsp->tbVerInfo, i);
      if (tEncodeCStr(&encoder, pVer->tbFName) < 0) return -1;
      if (tEncodeI32(&encoder, pVer->sversion) < 0) return -1;
      if (tEncodeI32(&encoder, pVer->tversion) < 0) return -1;
    }
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  return tlen;
}

int32_t tDeserializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeI32(&decoder, &pRsp->code) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->affectedRows) < 0) return -1;
  int32_t tbNum = 0;
  if (tDecodeI32(&decoder, &tbNum) < 0) return -1;
  if (tbNum > 0) {
    pRsp->tbVerInfo = taosArrayInit(tbNum, sizeof(STbVerInfo));
    if (NULL == pRsp->tbVerInfo) return -1;
    STbVerInfo tbVer;
    if (tDecodeCStrTo(&decoder, tbVer.tbFName) < 0) return -1;
    if (tDecodeI32(&decoder, &tbVer.sversion) < 0) return -1;
    if (tDecodeI32(&decoder, &tbVer.tversion) < 0) return -1;
    if (NULL == taosArrayPush(pRsp->tbVerInfo, &tbVer)) return -1;
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
      if (tEncodeI32(&encoder, status->execId) < 0) return -1;
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
      if (tDecodeI32(&decoder, &status.execId) < 0) return -1;
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

// int32_t tSerializeSVCreateTbBatchRsp(void *buf, int32_t bufLen, SVCreateTbBatchRsp *pRsp) {
//   // SEncoder encoder = {0};
//   // tEncoderInit(&encoder, buf, bufLen);

//   // if (tStartEncode(&encoder) < 0) return -1;
//   // if (pRsp->rspList) {
//   //   int32_t num = taosArrayGetSize(pRsp->rspList);
//   //   if (tEncodeI32(&encoder, num) < 0) return -1;
//   //   for (int32_t i = 0; i < num; ++i) {
//   //     SVCreateTbRsp *rsp = taosArrayGet(pRsp->rspList, i);
//   //     if (tEncodeI32(&encoder, rsp->code) < 0) return -1;
//   //   }
//   // } else {
//   //   if (tEncodeI32(&encoder, 0) < 0) return -1;
//   // }
//   // tEndEncode(&encoder);

//   // int32_t tlen = encoder.pos;
//   // tEncoderClear(&encoder);
//   // reture tlen;
//   return 0;
// }

//int32_t tDeserializeSVCreateTbBatchRsp(void *buf, int32_t bufLen, SVCreateTbBatchRsp *pRsp) {
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
//  return 0;
//}

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
  if (tEncodeI32(pCoder, pSma->dstVgId) < 0) return -1;
  if (tEncodeCStr(pCoder, pSma->indexName) < 0) return -1;
  if (tEncodeI32(pCoder, pSma->exprLen) < 0) return -1;
  if (tEncodeI32(pCoder, pSma->tagsFilterLen) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->indexUid) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->tableUid) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->dstTbUid) < 0) return -1;
  if (tEncodeCStr(pCoder, pSma->dstTbName) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->interval) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->offset) < 0) return -1;
  if (tEncodeI64(pCoder, pSma->sliding) < 0) return -1;
  if (pSma->exprLen > 0) {
    if (tEncodeCStr(pCoder, pSma->expr) < 0) return -1;
  }
  if (pSma->tagsFilterLen > 0) {
    if (tEncodeCStr(pCoder, pSma->tagsFilter) < 0) return -1;
  }

  tEncodeSSchemaWrapper(pCoder, &pSma->schemaRow);
  tEncodeSSchemaWrapper(pCoder, &pSma->schemaTag);

  return 0;
}

int32_t tDecodeTSma(SDecoder *pCoder, STSma *pSma, bool deepCopy) {
  if (tDecodeI8(pCoder, &pSma->version) < 0) return -1;
  if (tDecodeI8(pCoder, &pSma->intervalUnit) < 0) return -1;
  if (tDecodeI8(pCoder, &pSma->slidingUnit) < 0) return -1;
  if (tDecodeI8(pCoder, &pSma->timezoneInt) < 0) return -1;
  if (tDecodeI32(pCoder, &pSma->dstVgId) < 0) return -1;
  if (tDecodeCStrTo(pCoder, pSma->indexName) < 0) return -1;
  if (tDecodeI32(pCoder, &pSma->exprLen) < 0) return -1;
  if (tDecodeI32(pCoder, &pSma->tagsFilterLen) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->indexUid) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->tableUid) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->dstTbUid) < 0) return -1;
  if (deepCopy) {
    if (tDecodeCStrAlloc(pCoder, &pSma->dstTbName) < 0) return -1;
  } else {
    if (tDecodeCStr(pCoder, &pSma->dstTbName) < 0) return -1;
  }

  if (tDecodeI64(pCoder, &pSma->interval) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->offset) < 0) return -1;
  if (tDecodeI64(pCoder, &pSma->sliding) < 0) return -1;
  if (pSma->exprLen > 0) {
    if (deepCopy) {
      if (tDecodeCStrAlloc(pCoder, &pSma->expr) < 0) return -1;
    } else {
      if (tDecodeCStr(pCoder, &pSma->expr) < 0) return -1;
    }
  } else {
    pSma->expr = NULL;
  }
  if (pSma->tagsFilterLen > 0) {
    if (deepCopy) {
      if (tDecodeCStrAlloc(pCoder, &pSma->tagsFilter) < 0) return -1;
    } else {
      if (tDecodeCStr(pCoder, &pSma->tagsFilter) < 0) return -1;
    }
  } else {
    pSma->tagsFilter = NULL;
  }
  // only needed in dstVgroup
  tDecodeSSchemaWrapperEx(pCoder, &pSma->schemaRow);
  tDecodeSSchemaWrapperEx(pCoder, &pSma->schemaTag);

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

  tDecodeTSma(pCoder, pReq, false);

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

// int32_t tDecodeSVDropTSmaReq(SDecoder *pCoder, SVDropTSmaReq *pReq) {
//   if (tStartDecode(pCoder) < 0) return -1;

//   if (tDecodeI64(pCoder, &pReq->indexUid) < 0) return -1;
//   if (tDecodeCStrTo(pCoder, pReq->indexName) < 0) return -1;

//   tEndDecode(pCoder);
//   return 0;
// }

int32_t tSerializeSVDeleteReq(void *buf, int32_t bufLen, SVDeleteReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->sId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->queryId) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->taskId) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->sqlLen) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (tEncodeBinary(&encoder, pReq->msg, pReq->phyLen) < 0) return -1;
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

int32_t tDeserializeSVDeleteReq(void *buf, int32_t bufLen, SVDeleteReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->sId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->queryId) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->taskId) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->sqlLen) < 0) return -1;
  pReq->sql = taosMemoryCalloc(1, pReq->sqlLen + 1);
  if (NULL == pReq->sql) return -1;
  if (tDecodeCStrTo(&decoder, pReq->sql) < 0) return -1;
  uint64_t msgLen = 0;
  if (tDecodeBinaryAlloc(&decoder, (void **)&pReq->msg, &msgLen) < 0) return -1;
  pReq->phyLen = msgLen;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tEncodeSVDeleteRsp(SEncoder *pCoder, const SVDeleteRsp *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI64(pCoder, pReq->affectedRows) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDeleteRsp(SDecoder *pCoder, SVDeleteRsp *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pReq->affectedRows) < 0) return -1;

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
  if (tEncodeI8(&encoder, pReq->fillHistory) < 0) return -1;
  if (tEncodeI32(&encoder, sqlLen) < 0) return -1;
  if (tEncodeI32(&encoder, astLen) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->triggerType) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->maxDelay) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->watermark) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExpired) < 0) return -1;
  if (sqlLen > 0 && tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (astLen > 0 && tEncodeCStr(&encoder, pReq->ast) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfTags) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField *pField = taosArrayGet(pReq->pTags, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI8(&encoder, pField->flags) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }

  if (tEncodeI8(&encoder, pReq->createStb) < 0) return -1;
  if (tEncodeU64(&encoder, pReq->targetStbUid) < 0) return -1;

  if (tEncodeI32(&encoder, taosArrayGetSize(pReq->fillNullCols)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pReq->fillNullCols); ++i) {
    SColLocation *pCol = taosArrayGet(pReq->fillNullCols, i);
    if (tEncodeI16(&encoder, pCol->slotId) < 0) return -1;
    if (tEncodeI16(&encoder, pCol->colId) < 0) return -1;
    if (tEncodeI8(&encoder, pCol->type) < 0) return -1;
  }

  if (tEncodeI64(&encoder, pReq->deleteMark) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igUpdate) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->lastTs) < 0) return -1;

  if (tEncodeI32(&encoder, taosArrayGetSize(pReq->pVgroupVerList)) < 0) return -1;

  for(int32_t i = 0; i < taosArrayGetSize(pReq->pVgroupVerList); ++i) {
    SVgroupVer* p = taosArrayGet(pReq->pVgroupVerList, i);
    if (tEncodeI32(&encoder, p->vgId) < 0) return -1;
    if (tEncodeI64(&encoder, p->ver) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateStreamReq(void *buf, int32_t bufLen, SCMCreateStreamReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;
  int32_t numOfFillNullCols = 0;
  int32_t numOfVgVer = 0;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->sourceDB) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->targetStbFullName) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->fillHistory) < 0) return -1;
  if (tDecodeI32(&decoder, &sqlLen) < 0) return -1;
  if (tDecodeI32(&decoder, &astLen) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->triggerType) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->maxDelay) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->watermark) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExpired) < 0) return -1;

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

  if (tDecodeI32(&decoder, &pReq->numOfTags) < 0) return -1;
  if (pReq->numOfTags > 0) {
    pReq->pTags = taosArrayInit(pReq->numOfTags, sizeof(SField));
    if (pReq->pTags == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < pReq->numOfTags; ++i) {
      SField field = {0};
      if (tDecodeI8(&decoder, &field.type) < 0) return -1;
      if (tDecodeI8(&decoder, &field.flags) < 0) return -1;
      if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
      if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
      if (taosArrayPush(pReq->pTags, &field) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
    }
  }
  if (tDecodeI8(&decoder, &pReq->createStb) < 0) return -1;
  if (tDecodeU64(&decoder, &pReq->targetStbUid) < 0) return -1;
  if (tDecodeI32(&decoder, &numOfFillNullCols) < 0) return -1;
  if (numOfFillNullCols > 0) {
    pReq->fillNullCols = taosArrayInit(numOfFillNullCols, sizeof(SColLocation));
    if (pReq->fillNullCols == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < numOfFillNullCols; ++i) {
      SColLocation col = {0};
      if (tDecodeI16(&decoder, &col.slotId) < 0) return -1;
      if (tDecodeI16(&decoder, &col.colId) < 0) return -1;
      if (tDecodeI8(&decoder, &col.type) < 0) return -1;
      if (taosArrayPush(pReq->fillNullCols, &col) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
    }
  }

  if (tDecodeI64(&decoder, &pReq->deleteMark) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igUpdate) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->lastTs) < 0) return -1;

  if (tDecodeI32(&decoder, &numOfVgVer) < 0) return -1;
  if (numOfVgVer > 0) {
    pReq->pVgroupVerList = taosArrayInit(numOfVgVer, sizeof(SVgroupVer));
    if (pReq->pVgroupVerList == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < numOfVgVer; ++i) {
      SVgroupVer v = {0};
      if (tDecodeI32(&decoder, &v.vgId) < 0) return -1;
      if (tDecodeI64(&decoder, &v.ver) < 0) return -1;
      if (taosArrayPush(pReq->pVgroupVerList, &v) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
    }
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);

  return 0;
}

int32_t tSerializeSMDropStreamReq(void *buf, int32_t bufLen, const SMDropStreamReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;

  ENCODESQL();

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropStreamReq(void *buf, int32_t bufLen, SMDropStreamReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;

  DECODESQL();

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeMDropStreamReq(SMDropStreamReq *pReq) { FREESQL(); }

// int32_t tSerializeSMRecoverStreamReq(void *buf, int32_t bufLen, const SMRecoverStreamReq *pReq) {
//   SEncoder encoder = {0};
//   tEncoderInit(&encoder, buf, bufLen);

//   if (tStartEncode(&encoder) < 0) return -1;
//   if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
//   if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;

//   tEndEncode(&encoder);

//   int32_t tlen = encoder.pos;
//   tEncoderClear(&encoder);
//   return tlen;
// }

// int32_t tDeserializeSMRecoverStreamReq(void *buf, int32_t bufLen, SMRecoverStreamReq *pReq) {
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
//   if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;

//   tEndDecode(&decoder);

//   tDecoderClear(&decoder);
//   return 0;
// }

void tFreeSCMCreateStreamReq(SCMCreateStreamReq *pReq) {
  if (NULL == pReq) {
    return;
  }
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
  taosArrayDestroy(pReq->pTags);
  taosArrayDestroy(pReq->fillNullCols);
  taosArrayDestroy(pReq->pVgroupVerList);
}

int32_t tEncodeSRSmaParam(SEncoder *pCoder, const SRSmaParam *pRSmaParam) {
  for (int32_t i = 0; i < 2; ++i) {
    if (tEncodeI64v(pCoder, pRSmaParam->maxdelay[i]) < 0) return -1;
    if (tEncodeI64v(pCoder, pRSmaParam->watermark[i]) < 0) return -1;
    if (tEncodeI32v(pCoder, pRSmaParam->qmsgLen[i]) < 0) return -1;
    if (pRSmaParam->qmsgLen[i] > 0) {
      if (tEncodeBinary(pCoder, pRSmaParam->qmsg[i], (uint64_t)pRSmaParam->qmsgLen[i]) <
          0)  // qmsgLen contains len of '\0'
        return -1;
    }
  }

  return 0;
}

int32_t tDecodeSRSmaParam(SDecoder *pCoder, SRSmaParam *pRSmaParam) {
  for (int32_t i = 0; i < 2; ++i) {
    if (tDecodeI64v(pCoder, &pRSmaParam->maxdelay[i]) < 0) return -1;
    if (tDecodeI64v(pCoder, &pRSmaParam->watermark[i]) < 0) return -1;
    if (tDecodeI32v(pCoder, &pRSmaParam->qmsgLen[i]) < 0) return -1;
    if (pRSmaParam->qmsgLen[i] > 0) {
      if (tDecodeBinary(pCoder, (uint8_t **)&pRSmaParam->qmsg[i], NULL) < 0) return -1;  // qmsgLen contains len of '\0'
    } else {
      pRSmaParam->qmsg[i] = NULL;
    }
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
    if (tEncodeSRSmaParam(pCoder, &pReq->rsmaParam) < 0) return -1;
  }

  if (tEncodeI32(pCoder, pReq->alterOriDataLen) < 0) return -1;
  if (pReq->alterOriDataLen > 0) {
    if (tEncodeBinary(pCoder, pReq->alterOriData, pReq->alterOriDataLen) < 0) return -1;
  }
  if (tEncodeI8(pCoder, pReq->source) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateStbReq(SDecoder *pCoder, SVCreateStbReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->suid) < 0) return -1;
  if (tDecodeI8(pCoder, &pReq->rollup) < 0) return -1;
  if (tDecodeSSchemaWrapperEx(pCoder, &pReq->schemaRow) < 0) return -1;
  if (tDecodeSSchemaWrapperEx(pCoder, &pReq->schemaTag) < 0) return -1;
  if (pReq->rollup) {
    if (tDecodeSRSmaParam(pCoder, &pReq->rsmaParam) < 0) return -1;
  }

  if (tDecodeI32(pCoder, &pReq->alterOriDataLen) < 0) return -1;
  if (pReq->alterOriDataLen > 0) {
    if (tDecodeBinary(pCoder, (uint8_t **)&pReq->alterOriData, NULL) < 0) return -1;
  }

  if (!tDecodeIsEnd(pCoder)) {
    if (tDecodeI8(pCoder, &pReq->source) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int tEncodeSVCreateTbReq(SEncoder *pCoder, const SVCreateTbReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, pReq->flags) < 0) return -1;
  if (tEncodeCStr(pCoder, pReq->name) < 0) return -1;
  if (tEncodeI64(pCoder, pReq->uid) < 0) return -1;
  if (tEncodeI64(pCoder, pReq->btime) < 0) return -1;
  if (tEncodeI32(pCoder, pReq->ttl) < 0) return -1;
  if (tEncodeI8(pCoder, pReq->type) < 0) return -1;
  if (tEncodeI32(pCoder, pReq->commentLen) < 0) return -1;
  if (pReq->commentLen > 0) {
    if (tEncodeCStr(pCoder, pReq->comment) < 0) return -1;
  }

  if (pReq->type == TSDB_CHILD_TABLE) {
    if (tEncodeCStr(pCoder, pReq->ctb.stbName) < 0) return -1;
    if (tEncodeU8(pCoder, pReq->ctb.tagNum) < 0) return -1;
    if (tEncodeI64(pCoder, pReq->ctb.suid) < 0) return -1;
    if (tEncodeTag(pCoder, (const STag *)pReq->ctb.pTag) < 0) return -1;
    int32_t len = taosArrayGetSize(pReq->ctb.tagName);
    if (tEncodeI32(pCoder, len) < 0) return -1;
    for (int32_t i = 0; i < len; i++) {
      char *name = taosArrayGet(pReq->ctb.tagName, i);
      if (tEncodeCStr(pCoder, name) < 0) return -1;
    }
  } else if (pReq->type == TSDB_NORMAL_TABLE) {
    if (tEncodeSSchemaWrapper(pCoder, &pReq->ntb.schemaRow) < 0) return -1;
  } else {
    ASSERT(0);
  }
  // ENCODESQL
  if (pReq->sqlLen > 0 && pReq->sql != NULL) {
    if (tEncodeI32(pCoder, pReq->sqlLen) < 0) return -1;
    if (tEncodeBinary(pCoder, pReq->sql, pReq->sqlLen) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbReq(SDecoder *pCoder, SVCreateTbReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32v(pCoder, &pReq->flags) < 0) return -1;
  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->uid) < 0) return -1;
  if (tDecodeI64(pCoder, &pReq->btime) < 0) return -1;
  if (tDecodeI32(pCoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI8(pCoder, &pReq->type) < 0) return -1;
  if (tDecodeI32(pCoder, &pReq->commentLen) < 0) return -1;
  if (pReq->commentLen > 0) {
    pReq->comment = taosMemoryMalloc(pReq->commentLen + 1);
    if (pReq->comment == NULL) return -1;
    if (tDecodeCStrTo(pCoder, pReq->comment) < 0) return -1;
  }

  if (pReq->type == TSDB_CHILD_TABLE) {
    if (tDecodeCStr(pCoder, &pReq->ctb.stbName) < 0) return -1;
    if (tDecodeU8(pCoder, &pReq->ctb.tagNum) < 0) return -1;
    if (tDecodeI64(pCoder, &pReq->ctb.suid) < 0) return -1;
    if (tDecodeTag(pCoder, (STag **)&pReq->ctb.pTag) < 0) return -1;
    int32_t len = 0;
    if (tDecodeI32(pCoder, &len) < 0) return -1;
    pReq->ctb.tagName = taosArrayInit(len, TSDB_COL_NAME_LEN);
    if (pReq->ctb.tagName == NULL) return -1;
    for (int32_t i = 0; i < len; i++) {
      char  name[TSDB_COL_NAME_LEN] = {0};
      char *tmp = NULL;
      if (tDecodeCStr(pCoder, &tmp) < 0) return -1;
      strncpy(name, tmp, TSDB_COL_NAME_LEN - 1);
      taosArrayPush(pReq->ctb.tagName, name);
    }
  } else if (pReq->type == TSDB_NORMAL_TABLE) {
    if (tDecodeSSchemaWrapperEx(pCoder, &pReq->ntb.schemaRow) < 0) return -1;
  } else {
    ASSERT(0);
  }

  // DECODESQL
  if (!tDecodeIsEnd(pCoder)) {
    if (tDecodeI32(pCoder, &pReq->sqlLen) < 0) return -1;
    if (pReq->sqlLen > 0) {
      if (tDecodeBinaryAlloc(pCoder, (void **)&pReq->sql, NULL) < 0) return -1;
    }
  }

  tEndDecode(pCoder);
  return 0;
}

void tDestroySVCreateTbReq(SVCreateTbReq *pReq, int32_t flags) {
  if (pReq == NULL) return;

  if (flags & TSDB_MSG_FLG_ENCODE) {
    // TODO
  } else if (flags & TSDB_MSG_FLG_DECODE) {
    if (pReq->comment) {
      pReq->comment = NULL;
      taosMemoryFree(pReq->comment);
    }

    if (pReq->type == TSDB_CHILD_TABLE) {
      if (pReq->ctb.tagName) taosArrayDestroy(pReq->ctb.tagName);
    } else if (pReq->type == TSDB_NORMAL_TABLE) {
      if (pReq->ntb.schemaRow.pSchema) taosMemoryFree(pReq->ntb.schemaRow.pSchema);
    }
  }

  if (pReq->sql != NULL) {
    taosMemoryFree(pReq->sql);
  }
  pReq->sql = NULL;
}

int tEncodeSVCreateTbBatchReq(SEncoder *pCoder, const SVCreateTbBatchReq *pReq) {
  int32_t nReq = taosArrayGetSize(pReq->pArray);

  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, nReq) < 0) return -1;
  for (int iReq = 0; iReq < nReq; iReq++) {
    if (tEncodeSVCreateTbReq(pCoder, (SVCreateTbReq *)taosArrayGet(pReq->pArray, iReq)) < 0) return -1;
  }

  if (tEncodeI8(pCoder, pReq->source) < 0) return -1;

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

  if (!tDecodeIsEnd(pCoder)) {
    if (tDecodeI8(pCoder, &pReq->source) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}

int tEncodeSVCreateTbRsp(SEncoder *pCoder, const SVCreateTbRsp *pRsp) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32(pCoder, pRsp->code) < 0) return -1;
  if (tEncodeI32(pCoder, pRsp->pMeta ? 1 : 0) < 0) return -1;
  if (pRsp->pMeta) {
    if (tEncodeSTableMetaRsp(pCoder, pRsp->pMeta) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbRsp(SDecoder *pCoder, SVCreateTbRsp *pRsp) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32(pCoder, &pRsp->code) < 0) return -1;

  int32_t meta = 0;
  if (tDecodeI32(pCoder, &meta) < 0) return -1;
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) return -1;
    if (tDecodeSTableMetaRsp(pCoder, pRsp->pMeta) < 0) return -1;
  } else {
    pRsp->pMeta = NULL;
  }

  tEndDecode(pCoder);
  return 0;
}

void tFreeSVCreateTbRsp(void *param) {
  if (NULL == param) {
    return;
  }

  SVCreateTbRsp *pRsp = (SVCreateTbRsp *)param;
  if (pRsp->pMeta) {
    taosMemoryFree(pRsp->pMeta->pSchemas);
    taosMemoryFree(pRsp->pMeta);
  }
}

// TDMT_VND_DROP_TABLE =================
static int32_t tEncodeSVDropTbReq(SEncoder *pCoder, const SVDropTbReq *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeCStr(pCoder, pReq->name) < 0) return -1;
  if (tEncodeU64(pCoder, pReq->suid) < 0) return -1;
  if (tEncodeI8(pCoder, pReq->igNotExists) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSVDropTbReq(SDecoder *pCoder, SVDropTbReq *pReq) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeCStr(pCoder, &pReq->name) < 0) return -1;
  if (tDecodeU64(pCoder, &pReq->suid) < 0) return -1;
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

// static int32_t tEncodeSVSubmitBlk(SEncoder *pCoder, const SVSubmitBlk *pBlock, int32_t flags) {
//   if (tStartEncode(pCoder) < 0) return -1;

//   if (tEncodeI64(pCoder, pBlock->suid) < 0) return -1;
//   if (tEncodeI64(pCoder, pBlock->uid) < 0) return -1;
//   if (tEncodeI32v(pCoder, pBlock->sver) < 0) return -1;
//   if (tEncodeBinary(pCoder, pBlock->pData, pBlock->nData) < 0) return -1;

//   if (flags & TD_AUTO_CREATE_TABLE) {
//     if (tEncodeSVCreateTbReq(pCoder, &pBlock->cTbReq) < 0) return -1;
//   }

//   tEndEncode(pCoder);
//   return 0;
// }

// static int32_t tDecodeSVSubmitBlk(SDecoder *pCoder, SVSubmitBlk *pBlock, int32_t flags) {
//   if (tStartDecode(pCoder) < 0) return -1;

//   if (tDecodeI64(pCoder, &pBlock->suid) < 0) return -1;
//   if (tDecodeI64(pCoder, &pBlock->uid) < 0) return -1;
//   if (tDecodeI32v(pCoder, &pBlock->sver) < 0) return -1;
//   if (tDecodeBinary(pCoder, &pBlock->pData, &pBlock->nData) < 0) return -1;

//   if (flags & TD_AUTO_CREATE_TABLE) {
//     if (tDecodeSVCreateTbReq(pCoder, &pBlock->cTbReq) < 0) return -1;
//   }

//   tEndDecode(pCoder);
//   return 0;
// }

static int32_t tEncodeSSubmitBlkRsp(SEncoder *pEncoder, const SSubmitBlkRsp *pBlock) {
  if (tStartEncode(pEncoder) < 0) return -1;

  if (tEncodeI32(pEncoder, pBlock->code) < 0) return -1;
  if (tEncodeI64(pEncoder, pBlock->uid) < 0) return -1;
  if (pBlock->tblFName) {
    if (tEncodeCStr(pEncoder, pBlock->tblFName) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }
  if (tEncodeI32v(pEncoder, pBlock->numOfRows) < 0) return -1;
  if (tEncodeI32v(pEncoder, pBlock->affectedRows) < 0) return -1;
  if (tEncodeI64v(pEncoder, pBlock->sver) < 0) return -1;
  if (tEncodeI32(pEncoder, pBlock->pMeta ? 1 : 0) < 0) return -1;
  if (pBlock->pMeta) {
    if (tEncodeSTableMetaRsp(pEncoder, pBlock->pMeta) < 0) return -1;
  }

  tEndEncode(pEncoder);
  return 0;
}

static int32_t tDecodeSSubmitBlkRsp(SDecoder *pDecoder, SSubmitBlkRsp *pBlock) {
  if (tStartDecode(pDecoder) < 0) return -1;

  if (tDecodeI32(pDecoder, &pBlock->code) < 0) return -1;
  if (tDecodeI64(pDecoder, &pBlock->uid) < 0) return -1;
  pBlock->tblFName = taosMemoryCalloc(TSDB_TABLE_FNAME_LEN, 1);
  if (NULL == pBlock->tblFName) return -1;
  if (tDecodeCStrTo(pDecoder, pBlock->tblFName) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pBlock->numOfRows) < 0) return -1;
  if (tDecodeI32v(pDecoder, &pBlock->affectedRows) < 0) return -1;
  if (tDecodeI64v(pDecoder, &pBlock->sver) < 0) return -1;

  int32_t meta = 0;
  if (tDecodeI32(pDecoder, &meta) < 0) return -1;
  if (meta) {
    pBlock->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pBlock->pMeta) return -1;
    if (tDecodeSTableMetaRsp(pDecoder, pBlock->pMeta) < 0) return -1;
  } else {
    pBlock->pMeta = NULL;
  }

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

// void tFreeSSubmitBlkRsp(void *param) {
//   if (NULL == param) {
//     return;
//   }

//   SSubmitBlkRsp *pRsp = (SSubmitBlkRsp *)param;

//   taosMemoryFree(pRsp->tblFName);
//   if (pRsp->pMeta) {
//     taosMemoryFree(pRsp->pMeta->pSchemas);
//     taosMemoryFree(pRsp->pMeta);
//   }
// }

void tFreeSSubmitRsp(SSubmitRsp *pRsp) {
  if (NULL == pRsp) return;

  if (pRsp->pBlocks) {
    for (int32_t i = 0; i < pRsp->nBlocks; ++i) {
      SSubmitBlkRsp *sRsp = pRsp->pBlocks + i;
      taosMemoryFree(sRsp->tblFName);
      tFreeSTableMetaRsp(sRsp->pMeta);
      taosMemoryFree(sRsp->pMeta);
    }

    taosMemoryFree(pRsp->pBlocks);
  }

  taosMemoryFree(pRsp);
}

int32_t tEncodeSVAlterTbReq(SEncoder *pEncoder, const SVAlterTbReq *pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;

  if (tEncodeCStr(pEncoder, pReq->tbName) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->action) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->colId) < 0) return -1;
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
      if (tEncodeI8(pEncoder, pReq->colModType) < 0) return -1;
      if (tEncodeI32v(pEncoder, pReq->colModBytes) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (tEncodeCStr(pEncoder, pReq->colName) < 0) return -1;
      if (tEncodeCStr(pEncoder, pReq->colNewName) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      if (tEncodeCStr(pEncoder, pReq->tagName) < 0) return -1;
      if (tEncodeI8(pEncoder, pReq->isNull) < 0) return -1;
      if (tEncodeI8(pEncoder, pReq->tagType) < 0) return -1;
      if (!pReq->isNull) {
        if (tEncodeBinary(pEncoder, pReq->pTagVal, pReq->nTagVal) < 0) return -1;
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      if (tEncodeI8(pEncoder, pReq->updateTTL) < 0) return -1;
      if (pReq->updateTTL) {
        if (tEncodeI32v(pEncoder, pReq->newTTL) < 0) return -1;
      }
      if (tEncodeI32v(pEncoder, pReq->newCommentLen) < 0) return -1;
      if (pReq->newCommentLen > 0) {
        if (tEncodeCStr(pEncoder, pReq->newComment) < 0) return -1;
      }
      break;
    default:
      break;
  }
  if (tEncodeI64(pEncoder, pReq->ctimeMs) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->source) < 0) return -1;

  tEndEncode(pEncoder);
  return 0;
}

static int32_t tDecodeSVAlterTbReqCommon(SDecoder *pDecoder, SVAlterTbReq *pReq) {
  if (tDecodeCStr(pDecoder, &pReq->tbName) < 0) return -1;
  if (tDecodeI8(pDecoder, &pReq->action) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->colId) < 0) return -1;
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
      if (tDecodeI8(pDecoder, &pReq->colModType) < 0) return -1;
      if (tDecodeI32v(pDecoder, &pReq->colModBytes) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      if (tDecodeCStr(pDecoder, &pReq->colName) < 0) return -1;
      if (tDecodeCStr(pDecoder, &pReq->colNewName) < 0) return -1;
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      if (tDecodeCStr(pDecoder, &pReq->tagName) < 0) return -1;
      if (tDecodeI8(pDecoder, &pReq->isNull) < 0) return -1;
      if (tDecodeI8(pDecoder, &pReq->tagType) < 0) return -1;
      if (!pReq->isNull) {
        if (tDecodeBinary(pDecoder, &pReq->pTagVal, &pReq->nTagVal) < 0) return -1;
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      if (tDecodeI8(pDecoder, &pReq->updateTTL) < 0) return -1;
      if (pReq->updateTTL) {
        if (tDecodeI32v(pDecoder, &pReq->newTTL) < 0) return -1;
      }
      if (tDecodeI32v(pDecoder, &pReq->newCommentLen) < 0) return -1;
      if (pReq->newCommentLen > 0) {
        if (tDecodeCStr(pDecoder, &pReq->newComment) < 0) return -1;
      }
      break;
    default:
      break;
  }
  return 0;
}

int32_t tDecodeSVAlterTbReq(SDecoder *pDecoder, SVAlterTbReq *pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeSVAlterTbReqCommon(pDecoder, pReq) < 0) return -1;

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    if (tDecodeI64(pDecoder, &pReq->ctimeMs) < 0) return -1;
  }
  if (!tDecodeIsEnd(pDecoder)) {
    if (tDecodeI8(pDecoder, &pReq->source) < 0) return -1;
  }

  tEndDecode(pDecoder);
  return 0;
}

int32_t tDecodeSVAlterTbReqSetCtime(SDecoder *pDecoder, SVAlterTbReq *pReq, int64_t ctimeMs) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeSVAlterTbReqCommon(pDecoder, pReq) < 0) return -1;

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    *(int64_t *)(pDecoder->data + pDecoder->pos) = ctimeMs;
    if (tDecodeI64(pDecoder, &pReq->ctimeMs) < 0) return -1;
  }

  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSVAlterTbRsp(SEncoder *pEncoder, const SVAlterTbRsp *pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->code) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->pMeta ? 1 : 0) < 0) return -1;
  if (pRsp->pMeta) {
    if (tEncodeSTableMetaRsp(pEncoder, pRsp->pMeta) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeSVAlterTbRsp(SDecoder *pDecoder, SVAlterTbRsp *pRsp) {
  int32_t meta = 0;
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->code) < 0) return -1;
  if (tDecodeI32(pDecoder, &meta) < 0) return -1;
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) return -1;
    if (tDecodeSTableMetaRsp(pDecoder, pRsp->pMeta) < 0) return -1;
  }
  tEndDecode(pDecoder);
  return 0;
}

// int32_t tDeserializeSVAlterTbRsp(void *buf, int32_t bufLen, SVAlterTbRsp *pRsp) {
//   int32_t  meta = 0;
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &pRsp->code) < 0) return -1;
//   if (tDecodeI32(&decoder, &meta) < 0) return -1;
//   if (meta) {
//     pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
//     if (NULL == pRsp->pMeta) return -1;
//     if (tDecodeSTableMetaRsp(&decoder, pRsp->pMeta) < 0) return -1;
//   }
//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

int32_t tEncodeSMAlterStbRsp(SEncoder *pEncoder, const SMAlterStbRsp *pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->pMeta->pSchemas ? 1 : 0) < 0) return -1;
  if (pRsp->pMeta->pSchemas) {
    if (tEncodeSTableMetaRsp(pEncoder, pRsp->pMeta) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeSMAlterStbRsp(SDecoder *pDecoder, SMAlterStbRsp *pRsp) {
  int32_t meta = 0;
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &meta) < 0) return -1;
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) return -1;
    if (tDecodeSTableMetaRsp(pDecoder, pRsp->pMeta) < 0) return -1;
  }
  tEndDecode(pDecoder);
  return 0;
}

// int32_t tDeserializeSMAlterStbRsp(void *buf, int32_t bufLen, SMAlterStbRsp *pRsp) {
//   int32_t  meta = 0;
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &meta) < 0) return -1;
//   if (meta) {
//     pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
//     if (NULL == pRsp->pMeta) return -1;
//     if (tDecodeSTableMetaRsp(&decoder, pRsp->pMeta) < 0) return -1;
//   }
//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

void tFreeSMAlterStbRsp(SMAlterStbRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  if (pRsp->pMeta) {
    taosMemoryFree(pRsp->pMeta->pSchemas);
    taosMemoryFree(pRsp->pMeta);
  }
}

int32_t tEncodeSMCreateStbRsp(SEncoder *pEncoder, const SMCreateStbRsp *pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->pMeta->pSchemas ? 1 : 0) < 0) return -1;
  if (pRsp->pMeta->pSchemas) {
    if (tEncodeSTableMetaRsp(pEncoder, pRsp->pMeta) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return 0;
}

int32_t tDecodeSMCreateStbRsp(SDecoder *pDecoder, SMCreateStbRsp *pRsp) {
  int32_t meta = 0;
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &meta) < 0) return -1;
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) return -1;
    if (tDecodeSTableMetaRsp(pDecoder, pRsp->pMeta) < 0) return -1;
  }
  tEndDecode(pDecoder);
  return 0;
}

// int32_t tDeserializeSMCreateStbRsp(void *buf, int32_t bufLen, SMCreateStbRsp *pRsp) {
//   int32_t  meta = 0;
//   SDecoder decoder = {0};
//   tDecoderInit(&decoder, buf, bufLen);

//   if (tStartDecode(&decoder) < 0) return -1;
//   if (tDecodeI32(&decoder, &meta) < 0) return -1;
//   if (meta) {
//     pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
//     if (NULL == pRsp->pMeta) return -1;
//     if (tDecodeSTableMetaRsp(&decoder, pRsp->pMeta) < 0) return -1;
//   }
//   tEndDecode(&decoder);
//   tDecoderClear(&decoder);
//   return 0;
// }

void tFreeSMCreateStbRsp(SMCreateStbRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  if (pRsp->pMeta) {
    taosMemoryFree(pRsp->pMeta->pSchemas);
    taosMemoryFree(pRsp->pMeta);
  }
}

int32_t tEncodeSTqOffsetVal(SEncoder *pEncoder, const STqOffsetVal *pOffsetVal) {
  if (tEncodeI8(pEncoder, pOffsetVal->type) < 0) return -1;
  if (pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_DATA || pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_META) {
    if (tEncodeI64(pEncoder, pOffsetVal->uid) < 0) return -1;
    if (tEncodeI64(pEncoder, pOffsetVal->ts) < 0) return -1;
  } else if (pOffsetVal->type == TMQ_OFFSET__LOG) {
    if (tEncodeI64(pEncoder, pOffsetVal->version) < 0) return -1;
  } else {
    // do nothing
  }
  return 0;
}

int32_t tDecodeSTqOffsetVal(SDecoder *pDecoder, STqOffsetVal *pOffsetVal) {
  if (tDecodeI8(pDecoder, &pOffsetVal->type) < 0) return -1;
  if (pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_DATA || pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_META) {
    if (tDecodeI64(pDecoder, &pOffsetVal->uid) < 0) return -1;
    if (tDecodeI64(pDecoder, &pOffsetVal->ts) < 0) return -1;
  } else if (pOffsetVal->type == TMQ_OFFSET__LOG) {
    if (tDecodeI64(pDecoder, &pOffsetVal->version) < 0) return -1;
  } else {
    // do nothing
  }
  return 0;
}

int32_t tFormatOffset(char *buf, int32_t maxLen, const STqOffsetVal *pVal) {
  if (pVal->type == TMQ_OFFSET__RESET_NONE) {
    snprintf(buf, maxLen, "none");
  } else if (pVal->type == TMQ_OFFSET__RESET_EARLIEST) {
    snprintf(buf, maxLen, "earliest");
  } else if (pVal->type == TMQ_OFFSET__RESET_LATEST) {
    snprintf(buf, maxLen, "latest");
  } else if (pVal->type == TMQ_OFFSET__LOG) {
    snprintf(buf, maxLen, "wal:%" PRId64, pVal->version);
  } else if (pVal->type == TMQ_OFFSET__SNAPSHOT_DATA || pVal->type == TMQ_OFFSET__SNAPSHOT_META) {
    snprintf(buf, maxLen, "tsdb:%" PRId64 "|%" PRId64, pVal->uid, pVal->ts);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
}

bool tOffsetEqual(const STqOffsetVal *pLeft, const STqOffsetVal *pRight) {
  if (pLeft->type == pRight->type) {
    if (pLeft->type == TMQ_OFFSET__LOG) {
      return pLeft->version == pRight->version;
    } else if (pLeft->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      return pLeft->uid == pRight->uid && pLeft->ts == pRight->ts;
    } else if (pLeft->type == TMQ_OFFSET__SNAPSHOT_META) {
      return pLeft->uid == pRight->uid;
    } else {
      uError("offset type:%d", pLeft->type);
      ASSERT(0);
    }
  }
  return false;
}

int32_t tEncodeSTqOffset(SEncoder *pEncoder, const STqOffset *pOffset) {
  if (tEncodeSTqOffsetVal(pEncoder, &pOffset->val) < 0) return -1;
  if (tEncodeCStr(pEncoder, pOffset->subKey) < 0) return -1;
  return 0;
}

int32_t tDecodeSTqOffset(SDecoder *pDecoder, STqOffset *pOffset) {
  if (tDecodeSTqOffsetVal(pDecoder, &pOffset->val) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pOffset->subKey) < 0) return -1;
  return 0;
}

int32_t tEncodeMqVgOffset(SEncoder *pEncoder, const SMqVgOffset *pOffset) {
  if (tEncodeSTqOffset(pEncoder, &pOffset->offset) < 0) return -1;
  if (tEncodeI64(pEncoder, pOffset->consumerId) < 0) return -1;
  return 0;
}

int32_t tDecodeMqVgOffset(SDecoder *pDecoder, SMqVgOffset *pOffset) {
  if (tDecodeSTqOffset(pDecoder, &pOffset->offset) < 0) return -1;
  if (tDecodeI64(pDecoder, &pOffset->consumerId) < 0) return -1;
  return 0;
}

int32_t tEncodeSTqCheckInfo(SEncoder *pEncoder, const STqCheckInfo *pInfo) {
  if (tEncodeCStr(pEncoder, pInfo->topic) < 0) return -1;
  if (tEncodeI64(pEncoder, pInfo->ntbUid) < 0) return -1;
  int32_t sz = taosArrayGetSize(pInfo->colIdList);
  if (tEncodeI32(pEncoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; i++) {
    int16_t colId = *(int16_t *)taosArrayGet(pInfo->colIdList, i);
    if (tEncodeI16(pEncoder, colId) < 0) return -1;
  }
  return pEncoder->pos;
}

int32_t tDecodeSTqCheckInfo(SDecoder *pDecoder, STqCheckInfo *pInfo) {
  if (tDecodeCStrTo(pDecoder, pInfo->topic) < 0) return -1;
  if (tDecodeI64(pDecoder, &pInfo->ntbUid) < 0) return -1;
  int32_t sz;
  if (tDecodeI32(pDecoder, &sz) < 0) return -1;
  pInfo->colIdList = taosArrayInit(sz, sizeof(int16_t));
  if (pInfo->colIdList == NULL) return -1;
  for (int32_t i = 0; i < sz; i++) {
    int16_t colId;
    if (tDecodeI16(pDecoder, &colId) < 0) return -1;
    taosArrayPush(pInfo->colIdList, &colId);
  }
  return 0;
}
void tDeleteSTqCheckInfo(STqCheckInfo *pInfo) { taosArrayDestroy(pInfo->colIdList); }

int32_t tEncodeDeleteRes(SEncoder *pCoder, const SDeleteRes *pRes) {
  int32_t nUid = taosArrayGetSize(pRes->uidList);

  if (tEncodeU64(pCoder, pRes->suid) < 0) return -1;
  if (tEncodeI32v(pCoder, nUid) < 0) return -1;
  for (int32_t iUid = 0; iUid < nUid; iUid++) {
    if (tEncodeU64(pCoder, *(uint64_t *)taosArrayGet(pRes->uidList, iUid)) < 0) return -1;
  }
  if (tEncodeI64(pCoder, pRes->skey) < 0) return -1;
  if (tEncodeI64(pCoder, pRes->ekey) < 0) return -1;
  if (tEncodeI64v(pCoder, pRes->affectedRows) < 0) return -1;

  if (tEncodeCStr(pCoder, pRes->tableFName) < 0) return -1;
  if (tEncodeCStr(pCoder, pRes->tsColName) < 0) return -1;
  if (tEncodeI64(pCoder, pRes->ctimeMs) < 0) return -1;
  return 0;
}

int32_t tDecodeDeleteRes(SDecoder *pCoder, SDeleteRes *pRes) {
  int32_t  nUid;
  uint64_t uid;

  if (tDecodeU64(pCoder, &pRes->suid) < 0) return -1;
  if (tDecodeI32v(pCoder, &nUid) < 0) return -1;
  for (int32_t iUid = 0; iUid < nUid; iUid++) {
    if (tDecodeU64(pCoder, &uid) < 0) return -1;
    if (pRes->uidList) taosArrayPush(pRes->uidList, &uid);
  }
  if (tDecodeI64(pCoder, &pRes->skey) < 0) return -1;
  if (tDecodeI64(pCoder, &pRes->ekey) < 0) return -1;
  if (tDecodeI64v(pCoder, &pRes->affectedRows) < 0) return -1;

  if (tDecodeCStrTo(pCoder, pRes->tableFName) < 0) return -1;
  if (tDecodeCStrTo(pCoder, pRes->tsColName) < 0) return -1;

  pRes->ctimeMs = 0;
  if (!tDecodeIsEnd(pCoder)) {
    if (tDecodeI64(pCoder, &pRes->ctimeMs) < 0) return -1;
  }
  return 0;
}

int32_t tEncodeMqMetaRsp(SEncoder *pEncoder, const SMqMetaRsp *pRsp) {
  if (tEncodeSTqOffsetVal(pEncoder, &pRsp->rspOffset) < 0) return -1;
  if (tEncodeI16(pEncoder, pRsp->resMsgType)) return -1;
  if (tEncodeBinary(pEncoder, pRsp->metaRsp, pRsp->metaRspLen)) return -1;
  return 0;
}

int32_t tDecodeMqMetaRsp(SDecoder *pDecoder, SMqMetaRsp *pRsp) {
  if (tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset) < 0) return -1;
  if (tDecodeI16(pDecoder, &pRsp->resMsgType) < 0) return -1;
  if (tDecodeBinaryAlloc(pDecoder, &pRsp->metaRsp, (uint64_t *)&pRsp->metaRspLen) < 0) return -1;
  return 0;
}

int32_t tEncodeMqDataRspCommon(SEncoder *pEncoder, const SMqDataRsp *pRsp) {
  if (tEncodeSTqOffsetVal(pEncoder, &pRsp->reqOffset) < 0) return -1;
  if (tEncodeSTqOffsetVal(pEncoder, &pRsp->rspOffset) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->blockNum) < 0) return -1;
  if (pRsp->blockNum != 0) {
    if (tEncodeI8(pEncoder, pRsp->withTbName) < 0) return -1;
    if (tEncodeI8(pEncoder, pRsp->withSchema) < 0) return -1;

    for (int32_t i = 0; i < pRsp->blockNum; i++) {
      int32_t bLen = *(int32_t *)taosArrayGet(pRsp->blockDataLen, i);
      void *  data = taosArrayGetP(pRsp->blockData, i);
      if (tEncodeBinary(pEncoder, (const uint8_t *)data, bLen) < 0) return -1;
      if (pRsp->withSchema) {
        SSchemaWrapper *pSW = (SSchemaWrapper *)taosArrayGetP(pRsp->blockSchema, i);
        if (tEncodeSSchemaWrapper(pEncoder, pSW) < 0) return -1;
      }
      if (pRsp->withTbName) {
        char *tbName = (char *)taosArrayGetP(pRsp->blockTbName, i);
        if (tEncodeCStr(pEncoder, tbName) < 0) return -1;
      }
    }
  }
  return 0;
}

int32_t tEncodeMqDataRsp(SEncoder *pEncoder, const SMqDataRsp *pRsp) {
  if (tEncodeMqDataRspCommon(pEncoder, pRsp) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->sleepTime) < 0) return -1;
  return 0;
}

int32_t tDecodeMqDataRspCommon(SDecoder *pDecoder, SMqDataRsp *pRsp) {
  if (tDecodeSTqOffsetVal(pDecoder, &pRsp->reqOffset) < 0) return -1;
  if (tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->blockNum) < 0) return -1;
  if (pRsp->blockNum != 0) {
    pRsp->blockData = taosArrayInit(pRsp->blockNum, sizeof(void *));
    pRsp->blockDataLen = taosArrayInit(pRsp->blockNum, sizeof(int32_t));
    if (tDecodeI8(pDecoder, &pRsp->withTbName) < 0) return -1;
    if (tDecodeI8(pDecoder, &pRsp->withSchema) < 0) return -1;
    if (pRsp->withTbName) {
      pRsp->blockTbName = taosArrayInit(pRsp->blockNum, sizeof(void *));
    }
    if (pRsp->withSchema) {
      pRsp->blockSchema = taosArrayInit(pRsp->blockNum, sizeof(void *));
    }

    for (int32_t i = 0; i < pRsp->blockNum; i++) {
      void *   data;
      uint64_t bLen;
      if (tDecodeBinaryAlloc(pDecoder, &data, &bLen) < 0) return -1;
      taosArrayPush(pRsp->blockData, &data);
      int32_t len = bLen;
      taosArrayPush(pRsp->blockDataLen, &len);

      if (pRsp->withSchema) {
        SSchemaWrapper *pSW = (SSchemaWrapper *)taosMemoryCalloc(1, sizeof(SSchemaWrapper));
        if (pSW == NULL) return -1;
        if (tDecodeSSchemaWrapper(pDecoder, pSW) < 0) {
          taosMemoryFree(pSW);
          return -1;
        }

        taosArrayPush(pRsp->blockSchema, &pSW);
      }

      if (pRsp->withTbName) {
        char *tbName;
        if (tDecodeCStrAlloc(pDecoder, &tbName) < 0) return -1;
        taosArrayPush(pRsp->blockTbName, &tbName);
      }
    }
  }

  return 0;
}

int32_t tDecodeMqDataRsp(SDecoder *pDecoder, SMqDataRsp *pRsp) {
  if (tDecodeMqDataRspCommon(pDecoder, pRsp) < 0) return -1;
  if (!tDecodeIsEnd(pDecoder)) {
    if (tDecodeI64(pDecoder, &pRsp->sleepTime) < 0) return -1;
  }

  return 0;
}

void tDeleteMqDataRsp(SMqDataRsp *pRsp) {
  pRsp->blockDataLen = taosArrayDestroy(pRsp->blockDataLen);
  taosArrayDestroyP(pRsp->blockData, (FDelete)taosMemoryFree);
  pRsp->blockData = NULL;
  taosArrayDestroyP(pRsp->blockSchema, (FDelete)tDeleteSchemaWrapper);
  pRsp->blockSchema = NULL;
  taosArrayDestroyP(pRsp->blockTbName, (FDelete)taosMemoryFree);
  pRsp->blockTbName = NULL;
}

int32_t tEncodeSTaosxRsp(SEncoder *pEncoder, const STaosxRsp *pRsp) {
  if (tEncodeMqDataRspCommon(pEncoder, (const SMqDataRsp *)pRsp) < 0) return -1;

  if (tEncodeI32(pEncoder, pRsp->createTableNum) < 0) return -1;
  if (pRsp->createTableNum) {
    for (int32_t i = 0; i < pRsp->createTableNum; i++) {
      void *  createTableReq = taosArrayGetP(pRsp->createTableReq, i);
      int32_t createTableLen = *(int32_t *)taosArrayGet(pRsp->createTableLen, i);
      if (tEncodeBinary(pEncoder, createTableReq, createTableLen) < 0) return -1;
    }
  }
  return 0;
}

int32_t tDecodeSTaosxRsp(SDecoder *pDecoder, STaosxRsp *pRsp) {
  if (tDecodeMqDataRspCommon(pDecoder, (SMqDataRsp *)pRsp) < 0) return -1;

  if (tDecodeI32(pDecoder, &pRsp->createTableNum) < 0) return -1;
  if (pRsp->createTableNum) {
    pRsp->createTableLen = taosArrayInit(pRsp->createTableNum, sizeof(int32_t));
    pRsp->createTableReq = taosArrayInit(pRsp->createTableNum, sizeof(void *));
    for (int32_t i = 0; i < pRsp->createTableNum; i++) {
      void *   pCreate = NULL;
      uint64_t len;
      if (tDecodeBinaryAlloc(pDecoder, &pCreate, &len) < 0) return -1;
      int32_t l = (int32_t)len;
      taosArrayPush(pRsp->createTableLen, &l);
      taosArrayPush(pRsp->createTableReq, &pCreate);
    }
  }

  return 0;
}

void tDeleteSTaosxRsp(STaosxRsp *pRsp) {
  pRsp->blockDataLen = taosArrayDestroy(pRsp->blockDataLen);
  taosArrayDestroyP(pRsp->blockData, (FDelete)taosMemoryFree);
  pRsp->blockData = NULL;
  taosArrayDestroyP(pRsp->blockSchema, (FDelete)tDeleteSchemaWrapper);
  pRsp->blockSchema = NULL;
  taosArrayDestroyP(pRsp->blockTbName, (FDelete)taosMemoryFree);
  pRsp->blockTbName = NULL;

  pRsp->createTableLen = taosArrayDestroy(pRsp->createTableLen);
  taosArrayDestroyP(pRsp->createTableReq, (FDelete)taosMemoryFree);
  pRsp->createTableReq = NULL;
}

int32_t tEncodeSSingleDeleteReq(SEncoder *pEncoder, const SSingleDeleteReq *pReq) {
  if (tEncodeCStr(pEncoder, pReq->tbname) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->startTs) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->endTs) < 0) return -1;
  return 0;
}

int32_t tDecodeSSingleDeleteReq(SDecoder *pDecoder, SSingleDeleteReq *pReq) {
  if (tDecodeCStrTo(pDecoder, pReq->tbname) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->startTs) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->endTs) < 0) return -1;
  return 0;
}

int32_t tEncodeSBatchDeleteReq(SEncoder *pEncoder, const SBatchDeleteReq *pReq) {
  if (tEncodeI64(pEncoder, pReq->suid) < 0) return -1;
  int32_t sz = taosArrayGetSize(pReq->deleteReqs);
  if (tEncodeI32(pEncoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq *pOneReq = taosArrayGet(pReq->deleteReqs, i);
    if (tEncodeSSingleDeleteReq(pEncoder, pOneReq) < 0) return -1;
  }
  if (tEncodeI64(pEncoder, pReq->ctimeMs) < 0) return -1;
  if (tEncodeI8(pEncoder, pReq->level) < 0) return -1;
  return 0;
}

static int32_t tDecodeSBatchDeleteReqCommon(SDecoder *pDecoder, SBatchDeleteReq *pReq) {
  if (tDecodeI64(pDecoder, &pReq->suid) < 0) return -1;
  int32_t sz;
  if (tDecodeI32(pDecoder, &sz) < 0) return -1;
  pReq->deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
  if (pReq->deleteReqs == NULL) return -1;
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq deleteReq;
    if (tDecodeSSingleDeleteReq(pDecoder, &deleteReq) < 0) return -1;
    taosArrayPush(pReq->deleteReqs, &deleteReq);
  }
  return 0;
}

int32_t tDecodeSBatchDeleteReq(SDecoder *pDecoder, SBatchDeleteReq *pReq) {
  if (tDecodeSBatchDeleteReqCommon(pDecoder, pReq)) return -1;

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    if (tDecodeI64(pDecoder, &pReq->ctimeMs) < 0) return -1;
  }
  if (!tDecodeIsEnd(pDecoder)) {
    if (tDecodeI8(pDecoder, &pReq->level) < 0) return -1;
  }
  return 0;
}

int32_t tDecodeSBatchDeleteReqSetCtime(SDecoder *pDecoder, SBatchDeleteReq *pReq, int64_t ctimeMs) {
  if (tDecodeSBatchDeleteReqCommon(pDecoder, pReq)) return -1;

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    *(int64_t *)(pDecoder->data + pDecoder->pos) = ctimeMs;
    if (tDecodeI64(pDecoder, &pReq->ctimeMs) < 0) return -1;
  }
  return 0;
}

static int32_t tEncodeSSubmitTbData(SEncoder *pCoder, const SSubmitTbData *pSubmitTbData) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, pSubmitTbData->flags) < 0) return -1;

  // auto create table
  if (pSubmitTbData->flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    ASSERT(pSubmitTbData->pCreateTbReq);
    if (tEncodeSVCreateTbReq(pCoder, pSubmitTbData->pCreateTbReq) < 0) return -1;
  }

  // submit data
  if (tEncodeI64(pCoder, pSubmitTbData->suid) < 0) return -1;
  if (tEncodeI64(pCoder, pSubmitTbData->uid) < 0) return -1;
  if (tEncodeI32v(pCoder, pSubmitTbData->sver) < 0) return -1;

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t  nColData = TARRAY_SIZE(pSubmitTbData->aCol);
    SColData *aColData = (SColData *)TARRAY_DATA(pSubmitTbData->aCol);

    if (tEncodeU64v(pCoder, nColData) < 0) return -1;

    for (uint64_t i = 0; i < nColData; i++) {
      pCoder->pos += tPutColData(pCoder->data ? pCoder->data + pCoder->pos : NULL, &aColData[i]);
    }
  } else {
    if (tEncodeU64v(pCoder, TARRAY_SIZE(pSubmitTbData->aRowP)) < 0) return -1;

    SRow **rows = (SRow **)TARRAY_DATA(pSubmitTbData->aRowP);
    for (int32_t iRow = 0; iRow < TARRAY_SIZE(pSubmitTbData->aRowP); ++iRow) {
      if (pCoder->data) memcpy(pCoder->data + pCoder->pos, rows[iRow], rows[iRow]->len);
      pCoder->pos += rows[iRow]->len;
    }
  }
  if (tEncodeI64(pCoder, pSubmitTbData->ctimeMs) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSSubmitTbData(SDecoder *pCoder, SSubmitTbData *pSubmitTbData) {
  int32_t code = 0;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (tDecodeI32v(pCoder, &pSubmitTbData->flags) < 0) return -1;

  if (pSubmitTbData->flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    pSubmitTbData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (pSubmitTbData->pCreateTbReq == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    if (tDecodeSVCreateTbReq(pCoder, pSubmitTbData->pCreateTbReq) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
  }

  // submit data
  if (tDecodeI64(pCoder, &pSubmitTbData->suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  if (tDecodeI64(pCoder, &pSubmitTbData->uid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  if (tDecodeI32v(pCoder, &pSubmitTbData->sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData;

    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    pSubmitTbData->aCol = taosArrayInit(nColData, sizeof(SColData));
    if (pSubmitTbData->aCol == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    for (int32_t i = 0; i < nColData; ++i) {
      pCoder->pos += tGetColData(pCoder->data + pCoder->pos, taosArrayReserve(pSubmitTbData->aCol, 1));
    }
  } else {
    uint64_t nRow;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    pSubmitTbData->aRowP = taosArrayInit(nRow, sizeof(SRow *));
    if (pSubmitTbData->aRowP == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow **ppRow = taosArrayReserve(pSubmitTbData->aRowP, 1);

      *ppRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += (*ppRow)->len;
    }
  }

  pSubmitTbData->ctimeMs = 0;
  if (!tDecodeIsEnd(pCoder)) {
    if (tDecodeI64(pCoder, &pSubmitTbData->ctimeMs) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
  }

  tEndDecode(pCoder);

_exit:
  if (code) {
    // TODO: clear
  }
  return 0;
}

int32_t tEncodeSubmitReq(SEncoder *pCoder, const SSubmitReq2 *pReq) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeU64v(pCoder, taosArrayGetSize(pReq->aSubmitTbData)) < 0) return -1;
  for (uint64_t i = 0; i < taosArrayGetSize(pReq->aSubmitTbData); i++) {
    if (tEncodeSSubmitTbData(pCoder, taosArrayGet(pReq->aSubmitTbData, i)) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSubmitReq(SDecoder *pCoder, SSubmitReq2 *pReq) {
  int32_t code = 0;

  memset(pReq, 0, sizeof(*pReq));

  // decode
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  uint64_t nSubmitTbData;
  if (tDecodeU64v(pCoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  pReq->aSubmitTbData = taosArrayInit(nSubmitTbData, sizeof(SSubmitTbData));
  if (pReq->aSubmitTbData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (uint64_t i = 0; i < nSubmitTbData; i++) {
    if (tDecodeSSubmitTbData(pCoder, taosArrayReserve(pReq->aSubmitTbData, 1)) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
  }

  tEndDecode(pCoder);

_exit:
  if (code) {
    if (pReq->aSubmitTbData) {
      // todo
      taosArrayDestroy(pReq->aSubmitTbData);
      pReq->aSubmitTbData = NULL;
    }
  }
  return code;
}

void tDestroySubmitTbData(SSubmitTbData *pTbData, int32_t flag) {
  if (NULL == pTbData) {
    return;
  }

  if (flag == TSDB_MSG_FLG_ENCODE || flag == TSDB_MSG_FLG_CMPT) {
    if (pTbData->pCreateTbReq) {
      if (flag == TSDB_MSG_FLG_ENCODE) {
        tdDestroySVCreateTbReq(pTbData->pCreateTbReq);
      } else {
        tDestroySVCreateTbReq(pTbData->pCreateTbReq, TSDB_MSG_FLG_DECODE);
      }
      taosMemoryFreeClear(pTbData->pCreateTbReq);
    }

    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      int32_t   nColData = TARRAY_SIZE(pTbData->aCol);
      SColData *aColData = (SColData *)TARRAY_DATA(pTbData->aCol);

      for (int32_t i = 0; i < nColData; ++i) {
        tColDataDestroy(&aColData[i]);
      }
      taosArrayDestroy(pTbData->aCol);
    } else {
      int32_t nRow = TARRAY_SIZE(pTbData->aRowP);
      SRow ** rows = (SRow **)TARRAY_DATA(pTbData->aRowP);

      for (int32_t i = 0; i < nRow; ++i) {
        tRowDestroy(rows[i]);
      }
      taosArrayDestroy(pTbData->aRowP);
    }
  } else if (flag == TSDB_MSG_FLG_DECODE) {
    if (pTbData->pCreateTbReq) {
      tDestroySVCreateTbReq(pTbData->pCreateTbReq, TSDB_MSG_FLG_DECODE);
      taosMemoryFree(pTbData->pCreateTbReq);
    }

    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      taosArrayDestroy(pTbData->aCol);
    } else {
      taosArrayDestroy(pTbData->aRowP);
    }
  }
}

void tDestroySubmitReq(SSubmitReq2 *pReq, int32_t flag) {
  if (pReq->aSubmitTbData == NULL) return;

  int32_t        nSubmitTbData = TARRAY_SIZE(pReq->aSubmitTbData);
  SSubmitTbData *aSubmitTbData = (SSubmitTbData *)TARRAY_DATA(pReq->aSubmitTbData);

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    tDestroySubmitTbData(&aSubmitTbData[i], flag);
  }
  taosArrayDestroy(pReq->aSubmitTbData);
  pReq->aSubmitTbData = NULL;
}

int32_t tEncodeSSubmitRsp2(SEncoder *pCoder, const SSubmitRsp2 *pRsp) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32v(pCoder, pRsp->affectedRows) < 0) return -1;

  if (tEncodeU64v(pCoder, taosArrayGetSize(pRsp->aCreateTbRsp)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pRsp->aCreateTbRsp); ++i) {
    if (tEncodeSVCreateTbRsp(pCoder, taosArrayGet(pRsp->aCreateTbRsp, i)) < 0) return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSSubmitRsp2(SDecoder *pCoder, SSubmitRsp2 *pRsp) {
  int32_t code = 0;

  memset(pRsp, 0, sizeof(SSubmitRsp2));

  // decode
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (tDecodeI32v(pCoder, &pRsp->affectedRows) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  uint64_t nCreateTbRsp;
  if (tDecodeU64v(pCoder, &nCreateTbRsp) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (nCreateTbRsp) {
    pRsp->aCreateTbRsp = taosArrayInit(nCreateTbRsp, sizeof(SVCreateTbRsp));
    if (pRsp->aCreateTbRsp == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    for (int32_t i = 0; i < nCreateTbRsp; ++i) {
      SVCreateTbRsp *pCreateTbRsp = taosArrayReserve(pRsp->aCreateTbRsp, 1);
      if (tDecodeSVCreateTbRsp(pCoder, pCreateTbRsp) < 0) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }
    }
  }

  tEndDecode(pCoder);

_exit:
  if (code) {
    if (pRsp->aCreateTbRsp) {
      taosArrayDestroyEx(pRsp->aCreateTbRsp, NULL /* todo */);
    }
  }
  return code;
}

void tDestroySSubmitRsp2(SSubmitRsp2 *pRsp, int32_t flag) {
  if (NULL == pRsp) {
    return;
  }

  if (flag & TSDB_MSG_FLG_ENCODE) {
    if (pRsp->aCreateTbRsp) {
      int32_t        nCreateTbRsp = TARRAY_SIZE(pRsp->aCreateTbRsp);
      SVCreateTbRsp *aCreateTbRsp = TARRAY_DATA(pRsp->aCreateTbRsp);
      for (int32_t i = 0; i < nCreateTbRsp; ++i) {
        if (aCreateTbRsp[i].pMeta) {
          taosMemoryFree(aCreateTbRsp[i].pMeta);
        }
      }
      taosArrayDestroy(pRsp->aCreateTbRsp);
    }
  } else if (flag & TSDB_MSG_FLG_DECODE) {
    if (pRsp->aCreateTbRsp) {
      int32_t        nCreateTbRsp = TARRAY_SIZE(pRsp->aCreateTbRsp);
      SVCreateTbRsp *aCreateTbRsp = TARRAY_DATA(pRsp->aCreateTbRsp);
      for (int32_t i = 0; i < nCreateTbRsp; ++i) {
        if (aCreateTbRsp[i].pMeta) {
          taosMemoryFree(aCreateTbRsp[i].pMeta);
        }
      }
      taosArrayDestroy(pRsp->aCreateTbRsp);
    }
  }
}

int32_t tSerializeSMPauseStreamReq(void *buf, int32_t bufLen, const SMPauseStreamReq *pReq) {
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

int32_t tDeserializeSMPauseStreamReq(void *buf, int32_t bufLen, SMPauseStreamReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMResumeStreamReq(void *buf, int32_t bufLen, const SMResumeStreamReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igUntreated) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMResumeStreamReq(void *buf, int32_t bufLen, SMResumeStreamReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igUntreated) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

int32_t tEncodeMqSubTopicEp(void **buf, const SMqSubTopicEp *pTopicEp) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pTopicEp->topic);
  tlen += taosEncodeString(buf, pTopicEp->db);
  int32_t sz = taosArrayGetSize(pTopicEp->vgs);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubVgEp *pVgEp = (SMqSubVgEp *)taosArrayGet(pTopicEp->vgs, i);
    tlen += tEncodeSMqSubVgEp(buf, pVgEp);
  }
  tlen += taosEncodeSSchemaWrapper(buf, &pTopicEp->schema);
  return tlen;
}

void *tDecodeMqSubTopicEp(void *buf, SMqSubTopicEp *pTopicEp) {
  buf = taosDecodeStringTo(buf, pTopicEp->topic);
  buf = taosDecodeStringTo(buf, pTopicEp->db);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pTopicEp->vgs = taosArrayInit(sz, sizeof(SMqSubVgEp));
  if (pTopicEp->vgs == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubVgEp vgEp;
    buf = tDecodeSMqSubVgEp(buf, &vgEp);
    taosArrayPush(pTopicEp->vgs, &vgEp);
  }
  buf = taosDecodeSSchemaWrapper(buf, &pTopicEp->schema);
  return buf;
}

void tDeleteMqSubTopicEp(SMqSubTopicEp *pSubTopicEp) {
  taosMemoryFreeClear(pSubTopicEp->schema.pSchema);
  pSubTopicEp->schema.nCols = 0;
  taosArrayDestroy(pSubTopicEp->vgs);
}

int32_t tSerializeSCMCreateViewReq(void *buf, int32_t bufLen, const SCMCreateViewReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fullname) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->querySql) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->orReplace) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfCols) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfCols; ++i) {
    SSchema *pSchema = &pReq->pSchema[i];
    if (tEncodeSSchema(&encoder, pSchema) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateViewReq(void *buf, int32_t bufLen, SCMCreateViewReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fullname) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dbFName) < 0) return -1;
  if (tDecodeCStrAlloc(&decoder, &pReq->querySql) < 0) return -1;
  if (tDecodeCStrAlloc(&decoder, &pReq->sql) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->orReplace) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfCols) < 0) return -1;

  if (pReq->numOfCols > 0) {
    pReq->pSchema = taosMemoryCalloc(pReq->numOfCols, sizeof(SSchema));
    if (pReq->pSchema == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < pReq->numOfCols; ++i) {
      SSchema *pSchema = pReq->pSchema + i;
      if (tDecodeSSchema(&decoder, pSchema) < 0) return -1;
    }
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSCMCreateViewReq(SCMCreateViewReq *pReq) {
  if (NULL == pReq) {
    return;
  }

  taosMemoryFreeClear(pReq->querySql);
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->pSchema);
}

int32_t tSerializeSCMDropViewReq(void *buf, int32_t bufLen, const SCMDropViewReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fullname) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMDropViewReq(void *buf, int32_t bufLen, SCMDropViewReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fullname) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dbFName) < 0) return -1;
  if (tDecodeCStrAlloc(&decoder, &pReq->sql) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}
void tFreeSCMDropViewReq(SCMDropViewReq *pReq) {
  if (NULL == pReq) {
    return;
  }

  taosMemoryFree(pReq->sql);
}

int32_t tSerializeSViewMetaReq(void *buf, int32_t bufLen, const SViewMetaReq *pReq) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fullname) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSViewMetaReq(void *buf, int32_t bufLen, SViewMetaReq *pReq) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fullname) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

static int32_t tEncodeSViewMetaRsp(SEncoder *pEncoder, const SViewMetaRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->name) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->user) < 0) return -1;
  if (tEncodeU64(pEncoder, pRsp->dbId) < 0) return -1;
  if (tEncodeU64(pEncoder, pRsp->viewId) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->querySql) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->precision) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->type) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->version) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->numOfCols) < 0) return -1;
  for (int32_t i = 0; i < pRsp->numOfCols; ++i) {
    SSchema *pSchema = &pRsp->pSchema[i];
    if (tEncodeSSchema(pEncoder, pSchema) < 0) return -1;
  }

  return 0;
}

int32_t tSerializeSViewMetaRsp(void *buf, int32_t bufLen, const SViewMetaRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSViewMetaRsp(&encoder, pRsp) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

static int32_t tDecodeSViewMetaRsp(SDecoder *pDecoder, SViewMetaRsp *pRsp) {
  if (tDecodeCStrTo(pDecoder, pRsp->name) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pRsp->user) < 0) return -1;
  if (tDecodeU64(pDecoder, &pRsp->dbId) < 0) return -1;
  if (tDecodeU64(pDecoder, &pRsp->viewId) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pRsp->querySql) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->precision) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->type) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->version) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->numOfCols) < 0) return -1;
  if (pRsp->numOfCols > 0) {
    pRsp->pSchema = taosMemoryCalloc(pRsp->numOfCols, sizeof(SSchema));
    if (pRsp->pSchema == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < pRsp->numOfCols; ++i) {
      SSchema *pSchema = pRsp->pSchema + i;
      if (tDecodeSSchema(pDecoder, pSchema) < 0) return -1;
    }
  }

  return 0;
}

int32_t tDeserializeSViewMetaRsp(void *buf, int32_t bufLen, SViewMetaRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSViewMetaRsp(&decoder, pRsp) < 0) return -1;

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSViewMetaRsp(SViewMetaRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosMemoryFree(pRsp->user);
  taosMemoryFree(pRsp->querySql);
  taosMemoryFree(pRsp->pSchema);
}

int32_t tSerializeSViewHbRsp(void *buf, int32_t bufLen, SViewHbRsp *pRsp) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfMeta = taosArrayGetSize(pRsp->pViewRsp);
  if (tEncodeI32(&encoder, numOfMeta) < 0) return -1;
  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *pMetaRsp = taosArrayGetP(pRsp->pViewRsp, i);
    if (tEncodeSViewMetaRsp(&encoder, pMetaRsp) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSViewHbRsp(void *buf, int32_t bufLen, SViewHbRsp *pRsp) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t numOfMeta = 0;
  if (tDecodeI32(&decoder, &numOfMeta) < 0) return -1;
  pRsp->pViewRsp = taosArrayInit(numOfMeta, POINTER_BYTES);
  if (pRsp->pViewRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *metaRsp = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
    if (NULL == metaRsp) return -1;
    if (tDecodeSViewMetaRsp(&decoder, metaRsp) < 0) return -1;
    taosArrayPush(pRsp->pViewRsp, &metaRsp);
  }

  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSViewHbRsp(SViewHbRsp *pRsp) {
  int32_t numOfMeta = taosArrayGetSize(pRsp->pViewRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *pMetaRsp = taosArrayGetP(pRsp->pViewRsp, i);
    tFreeSViewMetaRsp(pMetaRsp);
    taosMemoryFree(pMetaRsp);
  }

  taosArrayDestroy(pRsp->pViewRsp);
}
