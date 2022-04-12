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

int32_t tInitSubmitMsgIter(const SSubmitReq *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  pIter->totalLen = pMsg->length;
  ASSERT(pIter->totalLen > 0);
  pIter->len = 0;
  pIter->pMsg = pMsg;
  if (pMsg->length <= sizeof(SSubmitReq)) {
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
    SSubmitBlk *pSubmitBlk = (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);
    pIter->len += (sizeof(SSubmitBlk) + pSubmitBlk->dataLen + pSubmitBlk->schemaLen);
    ASSERT(pIter->len > 0);
  }

  if (pIter->len > pIter->totalLen) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    *pPBlock = NULL;
    return -1;
  }

  *pPBlock = (pIter->len == pIter->totalLen) ? NULL : (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);

  return 0;
}

int32_t tInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pBlock->dataLen <= 0) return -1;
  pIter->totalLen = pBlock->dataLen;
  pIter->len = 0;
  pIter->row = (STSRow *)(pBlock->data + pBlock->schemaLen);
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

int32_t tEncodeSEpSet(SCoder *pEncoder, const SEpSet *pEp) {
  if (tEncodeI8(pEncoder, pEp->inUse) < 0) return -1;
  if (tEncodeI8(pEncoder, pEp->numOfEps) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    if (tEncodeU16(pEncoder, pEp->eps[i].port) < 0) return -1;
    if (tEncodeCStr(pEncoder, pEp->eps[i].fqdn) < 0) return -1;
  }
  return 0;
}

int32_t tDecodeSEpSet(SCoder *pDecoder, SEpSet *pEp) {
  if (tDecodeI8(pDecoder, &pEp->inUse) < 0) return -1;
  if (tDecodeI8(pDecoder, &pEp->numOfEps) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    if (tDecodeU16(pDecoder, &pEp->eps[i].port) < 0) return -1;
    if (tDecodeCStrTo(pDecoder, pEp->eps[i].fqdn) < 0) return -1;
  }
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

void *taosDecodeSEpSet(void *buf, SEpSet *pEp) {
  buf = taosDecodeFixedI8(buf, &pEp->inUse);
  buf = taosDecodeFixedI8(buf, &pEp->numOfEps);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    buf = taosDecodeFixedU16(buf, &pEp->eps[i].port);
    buf = taosDecodeStringTo(buf, pEp->eps[i].fqdn);
  }
  return buf;
}

static int32_t tSerializeSClientHbReq(SCoder *pEncoder, const SClientHbReq *pReq) {
  if (tEncodeSClientHbKey(pEncoder, &pReq->connKey) < 0) return -1;

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

static int32_t tDeserializeSClientHbReq(SCoder *pDecoder, SClientHbReq *pReq) {
  if (tDecodeSClientHbKey(pDecoder, &pReq->connKey) < 0) return -1;

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

static int32_t tSerializeSClientHbRsp(SCoder *pEncoder, const SClientHbRsp *pRsp) {
  if (tEncodeSClientHbKey(pEncoder, &pRsp->connKey) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->status) < 0) return -1;

  int32_t kvNum = taosArrayGetSize(pRsp->info);
  if (tEncodeI32(pEncoder, kvNum) < 0) return -1;
  for (int32_t i = 0; i < kvNum; i++) {
    SKv *kv = taosArrayGet(pRsp->info, i);
    if (tEncodeSKv(pEncoder, kv) < 0) return -1;
  }

  return 0;
}

static int32_t tDeserializeSClientHbRsp(SCoder *pDecoder, SClientHbRsp *pRsp) {
  if (tDecodeSClientHbKey(pDecoder, &pRsp->connKey) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->status) < 0) return -1;

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
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSClientHbBatchReq(void *buf, int32_t bufLen, SClientHbBatchReq *pBatchReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pBatchReq->reqId) < 0) return -1;

  int32_t reqNum = 0;
  if (tDecodeI32(&decoder, &reqNum) < 0) return -1;
  if (pBatchReq->reqs == NULL) {
    pBatchReq->reqs = taosArrayInit(reqNum, sizeof(SClientHbReq));
  }
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq req = {0};
    tDeserializeSClientHbReq(&decoder, &req);
    taosArrayPush(pBatchReq->reqs, &req);
  }

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSClientHbBatchRsp(void *buf, int32_t bufLen, const SClientHbBatchRsp *pBatchRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSClientHbBatchRsp(void *buf, int32_t bufLen, SClientHbBatchRsp *pBatchRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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
  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVCreateTbReq(void **buf, SVCreateTbReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedI64(buf, pReq->ver);
  tlen += taosEncodeString(buf, pReq->dbFName);
  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedU32(buf, pReq->ttl);
  tlen += taosEncodeFixedU32(buf, pReq->keep);
  tlen += taosEncodeFixedU8(buf, pReq->info);

  switch (pReq->type) {
    case TD_SUPER_TABLE:
      tlen += taosEncodeFixedI64(buf, pReq->stbCfg.suid);
      tlen += taosEncodeFixedI16(buf, pReq->stbCfg.nCols);
      tlen += taosEncodeFixedI16(buf, pReq->stbCfg.nBSmaCols);
      for (col_id_t i = 0; i < pReq->stbCfg.nCols; ++i) {
        tlen += taosEncodeFixedI8(buf, pReq->stbCfg.pSchema[i].type);
        tlen += taosEncodeFixedI8(buf, pReq->stbCfg.pSchema[i].sma);
        tlen += taosEncodeFixedI16(buf, pReq->stbCfg.pSchema[i].colId);
        tlen += taosEncodeFixedI32(buf, pReq->stbCfg.pSchema[i].bytes);
        tlen += taosEncodeString(buf, pReq->stbCfg.pSchema[i].name);
      }
      tlen += taosEncodeFixedI16(buf, pReq->stbCfg.nTagCols);
      for (col_id_t i = 0; i < pReq->stbCfg.nTagCols; ++i) {
        tlen += taosEncodeFixedI8(buf, pReq->stbCfg.pTagSchema[i].type);
        tlen += taosEncodeFixedI8(buf, pReq->stbCfg.pTagSchema[i].index);
        tlen += taosEncodeFixedI16(buf, pReq->stbCfg.pTagSchema[i].colId);
        tlen += taosEncodeFixedI32(buf, pReq->stbCfg.pTagSchema[i].bytes);
        tlen += taosEncodeString(buf, pReq->stbCfg.pTagSchema[i].name);
      }
      if (pReq->rollup && pReq->stbCfg.pRSmaParam) {
        SRSmaParam *param = pReq->stbCfg.pRSmaParam;
        tlen += taosEncodeBinary(buf, (const void *)&param->xFilesFactor, sizeof(param->xFilesFactor));
        tlen += taosEncodeFixedI32(buf, param->delay);
        tlen += taosEncodeFixedI8(buf, param->nFuncIds);
        for (int8_t i = 0; i < param->nFuncIds; ++i) {
          tlen += taosEncodeFixedI32(buf, param->pFuncIds[i]);
        }
      }
      break;
    case TD_CHILD_TABLE:
      tlen += taosEncodeFixedI64(buf, pReq->ctbCfg.suid);
      tlen += tdEncodeKVRow(buf, pReq->ctbCfg.pTag);
      break;
    case TD_NORMAL_TABLE:
      tlen += taosEncodeFixedI16(buf, pReq->ntbCfg.nCols);
      tlen += taosEncodeFixedI16(buf, pReq->ntbCfg.nBSmaCols);
      for (col_id_t i = 0; i < pReq->ntbCfg.nCols; ++i) {
        tlen += taosEncodeFixedI8(buf, pReq->ntbCfg.pSchema[i].type);
        tlen += taosEncodeFixedI8(buf, pReq->ntbCfg.pSchema[i].sma);
        tlen += taosEncodeFixedI16(buf, pReq->ntbCfg.pSchema[i].colId);
        tlen += taosEncodeFixedI32(buf, pReq->ntbCfg.pSchema[i].bytes);
        tlen += taosEncodeString(buf, pReq->ntbCfg.pSchema[i].name);
      }
      if (pReq->rollup && pReq->ntbCfg.pRSmaParam) {
        SRSmaParam *param = pReq->ntbCfg.pRSmaParam;
        tlen += taosEncodeBinary(buf, (const void *)&param->xFilesFactor, sizeof(param->xFilesFactor));
        tlen += taosEncodeFixedI32(buf, param->delay);
        tlen += taosEncodeFixedI8(buf, param->nFuncIds);
        for (int8_t i = 0; i < param->nFuncIds; ++i) {
          tlen += taosEncodeFixedI32(buf, param->pFuncIds[i]);
        }
      }
      break;
    default:
      ASSERT(0);
  }

  return tlen;
}

void *tDeserializeSVCreateTbReq(void *buf, SVCreateTbReq *pReq) {
  buf = taosDecodeFixedI64(buf, &(pReq->ver));
  buf = taosDecodeString(buf, &(pReq->dbFName));
  buf = taosDecodeString(buf, &(pReq->name));
  buf = taosDecodeFixedU32(buf, &(pReq->ttl));
  buf = taosDecodeFixedU32(buf, &(pReq->keep));
  buf = taosDecodeFixedU8(buf, &(pReq->info));

  switch (pReq->type) {
    case TD_SUPER_TABLE:
      buf = taosDecodeFixedI64(buf, &(pReq->stbCfg.suid));
      buf = taosDecodeFixedI16(buf, &(pReq->stbCfg.nCols));
      buf = taosDecodeFixedI16(buf, &(pReq->stbCfg.nBSmaCols));
      pReq->stbCfg.pSchema = (SSchemaEx *)taosMemoryMalloc(pReq->stbCfg.nCols * sizeof(SSchemaEx));
      for (col_id_t i = 0; i < pReq->stbCfg.nCols; ++i) {
        buf = taosDecodeFixedI8(buf, &(pReq->stbCfg.pSchema[i].type));
        buf = taosDecodeFixedI8(buf, &(pReq->stbCfg.pSchema[i].sma));
        buf = taosDecodeFixedI16(buf, &(pReq->stbCfg.pSchema[i].colId));
        buf = taosDecodeFixedI32(buf, &(pReq->stbCfg.pSchema[i].bytes));
        buf = taosDecodeStringTo(buf, pReq->stbCfg.pSchema[i].name);
      }
      buf = taosDecodeFixedI16(buf, &pReq->stbCfg.nTagCols);
      pReq->stbCfg.pTagSchema = (SSchema *)taosMemoryMalloc(pReq->stbCfg.nTagCols * sizeof(SSchema));
      for (col_id_t i = 0; i < pReq->stbCfg.nTagCols; ++i) {
        buf = taosDecodeFixedI8(buf, &(pReq->stbCfg.pTagSchema[i].type));
        buf = taosDecodeFixedI8(buf, &(pReq->stbCfg.pTagSchema[i].index));
        buf = taosDecodeFixedI16(buf, &pReq->stbCfg.pTagSchema[i].colId);
        buf = taosDecodeFixedI32(buf, &pReq->stbCfg.pTagSchema[i].bytes);
        buf = taosDecodeStringTo(buf, pReq->stbCfg.pTagSchema[i].name);
      }
      if (pReq->rollup) {
        pReq->stbCfg.pRSmaParam = (SRSmaParam *)taosMemoryMalloc(sizeof(SRSmaParam));
        SRSmaParam *param = pReq->stbCfg.pRSmaParam;
        buf = taosDecodeBinaryTo(buf, (void *)&param->xFilesFactor, sizeof(param->xFilesFactor));
        buf = taosDecodeFixedI32(buf, &param->delay);
        buf = taosDecodeFixedI8(buf, &param->nFuncIds);
        if (param->nFuncIds > 0) {
          param->pFuncIds = (func_id_t *)taosMemoryMalloc(param->nFuncIds * sizeof(func_id_t));
          for (int8_t i = 0; i < param->nFuncIds; ++i) {
            buf = taosDecodeFixedI32(buf, param->pFuncIds + i);
          }
        } else {
          param->pFuncIds = NULL;
        }
      } else {
        pReq->stbCfg.pRSmaParam = NULL;
      }
      break;
    case TD_CHILD_TABLE:
      buf = taosDecodeFixedI64(buf, &pReq->ctbCfg.suid);
      buf = tdDecodeKVRow(buf, &pReq->ctbCfg.pTag);
      break;
    case TD_NORMAL_TABLE:
      buf = taosDecodeFixedI16(buf, &pReq->ntbCfg.nCols);
      buf = taosDecodeFixedI16(buf, &(pReq->ntbCfg.nBSmaCols));
      pReq->ntbCfg.pSchema = (SSchemaEx *)taosMemoryMalloc(pReq->ntbCfg.nCols * sizeof(SSchemaEx));
      for (col_id_t i = 0; i < pReq->ntbCfg.nCols; ++i) {
        buf = taosDecodeFixedI8(buf, &pReq->ntbCfg.pSchema[i].type);
        buf = taosDecodeFixedI8(buf, &pReq->ntbCfg.pSchema[i].sma);
        buf = taosDecodeFixedI16(buf, &pReq->ntbCfg.pSchema[i].colId);
        buf = taosDecodeFixedI32(buf, &pReq->ntbCfg.pSchema[i].bytes);
        buf = taosDecodeStringTo(buf, pReq->ntbCfg.pSchema[i].name);
      }
      if (pReq->rollup) {
        pReq->ntbCfg.pRSmaParam = (SRSmaParam *)taosMemoryMalloc(sizeof(SRSmaParam));
        SRSmaParam *param = pReq->ntbCfg.pRSmaParam;
        buf = taosDecodeBinaryTo(buf, (void *)&param->xFilesFactor, sizeof(param->xFilesFactor));
        buf = taosDecodeFixedI32(buf, &param->delay);
        buf = taosDecodeFixedI8(buf, &param->nFuncIds);
        if (param->nFuncIds > 0) {
          param->pFuncIds = (func_id_t *)taosMemoryMalloc(param->nFuncIds * sizeof(func_id_t));
          for (int8_t i = 0; i < param->nFuncIds; ++i) {
            buf = taosDecodeFixedI32(buf, param->pFuncIds + i);
          }
        } else {
          param->pFuncIds = NULL;
        }
      } else {
        pReq->ntbCfg.pRSmaParam = NULL;
      }
      break;
    default:
      ASSERT(0);
  }

  return buf;
}

int32_t tSerializeSVCreateTbBatchReq(void **buf, SVCreateTbBatchReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedI64(buf, pReq->ver);
  tlen += taosEncodeFixedU32(buf, taosArrayGetSize(pReq->pArray));
  for (size_t i = 0; i < taosArrayGetSize(pReq->pArray); i++) {
    SVCreateTbReq *pCreateTbReq = taosArrayGet(pReq->pArray, i);
    tlen += tSerializeSVCreateTbReq(buf, pCreateTbReq);
  }

  return tlen;
}

void *tDeserializeSVCreateTbBatchReq(void *buf, SVCreateTbBatchReq *pReq) {
  uint32_t nsize = 0;

  buf = taosDecodeFixedI64(buf, &pReq->ver);
  buf = taosDecodeFixedU32(buf, &nsize);
  pReq->pArray = taosArrayInit(nsize, sizeof(SVCreateTbReq));
  for (size_t i = 0; i < nsize; i++) {
    SVCreateTbReq req = {0};
    buf = tDeserializeSVCreateTbReq(buf, &req);
    taosArrayPush(pReq->pArray, &req);
  }

  return buf;
}

int32_t tSerializeSVDropTbReq(void **buf, SVDropTbReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->ver);
  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedU8(buf, pReq->type);
  return tlen;
}

void *tDeserializeSVDropTbReq(void *buf, SVDropTbReq *pReq) {
  buf = taosDecodeFixedI64(buf, &pReq->ver);
  buf = taosDecodeString(buf, &pReq->name);
  buf = taosDecodeFixedU8(buf, &pReq->type);
  return buf;
}

int32_t tSerializeSMCreateStbReq(void *buf, int32_t bufLen, SMCreateStbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeFloat(&encoder, pReq->xFilesFactor) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->aggregationMethod) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->delay) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ttl) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfColumns) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfTags) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfSmas) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commentLen) < 0) return -1;

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField *pField = taosArrayGet(pReq->pColumns, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField *pField = taosArrayGet(pReq->pTags, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }

  for (int32_t i = 0; i < pReq->numOfSmas; ++i) {
    SField *pField = taosArrayGet(pReq->pSmas, i);
    if (tEncodeI8(&encoder, pField->type) < 0) return -1;
    if (tEncodeI32(&encoder, pField->bytes) < 0) return -1;
    if (tEncodeCStr(&encoder, pField->name) < 0) return -1;
  }

  if (pReq->commentLen > 0) {
    if (tEncodeBinary(&encoder, pReq->comment, pReq->commentLen) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateStbReq(void *buf, int32_t bufLen, SMCreateStbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeFloat(&decoder, &pReq->xFilesFactor) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->aggregationMethod) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->delay) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfColumns) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfTags) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfSmas) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commentLen) < 0) return -1;

  pReq->pColumns = taosArrayInit(pReq->numOfColumns, sizeof(SField));
  pReq->pTags = taosArrayInit(pReq->numOfTags, sizeof(SField));
  pReq->pSmas = taosArrayInit(pReq->numOfSmas, sizeof(SField));
  if (pReq->pColumns == NULL || pReq->pTags == NULL || pReq->pSmas == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
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
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (taosArrayPush(pReq->pTags, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int32_t i = 0; i < pReq->numOfSmas; ++i) {
    SField field = {0};
    if (tDecodeI8(&decoder, &field.type) < 0) return -1;
    if (tDecodeI32(&decoder, &field.bytes) < 0) return -1;
    if (tDecodeCStrTo(&decoder, field.name) < 0) return -1;
    if (taosArrayPush(pReq->pSmas, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  if (pReq->commentLen > 0) {
    pReq->comment = taosMemoryMalloc(pReq->commentLen);
    if (pReq->comment == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->comment) < 0) return -1;
  }

  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

void tFreeSMCreateStbReq(SMCreateStbReq *pReq) {
  taosArrayDestroy(pReq->pColumns);
  taosArrayDestroy(pReq->pTags);
  taosArrayDestroy(pReq->pSmas);
  taosMemoryFreeClear(pReq->comment);
  pReq->pColumns = NULL;
  pReq->pTags = NULL;
  pReq->pSmas = NULL;
}

int32_t tSerializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMAlterStbReq(void *buf, int32_t bufLen, SMAltertbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMAlterStbReq(void *buf, int32_t bufLen, SMAltertbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

void tFreeSMAltertbReq(SMAltertbReq *pReq) {
  taosArrayDestroy(pReq->pFields);
  pReq->pFields = NULL;
}

int32_t tSerializeSMCreateSmaReq(void *buf, int32_t bufLen, SMCreateSmaReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateSmaReq(void *buf, int32_t bufLen, SMCreateSmaReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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
  tCoderClear(&decoder);
  return 0;
}

void tFreeSMCreateSmaReq(SMCreateSmaReq *pReq) {
  taosMemoryFreeClear(pReq->expr);
  taosMemoryFreeClear(pReq->tagsFilter);
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
}

int32_t tSerializeSMDropSmaReq(void *buf, int32_t bufLen, SMDropSmaReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;

  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropSmaReq(void *buf, int32_t bufLen, SMDropSmaReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}
int32_t tSerializeSMCreateFullTextReq(void *buf, int32_t bufLen, SMCreateFullTextReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMCreateFullTextReq(void *buf, int32_t bufLen, SMCreateFullTextReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);
  if (tStartDecode(&decoder) < 0) return -1;

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}
void tFreeSMCreateFullTextReq(SMCreateFullTextReq *pReq) {
  // impl later
  return;
}
int32_t tSerializeSMDropFullTextReq(void *buf, int32_t bufLen, SMDropFullTextReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;

  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMDropFullTextReq(void *buf, int32_t bufLen, SMDropFullTextReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);
  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSStatusReq(void *buf, int32_t bufLen, SStatusReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;

  // status
  if (tEncodeI32(&encoder, pReq->sver) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dver) < 0) return -1;
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
    if (tEncodeI8(&encoder, pload->role) < 0) return -1;
    if (tEncodeI64(&encoder, pload->numOfTables) < 0) return -1;
    if (tEncodeI64(&encoder, pload->numOfTimeSeries) < 0) return -1;
    if (tEncodeI64(&encoder, pload->totalStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->compStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pload->pointsWritten) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatusReq(void *buf, int32_t bufLen, SStatusReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;

  // status
  if (tDecodeI32(&decoder, &pReq->sver) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dver) < 0) return -1;
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
    if (tDecodeI8(&decoder, &vload.role) < 0) return -1;
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

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSStatusRsp(void *buf, int32_t bufLen, SStatusRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;

  // status
  if (tEncodeI64(&encoder, pRsp->dver) < 0) return -1;

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
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatusRsp(void *buf, int32_t bufLen, SStatusRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;

  // status
  if (tDecodeI64(&decoder, &pRsp->dver) < 0) return -1;

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
  tCoderClear(&decoder);
  return 0;
}

void tFreeSStatusRsp(SStatusRsp *pRsp) { taosArrayDestroy(pRsp->pDnodeEps); }

int32_t tSerializeSCreateAcctReq(void *buf, int32_t bufLen, SCreateAcctReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateAcctReq(void *buf, int32_t bufLen, SCreateAcctReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropUserReq(void *buf, int32_t bufLen, SDropUserReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropUserReq(void *buf, int32_t bufLen, SDropUserReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->createType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->superUser) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->createType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->superUser) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->alterType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->superUser) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pass) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbname) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->alterType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->superUser) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pass) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dbname) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSGetUserAuthReq(void *buf, int32_t bufLen, SGetUserAuthReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserAuthReq(void *buf, int32_t bufLen, SGetUserAuthReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSGetUserAuthRsp(void *buf, int32_t bufLen, SGetUserAuthRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->user) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->superAuth) < 0) return -1;

  int32_t numOfReadDbs = taosHashGetSize(pRsp->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pRsp->writeDbs);
  if (tEncodeI32(&encoder, numOfReadDbs) < 0) return -1;
  if (tEncodeI32(&encoder, numOfWriteDbs) < 0) return -1;

  char *db = taosHashIterate(pRsp->readDbs, NULL);
  while (db != NULL) {
    if (tEncodeCStr(&encoder, db) < 0) return -1;
    db = taosHashIterate(pRsp->readDbs, db);
  }

  db = taosHashIterate(pRsp->writeDbs, NULL);
  while (db != NULL) {
    if (tEncodeCStr(&encoder, db) < 0) return -1;
    db = taosHashIterate(pRsp->writeDbs, db);
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserAuthRsp(void *buf, int32_t bufLen, SGetUserAuthRsp *pRsp) {
  pRsp->readDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  pRsp->writeDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  if (pRsp->readDbs == NULL || pRsp->writeDbs == NULL) {
    return -1;
  }

  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->user) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->superAuth) < 0) return -1;

  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  if (tDecodeI32(&decoder, &numOfReadDbs) < 0) return -1;
  if (tDecodeI32(&decoder, &numOfWriteDbs) < 0) return -1;

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(&decoder, db) < 0) return -1;
    int32_t len = strlen(db) + 1;
    taosHashPut(pRsp->readDbs, db, len, db, len);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(&decoder, db) < 0) return -1;
    int32_t len = strlen(db) + 1;
    taosHashPut(pRsp->writeDbs, db, len, db, len);
  }

  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateDropMQSBNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDropMQSBNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fqdn) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->port) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fqdn) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->port) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->config) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->value) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->config) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->value) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateDnodeReq(void *buf, int32_t bufLen, SCreateDnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->fqdn) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->port) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDnodeReq(void *buf, int32_t bufLen, SCreateDnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->fqdn) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->port) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateFuncReq(void *buf, int32_t bufLen, SCreateFuncReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->funcType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->scriptType) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->outputType) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->outputLen) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->bufSize) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->signature) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commentSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->codeSize) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pComment) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->pCode) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateFuncReq(void *buf, int32_t bufLen, SCreateFuncReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->funcType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->scriptType) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->outputType) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->outputLen) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->bufSize) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->signature) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commentSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->codeSize) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pComment) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->pCode) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropFuncReq(void *buf, int32_t bufLen, SDropFuncReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropFuncReq(void *buf, int32_t bufLen, SDropFuncReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSRetrieveFuncReq(void *buf, int32_t bufLen, SRetrieveFuncReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfFuncs) < 0) return -1;

  if (pReq->numOfFuncs != (int32_t)taosArrayGetSize(pReq->pFuncNames)) return -1;
  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char *fname = taosArrayGet(pReq->pFuncNames, i);
    if (tEncodeCStr(&encoder, fname) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveFuncReq(void *buf, int32_t bufLen, SRetrieveFuncReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfFuncs) < 0) return -1;

  pReq->pFuncNames = taosArrayInit(pReq->numOfFuncs, TSDB_FUNC_NAME_LEN);
  if (pReq->pFuncNames == NULL) return -1;

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char fname[TSDB_FUNC_NAME_LEN] = {0};
    if (tDecodeCStrTo(&decoder, fname) < 0) return -1;
    taosArrayPush(pReq->pFuncNames, fname);
  }
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSRetrieveFuncRsp(void *buf, int32_t bufLen, SRetrieveFuncRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
    if (tEncodeI32(&encoder, pInfo->commentSize) < 0) return -1;
    if (tEncodeI32(&encoder, pInfo->codeSize) < 0) return -1;
    if (tEncodeCStr(&encoder, pInfo->pComment) < 0) return -1;
    if (tEncodeCStr(&encoder, pInfo->pCode) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveFuncRsp(void *buf, int32_t bufLen, SRetrieveFuncRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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
    if (tDecodeI32(&decoder, &fInfo.commentSize) < 0) return -1;
    if (tDecodeI32(&decoder, &fInfo.codeSize) < 0) return -1;
    if (tDecodeCStrTo(&decoder, fInfo.pComment) < 0) return -1;
    if (tDecodeCStrTo(&decoder, fInfo.pCode) < 0) return -1;
    taosArrayPush(pRsp->pFuncInfos, &fInfo);
  }
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCreateDbReq(void *buf, int32_t bufLen, SCreateDbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfVgroups) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->cacheBlockSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->totalBlocks) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commitTime) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->ttl) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->quorum) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->update) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreExist) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->streamMode) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->singleSTable) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    if (tEncodeI32(&encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI32(&encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->keepUnit) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDbReq(void *buf, int32_t bufLen, SCreateDbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfVgroups) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->cacheBlockSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->totalBlocks) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commitTime) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->ttl) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->quorum) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->update) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->ignoreExist) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->streamMode) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->singleSTable) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfRetensions) < 0) return -1;
  pReq->pRetensions = taosArrayInit(pReq->numOfRetensions, sizeof(SRetention));
  if (pReq->pRetensions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention rentension = {0};
    if (tDecodeI32(&decoder, &rentension.freq) < 0) return -1;
    if (tDecodeI32(&decoder, &rentension.keep) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.freqUnit) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.keepUnit) < 0) return -1;
    if (taosArrayPush(pReq->pRetensions, &rentension) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

void tFreeSCreateDbReq(SCreateDbReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
}

int32_t tSerializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->totalBlocks) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->quorum) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->totalBlocks) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->quorum) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->ignoreNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDbRsp(void *buf, int32_t bufLen, SDropDbRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->db) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->uid) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDbRsp(void *buf, int32_t bufLen, SDropDbRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->uid) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->numOfTable) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->numOfTable) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQnodeListReq(void *buf, int32_t bufLen, SQnodeListReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->rowNum) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeListReq(void *buf, int32_t bufLen, SQnodeListReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->rowNum) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  int32_t num = taosArrayGetSize(pRsp->epSetList);
  if (tEncodeI32(&encoder, num) < 0) return -1;
  for (int32_t i = 0; i < num; ++i) {
    SEpSet *epSet = taosArrayGet(pRsp->epSetList, i);
    if (tEncodeSEpSet(&encoder, epSet) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  int32_t num = 0;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  pRsp->epSetList = taosArrayInit(num, sizeof(SEpSet));
  if (NULL == pRsp->epSetList) return -1;
  for (int32_t i = 0; i < num; ++i) {
    if (tDecodeSEpSet(&decoder, TARRAY_GET_ELEM(pRsp->epSetList, i)) < 0) return -1;
  }
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

void tFreeSQnodeListRsp(SQnodeListRsp *pRsp) { taosArrayDestroy(pRsp->epSetList); }

int32_t tSerializeSSyncDbReq(void *buf, int32_t bufLen, SSyncDbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSyncDbReq(void *buf, int32_t bufLen, SSyncDbReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUseDbRspImp(SCoder *pEncoder, const SUseDbRsp *pRsp) {
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
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tSerializeSUseDbRspImp(&encoder, pRsp) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSUseDbBatchRsp(void *buf, int32_t bufLen, SUseDbBatchRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tEncodeI32(&encoder, numOfBatch) < 0) return -1;
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SUseDbRsp *pUsedbRsp = taosArrayGet(pRsp->pArray, i);
    if (tSerializeSUseDbRspImp(&encoder, pUsedbRsp) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUseDbRspImp(SCoder *pDecoder, SUseDbRsp *pRsp) {
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
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDeserializeSUseDbRspImp(&decoder, pRsp) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tDeserializeSUseDbBatchRsp(void *buf, int32_t bufLen, SUseDbBatchRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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

  tCoderClear(&decoder);
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

int32_t tSerializeSDbCfgReq(void *buf, int32_t bufLen, SDbCfgReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgReq(void *buf, int32_t bufLen, SDbCfgReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDbCfgRsp(void *buf, int32_t bufLen, const SDbCfgRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfVgroups) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->cacheBlockSize) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->totalBlocks) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->commitTime) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->fsyncPeriod) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->replications) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->quorum) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->update) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->streamMode) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgRsp(void *buf, int32_t bufLen, SDbCfgRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfVgroups) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->cacheBlockSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->totalBlocks) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->commitTime) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->fsyncPeriod) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->replications) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->quorum) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->update) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->streamMode) < 0) return -1;

  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUserIndexReq(void *buf, int32_t bufLen, SUserIndexReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->indexFName) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserIndexReq(void *buf, int32_t bufLen, SUserIndexReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->indexFName) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUserIndexRsp(void *buf, int32_t bufLen, const SUserIndexRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->tblFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->colName) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->indexType) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->indexExts) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserIndexRsp(void *buf, int32_t bufLen, SUserIndexRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->tblFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->colName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->indexType) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->indexExts) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->type) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->payloadLen) < 0) return -1;
  if (pReq->payloadLen > 0) {
    if (tEncodeBinary(&encoder, pReq->payload, pReq->payloadLen) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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
  tCoderClear(&decoder);
  return 0;
}

void tFreeSShowReq(SShowReq *pReq) { taosMemoryFreeClear(pReq->payload); }

int32_t tSerializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->showId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->type) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->free) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->tb) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->showId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->type) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->free) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->tb) < 0) return -1;
  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

static int32_t tEncodeSTableMetaRsp(SCoder *pEncoder, STableMetaRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->tbName) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->stbName) < 0) return -1;
  if (tEncodeCStr(pEncoder, pRsp->dbFName) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->dbId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->numOfTags) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->numOfColumns) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->precision) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->tableType) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->update) < 0) return -1;
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

static int32_t tDecodeSTableMetaRsp(SCoder *pDecoder, STableMetaRsp *pRsp) {
  if (tDecodeCStrTo(pDecoder, pRsp->tbName) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pRsp->stbName) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pRsp->dbFName) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->dbId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->numOfTags) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->numOfColumns) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->precision) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->tableType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->update) < 0) return -1;
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
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSTableMetaRsp(&encoder, pRsp) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSTableMetaBatchRsp(void *buf, int32_t bufLen, STableMetaBatchRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  if (tEncodeI32(&encoder, numOfBatch) < 0) return -1;
  for (int32_t i = 0; i < numOfBatch; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pArray, i);
    if (tEncodeSTableMetaRsp(&encoder, pMetaRsp) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableMetaRsp(void *buf, int32_t bufLen, STableMetaRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSTableMetaRsp(&decoder, pRsp) < 0) return -1;

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

int32_t tDeserializeSTableMetaBatchRsp(void *buf, int32_t bufLen, STableMetaBatchRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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

  tCoderClear(&decoder);
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
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->showId) < 0) return -1;
  if (tEncodeSTableMetaRsp(&encoder, &pRsp->tableMeta) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowRsp(void *buf, int32_t bufLen, SShowRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->showId) < 0) return -1;
  if (tDecodeSTableMetaRsp(&decoder, &pRsp->tableMeta) < 0) return -1;

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

void tFreeSShowRsp(SShowRsp *pRsp) { tFreeSTableMetaRsp(&pRsp->tableMeta); }

int32_t tSerializeSTableInfoReq(void *buf, int32_t bufLen, STableInfoReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->dbFName) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->tbName) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);

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

  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, (char *)buf + headLen, bufLen - headLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->dbFName) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->tbName) < 0) return -1;

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMDropTopicReq(void *buf, int32_t bufLen, SMDropTopicReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igNotExists) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropTopicReq(void *buf, int32_t bufLen, SMDropTopicReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igNotExists) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSCMCreateTopicReq(void *buf, int32_t bufLen, const SCMCreateTopicReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;
  if (pReq->sql != NULL) sqlLen = (int32_t)strlen(pReq->sql);
  if (pReq->ast != NULL) astLen = (int32_t)strlen(pReq->ast);

  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI32(&encoder, sqlLen) < 0) return -1;
  if (tEncodeI32(&encoder, astLen) < 0) return -1;
  if (sqlLen > 0 && tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (astLen > 0 && tEncodeCStr(&encoder, pReq->ast) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateTopicReq(void *buf, int32_t bufLen, SCMCreateTopicReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;

  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
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
  }

  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

void tFreeSCMCreateTopicReq(SCMCreateTopicReq *pReq) {
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
}

int32_t tSerializeSCMCreateTopicRsp(void *buf, int32_t bufLen, const SCMCreateTopicRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->topicId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateTopicRsp(void *buf, int32_t bufLen, SCMCreateTopicRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->topicId) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSConnectReq(void *buf, int32_t bufLen, SConnectReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->pid) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->app) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->startTime) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConnectReq(void *buf, int32_t bufLen, SConnectReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->pid) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->app) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->startTime) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSConnectRsp(void *buf, int32_t bufLen, SConnectRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->acctId) < 0) return -1;
  if (tEncodeI64(&encoder, pRsp->clusterId) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->connId) < 0) return -1;
  if (tEncodeI8(&encoder, pRsp->superUser) < 0) return -1;
  if (tEncodeSEpSet(&encoder, &pRsp->epSet) < 0) return -1;
  if (tEncodeCStr(&encoder, pRsp->sVersion) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConnectRsp(void *buf, int32_t bufLen, SConnectRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->acctId) < 0) return -1;
  if (tDecodeI64(&decoder, &pRsp->clusterId) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->connId) < 0) return -1;
  if (tDecodeI8(&decoder, &pRsp->superUser) < 0) return -1;
  if (tDecodeSEpSet(&decoder, &pRsp->epSet) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pRsp->sVersion) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSMTimerMsg(void *buf, int32_t bufLen, SMTimerReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->reserved) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMTimerMsg(void *buf, int32_t bufLen, SMTimerReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->reserved) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tEncodeSReplica(SCoder *pEncoder, SReplica *pReplica) {
  if (tEncodeI32(pEncoder, pReplica->id) < 0) return -1;
  if (tEncodeU16(pEncoder, pReplica->port) < 0) return -1;
  if (tEncodeCStr(pEncoder, pReplica->fqdn) < 0) return -1;
  return 0;
}

int32_t tDecodeSReplica(SCoder *pDecoder, SReplica *pReplica) {
  if (tDecodeI32(pDecoder, &pReplica->id) < 0) return -1;
  if (tDecodeU16(pDecoder, &pReplica->port) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pReplica->fqdn) < 0) return -1;
  return 0;
}

int32_t tSerializeSCreateVnodeReq(void *buf, int32_t bufLen, SCreateVnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->cacheBlockSize) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->totalBlocks) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysPerFile) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep0) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep1) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->daysToKeep2) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->commitTime) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->fsyncPeriod) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->hashBegin) < 0) return -1;
  if (tEncodeU32(&encoder, pReq->hashEnd) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->hashMethod) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->quorum) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->update) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->selfIndex) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->streamMode) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  if (tEncodeI32(&encoder, pReq->numOfRetensions) < 0) return -1;
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    if (tEncodeI32(&encoder, pRetension->freq) < 0) return -1;
    if (tEncodeI32(&encoder, pRetension->keep) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->freqUnit) < 0) return -1;
    if (tEncodeI8(&encoder, pRetension->keepUnit) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateVnodeReq(void *buf, int32_t bufLen, SCreateVnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->cacheBlockSize) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->totalBlocks) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysPerFile) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep0) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep1) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->daysToKeep2) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->commitTime) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->fsyncPeriod) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->hashBegin) < 0) return -1;
  if (tDecodeU32(&decoder, &pReq->hashEnd) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->hashMethod) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->quorum) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->update) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replica) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->selfIndex) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->streamMode) < 0) return -1;
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
    if (tDecodeI32(&decoder, &rentension.freq) < 0) return -1;
    if (tDecodeI32(&decoder, &rentension.keep) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.freqUnit) < 0) return -1;
    if (tDecodeI8(&decoder, &rentension.keepUnit) < 0) return -1;
    if (taosArrayPush(pReq->pRetensions, &rentension) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

int32_t tFreeSCreateVnodeReq(SCreateVnodeReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
  return 0;
}

int32_t tSerializeSDropVnodeReq(void *buf, int32_t bufLen, SDropVnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->dbUid) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropVnodeReq(void *buf, int32_t bufLen, SDropVnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeI64(&decoder, &pReq->dbUid) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->db) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->connId) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->queryId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->connId) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->queryId) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->connId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->connId) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSKillTransReq(void *buf, int32_t bufLen, SKillTransReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->transId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillTransReq(void *buf, int32_t bufLen, SKillTransReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->transId) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tEncodeSReplica(&encoder, pReplica) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replica) < 0) return -1;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    if (tDecodeSReplica(&decoder, pReplica) < 0) return -1;
  }
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSAuthReq(void *buf, int32_t bufLen, SAuthReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->user) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->spi) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->encrypt) < 0) return -1;
  if (tEncodeBinary(&encoder, pReq->secret, TSDB_PASSWORD_LEN) < 0) return -1;
  if (tEncodeBinary(&encoder, pReq->ckey, TSDB_PASSWORD_LEN) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAuthReq(void *buf, int32_t bufLen, SAuthReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->user) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->spi) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->encrypt) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->secret) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->ckey) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tEncodeSMqOffset(SCoder *encoder, const SMqOffset *pOffset) {
  if (tEncodeI32(encoder, pOffset->vgId) < 0) return -1;
  if (tEncodeI64(encoder, pOffset->offset) < 0) return -1;
  if (tEncodeCStr(encoder, pOffset->topicName) < 0) return -1;
  if (tEncodeCStr(encoder, pOffset->cgroup) < 0) return -1;
  return encoder->pos;
}

int32_t tDecodeSMqOffset(SCoder *decoder, SMqOffset *pOffset) {
  if (tDecodeI32(decoder, &pOffset->vgId) < 0) return -1;
  if (tDecodeI64(decoder, &pOffset->offset) < 0) return -1;
  if (tDecodeCStrTo(decoder, pOffset->topicName) < 0) return -1;
  if (tDecodeCStrTo(decoder, pOffset->cgroup) < 0) return -1;
  return 0;
}

int32_t tEncodeSMqCMCommitOffsetReq(SCoder *encoder, const SMqCMCommitOffsetReq *pReq) {
  if (tStartEncode(encoder) < 0) return -1;
  if (tEncodeI32(encoder, pReq->num) < 0) return -1;
  for (int32_t i = 0; i < pReq->num; i++) {
    tEncodeSMqOffset(encoder, &pReq->offsets[i]);
  }
  tEndEncode(encoder);
  return encoder->pos;
}

int32_t tDecodeSMqCMCommitOffsetReq(SCoder *decoder, SMqCMCommitOffsetReq *pReq) {
  if (tStartDecode(decoder) < 0) return -1;
  if (tDecodeI32(decoder, &pReq->num) < 0) return -1;
  TCODER_MALLOC(pReq->offsets, SMqOffset *, pReq->num * sizeof(SMqOffset), decoder);
  if (pReq->offsets == NULL) return -1;
  for (int32_t i = 0; i < pReq->num; i++) {
    tDecodeSMqOffset(decoder, &pReq->offsets[i]);
  }
  tEndDecode(decoder);
  return 0;
}

int32_t tSerializeSExplainRsp(void *buf, int32_t bufLen, SExplainRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->numOfPlans) < 0) return -1;
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    SExplainExecInfo *info = &pRsp->subplanInfo[i];
    if (tEncodeU64(&encoder, info->startupCost) < 0) return -1;
    if (tEncodeU64(&encoder, info->totalCost) < 0) return -1;
    if (tEncodeU64(&encoder, info->numOfRows) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSExplainRsp(void *buf, int32_t bufLen, SExplainRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->numOfPlans) < 0) return -1;
  if (pRsp->numOfPlans > 0) {
    pRsp->subplanInfo = taosMemoryMalloc(pRsp->numOfPlans * sizeof(SExplainExecInfo));
    if (pRsp->subplanInfo == NULL) return -1;
  }
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    if (tDecodeU64(&decoder, &pRsp->subplanInfo[i].startupCost) < 0) return -1;
    if (tDecodeU64(&decoder, &pRsp->subplanInfo[i].totalCost) < 0) return -1;
    if (tDecodeU64(&decoder, &pRsp->subplanInfo[i].numOfRows) < 0) return -1;
  }

  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSSchedulerHbReq(void *buf, int32_t bufLen, SSchedulerHbReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tCoderClear(&encoder);

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

  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, (char *)buf + headLen, bufLen - headLen, TD_DECODER);

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

  tCoderClear(&decoder);
  return 0;
}

void tFreeSSchedulerHbReq(SSchedulerHbReq *pReq) { taosArrayDestroy(pReq->taskAction); }

int32_t tSerializeSSchedulerHbRsp(void *buf, int32_t bufLen, SSchedulerHbRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

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
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSchedulerHbRsp(void *buf, int32_t bufLen, SSchedulerHbRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

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

  tCoderClear(&decoder);
  return 0;
}

void tFreeSSchedulerHbRsp(SSchedulerHbRsp *pRsp) { taosArrayDestroy(pRsp->taskStatus); }

int32_t tSerializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pRsp->code) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pRsp->code) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVCreateTbBatchRsp(void *buf, int32_t bufLen, SVCreateTbBatchRsp *pRsp) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (pRsp->rspList) {
    int32_t num = taosArrayGetSize(pRsp->rspList);
    if (tEncodeI32(&encoder, num) < 0) return -1;
    for (int32_t i = 0; i < num; ++i) {
      SVCreateTbRsp *rsp = taosArrayGet(pRsp->rspList, i);
      if (tEncodeI32(&encoder, rsp->code) < 0) return -1;
    }
  } else {
    if (tEncodeI32(&encoder, 0) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVCreateTbBatchRsp(void *buf, int32_t bufLen, SVCreateTbBatchRsp *pRsp) {
  SCoder  decoder = {0};
  int32_t num = 0;
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &num) < 0) return -1;
  if (num > 0) {
    pRsp->rspList = taosArrayInit(num, sizeof(SVCreateTbRsp));
    if (NULL == pRsp->rspList) return -1;
    for (int32_t i = 0; i < num; ++i) {
      SVCreateTbRsp rsp = {0};
      if (tDecodeI32(&decoder, &rsp.code) < 0) return -1;
      if (NULL == taosArrayPush(pRsp->rspList, &rsp)) return -1;
    }
  } else {
    pRsp->rspList = NULL;
  }
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSVCreateTSmaReq(void **buf, SVCreateTSmaReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedI64(buf, pReq->ver);
  tlen += tEncodeTSma(buf, &pReq->tSma);

  return tlen;
}

void *tDeserializeSVCreateTSmaReq(void *buf, SVCreateTSmaReq *pReq) {
  buf = taosDecodeFixedI64(buf, &(pReq->ver));

  if ((buf = tDecodeTSma(buf, &pReq->tSma)) == NULL) {
    tdDestroyTSma(&pReq->tSma);
  }
  return buf;
}

int32_t tSerializeSVDropTSmaReq(void **buf, SVDropTSmaReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedI64(buf, pReq->ver);
  tlen += taosEncodeFixedI64(buf, pReq->indexUid);
  tlen += taosEncodeString(buf, pReq->indexName);

  return tlen;
}
void *tDeserializeSVDropTSmaReq(void *buf, SVDropTSmaReq *pReq) {
  buf = taosDecodeFixedI64(buf, &(pReq->ver));
  buf = taosDecodeFixedI64(buf, &(pReq->indexUid));
  buf = taosDecodeStringTo(buf, pReq->indexName);

  return buf;
}

int32_t tSerializeSCMCreateStreamReq(void *buf, int32_t bufLen, const SCMCreateStreamReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;
  if (pReq->sql != NULL) sqlLen = (int32_t)strlen(pReq->sql);
  if (pReq->ast != NULL) astLen = (int32_t)strlen(pReq->ast);

  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->name) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->outputSTbName) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->igExists) < 0) return -1;
  if (tEncodeI32(&encoder, sqlLen) < 0) return -1;
  if (tEncodeI32(&encoder, astLen) < 0) return -1;
  if (sqlLen > 0 && tEncodeCStr(&encoder, pReq->sql) < 0) return -1;
  if (astLen > 0 && tEncodeCStr(&encoder, pReq->ast) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateStreamReq(void *buf, int32_t bufLen, SCMCreateStreamReq *pReq) {
  int32_t sqlLen = 0;
  int32_t astLen = 0;

  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->name) < 0) return -1;
  if (tDecodeCStrTo(&decoder, pReq->outputSTbName) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->igExists) < 0) return -1;
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
  }
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

void tFreeSCMCreateStreamReq(SCMCreateStreamReq *pReq) {
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
}
