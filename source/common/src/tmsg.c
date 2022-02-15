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

int32_t tInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter) {
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

int32_t tGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock) {
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
    pBatchRsp->rsps = taosArrayInit(rspNum, sizeof(SClientHbReq));
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

int32_t tSerializeSVCreateTbBatchReq(void **buf, SVCreateTbBatchReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedU64(buf, pReq->ver);
  tlen += taosEncodeFixedU32(buf, taosArrayGetSize(pReq->pArray));
  for (size_t i = 0; i < taosArrayGetSize(pReq->pArray); i++) {
    SVCreateTbReq *pCreateTbReq = taosArrayGet(pReq->pArray, i);
    tlen += tSerializeSVCreateTbReq(buf, pCreateTbReq);
  }

  return tlen;
}

void *tDeserializeSVCreateTbBatchReq(void *buf, SVCreateTbBatchReq *pReq) {
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

int32_t tSerializeSVDropTbReq(void **buf, SVDropTbReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedU64(buf, pReq->ver);
  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedU8(buf, pReq->type);
  return tlen;
}

void *tDeserializeSVDropTbReq(void *buf, SVDropTbReq *pReq) {
  buf = taosDecodeFixedU64(buf, &pReq->ver);
  buf = taosDecodeString(buf, &pReq->name);
  buf = taosDecodeFixedU8(buf, &pReq->type);
  return buf;
}

int32_t tSerializeSMCreateStbReq(void **buf, SMCreateStbReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedI8(buf, pReq->igExists);
  tlen += taosEncodeFixedI32(buf, pReq->numOfColumns);
  tlen += taosEncodeFixedI32(buf, pReq->numOfTags);

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField *pField = taosArrayGet(pReq->pColumns, i);
    tlen += taosEncodeFixedI8(buf, pField->type);
    tlen += taosEncodeFixedI32(buf, pField->bytes);
    tlen += taosEncodeString(buf, pField->name);
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField *pField = taosArrayGet(pReq->pTags, i);
    tlen += taosEncodeFixedI8(buf, pField->type);
    tlen += taosEncodeFixedI32(buf, pField->bytes);
    tlen += taosEncodeString(buf, pField->name);
  }

  tlen += taosEncodeString(buf, pReq->comment);
  return tlen;
}

void *tDeserializeSMCreateStbReq(void *buf, SMCreateStbReq *pReq) {
  buf = taosDecodeStringTo(buf, pReq->name);
  buf = taosDecodeFixedI8(buf, &pReq->igExists);
  buf = taosDecodeFixedI32(buf, &pReq->numOfColumns);
  buf = taosDecodeFixedI32(buf, &pReq->numOfTags);

  pReq->pColumns = taosArrayInit(pReq->numOfColumns, sizeof(SField));
  pReq->pTags = taosArrayInit(pReq->numOfTags, sizeof(SField));
  if (pReq->pColumns == NULL || pReq->pTags == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SField field = {0};
    buf = taosDecodeFixedI8(buf, &field.type);
    buf = taosDecodeFixedI32(buf, &field.bytes);
    buf = taosDecodeStringTo(buf, field.name);
    if (taosArrayPush(pReq->pColumns, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField field = {0};
    buf = taosDecodeFixedI8(buf, &field.type);
    buf = taosDecodeFixedI32(buf, &field.bytes);
    buf = taosDecodeStringTo(buf, field.name);
    if (taosArrayPush(pReq->pTags, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
  }

  buf = taosDecodeStringTo(buf, pReq->comment);
  return buf;
}

void tFreeSMCreateStbReq(SMCreateStbReq *pReq) {
  taosArrayDestroy(pReq->pColumns);
  taosArrayDestroy(pReq->pTags);
}

int32_t tSerializeSMDropStbReq(void **buf, SMDropStbReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedI8(buf, pReq->igNotExists);

  return tlen;
}

void *tDeserializeSMDropStbReq(void *buf, SMDropStbReq *pReq) {
  buf = taosDecodeStringTo(buf, pReq->name);
  buf = taosDecodeFixedI8(buf, &pReq->igNotExists);

  return buf;
}

int32_t tSerializeSMAlterStbReq(void **buf, SMAltertbReq *pReq) {
  int32_t tlen = 0;

  tlen += taosEncodeString(buf, pReq->name);
  tlen += taosEncodeFixedI8(buf, pReq->alterType);
  tlen += taosEncodeFixedI32(buf, pReq->numOfFields);

  for (int32_t i = 0; i < pReq->numOfFields; ++i) {
    SField *pField = taosArrayGet(pReq->pFields, i);
    tlen += taosEncodeFixedU8(buf, pField->type);
    tlen += taosEncodeFixedI32(buf, pField->bytes);
    tlen += taosEncodeString(buf, pField->name);
  }

  return tlen;
}

void *tDeserializeSMAlterStbReq(void *buf, SMAltertbReq *pReq) {
  buf = taosDecodeStringTo(buf, pReq->name);
  buf = taosDecodeFixedI8(buf, &pReq->alterType);
  buf = taosDecodeFixedI32(buf, &pReq->numOfFields);

  pReq->pFields = taosArrayInit(pReq->numOfFields, sizeof(SField));
  if (pReq->pFields == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < pReq->numOfFields; ++i) {
    SField field = {0};
    buf = taosDecodeFixedU8(buf, &field.type);
    buf = taosDecodeFixedI32(buf, &field.bytes);
    buf = taosDecodeStringTo(buf, field.name);
    if (taosArrayPush(pReq->pFields, &field) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
  }

  return buf;
}

int32_t tSerializeSStatusReq(void **buf, SStatusReq *pReq) {
  int32_t tlen = 0;

  // status
  tlen += taosEncodeFixedI32(buf, pReq->sver);
  tlen += taosEncodeFixedI64(buf, pReq->dver);
  tlen += taosEncodeFixedI32(buf, pReq->dnodeId);
  tlen += taosEncodeFixedI64(buf, pReq->clusterId);
  tlen += taosEncodeFixedI64(buf, pReq->rebootTime);
  tlen += taosEncodeFixedI64(buf, pReq->updateTime);
  tlen += taosEncodeFixedI32(buf, pReq->numOfCores);
  tlen += taosEncodeFixedI32(buf, pReq->numOfSupportVnodes);
  tlen += taosEncodeString(buf, pReq->dnodeEp);

  // cluster cfg
  tlen += taosEncodeFixedI32(buf, pReq->clusterCfg.statusInterval);
  tlen += taosEncodeFixedI64(buf, pReq->clusterCfg.checkTime);
  tlen += taosEncodeString(buf, pReq->clusterCfg.timezone);
  tlen += taosEncodeString(buf, pReq->clusterCfg.locale);
  tlen += taosEncodeString(buf, pReq->clusterCfg.charset);

  // vnode loads
  int32_t vlen = (int32_t)taosArrayGetSize(pReq->pVloads);
  tlen += taosEncodeFixedI32(buf, vlen);
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    tlen += taosEncodeFixedI32(buf, pload->vgId);
    tlen += taosEncodeFixedI8(buf, pload->role);
    tlen += taosEncodeFixedI64(buf, pload->numOfTables);
    tlen += taosEncodeFixedI64(buf, pload->numOfTimeSeries);
    tlen += taosEncodeFixedI64(buf, pload->totalStorage);
    tlen += taosEncodeFixedI64(buf, pload->compStorage);
    tlen += taosEncodeFixedI64(buf, pload->pointsWritten);
  }

  return tlen;
}

void *tDeserializeSStatusReq(void *buf, SStatusReq *pReq) {
  // status
  buf = taosDecodeFixedI32(buf, &pReq->sver);
  buf = taosDecodeFixedI64(buf, &pReq->dver);
  buf = taosDecodeFixedI32(buf, &pReq->dnodeId);
  buf = taosDecodeFixedI64(buf, &pReq->clusterId);
  buf = taosDecodeFixedI64(buf, &pReq->rebootTime);
  buf = taosDecodeFixedI64(buf, &pReq->updateTime);
  buf = taosDecodeFixedI32(buf, &pReq->numOfCores);
  buf = taosDecodeFixedI32(buf, &pReq->numOfSupportVnodes);
  buf = taosDecodeStringTo(buf, pReq->dnodeEp);

  // cluster cfg
  buf = taosDecodeFixedI32(buf, &pReq->clusterCfg.statusInterval);
  buf = taosDecodeFixedI64(buf, &pReq->clusterCfg.checkTime);
  buf = taosDecodeStringTo(buf, pReq->clusterCfg.timezone);
  buf = taosDecodeStringTo(buf, pReq->clusterCfg.locale);
  buf = taosDecodeStringTo(buf, pReq->clusterCfg.charset);

  // vnode loads
  int32_t vlen = 0;
  buf = taosDecodeFixedI32(buf, &vlen);
  pReq->pVloads = taosArrayInit(vlen, sizeof(SVnodeLoad));
  if (pReq->pVloads == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad vload = {0};
    buf = taosDecodeFixedI32(buf, &vload.vgId);
    buf = taosDecodeFixedI8(buf, &vload.role);
    buf = taosDecodeFixedI64(buf, &vload.numOfTables);
    buf = taosDecodeFixedI64(buf, &vload.numOfTimeSeries);
    buf = taosDecodeFixedI64(buf, &vload.totalStorage);
    buf = taosDecodeFixedI64(buf, &vload.compStorage);
    buf = taosDecodeFixedI64(buf, &vload.pointsWritten);
    if (taosArrayPush(pReq->pVloads, &vload) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
  }

  return buf;
}

int32_t tSerializeSStatusRsp(void **buf, SStatusRsp *pRsp) {
  int32_t tlen = 0;

  // status;
  tlen += taosEncodeFixedI64(buf, pRsp->dver);

  // dnode cfg
  tlen += taosEncodeFixedI32(buf, pRsp->dnodeCfg.dnodeId);
  tlen += taosEncodeFixedI64(buf, pRsp->dnodeCfg.clusterId);

  // dnode eps
  int32_t dlen = (int32_t)taosArrayGetSize(pRsp->pDnodeEps);
  tlen += taosEncodeFixedI32(buf, dlen);
  for (int32_t i = 0; i < dlen; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pRsp->pDnodeEps, i);
    tlen += taosEncodeFixedI32(buf, pDnodeEp->id);
    tlen += taosEncodeFixedI8(buf, pDnodeEp->isMnode);
    tlen += taosEncodeString(buf, pDnodeEp->ep.fqdn);
    tlen += taosEncodeFixedU16(buf, pDnodeEp->ep.port);
  }

  return tlen;
}

void *tDeserializeSStatusRsp(void *buf, SStatusRsp *pRsp) {
  // status
  buf = taosDecodeFixedI64(buf, &pRsp->dver);

  // cluster cfg
  buf = taosDecodeFixedI32(buf, &pRsp->dnodeCfg.dnodeId);
  buf = taosDecodeFixedI64(buf, &pRsp->dnodeCfg.clusterId);

  // dnode eps
  int32_t dlen = 0;
  buf = taosDecodeFixedI32(buf, &dlen);
  pRsp->pDnodeEps = taosArrayInit(dlen, sizeof(SDnodeEp));
  if (pRsp->pDnodeEps == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < dlen; ++i) {
    SDnodeEp dnodeEp = {0};
    buf = taosDecodeFixedI32(buf, &dnodeEp.id);
    buf = taosDecodeFixedI8(buf, &dnodeEp.isMnode);
    buf = taosDecodeStringTo(buf, dnodeEp.ep.fqdn);
    buf = taosDecodeFixedU16(buf, &dnodeEp.ep.port);
    if (taosArrayPush(pRsp->pDnodeEps, &dnodeEp) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
  }

  return buf;
}

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

int32_t tSerializeSMCreateDropQSBNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->dnodeId) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tCoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateDropQSBNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SCoder decoder = {0};
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_DECODER);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pReq->dnodeId) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  return tSerializeSMCreateDropQSBNodeReq(buf, bufLen, (SMCreateQnodeReq *)pReq);
}

int32_t tDeserializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  return tDeserializeSMCreateDropQSBNodeReq(buf, bufLen, (SMCreateQnodeReq *)pReq);
}

int32_t tSerializeSMCreateDropMnodeReq(void *buf, int32_t bufLen, SMCreateMnodeReq *pReq) {
  return tSerializeSMCreateDropQSBNodeReq(buf, bufLen, (SMCreateQnodeReq *)pReq);
}

int32_t tDeserializeSMCreateDropMnodeReq(void *buf, int32_t bufLen, SMCreateMnodeReq *pReq) {
  return tDeserializeSMCreateDropQSBNodeReq(buf, bufLen, (SMCreateQnodeReq *)pReq);
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
  if (tEncodeI8(&encoder, pReq->walLevel) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->precision) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->compression) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->replications) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->quorum) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->update) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->cacheLastRow) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->ignoreExist) < 0) return -1;
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
  if (tDecodeI8(&decoder, &pReq->walLevel) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->precision) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->compression) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->replications) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->quorum) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->update) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->cacheLastRow) < 0) return -1;
  if (tDecodeI8(&decoder, &pReq->ignoreExist) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
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
  if (tEncodeU64(&encoder, pRsp->uid) < 0) return -1;
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
  if (tDecodeU64(&decoder, &pRsp->uid) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

int32_t tSerializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->vgVersion) < 0) return -1;
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
  if (tDecodeI32(&decoder, &pReq->vgVersion) < 0) return -1;
  tEndDecode(&decoder);

  tCoderClear(&decoder);
  return 0;
}

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

static int32_t tSerializeSUseDbRspImp(SCoder *pEncoder, SUseDbRsp *pRsp) {
  if (tEncodeCStr(pEncoder, pRsp->db) < 0) return -1;
  if (tEncodeU64(pEncoder, pRsp->uid) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgVersion) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->vgNum) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->hashMethod) < 0) return -1;

  for (int32_t i = 0; i < pRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(pRsp->pVgroupInfos, i);
    if (tEncodeI32(pEncoder, pVgInfo->vgId) < 0) return -1;
    if (tEncodeU32(pEncoder, pVgInfo->hashBegin) < 0) return -1;
    if (tEncodeU32(pEncoder, pVgInfo->hashEnd) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pVgInfo->epset) < 0) return -1;
  }

  return 0;
}

int32_t tSerializeSUseDbRsp(void *buf, int32_t bufLen, SUseDbRsp *pRsp) {
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
  if (tDecodeU64(pDecoder, &pRsp->uid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->vgVersion) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->vgNum) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->hashMethod) < 0) return -1;

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
    if (tDecodeSEpSet(pDecoder, &vgInfo.epset) < 0) return -1;
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

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(SUseDbBatchRsp));
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

int32_t tSerializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->type) < 0) return -1;
  if (tEncodeCStr(&encoder, pReq->db) < 0) return -1;
  if (tEncodeI32(&encoder, pReq->payloadLen) < 0) return -1;
  if (pReq->payloadLen > 0) {
    if (tEncodeCStr(&encoder, pReq->payload) < 0) return -1;
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
    pReq->payload = malloc(pReq->payloadLen);
    if (pReq->payload == NULL) return -1;
    if (tDecodeCStrTo(&decoder, pReq->payload) < 0) return -1;
  }

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}

void tFreeSShowReq(SShowReq *pReq) { free(pReq->payload); }

int32_t tSerializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SCoder encoder = {0};
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf, bufLen, TD_ENCODER);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pReq->showId) < 0) return -1;
  if (tEncodeI8(&encoder, pReq->free) < 0) return -1;
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
  if (tDecodeI8(&decoder, &pReq->free) < 0) return -1;

  tEndDecode(&decoder);
  tCoderClear(&decoder);
  return 0;
}
