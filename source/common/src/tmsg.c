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

int32_t tSerializeSClientHbReq(void **buf, const SClientHbReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeSClientHbKey(buf, &pReq->connKey);

  int32_t kvNum = taosHashGetSize(pReq->info);
  tlen += taosEncodeFixedI32(buf, kvNum);
  SKv  *kv;
  void *pIter = taosHashIterate(pReq->info, NULL);
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
  for (int32_t i = 0; i < kvNum; i++) {
    SKv kv;
    buf = taosDecodeSKv(buf, &kv);
    taosHashPut(pReq->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  }

  return buf;
}

int32_t tSerializeSClientHbRsp(void **buf, const SClientHbRsp *pRsp) {
  int32_t tlen = 0;
  int32_t kvNum = taosArrayGetSize(pRsp->info);
  tlen += taosEncodeSClientHbKey(buf, &pRsp->connKey);
  tlen += taosEncodeFixedI32(buf, pRsp->status);
  tlen += taosEncodeFixedI32(buf, kvNum);
  for (int32_t i = 0; i < kvNum; i++) {
    SKv *kv = (SKv *)taosArrayGet(pRsp->info, i);
    tlen += taosEncodeSKv(buf, kv);
  }
  return tlen;
}

void *tDeserializeSClientHbRsp(void *buf, SClientHbRsp *pRsp) {
  int32_t kvNum = 0;
  buf = taosDecodeSClientHbKey(buf, &pRsp->connKey);
  buf = taosDecodeFixedI32(buf, &pRsp->status);
  buf = taosDecodeFixedI32(buf, &kvNum);
  pRsp->info = taosArrayInit(kvNum, sizeof(SKv));
  for (int32_t i = 0; i < kvNum; i++) {
    SKv kv = {0};
    buf = taosDecodeSKv(buf, &kv);
    taosArrayPush(pRsp->info, &kv);
  }

  return buf;
}

int32_t tSerializeSClientHbBatchReq(void **buf, const SClientHbBatchReq *pBatchReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pBatchReq->reqId);
  int32_t reqNum = taosArrayGetSize(pBatchReq->reqs);
  tlen += taosEncodeFixedI32(buf, reqNum);
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq *pReq = taosArrayGet(pBatchReq->reqs, i);
    tlen += tSerializeSClientHbReq(buf, pReq);
  }
  return tlen;
}

void *tDeserializeSClientHbBatchReq(void *buf, SClientHbBatchReq *pBatchReq) {
  buf = taosDecodeFixedI64(buf, &pBatchReq->reqId);
  if (pBatchReq->reqs == NULL) {
    pBatchReq->reqs = taosArrayInit(0, sizeof(SClientHbReq));
  }

  int32_t reqNum;
  buf = taosDecodeFixedI32(buf, &reqNum);
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq req = {0};
    buf = tDeserializeSClientHbReq(buf, &req);
    taosArrayPush(pBatchReq->reqs, &req);
  }
  return buf;
}

int32_t tSerializeSClientHbBatchRsp(void **buf, const SClientHbBatchRsp *pBatchRsp) {
  int32_t tlen = 0;
  int32_t sz = taosArrayGetSize(pBatchRsp->rsps);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SClientHbRsp *pRsp = taosArrayGet(pBatchRsp->rsps, i);
    tlen += tSerializeSClientHbRsp(buf, pRsp);
  }
  return tlen;
}

void *tDeserializeSClientHbBatchRsp(void *buf, SClientHbBatchRsp *pBatchRsp) {
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pBatchRsp->rsps = taosArrayInit(sz, sizeof(SClientHbRsp));
  for (int32_t i = 0; i < sz; i++) {
    SClientHbRsp rsp = {0};
    buf = tDeserializeSClientHbRsp(buf, &rsp);
    taosArrayPush(pBatchRsp->rsps, &rsp);
  }
  return buf;
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

int32_t tSerializeSCreateAcctReq(void **buf, SCreateAcctReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pReq->user);
  tlen += taosEncodeString(buf, pReq->pass);
  tlen += taosEncodeFixedI32(buf, pReq->maxUsers);
  tlen += taosEncodeFixedI32(buf, pReq->maxDbs);
  tlen += taosEncodeFixedI32(buf, pReq->maxTimeSeries);
  tlen += taosEncodeFixedI32(buf, pReq->maxStreams);
  tlen += taosEncodeFixedI32(buf, pReq->accessState);
  tlen += taosEncodeFixedI64(buf, pReq->maxStorage);
  return tlen;
}

void *tDeserializeSCreateAcctReq(void *buf, SCreateAcctReq *pReq) {
  buf = taosDecodeStringTo(buf, pReq->user);
  buf = taosDecodeStringTo(buf, pReq->pass);
  buf = taosDecodeFixedI32(buf, &pReq->maxUsers);
  buf = taosDecodeFixedI32(buf, &pReq->maxDbs);
  buf = taosDecodeFixedI32(buf, &pReq->maxTimeSeries);
  buf = taosDecodeFixedI32(buf, &pReq->maxStreams);
  buf = taosDecodeFixedI32(buf, &pReq->accessState);
  buf = taosDecodeFixedI64(buf, &pReq->maxStorage);
  return buf;
}

int32_t tSerializeSDropUserReq(void **buf, SDropUserReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pReq->user);
  return tlen;
}

void *tDeserializeSDropUserReq(void *buf, SDropUserReq *pReq) {
  buf = taosDecodeStringTo(buf, pReq->user);
  return buf;
}

int32_t tSerializeSCreateUserReq(void **buf, SCreateUserReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pReq->createType);
  tlen += taosEncodeFixedI8(buf, pReq->superUser);
  tlen += taosEncodeString(buf, pReq->user);
  tlen += taosEncodeString(buf, pReq->pass);
  return tlen;
}

void *tDeserializeSCreateUserReq(void *buf, SCreateUserReq *pReq) {
  buf = taosDecodeFixedI8(buf, &pReq->createType);
  buf = taosDecodeFixedI8(buf, &pReq->superUser);
  buf = taosDecodeStringTo(buf, pReq->user);
  buf = taosDecodeStringTo(buf, pReq->pass);
  return buf;
}

int32_t tSerializeSAlterUserReq(void **buf, SAlterUserReq *pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pReq->alterType);
  tlen += taosEncodeString(buf, pReq->user);
  tlen += taosEncodeString(buf, pReq->pass);
  tlen += taosEncodeString(buf, pReq->dbname);
  tlen += taosEncodeFixedI8(buf, pReq->superUser);
  return tlen;
}

void *tDeserializeSAlterUserReq(void *buf, SAlterUserReq *pReq) {
  buf = taosDecodeFixedI8(buf, &pReq->alterType);
  buf = taosDecodeStringTo(buf, pReq->user);
  buf = taosDecodeStringTo(buf, pReq->pass);
  buf = taosDecodeStringTo(buf, pReq->dbname);
  buf = taosDecodeFixedI8(buf, &pReq->superUser);
  return buf;
}