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

int tSerializeSClientHbReq(void **buf, const SClientHbReq *pReq) {
  int tlen = 0;
  tlen += taosEncodeSClientHbKey(buf, &pReq->connKey);

  SKlv  klv;
  void* pIter = taosHashIterate(pReq->info, pIter);
  while (pIter != NULL) {
    taosHashGetKey(pIter, &klv.key, (size_t *)&klv.keyLen);
    klv.valueLen = taosHashGetDataLen(pIter);
    klv.value = pIter;
    taosEncodeSKlv(buf, &klv);

    pIter = taosHashIterate(pReq->info, pIter);
  }
  return tlen;
}

void *tDeserializeClientHbReq(void *buf, SClientHbReq *pReq) {
  ASSERT(pReq->info != NULL);
  buf = taosDecodeSClientHbKey(buf, &pReq->connKey);

  // TODO: error handling
  if (pReq->info == NULL) {
    pReq->info = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  SKlv klv;
  buf = taosDecodeSKlv(buf, &klv);
  taosHashPut(pReq->info, klv.key, klv.keyLen, klv.value, klv.valueLen);

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
