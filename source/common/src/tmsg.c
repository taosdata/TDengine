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

static int  tmsgStartEncode(SMsgEncoder *pME);
static void tmsgEndEncode(SMsgEncoder *pME);
static int  tmsgStartDecode(SMsgDecoder *pMD);
static void tmsgEndDecode(SMsgDecoder *pMD);

/* ------------------------ ENCODE/DECODE FUNCTIONS ------------------------ */
void tmsgInitMsgEncoder(SMsgEncoder *pME, td_endian_t endian, uint8_t *data, int64_t size) {
  tInitEncoder(&(pME->coder), endian, data, size);
  TD_SLIST_INIT(&(pME->eStack));
}

void tmsgClearMsgEncoder(SMsgEncoder *pME) {
  struct SMEListNode *pNode;
  for (;;) {
    pNode = TD_SLIST_HEAD(&(pME->eStack));
    if (TD_IS_NULL(pNode)) break;
    TD_SLIST_POP(&(pME->eStack));
    free(pNode);
  }
}

void tmsgInitMsgDecoder(SMsgDecoder *pMD, td_endian_t endian, uint8_t *data, int64_t size) {
  tInitDecoder(&pMD->coder, endian, data, size);
  TD_SLIST_INIT(&(pMD->dStack));
  TD_SLIST_INIT(&(pMD->freeList));
}

void tmsgClearMsgDecoder(SMsgDecoder *pMD) {
  {
    struct SMDFreeListNode *pNode;
    for (;;) {
      pNode = TD_SLIST_HEAD(&(pMD->freeList));
      if (TD_IS_NULL(pNode)) break;
      TD_SLIST_POP(&(pMD->freeList));
      free(pNode);
    }
  }
  {
    struct SMDListNode *pNode;
    for (;;) {
      pNode = TD_SLIST_HEAD(&(pMD->dStack));
      if (TD_IS_NULL(pNode)) break;
      TD_SLIST_POP(&(pMD->dStack));
      free(pNode);
    }
  }
}

/* ------------------------ MESSAGE ENCODE/DECODE ------------------------ */
int tmsgSVCreateTbReqEncode(SMsgEncoder *pCoder, SVCreateTbReq *pReq) {
  tmsgStartEncode(pCoder);
  // TODO

  tmsgEndEncode(pCoder);
  return 0;
}

int tmsgSVCreateTbReqDecode(SMsgDecoder *pCoder, SVCreateTbReq *pReq) {
  tmsgStartDecode(pCoder);

  // TODO: decode

  // Decode is not end
  if (pCoder->coder.pos != pCoder->coder.size) {
    // Continue decode
  }

  tmsgEndDecode(pCoder);
  return 0;
}

int tSerializeSVCreateTbReq(void **buf, const SVCreateTbReq *pReq) {
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

/* ------------------------ STATIC METHODS ------------------------ */
static int tmsgStartEncode(SMsgEncoder *pME) {
  struct SMEListNode *pNode = (struct SMEListNode *)malloc(sizeof(*pNode));
  if (TD_IS_NULL(pNode)) return -1;

  pNode->coder = pME->coder;
  TD_SLIST_PUSH(&(pME->eStack), pNode);
  TD_CODER_MOVE_POS(&(pME->coder), sizeof(int32_t));

  return 0;
}

static void tmsgEndEncode(SMsgEncoder *pME) {
  int32_t             size;
  struct SMEListNode *pNode;

  pNode = TD_SLIST_HEAD(&(pME->eStack));
  ASSERT(pNode);
  TD_SLIST_POP(&(pME->eStack));

  size = pME->coder.pos - pNode->coder.pos;
  tEncodeI32(&(pNode->coder), size);

  free(pNode);
}

static int tmsgStartDecode(SMsgDecoder *pMD) {
  struct SMDListNode *pNode;
  int32_t             size;

  pNode = (struct SMDListNode *)malloc(sizeof(*pNode));
  if (pNode == NULL) return -1;

  tDecodeI32(&(pMD->coder), &size);

  pNode->coder = pMD->coder;
  TD_SLIST_PUSH(&(pMD->dStack), pNode);

  pMD->coder.pos = 0;
  pMD->coder.size = size - sizeof(int32_t);
  pMD->coder.data = TD_CODER_CURRENT(&(pNode->coder));

  return 0;
}

static void tmsgEndDecode(SMsgDecoder *pMD) {
  ASSERT(pMD->coder.pos == pMD->coder.size);
  struct SMDListNode *pNode;

  pNode = TD_SLIST_HEAD(&(pMD->dStack));
  ASSERT(pNode);
  TD_SLIST_POP(&(pMD->dStack));

  pNode->coder.pos += pMD->coder.size;

  pMD->coder = pNode->coder;

  free(pNode);
}