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
#include "tencode.h"

#if __STDC_VERSION__ >= 201112LL
static_assert(sizeof(float) == sizeof(uint32_t), "sizeof(float) must equal to sizeof(uint32_t)");
static_assert(sizeof(double) == sizeof(uint64_t), "sizeof(double) must equal to sizeof(uint64_t)");
#endif

struct SEncoderNode {
  SEncoderNode* pNext;
  uint8_t*      data;
  uint32_t      size;
  uint32_t      pos;
};

struct SDecoderNode {
  SDecoderNode* pNext;
  uint8_t*      data;
  uint32_t      size;
  uint32_t      pos;
};

void tEncoderInit(SEncoder* pEncoder, uint8_t* data, uint32_t size) {
  if (data == NULL) size = 0;
  pEncoder->data = data;
  pEncoder->size = size;
  pEncoder->pos = 0;
  pEncoder->mList = NULL;
  pEncoder->eStack = NULL;
}

void tEncoderClear(SEncoder* pCoder) {
  for (SCoderMem* pMem = pCoder->mList; pMem; pMem = pCoder->mList) {
    pCoder->mList = pMem->next;
    taosMemoryFree(pMem);
  }
  memset(pCoder, 0, sizeof(*pCoder));
}

void tDecoderInit(SDecoder* pDecoder, uint8_t* data, uint32_t size) {
  pDecoder->data = data;
  pDecoder->size = size;
  pDecoder->pos = 0;
  pDecoder->mList = NULL;
  pDecoder->dStack = NULL;
}

void tDecoderClear(SDecoder* pCoder) {
  for (SCoderMem* pMem = pCoder->mList; pMem; pMem = pCoder->mList) {
    pCoder->mList = pMem->next;
    taosMemoryFree(pMem);
  }
  memset(pCoder, 0, sizeof(*pCoder));
}

int32_t tStartEncode(SEncoder* pCoder) {
  SEncoderNode* pNode;

  if (pCoder->data) {
    if (pCoder->size - pCoder->pos < sizeof(int32_t)) return -1;

    pNode = tEncoderMalloc(pCoder, sizeof(*pNode));
    if (pNode == NULL) return -1;

    pNode->data = pCoder->data;
    pNode->pos = pCoder->pos;
    pNode->size = pCoder->size;

    pCoder->data = pNode->data + pNode->pos + sizeof(int32_t);
    pCoder->pos = 0;
    pCoder->size = pNode->size - pNode->pos - sizeof(int32_t);

    pNode->pNext = pCoder->eStack;
    pCoder->eStack = pNode;
  } else {
    pCoder->pos += sizeof(int32_t);
  }

  return 0;
}

void tEndEncode(SEncoder* pCoder) {
  SEncoderNode* pNode;
  int32_t       len;

  if (pCoder->data) {
    pNode = pCoder->eStack;
    ASSERT(pNode);
    pCoder->eStack = pNode->pNext;

    len = pCoder->pos;

    pCoder->data = pNode->data;
    pCoder->size = pNode->size;
    pCoder->pos = pNode->pos;

    (void)tEncodeI32(pCoder, len);

    TD_CODER_MOVE_POS(pCoder, len);
  }
}

int32_t tStartDecode(SDecoder* pCoder) {
  SDecoderNode* pNode;
  int32_t       len;

  if (tDecodeI32(pCoder, &len) < 0) return -1;

  pNode = tDecoderMalloc(pCoder, sizeof(*pNode));
  if (pNode == NULL) return -1;

  pNode->data = pCoder->data;
  pNode->pos = pCoder->pos;
  pNode->size = pCoder->size;

  pCoder->data = pNode->data + pNode->pos;
  pCoder->size = len;
  pCoder->pos = 0;

  pNode->pNext = pCoder->dStack;
  pCoder->dStack = pNode;

  return 0;
}

void tEndDecode(SDecoder* pCoder) {
  SDecoderNode* pNode;

  pNode = pCoder->dStack;
  ASSERT(pNode);
  pCoder->dStack = pNode->pNext;

  pCoder->data = pNode->data;
  pCoder->pos = pCoder->size + pNode->pos;
  pCoder->size = pNode->size;
}
