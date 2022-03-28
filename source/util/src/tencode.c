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

#if __STDC_VERSION__ >= 201112L
static_assert(sizeof(float) == sizeof(uint32_t), "sizeof(float) must equal to sizeof(uint32_t)");
static_assert(sizeof(double) == sizeof(uint64_t), "sizeof(double) must equal to sizeof(uint64_t)");
#endif

void tCoderInit(SCoder* pCoder, td_endian_t endian, uint8_t* data, int32_t size, td_coder_t type) {
  if (type == TD_ENCODER) {
    if (data == NULL) size = 0;
  } else {
    ASSERT(data && size > 0);
  }

  pCoder->type = type;
  pCoder->endian = endian;
  pCoder->data = data;
  pCoder->size = size;
  pCoder->pos = 0;
  tFreeListInit(&(pCoder->fl));
  TD_SLIST_INIT(&(pCoder->stack));
}

void tCoderClear(SCoder* pCoder) {
  tFreeListClear(&(pCoder->fl));
  struct SCoderNode* pNode;
  for (;;) {
    pNode = TD_SLIST_HEAD(&(pCoder->stack));
    if (pNode == NULL) break;
    TD_SLIST_POP(&(pCoder->stack));
    taosMemoryFree(pNode);
  }
}

int32_t tStartEncode(SCoder* pCoder) {
  struct SCoderNode* pNode;

  ASSERT(pCoder->type == TD_ENCODER);
  if (pCoder->data) {
    if (pCoder->size - pCoder->pos < sizeof(int32_t)) return -1;

    pNode = taosMemoryMalloc(sizeof(*pNode));
    if (pNode == NULL) return -1;

    pNode->data = pCoder->data;
    pNode->pos = pCoder->pos;
    pNode->size = pCoder->size;

    pCoder->data = pNode->data + pNode->pos + sizeof(int32_t);
    pCoder->pos = 0;
    pCoder->size = pNode->size - pNode->pos - sizeof(int32_t);

    TD_SLIST_PUSH(&(pCoder->stack), pNode);
  } else {
    pCoder->pos += sizeof(int32_t);
  }
  return 0;
}

void tEndEncode(SCoder* pCoder) {
  struct SCoderNode* pNode;
  int32_t            len;

  ASSERT(pCoder->type == TD_ENCODER);
  if (pCoder->data) {
    pNode = TD_SLIST_HEAD(&(pCoder->stack));
    ASSERT(pNode);
    TD_SLIST_POP(&(pCoder->stack));

    len = pCoder->pos;

    pCoder->data = pNode->data;
    pCoder->size = pNode->size;
    pCoder->pos = pNode->pos;

    tEncodeI32(pCoder, len);

    TD_CODER_MOVE_POS(pCoder, len);

    taosMemoryFree(pNode);
  }
}

int32_t tStartDecode(SCoder* pCoder) {
  int32_t            len;
  struct SCoderNode* pNode;

  ASSERT(pCoder->type == TD_DECODER);
  if (tDecodeI32(pCoder, &len) < 0) return -1;

  pNode = taosMemoryMalloc(sizeof(*pNode));
  if (pNode == NULL) return -1;

  pNode->data = pCoder->data;
  pNode->pos = pCoder->pos;
  pNode->size = pCoder->size;

  pCoder->data = pNode->data + pNode->pos;
  pCoder->size = len;
  pCoder->pos = 0;

  TD_SLIST_PUSH(&(pCoder->stack), pNode);

  return 0;
}

void tEndDecode(SCoder* pCoder) {
  struct SCoderNode* pNode;

  ASSERT(pCoder->type == TD_DECODER);

  pNode = TD_SLIST_HEAD(&(pCoder->stack));
  ASSERT(pNode);
  TD_SLIST_POP(&(pCoder->stack));

  pCoder->data = pNode->data;
  pCoder->pos = pCoder->size + pNode->pos;
  pCoder->size = pNode->size;

  taosMemoryFree(pNode);
}
