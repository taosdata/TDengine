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

#include "encode.h"
#include "freelist.h"

#define CODER_NODE_FIELDS \
  uint8_t* data;          \
  int32_t  size;          \
  int32_t  pos;

struct SCoderNode {
  TD_SLIST_NODE(SCoderNode);
  CODER_NODE_FIELDS
};

typedef struct {
  td_endian_t endian;
  SFreeList   fl;
  CODER_NODE_FIELDS
  TD_SLIST(SCoderNode) stack;
} SCoder;

bool tDecodeIsEnd(SCoder* pCoder) { return (pCoder->size == pCoder->pos); }

void tCoderInit(SCoder* pCoder, td_endian_t endian, uint8_t* data, int32_t size) {
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
    free(pNode);
  }
}

int tStartEncode(SCoder* pCoder) {
  struct SCoderNode* pNode;

  if (pCoder->data) {
    if (pCoder->size - pCoder->pos < sizeof(int32_t)) return -1;

    pNode = malloc(sizeof(*pNode));
    if (pNode == NULL) return -1;

    pNode->data = pCoder->data;
    pNode->pos = pCoder->pos;
    pNode->size = pCoder->size;

    pCoder->data = pNode->data + sizeof(int32_t);
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

  if (pCoder->data) {
    pNode = TD_SLIST_HEAD(&(pCoder->stack));
    ASSERT(pNode);
    TD_SLIST_POP(&(pCoder->stack));

    // TODO: tEncodeI32(pNode, pCoder->pos);

    pCoder->data = pNode->data;
    pCoder->size = pNode->size;
    pCoder->pos = pNode->pos + pCoder->pos;

    free(pNode);
  }
}

int tStartDecode(SCoder* pCoder) {
  int32_t            size;
  struct SCoderNode* pNode;

  // TODO: if (tDecodeI32(pCoder, &size) < 0) return -1;

  pNode = malloc(sizeof(*pNode));
  if (pNode == NULL) return -1;

  pNode->data = pCoder->data;
  pNode->pos = pCoder->pos;
  pNode->size = pCoder->size;

  pCoder->data = pCoder->data;
  pCoder->size = size;
  pCoder->pos = 0;

  TD_SLIST_PUSH(&(pCoder->stack), pNode);

  return 0;
}

void tEndDecode(SCoder* pCoder) {
  ASSERT(tDecodeIsEnd(pCoder));

  struct SCoderNode* pNode;

  pNode = TD_SLIST_HEAD(&(pCoder->stack));
  ASSERT(pNode);
  TD_SLIST_POP(&(pCoder->stack));

  pCoder->data = pNode->data;
  pCoder->size = pNode->size;
  pCoder->pos = pCoder->pos + pNode->pos;

  free(pNode);
}
