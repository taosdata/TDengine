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
#include "indexFstUtil.h"
#include "indexFstCommon.h"

// A sentinel value used to indicate an empty final state
const CompiledAddr EMPTY_ADDRESS = 0;
/// A sentinel value used to indicate an invalid state.
const CompiledAddr NONE_ADDRESS = 1;

// This version number is written to every finite state transducer created by
// this version. When a finite state transducer is read, its version number is
// checked against this value.
const uint64_t VERSION = 3;

// The threshold (in number of transitions) at which an index is created for
// a node's transitions. This speeds up lookup time at the expense of FST size
const uint64_t TRANS_INDEX_THRESHOLD = 32;

uint8_t packSize(uint64_t n) {
  if (n < (1u << 8)) {
    return 1;
  } else if (n < (1u << 16)) {
    return 2;
  } else if (n < (1u << 24)) {
    return 3;
  } else if (n < ((uint64_t)(1) << 32)) {
    return 4;
  } else if (n < ((uint64_t)(1) << 40)) {
    return 5;
  } else if (n < ((uint64_t)(1) << 48)) {
    return 6;
  } else if (n < ((uint64_t)(1) << 56)) {
    return 7;
  } else {
    return 8;
  }
}

uint64_t unpackUint64(uint8_t* ch, uint8_t sz) {
  uint64_t n = 0;
  for (uint8_t i = 0; i < sz; i++) {
    n = n | (ch[i] << (8 * i));
  }
  return n;
}
uint8_t packDeltaSize(CompiledAddr nodeAddr, CompiledAddr transAddr) {
  if (transAddr == EMPTY_ADDRESS) {
    return packSize(EMPTY_ADDRESS);
  } else {
    return packSize(nodeAddr - transAddr);
  }
}
CompiledAddr unpackDelta(char* data, uint64_t len, uint64_t nodeAddr) {
  uint64_t delta = unpackUint64(data, len);
  // delta_add = u64_to_usize
  if (delta == EMPTY_ADDRESS) {
    return EMPTY_ADDRESS;
  } else {
    return nodeAddr - delta;
  }
}

// fst slice func

FstSlice fstSliceCreate(uint8_t* data, uint64_t len) {
  FstString* str = (FstString*)taosMemoryMalloc(sizeof(FstString));
  str->ref = 1;
  str->len = len;
  str->data = taosMemoryMalloc(len * sizeof(uint8_t));

  if (data != NULL) {
    memcpy(str->data, data, len);
  }

  FstSlice s = {.str = str, .start = 0, .end = len - 1};
  return s;
}
// just shallow copy
FstSlice fstSliceCopy(FstSlice* s, int32_t start, int32_t end) {
  FstString* str = s->str;
  atomic_add_fetch_32(&str->ref, 1);

  FstSlice t = {.str = str, .start = start + s->start, .end = end + s->start};
  return t;
}
FstSlice fstSliceDeepCopy(FstSlice* s, int32_t start, int32_t end) {
  int32_t tlen = end - start + 1;
  int32_t slen;

  uint8_t* data = fstSliceData(s, &slen);

  uint8_t* buf = taosMemoryMalloc(sizeof(uint8_t) * tlen);
  memcpy(buf, data + start, tlen);

  FstString* str = taosMemoryMalloc(sizeof(FstString));
  str->data = buf;
  str->len = tlen;
  str->ref = 1;

  FstSlice ans;
  ans.str = str;
  ans.start = 0;
  ans.end = tlen - 1;
  return ans;
}
bool fstSliceIsEmpty(FstSlice* s) { return s->str == NULL || s->str->len == 0 || s->start < 0 || s->end < 0; }

uint8_t* fstSliceData(FstSlice* s, int32_t* size) {
  FstString* str = s->str;
  if (size != NULL) {
    *size = s->end - s->start + 1;
  }
  return str->data + s->start;
}
void fstSliceDestroy(FstSlice* s) {
  FstString* str = s->str;

  int32_t ref = atomic_sub_fetch_32(&str->ref, 1);
  if (ref == 0) {
    taosMemoryFree(str->data);
    taosMemoryFree(str);
    s->str = NULL;
  }
}

int fstSliceCompare(FstSlice* a, FstSlice* b) {
  int32_t  alen, blen;
  uint8_t* aBuf = fstSliceData(a, &alen);
  uint8_t* bBuf = fstSliceData(b, &blen);

  uint32_t i, j;
  for (i = 0, j = 0; i < alen && j < blen; i++, j++) {
    uint8_t x = aBuf[i];
    uint8_t y = bBuf[j];
    if (x == y) {
      continue;
    } else if (x < y) {
      return -1;
    } else {
      return 1;
    };
  }
  if (i < alen) {
    return 1;
  } else if (j < blen) {
    return -1;
  } else {
    return 0;
  }
}
