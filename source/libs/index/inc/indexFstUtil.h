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

#ifndef __INDEX_FST_UTIL_H__
#define __INDEX_FST_UTIL_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "indexFstCommon.h"
#include "indexInt.h"

typedef uint64_t FstType;
typedef uint64_t CompiledAddr;
typedef uint64_t Output;
typedef uint8_t  PackSizes;

// A sentinel value used to indicate an empty final state
extern const CompiledAddr EMPTY_ADDRESS;
/// A sentinel value used to indicate an invalid state.
extern const CompiledAddr NONE_ADDRESS;

// This version number is written to every finite state transducer created by
// this version When a finite state transducer is read, its version number is
// checked against this value.
extern const uint64_t VERSION;
// The threshold (in number of transitions) at which an index is created for
// a node's transitions. This speeds up lookup time at the expense of FST size

extern const uint64_t TRANS_INDEX_THRESHOLD;
// high 4 bits is transition address packed size.
// low 4 bits is output value packed size.
//
// `0` is a legal value which means there are no transitions/outputs

#define FST_SET_TRANSITION_PACK_SIZE(v, sz) \
  do {                                      \
    v = (v & 0b00001111) | (sz << 4);       \
  } while (0)
#define FST_GET_TRANSITION_PACK_SIZE(v) (((v)&0b11110000) >> 4)
#define FST_SET_OUTPUT_PACK_SIZE(v, sz) \
  do {                                  \
    v = (v & 0b11110000) | sz;          \
  } while (0)
#define FST_GET_OUTPUT_PACK_SIZE(v) ((v)&0b00001111)

#define COMMON_INPUT(idx) COMMON_INPUTS_INV[(idx)-1]

#define COMMON_INDEX(v, max, val)                 \
  do {                                            \
    val = ((uint16_t)COMMON_INPUTS[v] + 1) % 256; \
    val = val > max ? 0 : val;                    \
  } while (0)

// uint8_t commonInput(uint8_t idx);
// uint8_t commonIdx(uint8_t v, uint8_t max);

uint8_t      packSize(uint64_t n);
uint64_t     unpackUint64(uint8_t* ch, uint8_t sz);
uint8_t      packDeltaSize(CompiledAddr nodeAddr, CompiledAddr transAddr);
CompiledAddr unpackDelta(char* data, uint64_t len, uint64_t nodeAddr);

typedef struct FstString {
  uint8_t* data;
  uint32_t len;
  int32_t  ref;
} FstString;

typedef struct FstSlice {
  FstString* str;
  int32_t    start;
  int32_t    end;
} FstSlice;

FstSlice fstSliceCreate(uint8_t* data, uint64_t len);
FstSlice fstSliceCopy(FstSlice* s, int32_t start, int32_t end);
FstSlice fstSliceDeepCopy(FstSlice* s, int32_t start, int32_t end);
bool     fstSliceIsEmpty(FstSlice* s);
int      fstSliceCompare(FstSlice* s1, FstSlice* s2);
void     fstSliceDestroy(FstSlice* s);
uint8_t* fstSliceData(FstSlice* s, int32_t* sz);

#define FST_SLICE_LEN(s) (s->end - s->start + 1)

//// stack
//
// typedef (*StackFreeElemFn)(void *elem);
//
// typedef struct FstStack {
//  void   *first;
//  void   *end;
//  size_t elemSize;
//  size_t nElem;
//  StackFreeElemFn fn;
//} FstStack;
//
//
// FstStack* fstStackCreate(size_t elemSize, stackFreeElem);
// void  *fstStackPush(FstStack *s, void *elem);
// void  *fstStackTop(FstStack *s);
// size_t fstStackLen(FstStack *s);
// void  fstStackDestory(FstStack *);
//

#ifdef __cplusplus
}
#endif

#endif
