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
#include "index_fst_util.h"



//A sentinel value used to indicate an empty final state
const CompiledAddr EMPTY_ADDRESS  = 0;
/// A sentinel value used to indicate an invalid state.
const CompiledAddr NONE_ADDRESS   = 1;

// This version number is written to every finite state transducer created by
// this crate. When a finite state transducer is read, its version number is
// checked against this value.
const uint64_t    version        = 3;
// The threshold (in number of transitions) at which an index is created for                                   
// a node's transitions. This speeds up lookup time at the expense of FST size 

const uint64_t TRANS_INDEX_THRESHOLD = 32;


//uint8_t commonInput(uint8_t idx) {
//  if (idx == 0) { return -1; }
//  else {
//    return COMMON_INPUTS_INV[idx - 1];     
//  }
//} 
//
//uint8_t commonIdx(uint8_t v, uint8_t max) {
//  uint8_t v = ((uint16_t)tCOMMON_INPUTS[v] + 1)%256;
//  return v > max ? 0: v;
//}



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

uint64_t unpackUint64(uint8_t *ch, uint8_t sz) {
  uint64_t n;
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
CompiledAddr unpackDelta(char *data, uint64_t len, uint64_t nodeAddr) {
  uint64_t delta = unpackUint64(data, len); 
  // delta_add = u64_to_usize
  if (delta == EMPTY_ADDRESS) {
    return EMPTY_ADDRESS;
  } else {
    return nodeAddr - delta;
  }
}

// fst slice func
FstSlice fstSliceCreate(uint8_t *data, uint64_t dLen) {
  FstSlice slice = {.data = data, .dLen = dLen, .start = 0, .end = dLen - 1};
  return slice;
} 
FstSlice fstSliceCopy(FstSlice *slice, uint32_t start, uint32_t end) {
  FstSlice t;
  if (start >= slice->dLen || end >= slice->dLen || start > end) {
    t.data = NULL;
    return t;
  };
   
  t.data  = slice->data;
  t.dLen  = slice->dLen;
  t.start = start; 
  t.end   = end; 
  return t;
}
bool fstSliceEmpty(FstSlice *slice) {
  return slice->data == NULL || slice->dLen <= 0;
}



