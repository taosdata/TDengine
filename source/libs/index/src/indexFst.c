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

#include "indexFst.h"
#include "indexFstAutomation.h"
#include "indexInt.h"
#include "tchecksum.h"
#include "tcoding.h"

static FORCE_INLINE void fstPackDeltaIn(IdxFstFile* wrt, CompiledAddr nodeAddr, CompiledAddr transAddr,
                                        uint8_t nBytes) {
  CompiledAddr deltaAddr = (transAddr == EMPTY_ADDRESS) ? EMPTY_ADDRESS : nodeAddr - transAddr;
  idxFilePackUintIn(wrt, deltaAddr, nBytes);
}
static FORCE_INLINE uint8_t fstPackDelta(IdxFstFile* wrt, CompiledAddr nodeAddr, CompiledAddr transAddr) {
  uint8_t nBytes = packDeltaSize(nodeAddr, transAddr);
  fstPackDeltaIn(wrt, nodeAddr, transAddr, nBytes);
  return nBytes;
}

FstUnFinishedNodes* fstUnFinishedNodesCreate() {
  FstUnFinishedNodes* nodes = taosMemoryMalloc(sizeof(FstUnFinishedNodes));
  if (nodes == NULL) {
    return NULL;
  }

  nodes->stack = (SArray*)taosArrayInit(64, sizeof(FstBuilderNodeUnfinished));
  fstUnFinishedNodesPushEmpty(nodes, false);
  return nodes;
}
static void FORCE_INLINE unFinishedNodeDestroyElem(void* elem) {
  FstBuilderNodeUnfinished* b = (FstBuilderNodeUnfinished*)elem;
  fstBuilderNodeDestroy(b->node);
  taosMemoryFree(b->last);
  b->last = NULL;
}
void fstUnFinishedNodesDestroy(FstUnFinishedNodes* nodes) {
  if (nodes == NULL) {
    return;
  }

  taosArrayDestroyEx(nodes->stack, unFinishedNodeDestroyElem);
  taosMemoryFree(nodes);
}

void fstUnFinishedNodesPushEmpty(FstUnFinishedNodes* nodes, bool isFinal) {
  FstBuilderNode* node = taosMemoryMalloc(sizeof(FstBuilderNode));
  node->isFinal = isFinal;
  node->finalOutput = 0;
  node->trans = taosArrayInit(16, sizeof(FstTransition));

  FstBuilderNodeUnfinished un = {.node = node, .last = NULL};
  taosArrayPush(nodes->stack, &un);
}
FstBuilderNode* fstUnFinishedNodesPopRoot(FstUnFinishedNodes* nodes) {
  FstBuilderNodeUnfinished* un = taosArrayPop(nodes->stack);
  return un->node;
}

FstBuilderNode* fstUnFinishedNodesPopFreeze(FstUnFinishedNodes* nodes, CompiledAddr addr) {
  FstBuilderNodeUnfinished* un = taosArrayPop(nodes->stack);
  fstBuilderNodeUnfinishedLastCompiled(un, addr);
  // taosMemoryFree(un->last); // TODO add func FstLastTransitionFree()
  // un->last = NULL;
  return un->node;
}

FstBuilderNode* fstUnFinishedNodesPopEmpty(FstUnFinishedNodes* nodes) {
  FstBuilderNodeUnfinished* un = taosArrayPop(nodes->stack);
  return un->node;
}
void fstUnFinishedNodesSetRootOutput(FstUnFinishedNodes* nodes, Output out) {
  FstBuilderNodeUnfinished* un = taosArrayGet(nodes->stack, 0);
  un->node->isFinal = true;
  un->node->finalOutput = out;
  // un->node->trans       = NULL;
}
void fstUnFinishedNodesTopLastFreeze(FstUnFinishedNodes* nodes, CompiledAddr addr) {
  FstBuilderNodeUnfinished* un = taosArrayGet(nodes->stack, taosArrayGetSize(nodes->stack) - 1);
  fstBuilderNodeUnfinishedLastCompiled(un, addr);
}
void fstUnFinishedNodesAddSuffix(FstUnFinishedNodes* nodes, FstSlice bs, Output out) {
  FstSlice* s = &bs;
  if (fstSliceIsEmpty(s)) {
    return;
  }
  int32_t                   sz = taosArrayGetSize(nodes->stack) - 1;
  FstBuilderNodeUnfinished* un = taosArrayGet(nodes->stack, sz);
  ASSERTS(un->last == NULL, "index-fst meet unexpected node");
  if (un->last != NULL) return;

  // FstLastTransition *trn = taosMemoryMalloc(sizeof(FstLastTransition));
  // trn->inp = s->data[s->start];
  // trn->out = out;
  int32_t  len = 0;
  uint8_t* data = fstSliceData(s, &len);
  un->last = fstLastTransitionCreate(data[0], out);

  for (uint64_t i = 1; i < len; i++) {
    FstBuilderNode* n = taosMemoryMalloc(sizeof(FstBuilderNode));
    n->isFinal = false;
    n->finalOutput = 0;
    n->trans = taosArrayInit(16, sizeof(FstTransition));

    // FstLastTransition *trn = taosMemoryMalloc(sizeof(FstLastTransition));
    // trn->inp = s->data[i];
    // trn->out = out;
    FstLastTransition* trn = fstLastTransitionCreate(data[i], 0);

    FstBuilderNodeUnfinished un = {.node = n, .last = trn};
    taosArrayPush(nodes->stack, &un);
  }
  fstUnFinishedNodesPushEmpty(nodes, true);
}

uint64_t fstUnFinishedNodesFindCommPrefix(FstUnFinishedNodes* node, FstSlice bs) {
  FstSlice* s = &bs;

  int32_t  ssz = taosArrayGetSize(node->stack);  // stack size
  uint64_t count = 0;
  int32_t  lsz;  // data len
  uint8_t* data = fstSliceData(s, &lsz);
  for (int32_t i = 0; i < ssz && i < lsz; i++) {
    FstBuilderNodeUnfinished* un = taosArrayGet(node->stack, i);
    if (un->last->inp == data[i]) {
      count++;
    } else {
      break;
    }
  }
  return count;
}
uint64_t fstUnFinishedNodesFindCommPrefixAndSetOutput(FstUnFinishedNodes* node, FstSlice bs, Output in, Output* out) {
  FstSlice* s = &bs;

  int32_t lsz = (size_t)(s->end - s->start + 1);  // data len
  int32_t ssz = taosArrayGetSize(node->stack);    // stack size
  *out = in;
  uint64_t i = 0;
  for (i = 0; i < lsz && i < ssz; i++) {
    FstBuilderNodeUnfinished* un = taosArrayGet(node->stack, i);

    FstLastTransition* t = un->last;
    uint64_t           addPrefix = 0;
    uint8_t*           data = fstSliceData(s, NULL);
    if (t && t->inp == data[i]) {
      uint64_t commPrefix = TMIN(t->out, *out);
      uint64_t tAddPrefix = t->out - commPrefix;
      (*out) = (*out) - commPrefix;
      t->out = commPrefix;
      addPrefix = tAddPrefix;
    } else {
      break;
    }
    if (addPrefix != 0) {
      if (i + 1 < ssz) {
        FstBuilderNodeUnfinished* unf = taosArrayGet(node->stack, i + 1);
        fstBuilderNodeUnfinishedAddOutputPrefix(unf, addPrefix);
      }
    }
  }
  return i;
}

FstState fstStateCreateFrom(FstSlice* slice, CompiledAddr addr) {
  FstState fs = {.state = EmptyFinal, .val = 0};
  if (addr == EMPTY_ADDRESS) {
    return fs;
  }

  uint8_t* data = fstSliceData(slice, NULL);
  uint8_t  v = data[addr];
  uint8_t  t = (v & 0b11000000) >> 6;
  if (t == 0b11) {
    fs.state = OneTransNext;
  } else if (t == 0b10) {
    fs.state = OneTrans;
  } else {
    fs.state = AnyTrans;
  }
  fs.val = v;
  return fs;
}

static FstState fstStateDict[] = {{.state = OneTransNext, .val = 0b11000000},
                                  {.state = OneTrans, .val = 0b10000000},
                                  {.state = AnyTrans, .val = 0b00000000},
                                  {.state = EmptyFinal, .val = 0b00000000}};
// debug
static const char* fstStateStr[] = {"ONE_TRANS_NEXT", "ONE_TRANS", "ANY_TRANS", "EMPTY_FINAL"};

FstState fstStateCreate(State state) {
  uint8_t idx = (uint8_t)state;
  return fstStateDict[idx];
}
// compile
void fstStateCompileForOneTransNext(IdxFstFile* w, CompiledAddr addr, uint8_t inp) {
  FstState s = fstStateCreate(OneTransNext);
  fstStateSetCommInput(&s, inp);

  bool    null = false;
  uint8_t v = fstStateCommInput(&s, &null);
  if (null) {
    // w->write_all(&[inp])
    idxFileWrite(w, &inp, 1);
  }
  idxFileWrite(w, &(s.val), 1);
  // w->write_all(&[s.val])
  return;
}
void fstStateCompileForOneTrans(IdxFstFile* w, CompiledAddr addr, FstTransition* trn) {
  Output    out = trn->out;
  uint8_t   outPackSize = (out == 0 ? 0 : idxFilePackUint(w, out));
  uint8_t   transPackSize = fstPackDelta(w, addr, trn->addr);
  PackSizes packSizes = 0;

  FST_SET_OUTPUT_PACK_SIZE(packSizes, outPackSize);
  FST_SET_TRANSITION_PACK_SIZE(packSizes, transPackSize);
  idxFileWrite(w, (char*)&packSizes, sizeof(packSizes));

  FstState st = fstStateCreate(OneTrans);

  fstStateSetCommInput(&st, trn->inp);

  bool    null = false;
  uint8_t inp = fstStateCommInput(&st, &null);
  if (null == true) {
    idxFileWrite(w, (char*)&trn->inp, sizeof(trn->inp));
  }
  idxFileWrite(w, (char*)(&(st.val)), sizeof(st.val));
  return;
}
void fstStateCompileForAnyTrans(IdxFstFile* w, CompiledAddr addr, FstBuilderNode* node) {
  int32_t sz = taosArrayGetSize(node->trans);

  uint8_t tSize = 0;
  uint8_t oSize = packSize(node->finalOutput);

  // finalOutput.is_zero()
  bool anyOuts = (node->finalOutput != 0);
  for (int32_t i = 0; i < sz; i++) {
    FstTransition* t = taosArrayGet(node->trans, i);
    tSize = TMAX(tSize, packDeltaSize(addr, t->addr));
    oSize = TMAX(oSize, packSize(t->out));
    anyOuts = anyOuts || (t->out != 0);
  }

  PackSizes packSizes = 0;
  if (anyOuts) {
    FST_SET_OUTPUT_PACK_SIZE(packSizes, oSize);
  } else {
    FST_SET_OUTPUT_PACK_SIZE(packSizes, 0);
  }

  FST_SET_TRANSITION_PACK_SIZE(packSizes, tSize);

  FstState st = fstStateCreate(AnyTrans);
  fstStateSetFinalState(&st, node->isFinal);
  fstStateSetStateNtrans(&st, (uint8_t)sz);

  if (anyOuts) {
    if (FST_BUILDER_NODE_IS_FINAL(node)) {
      idxFilePackUintIn(w, node->finalOutput, oSize);
    }
    for (int32_t i = sz - 1; i >= 0; i--) {
      FstTransition* t = taosArrayGet(node->trans, i);
      idxFilePackUintIn(w, t->out, oSize);
    }
  }
  for (int32_t i = sz - 1; i >= 0; i--) {
    FstTransition* t = taosArrayGet(node->trans, i);
    fstPackDeltaIn(w, addr, t->addr, tSize);
  }
  for (int32_t i = sz - 1; i >= 0; i--) {
    FstTransition* t = taosArrayGet(node->trans, i);
    idxFileWrite(w, (char*)&t->inp, 1);
  }
  if (sz > TRANS_INDEX_THRESHOLD) {
    // A value of 255 indicates that no transition exists for the byte at that idx
    uint8_t* index = (uint8_t*)taosMemoryMalloc(sizeof(uint8_t) * 256);
    memset(index, 255, sizeof(uint8_t) * 256);
    for (int32_t i = 0; i < sz; i++) {
      FstTransition* t = taosArrayGet(node->trans, i);
      index[t->inp] = i;
    }
    idxFileWrite(w, (char*)index, 256);
    taosMemoryFree(index);
  }
  idxFileWrite(w, (char*)&packSizes, 1);
  bool null = false;
  fstStateStateNtrans(&st, &null);
  if (null == true) {
    // 256 can't be represented in a u8, so we abuse the fact that
    // the # of transitions can never be 1 here, since 1 is always
    // encoded in the state byte.
    uint8_t v = 1;
    if (sz == 256) {
      idxFileWrite(w, (char*)&v, 1);
    } else {
      idxFileWrite(w, (char*)&sz, 1);
    }
  }
  idxFileWrite(w, (char*)(&(st.val)), 1);
  return;
}

// set_comm_input
void fstStateSetCommInput(FstState* s, uint8_t inp) {
  ASSERT(s->state == OneTransNext || s->state == OneTrans);

  uint8_t val;
  COMMON_INDEX(inp, 0b111111, val);
  s->val = (s->val & fstStateDict[s->state].val) | val;
}

// comm_input
uint8_t fstStateCommInput(FstState* s, bool* null) {
  ASSERT(s->state == OneTransNext || s->state == OneTrans);
  uint8_t v = s->val & 0b00111111;
  if (v == 0) {
    *null = true;
    return v;
  }
  // 0 indicate that common_input is None
  return COMMON_INPUT(v);
}

// input_len

uint64_t fstStateInputLen(FstState* s) {
  ASSERT(s->state == OneTransNext || s->state == OneTrans);
  bool null = false;
  fstStateCommInput(s, &null);
  return null ? 1 : 0;
}

// end_addr
uint64_t fstStateEndAddrForOneTransNext(FstState* s, FstSlice* data) {
  ASSERT(s->state == OneTransNext);
  return FST_SLICE_LEN(data) - 1 - fstStateInputLen(s);
}
uint64_t fstStateEndAddrForOneTrans(FstState* s, FstSlice* data, PackSizes sizes) {
  ASSERT(s->state == OneTrans);
  return FST_SLICE_LEN(data) - 1 - fstStateInputLen(s) - 1  // pack size
         - FST_GET_TRANSITION_PACK_SIZE(sizes) - FST_GET_OUTPUT_PACK_SIZE(sizes);
}
uint64_t fstStateEndAddrForAnyTrans(FstState* state, uint64_t version, FstSlice* date, PackSizes sizes,
                                    uint64_t nTrans) {
  uint8_t oSizes = FST_GET_OUTPUT_PACK_SIZE(sizes);
  uint8_t finalOsize = !fstStateIsFinalState(state) ? 0 : oSizes;
  return FST_SLICE_LEN(date) - 1 - fstStateNtransLen(state) - 1                     // pack size
         - fstStateTotalTransSize(state, version, sizes, nTrans) - nTrans * oSizes  // output values
         - finalOsize;                                                              // final output
}
// input
uint8_t fstStateInput(FstState* s, FstNode* node) {
  ASSERT(s->state == OneTransNext || s->state == OneTrans);
  FstSlice* slice = &node->data;
  bool      null = false;
  uint8_t   inp = fstStateCommInput(s, &null);
  uint8_t*  data = fstSliceData(slice, NULL);
  return null == false ? inp : data[node->start - 1];
}
uint8_t fstStateInputForAnyTrans(FstState* s, FstNode* node, uint64_t i) {
  ASSERT(s->state == AnyTrans);
  FstSlice* slice = &node->data;

  uint64_t at = node->start - fstStateNtransLen(s) - 1                             // pack size
                - fstStateTransIndexSize(s, node->version, node->nTrans) - i - 1;  // the output size

  uint8_t* data = fstSliceData(slice, NULL);
  return data[at];
}

// trans_addr
CompiledAddr fstStateTransAddr(FstState* s, FstNode* node) {
  ASSERT(s->state == OneTransNext || s->state == OneTrans);
  FstSlice* slice = &node->data;
  if (s->state == OneTransNext) {
    return (CompiledAddr)(node->end) - 1;
  } else {
    PackSizes sizes = node->sizes;
    uint8_t   tSizes = FST_GET_TRANSITION_PACK_SIZE(sizes);
    uint64_t  i = node->start - fstStateInputLen(s) - 1  // PackSizes
                 - tSizes;

    // refactor error logic
    uint8_t* data = fstSliceData(slice, NULL);
    return unpackDelta(data + i, tSizes, node->end);
  }
}
CompiledAddr fstStateTransAddrForAnyTrans(FstState* s, FstNode* node, uint64_t i) {
  ASSERT(s->state == AnyTrans);

  FstSlice* slice = &node->data;
  uint8_t   tSizes = FST_GET_TRANSITION_PACK_SIZE(node->sizes);
  uint64_t  at = node->start - fstStateNtransLen(s) - 1 - fstStateTransIndexSize(s, node->version, node->nTrans) -
                node->nTrans - (i * tSizes) - tSizes;
  uint8_t* data = fstSliceData(slice, NULL);
  return unpackDelta(data + at, tSizes, node->end);
}

// sizes
PackSizes fstStateSizes(FstState* s, FstSlice* slice) {
  ASSERT(s->state == OneTrans || s->state == AnyTrans);
  uint64_t i;
  if (s->state == OneTrans) {
    i = FST_SLICE_LEN(slice) - 1 - fstStateInputLen(s) - 1;
  } else {
    i = FST_SLICE_LEN(slice) - 1 - fstStateNtransLen(s) - 1;
  }

  uint8_t* data = fstSliceData(slice, NULL);
  return (PackSizes)(*(data + i));
}
// Output
Output fstStateOutput(FstState* s, FstNode* node) {
  ASSERT(s->state == OneTrans);

  uint8_t oSizes = FST_GET_OUTPUT_PACK_SIZE(node->sizes);
  if (oSizes == 0) {
    return 0;
  }
  FstSlice* slice = &node->data;
  uint8_t   tSizes = FST_GET_TRANSITION_PACK_SIZE(node->sizes);

  uint64_t i = node->start - fstStateInputLen(s) - 1 - tSizes - oSizes;
  uint8_t* data = fstSliceData(slice, NULL);
  return unpackUint64(data + i, oSizes);
}
Output fstStateOutputForAnyTrans(FstState* s, FstNode* node, uint64_t i) {
  ASSERT(s->state == AnyTrans);

  uint8_t oSizes = FST_GET_OUTPUT_PACK_SIZE(node->sizes);
  if (oSizes == 0) {
    return 0;
  }
  FstSlice* slice = &node->data;
  uint8_t*  data = fstSliceData(slice, NULL);
  uint64_t  at = node->start - fstStateNtransLen(s) - 1  // pack size
                - fstStateTotalTransSize(s, node->version, node->sizes, node->nTrans) - (i * oSizes) - oSizes;

  return unpackUint64(data + at, oSizes);
}

// anyTrans specify function

void fstStateSetFinalState(FstState* s, bool yes) {
  ASSERT(s->state == AnyTrans);
  if (yes) {
    s->val |= 0b01000000;
  }
  return;
}
bool fstStateIsFinalState(FstState* s) {
  ASSERT(s->state == AnyTrans);
  return (s->val & 0b01000000) == 0b01000000;
}

void fstStateSetStateNtrans(FstState* s, uint8_t n) {
  ASSERT(s->state == AnyTrans);
  if (n <= 0b00111111) {
    s->val = (s->val & 0b11000000) | n;
  }
  return;
}
// state_ntrans
uint8_t fstStateStateNtrans(FstState* s, bool* null) {
  ASSERT(s->state == AnyTrans);
  *null = false;
  uint8_t n = s->val & 0b00111111;

  if (n == 0) {
    *null = true;  // None
  }
  return n;
}
uint64_t fstStateTotalTransSize(FstState* s, uint64_t version, PackSizes sizes, uint64_t nTrans) {
  ASSERT(s->state == AnyTrans);
  uint64_t idxSize = fstStateTransIndexSize(s, version, nTrans);
  return nTrans + (nTrans * FST_GET_TRANSITION_PACK_SIZE(sizes)) + idxSize;
}
uint64_t fstStateTransIndexSize(FstState* s, uint64_t version, uint64_t nTrans) {
  ASSERT(s->state == AnyTrans);
  return (version >= 2 && nTrans > TRANS_INDEX_THRESHOLD) ? 256 : 0;
}
uint64_t fstStateNtransLen(FstState* s) {
  ASSERT(s->state == AnyTrans);
  bool null = false;
  fstStateStateNtrans(s, &null);
  return null == true ? 1 : 0;
}
uint64_t fstStateNtrans(FstState* s, FstSlice* slice) {
  bool    null = false;
  uint8_t n = fstStateStateNtrans(s, &null);
  if (null != true) {
    return n;
  }
  int32_t  len;
  uint8_t* data = fstSliceData(slice, &len);
  n = data[len - 2];
  return n == 1 ? 256 : n;  // // "1" is never a normal legal value here, because if there, // is only 1 transition,
                            // then it is encoded in the state byte
}
Output fstStateFinalOutput(FstState* s, uint64_t version, FstSlice* slice, PackSizes sizes, uint64_t nTrans) {
  uint8_t oSizes = FST_GET_OUTPUT_PACK_SIZE(sizes);
  if (oSizes == 0 || !fstStateIsFinalState(s)) {
    return 0;
  }

  uint64_t at = FST_SLICE_LEN(slice) - 1 - fstStateNtransLen(s) - 1  // pack size
                - fstStateTotalTransSize(s, version, sizes, nTrans) - (nTrans * oSizes) - oSizes;
  uint8_t* data = fstSliceData(slice, NULL);
  return unpackUint64(data + at, (uint8_t)oSizes);
}
uint64_t fstStateFindInput(FstState* s, FstNode* node, uint8_t b, bool* null) {
  ASSERT(s->state == AnyTrans);
  FstSlice* slice = &node->data;
  if (node->version >= 2 && node->nTrans > TRANS_INDEX_THRESHOLD) {
    uint64_t at = node->start - fstStateNtransLen(s) - 1  // pack size
                  - fstStateTransIndexSize(s, node->version, node->nTrans);
    int32_t  dlen = 0;
    uint8_t* data = fstSliceData(slice, &dlen);
    uint64_t i = data[at + b];
    if (i >= node->nTrans) {
      *null = true;
    }
    return i;
  } else {
    uint64_t start = node->start - fstStateNtransLen(s) - 1  // pack size
                     - node->nTrans;
    uint64_t end = start + node->nTrans;
    FstSlice t = fstSliceCopy(slice, start, end - 1);
    int32_t  len = 0;
    uint8_t* data = fstSliceData(&t, &len);
    for (int i = 0; i < len; i++) {
      uint8_t v = data[i];
      if (v == b) {
        fstSliceDestroy(&t);
        return node->nTrans - i - 1;  // bug
      }
      if (i + 1 == len) {
        *null = true;
      }
    }
    fstSliceDestroy(&t);
  }

  return 0;
}

// fst node function

FstNode* fstNodeCreate(int64_t version, CompiledAddr addr, FstSlice* slice) {
  FstNode* n = (FstNode*)taosMemoryMalloc(sizeof(FstNode));
  if (n == NULL) {
    return NULL;
  }

  FstState st = fstStateCreateFrom(slice, addr);

  if (st.state == EmptyFinal) {
    n->data = fstSliceCreate(NULL, 0);
    n->version = version;
    n->state = st;
    n->start = EMPTY_ADDRESS;
    n->end = EMPTY_ADDRESS;
    n->isFinal = true;
    n->nTrans = 0;
    n->sizes = 0;
    n->finalOutput = 0;
  } else if (st.state == OneTransNext) {
    n->data = fstSliceCopy(slice, 0, addr);
    n->version = version;
    n->state = st;
    n->start = addr;
    n->end = fstStateEndAddrForOneTransNext(&st, &n->data);  //? s.end_addr(data);
    n->isFinal = false;
    n->sizes = 0;
    n->nTrans = 1;
    n->finalOutput = 0;
  } else if (st.state == OneTrans) {
    FstSlice  data = fstSliceCopy(slice, 0, addr);
    PackSizes sz = fstStateSizes(&st, &data);
    n->data = data;
    n->version = version;
    n->state = st;
    n->start = addr;
    n->end = fstStateEndAddrForOneTrans(&st, &data, sz);  // s.end_addr(data, sz);
    n->isFinal = false;
    n->nTrans = 1;
    n->sizes = sz;
    n->finalOutput = 0;
  } else {
    FstSlice data = fstSliceCopy(slice, 0, addr);
    uint64_t sz = fstStateSizes(&st, &data);       // s.sizes(data)
    uint32_t nTrans = fstStateNtrans(&st, &data);  // s.ntrans(data)
    n->data = data;
    n->version = version;
    n->state = st;
    n->start = addr;
    n->end = fstStateEndAddrForAnyTrans(&st, version, &data, sz, nTrans);  // s.end_addr(version, data, sz, ntrans);
    n->isFinal = fstStateIsFinalState(&st);                                // s.is_final_state();
    n->nTrans = nTrans;
    n->sizes = sz;
    n->finalOutput =
        fstStateFinalOutput(&st, version, &data, sz, nTrans);  // s.final_output(version, data, sz, ntrans);
  }
  return n;
}

// debug state transition
static const char* fstNodeState(FstNode* node) {
  FstState* st = &node->state;
  return fstStateStr[st->state];
}

void fstNodeDestroy(FstNode* node) {
  if (node == NULL) {
    return;
  }
  fstSliceDestroy(&node->data);
  taosMemoryFree(node);
}
FstTransitions* fstNodeTransitions(FstNode* node) {
  FstTransitions* t = taosMemoryMalloc(sizeof(FstTransitions));
  if (NULL == t) {
    return NULL;
  }
  FstRange range = {.start = 0, .end = FST_NODE_LEN(node)};
  t->range = range;
  t->node = node;
  return t;
}

// Returns the transition at index `i`.
bool fstNodeGetTransitionAt(FstNode* node, uint64_t i, FstTransition* trn) {
  bool      s = true;
  FstState* st = &node->state;
  if (st->state == OneTransNext) {
    trn->inp = fstStateInput(st, node);
    trn->out = 0;
    trn->addr = fstStateTransAddr(st, node);
  } else if (st->state == OneTrans) {
    trn->inp = fstStateInput(st, node);
    trn->out = fstStateOutput(st, node);
    trn->addr = fstStateTransAddr(st, node);
  } else if (st->state == AnyTrans) {
    trn->inp = fstStateInputForAnyTrans(st, node, i);
    trn->out = fstStateOutputForAnyTrans(st, node, i);
    trn->addr = fstStateTransAddrForAnyTrans(st, node, i);
  } else {
    s = false;
  }
  return s;
}

// Returns the transition address of the `i`th transition
bool fstNodeGetTransitionAddrAt(FstNode* node, uint64_t i, CompiledAddr* res) {
  bool      s = true;
  FstState* st = &node->state;
  if (st->state == OneTransNext) {
    ASSERT(i == 0);
    fstStateTransAddr(st, node);
  } else if (st->state == OneTrans) {
    ASSERT(i == 0);
    fstStateTransAddr(st, node);
  } else if (st->state == AnyTrans) {
    fstStateTransAddrForAnyTrans(st, node, i);
  } else if (FST_STATE_EMPTY_FINAL(node)) {
    s = false;
  } else {
    ASSERT(0);
  }
  return s;
}

//  Finds the `i`th transition corresponding to the given input byte.
//  If no transition for this byte exists, then `false` is returned.
bool fstNodeFindInput(FstNode* node, uint8_t b, uint64_t* res) {
  bool      s = true;
  FstState* st = &node->state;
  if (st->state == OneTransNext) {
    if (fstStateInput(st, node) == b) {
      *res = 0;
    } else {
      s = false;
    }
  } else if (st->state == OneTrans) {
    if (fstStateInput(st, node) == b) {
      *res = 0;
    } else {
      s = false;
    }
  } else if (st->state == AnyTrans) {
    bool     null = false;
    uint64_t out = fstStateFindInput(st, node, b, &null);
    if (null == false) {
      *res = out;
    } else {
      s = false;
    }
  }
  return s;
}

bool fstNodeCompile(FstNode* node, void* w, CompiledAddr lastAddr, CompiledAddr addr, FstBuilderNode* builderNode) {
  int32_t sz = taosArrayGetSize(builderNode->trans);
  ASSERT(sz < 256);
  if (sz == 0 && builderNode->isFinal && builderNode->finalOutput == 0) {
    return true;
  } else if (sz != 1 || builderNode->isFinal) {
    fstStateCompileForAnyTrans(w, addr, builderNode);
  } else {
    FstTransition* tran = taosArrayGet(builderNode->trans, 0);
    if (tran->addr == lastAddr && tran->out == 0) {
      fstStateCompileForOneTransNext(w, addr, tran->inp);
      return true;
    } else {
      fstStateCompileForOneTrans(w, addr, tran);
      return true;
    }
  }
  return true;
}

bool fstBuilderNodeCompileTo(FstBuilderNode* b, IdxFstFile* wrt, CompiledAddr lastAddr, CompiledAddr startAddr) {
  return fstNodeCompile(NULL, wrt, lastAddr, startAddr, b);
}

FstBuilder* fstBuilderCreate(void* w, FstType ty) {
  FstBuilder* b = taosMemoryMalloc(sizeof(FstBuilder));
  if (NULL == b) {
    return b;
  }

  b->wrt = idxFileCreate(w);
  b->unfinished = fstUnFinishedNodesCreate();
  b->registry = fstRegistryCreate(10000, 2);
  b->last = fstSliceCreate(NULL, 0);
  b->lastAddr = NONE_ADDRESS;
  b->len = 0;

  char  buf64[8] = {0};
  void* pBuf64 = buf64;
  taosEncodeFixedU64(&pBuf64, VERSION);
  idxFileWrite(b->wrt, buf64, sizeof(buf64));

  pBuf64 = buf64;
  memset(buf64, 0, sizeof(buf64));
  taosEncodeFixedU64(&pBuf64, ty);
  idxFileWrite(b->wrt, buf64, sizeof(buf64));

  return b;
}
void fstBuilderDestroy(FstBuilder* b) {
  if (b == NULL) {
    return;
  }
  fstBuilderFinish(b);

  idxFileDestroy(b->wrt);
  fstUnFinishedNodesDestroy(b->unfinished);
  fstRegistryDestroy(b->registry);
  fstSliceDestroy(&b->last);
  taosMemoryFree(b);
}

bool fstBuilderInsert(FstBuilder* b, FstSlice bs, Output in) {
  FstOrderType t = fstBuilderCheckLastKey(b, bs, true);
  if (t == Ordered) {
    // add log info
    fstBuilderInsertOutput(b, bs, in);
    return true;
  }
  indexInfo("fst write key must be ordered");
  return false;
}

void fstBuilderInsertOutput(FstBuilder* b, FstSlice bs, Output in) {
  FstSlice* s = &bs;
  if (fstSliceIsEmpty(s)) {
    b->len = 1;
    fstUnFinishedNodesSetRootOutput(b->unfinished, in);
    return;
  }
  Output   out;
  uint64_t prefixLen = fstUnFinishedNodesFindCommPrefixAndSetOutput(b->unfinished, bs, in, &out);

  if (prefixLen == FST_SLICE_LEN(s)) {
    ASSERT(out == 0);
    return;
  }

  b->len += 1;
  fstBuilderCompileFrom(b, prefixLen);

  FstSlice sub = fstSliceCopy(s, prefixLen, s->end);
  fstUnFinishedNodesAddSuffix(b->unfinished, sub, out);
  fstSliceDestroy(&sub);
  return;
}

FstOrderType fstBuilderCheckLastKey(FstBuilder* b, FstSlice bs, bool ckDup) {
  FstSlice* input = &bs;
  if (fstSliceIsEmpty(&b->last)) {
    fstSliceDestroy(&b->last);
    // deep copy or not
    b->last = fstSliceDeepCopy(&bs, input->start, input->end);
  } else {
    int comp = fstSliceCompare(&b->last, &bs);
    if (comp == 0 && ckDup) {
      return DuplicateKey;
    } else if (comp == 1) {
      return OutOfOrdered;
    }
    // deep copy or not
    fstSliceDestroy(&b->last);
    b->last = fstSliceDeepCopy(&bs, input->start, input->end);
  }
  return Ordered;
}
void fstBuilderCompileFrom(FstBuilder* b, uint64_t istate) {
  CompiledAddr addr = NONE_ADDRESS;
  while (istate + 1 < FST_UNFINISHED_NODES_LEN(b->unfinished)) {
    FstBuilderNode* bn = NULL;
    if (addr == NONE_ADDRESS) {
      bn = fstUnFinishedNodesPopEmpty(b->unfinished);
    } else {
      bn = fstUnFinishedNodesPopFreeze(b->unfinished, addr);
    }
    addr = fstBuilderCompile(b, bn);

    fstBuilderNodeDestroy(bn);
    ASSERT(addr != NONE_ADDRESS);
  }
  fstUnFinishedNodesTopLastFreeze(b->unfinished, addr);
  return;
}

CompiledAddr fstBuilderCompile(FstBuilder* b, FstBuilderNode* bn) {
  if (FST_BUILDER_NODE_IS_FINAL(bn) && FST_BUILDER_NODE_TRANS_ISEMPTY(bn) && FST_BUILDER_NODE_FINALOUTPUT_ISZERO(bn)) {
    return EMPTY_ADDRESS;
  }
  FstRegistryEntry* entry = fstRegistryGetEntry(b->registry, bn);
  if (entry->state == FOUND) {
    CompiledAddr ret = entry->addr;
    fstRegistryEntryDestroy(entry);
    return ret;
  }
  CompiledAddr startAddr = (CompiledAddr)(FST_WRITER_COUNT(b->wrt));

  fstBuilderNodeCompileTo(bn, b->wrt, b->lastAddr, startAddr);
  b->lastAddr = (CompiledAddr)(FST_WRITER_COUNT(b->wrt) - 1);
  if (entry->state == NOTFOUND) {
    FST_REGISTRY_CELL_INSERT(entry->cell, b->lastAddr);
  }
  fstRegistryEntryDestroy(entry);

  return b->lastAddr;
}

void* fstBuilderInsertInner(FstBuilder* b) {
  fstBuilderCompileFrom(b, 0);
  FstBuilderNode* rootNode = fstUnFinishedNodesPopRoot(b->unfinished);
  CompiledAddr    rootAddr = fstBuilderCompile(b, rootNode);
  fstBuilderNodeDestroy(rootNode);

  char buf64[8] = {0};

  void* pBuf64 = buf64;
  taosEncodeFixedU64(&pBuf64, b->len);
  idxFileWrite(b->wrt, buf64, sizeof(buf64));

  pBuf64 = buf64;
  taosEncodeFixedU64(&pBuf64, rootAddr);
  idxFileWrite(b->wrt, buf64, sizeof(buf64));

  char     buf32[4] = {0};
  void*    pBuf32 = buf32;
  uint32_t sum = idxFileMaskedCheckSum(b->wrt);
  taosEncodeFixedU32(&pBuf32, sum);
  idxFileWrite(b->wrt, buf32, sizeof(buf32));

  idxFileFlush(b->wrt);
  return b->wrt;
}
void fstBuilderFinish(FstBuilder* b) { fstBuilderInsertInner(b); }

FstSlice fstNodeAsSlice(FstNode* node) {
  FstSlice* slice = &node->data;
  FstSlice  s = fstSliceCopy(slice, slice->end, FST_SLICE_LEN(slice) - 1);
  return s;
}

FstLastTransition* fstLastTransitionCreate(uint8_t inp, Output out) {
  FstLastTransition* trn = taosMemoryMalloc(sizeof(FstLastTransition));
  if (trn == NULL) {
    return NULL;
  }

  trn->inp = inp;
  trn->out = out;
  return trn;
}

void fstLastTransitionDestroy(FstLastTransition* trn) { taosMemoryFree(trn); }

void fstBuilderNodeUnfinishedLastCompiled(FstBuilderNodeUnfinished* unNode, CompiledAddr addr) {
  FstLastTransition* trn = unNode->last;
  if (trn == NULL) {
    return;
  }
  FstTransition t = {.inp = trn->inp, .out = trn->out, .addr = addr};
  taosArrayPush(unNode->node->trans, &t);
  fstLastTransitionDestroy(trn);
  unNode->last = NULL;
  return;
}

void fstBuilderNodeUnfinishedAddOutputPrefix(FstBuilderNodeUnfinished* unNode, Output out) {
  if (FST_BUILDER_NODE_IS_FINAL(unNode->node)) {
    unNode->node->finalOutput += out;
  }
  int32_t sz = taosArrayGetSize(unNode->node->trans);
  for (int32_t i = 0; i < sz; i++) {
    FstTransition* trn = taosArrayGet(unNode->node->trans, i);
    trn->out += out;
  }
  if (unNode->last) {
    unNode->last->out += out;
  }
  return;
}

Fst* fstCreate(FstSlice* slice) {
  int32_t slen;
  char*   buf = fstSliceData(slice, &slen);
  if (slen < 36) {
    return NULL;
  }
  uint64_t len = slen;
  uint64_t skip = 0;

  uint64_t version;
  taosDecodeFixedU64(buf, &version);
  skip += sizeof(version);
  if (version == 0 || version > VERSION) {
    return NULL;
  }

  uint64_t type;
  taosDecodeFixedU64(buf + skip, &type);
  skip += sizeof(type);

  uint32_t checkSum = 0;
  len -= sizeof(checkSum);
  taosDecodeFixedU32(buf + len, &checkSum);
  if (taosCheckChecksum(buf, len, checkSum)) {
    indexError("index file is corrupted");
    // verify fst
    return NULL;
  }
  CompiledAddr rootAddr;
  len -= sizeof(rootAddr);
  taosDecodeFixedU64(buf + len, &rootAddr);

  uint64_t fstLen;
  len -= sizeof(fstLen);
  taosDecodeFixedU64(buf + len, &fstLen);
  // TODO(validate root addr)
  Fst* fst = (Fst*)taosMemoryCalloc(1, sizeof(Fst));
  if (fst == NULL) {
    return NULL;
  }

  fst->meta = (FstMeta*)taosMemoryMalloc(sizeof(FstMeta));
  if (NULL == fst->meta) {
    goto FST_CREAT_FAILED;
  }

  fst->meta->version = version;
  fst->meta->rootAddr = rootAddr;
  fst->meta->ty = type;
  fst->meta->len = fstLen;
  fst->meta->checkSum = checkSum;

  FstSlice* s = taosMemoryCalloc(1, sizeof(FstSlice));
  *s = fstSliceCopy(slice, 0, FST_SLICE_LEN(slice) - 1);
  fst->data = s;

  taosThreadMutexInit(&fst->mtx, NULL);
  return fst;

FST_CREAT_FAILED:
  taosMemoryFree(fst->meta);
  taosMemoryFree(fst);

  return NULL;
}
void fstDestroy(Fst* fst) {
  if (fst) {
    taosMemoryFree(fst->meta);
    fstSliceDestroy(fst->data);
    taosMemoryFree(fst->data);
    taosThreadMutexDestroy(&fst->mtx);
  }
  taosMemoryFree(fst);
}

bool fstGet(Fst* fst, FstSlice* b, Output* out) {
  int      ret = false;
  FstNode* root = fstGetRoot(fst);
  Output   tOut = 0;
  int32_t  len;

  uint8_t* data = fstSliceData(b, &len);

  SArray* nodes = (SArray*)taosArrayInit(len, sizeof(FstNode*));
  taosArrayPush(nodes, &root);
  for (uint32_t i = 0; i < len; i++) {
    uint8_t inp = data[i];
    Output  res = 0;
    if (false == fstNodeFindInput(root, inp, &res)) {
      goto _return;
    }

    FstTransition trn;
    fstNodeGetTransitionAt(root, res, &trn);
    tOut += trn.out;
    root = fstGetNode(fst, trn.addr);
    taosArrayPush(nodes, &root);
  }
  if (!FST_NODE_IS_FINAL(root)) {
    goto _return;
  } else {
    tOut = tOut + FST_NODE_FINAL_OUTPUT(root);
    ret = true;
  }

_return:
  for (int32_t i = 0; i < taosArrayGetSize(nodes); i++) {
    FstNode** node = (FstNode**)taosArrayGet(nodes, i);
    fstNodeDestroy(*node);
  }
  taosArrayDestroy(nodes);
  *out = tOut;
  return ret;
}
FStmBuilder* fstSearch(Fst* fst, FAutoCtx* ctx) {
  // refactor later
  return stmBuilderCreate(fst, ctx);
}
FStmSt* stmBuilderIntoStm(FStmBuilder* sb) {
  if (sb == NULL) {
    return NULL;
  }
  return stmStCreate(sb->fst, sb->aut, sb->min, sb->max);
}
FStmStBuilder* fstSearchWithState(Fst* fst, FAutoCtx* ctx) {
  // refactor later
  return stmBuilderCreate(fst, ctx);
}

FstNode* fstGetRoot(Fst* fst) {
  CompiledAddr addr = fstGetRootAddr(fst);
  return fstGetNode(fst, addr);
}

FstNode* fstGetNode(Fst* fst, CompiledAddr addr) {
  // refactor later
  return fstNodeCreate(fst->meta->version, addr, fst->data);
}
FstType      fstGetType(Fst* fst) { return fst->meta->ty; }
CompiledAddr fstGetRootAddr(Fst* fst) { return fst->meta->rootAddr; }

Output fstEmptyFinalOutput(Fst* fst, bool* null) {
  Output   res = 0;
  FstNode* node = fstGetRoot(fst);
  if (FST_NODE_IS_FINAL(node)) {
    *null = false;
    res = FST_NODE_FINAL_OUTPUT(node);
  } else {
    *null = true;
  }
  fstNodeDestroy(node);
  return res;
}

bool fstVerify(Fst* fst) {
  uint32_t len, checkSum = fst->meta->checkSum;
  uint8_t* data = fstSliceData(fst->data, &len);
  TSCKSUM  initSum = 0;
  if (!taosCheckChecksumWhole(data, len)) {
    return false;
  }
  return true;
}

// data bound function
FstBoundWithData* fstBoundStateCreate(FstBound type, FstSlice* data) {
  FstBoundWithData* b = taosMemoryCalloc(1, sizeof(FstBoundWithData));
  if (b == NULL) {
    return NULL;
  }

  if (data != NULL) {
    b->data = fstSliceCopy(data, data->start, data->end);
  } else {
    b->data = fstSliceCreate(NULL, 0);
  }
  b->type = type;

  return b;
}

bool fstBoundWithDataExceededBy(FstBoundWithData* bound, FstSlice* slice) {
  int comp = fstSliceCompare(slice, &bound->data);
  if (bound->type == Included) {
    return comp > 0 ? true : false;
  } else if (bound->type == Excluded) {
    return comp >= 0 ? true : false;
  } else {
    return false;
  }
}
bool fstBoundWithDataIsEmpty(FstBoundWithData* bound) {
  if (bound->type == Unbounded) {
    return true;
  } else {
    return fstSliceIsEmpty(&bound->data);
  }
}

bool fstBoundWithDataIsIncluded(FstBoundWithData* bound) { return bound->type == Excluded ? false : true; }

void fstBoundDestroy(FstBoundWithData* bound) { taosMemoryFree(bound); }

FStmSt* stmStCreate(Fst* fst, FAutoCtx* automation, FstBoundWithData* min, FstBoundWithData* max) {
  FStmSt* sws = taosMemoryCalloc(1, sizeof(FStmSt));
  if (sws == NULL) {
    return NULL;
  }

  sws->fst = fst;
  sws->aut = automation;
  sws->inp = (SArray*)taosArrayInit(256, sizeof(uint8_t));

  sws->emptyOutput.null = true;
  sws->emptyOutput.out = 0;

  sws->stack = (SArray*)taosArrayInit(256, sizeof(FstStreamState));
  sws->endAt = max;
  stmStSeekMin(sws, min);

  return sws;
}
void stmStDestroy(FStmSt* sws) {
  if (sws == NULL) {
    return;
  }

  taosArrayDestroy(sws->inp);
  taosArrayDestroyEx(sws->stack, fstStreamStateDestroy);

  taosMemoryFree(sws);
}

bool stmStSeekMin(FStmSt* sws, FstBoundWithData* min) {
  FAutoCtx* aut = sws->aut;
  if (fstBoundWithDataIsEmpty(min)) {
    if (fstBoundWithDataIsIncluded(min)) {
      sws->emptyOutput.out = fstEmptyFinalOutput(sws->fst, &(sws->emptyOutput.null));
    }
    FstStreamState s = {.node = fstGetRoot(sws->fst),
                        .trans = 0,
                        .out = {.null = false, .out = 0},
                        .autState = automFuncs[aut->type].start(aut)};  // auto.start callback
    taosArrayPush(sws->stack, &s);
    return true;
  }
  FstSlice* key = NULL;
  bool      inclusize = false;

  if (min->type == Included) {
    key = &min->data;
    inclusize = true;
  } else if (min->type == Excluded) {
    key = &min->data;
  } else {
    return false;
  }

  FstNode* node = fstGetRoot(sws->fst);
  Output   out = 0;
  void*    autState = automFuncs[aut->type].start(aut);

  int32_t  len;
  uint8_t* data = fstSliceData(key, &len);
  for (uint32_t i = 0; i < len; i++) {
    uint8_t  b = data[i];
    uint64_t res = 0;
    if (fstNodeFindInput(node, b, &res)) {
      FstTransition trn;
      fstNodeGetTransitionAt(node, res, &trn);
      void* preState = autState;
      autState = automFuncs[aut->type].accept(aut, preState, b);
      taosArrayPush(sws->inp, &b);

      FstStreamState s = {.node = node, .trans = res + 1, .out = {.null = false, .out = out}, .autState = preState};
      node = NULL;

      taosArrayPush(sws->stack, &s);
      out += trn.out;
      node = fstGetNode(sws->fst, trn.addr);
    } else {
      // This is a little tricky. We're in this case if the
      // given bound is not a prefix of any key in the FST.
      // Since this is a minimum bound, we need to find the
      // first transition in this node that proceeds the current
      // input byte.
      FstTransitions* trans = fstNodeTransitions(node);
      uint64_t        i = 0;
      for (i = trans->range.start; i < trans->range.end; i++) {
        FstTransition trn;
        if (fstNodeGetTransitionAt(node, i, &trn) && trn.inp > b) {
          break;
        }
      }

      FstStreamState s = {.node = node, .trans = i, .out = {.null = false, .out = out}, .autState = autState};
      taosArrayPush(sws->stack, &s);
      taosMemoryFree(trans);
      return true;
    }
  }

  fstNodeDestroy(node);

  uint32_t sz = taosArrayGetSize(sws->stack);
  if (sz != 0) {
    FstStreamState* s = taosArrayGet(sws->stack, sz - 1);
    if (inclusize) {
      s->trans -= 1;
      taosArrayPop(sws->inp);
    } else {
      FstNode*      n = s->node;
      uint64_t      trans = s->trans;
      FstTransition trn;
      fstNodeGetTransitionAt(n, trans - 1, &trn);
      FstStreamState s = {
          .node = fstGetNode(sws->fst, trn.addr), .trans = 0, .out = {.null = false, .out = out}, .autState = autState};
      taosArrayPush(sws->stack, &s);
      return true;
    }
    return false;
  }

  return false;
}
FStmStRslt* stmStNextWith(FStmSt* sws, streamCallback__fn callback) {
  FAutoCtx* aut = sws->aut;
  FstOutput output = sws->emptyOutput;
  if (output.null == false) {
    FstSlice emptySlice = fstSliceCreate(NULL, 0);
    if (fstBoundWithDataExceededBy(sws->endAt, &emptySlice)) {
      taosArrayDestroyEx(sws->stack, fstStreamStateDestroy);
      sws->stack = (SArray*)taosArrayInit(256, sizeof(FstStreamState));
      return NULL;
    }
    void* start = automFuncs[aut->type].start(aut);
    if (automFuncs[aut->type].isMatch(aut, start)) {
      FstSlice s = fstSliceCreate(NULL, 0);
      return swsResultCreate(&s, output, callback == NULL ? NULL : callback(start));
    }
  }
  SArray* nodes = taosArrayInit(8, sizeof(FstNode*));
  while (taosArrayGetSize(sws->stack) > 0) {
    FstStreamState* p = (FstStreamState*)taosArrayPop(sws->stack);
    if (p->trans >= FST_NODE_LEN(p->node) || !automFuncs[aut->type].canMatch(aut, p->autState)) {
      if (FST_NODE_ADDR(p->node) != fstGetRootAddr(sws->fst)) {
        taosArrayPop(sws->inp);
      }
      fstStreamStateDestroy(p);
      continue;
    }
    FstTransition trn;
    fstNodeGetTransitionAt(p->node, p->trans, &trn);

    Output out = p->out.out + trn.out;
    void*  nextState = automFuncs[aut->type].accept(aut, p->autState, trn.inp);
    void*  tState = (callback == NULL) ? NULL : callback(nextState);
    bool   isMatch = automFuncs[aut->type].isMatch(aut, nextState);

    FstNode* nextNode = fstGetNode(sws->fst, trn.addr);
    taosArrayPush(nodes, &nextNode);
    taosArrayPush(sws->inp, &(trn.inp));

    if (FST_NODE_IS_FINAL(nextNode)) {
      void* eofState = automFuncs[aut->type].acceptEof(aut, nextState);
      if (eofState != NULL) {
        isMatch = automFuncs[aut->type].isMatch(aut, eofState);
      }
    }
    FstStreamState s1 = {.node = p->node, .trans = p->trans + 1, .out = p->out, .autState = p->autState};
    taosArrayPush(sws->stack, &s1);

    FstStreamState s2 = {.node = nextNode, .trans = 0, .out = {.null = false, .out = out}, .autState = nextState};
    taosArrayPush(sws->stack, &s2);

    int32_t  isz = taosArrayGetSize(sws->inp);
    uint8_t* buf = (uint8_t*)taosMemoryMalloc(isz * sizeof(uint8_t));
    for (uint32_t i = 0; i < isz; i++) {
      buf[i] = *(uint8_t*)taosArrayGet(sws->inp, i);
    }
    FstSlice slice = fstSliceCreate(buf, isz);
    if (fstBoundWithDataExceededBy(sws->endAt, &slice)) {
      taosArrayDestroyEx(sws->stack, fstStreamStateDestroy);
      sws->stack = (SArray*)taosArrayInit(256, sizeof(FstStreamState));
      taosMemoryFreeClear(buf);
      fstSliceDestroy(&slice);
      taosArrayDestroy(nodes);
      return NULL;
    }
    if (FST_NODE_IS_FINAL(nextNode) && isMatch) {
      FstOutput   fOutput = {.null = false, .out = out + FST_NODE_FINAL_OUTPUT(nextNode)};
      FStmStRslt* result = swsResultCreate(&slice, fOutput, tState);
      taosMemoryFreeClear(buf);
      fstSliceDestroy(&slice);
      taosArrayDestroy(nodes);
      nodes = NULL;
      return result;
    }
    taosMemoryFreeClear(buf);
    fstSliceDestroy(&slice);
  };
  taosArrayDestroy(nodes);
  return NULL;
}

FStmStRslt* swsResultCreate(FstSlice* data, FstOutput out, void* state) {
  FStmStRslt* result = taosMemoryCalloc(1, sizeof(FStmStRslt));
  if (result == NULL) {
    return NULL;
  }

  result->data = fstSliceCopy(data, 0, FST_SLICE_LEN(data) - 1);
  result->out = out;
  result->state = state;
  return result;
}
void swsResultDestroy(FStmStRslt* result) {
  if (NULL == result) {
    return;
  }

  fstSliceDestroy(&result->data);
  startWithStateValueDestroy(result->state);
  taosMemoryFree(result);
}

void fstStreamStateDestroy(void* s) {
  if (NULL == s) {
    return;
  }
  FstStreamState* ss = (FstStreamState*)s;
  fstNodeDestroy(ss->node);
}

FStmBuilder* stmBuilderCreate(Fst* fst, FAutoCtx* aut) {
  FStmBuilder* b = taosMemoryCalloc(1, sizeof(FStmBuilder));
  if (NULL == b) {
    return NULL;
  }

  b->fst = fst;
  b->aut = aut;
  b->min = fstBoundStateCreate(Unbounded, NULL);
  b->max = fstBoundStateCreate(Unbounded, NULL);
  return b;
}
void stmBuilderDestroy(FStmBuilder* b) {
  fstSliceDestroy(&b->min->data);
  fstSliceDestroy(&b->max->data);
  taosMemoryFreeClear(b->min);
  taosMemoryFreeClear(b->max);
  taosMemoryFree(b);
}
void stmBuilderSetRange(FStmBuilder* b, FstSlice* val, RangeType type) {
  if (b == NULL) {
    return;
  }
  if (type == GE) {
    b->min->type = Included;
    fstSliceDestroy(&(b->min->data));
    b->min->data = fstSliceDeepCopy(val, 0, FST_SLICE_LEN(val) - 1);
  } else if (type == GT) {
    b->min->type = Excluded;
    fstSliceDestroy(&(b->min->data));
    b->min->data = fstSliceDeepCopy(val, 0, FST_SLICE_LEN(val) - 1);
  } else if (type == LE) {
    b->max->type = Included;
    fstSliceDestroy(&(b->max->data));
    b->max->data = fstSliceDeepCopy(val, 0, FST_SLICE_LEN(val) - 1);
  } else if (type == LT) {
    b->max->type = Excluded;
    fstSliceDestroy(&(b->max->data));
    b->max->data = fstSliceDeepCopy(val, 0, FST_SLICE_LEN(val) - 1);
  }
}
