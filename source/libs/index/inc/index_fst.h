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

#ifndef __INDEX_FST_H__
#define __INDEX_FST_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "indexInt.h"
#include "index_fst_node.h"
#include "index_fst_automation.h"
#include "index_fst_counting_writer.h"
#include "index_fst_registry.h"
#include "index_fst_util.h"

#define OUTPUT_PREFIX(a, b) ((a) > (b) ? (b) : (a)

typedef struct Fst             Fst;
typedef struct FstNode         FstNode;
typedef struct StreamWithState StreamWithState;

typedef enum { Included, Excluded, Unbounded } FstBound;

typedef struct FstBoundWithData {
  FstSlice data;
  FstBound type;
} FstBoundWithData;

typedef struct FstStreamBuilder {
  Fst*              fst;
  AutomationCtx*    aut;
  FstBoundWithData* min;
  FstBoundWithData* max;
} FstStreamBuilder, FstStreamWithStateBuilder;

typedef struct FstRange {
  uint64_t start;
  uint64_t end;
} FstRange;

typedef enum { GE, GT, LE, LT } RangeType;
typedef enum { OneTransNext, OneTrans, AnyTrans, EmptyFinal } State;
typedef enum { Ordered, OutOfOrdered, DuplicateKey } OrderType;

FstBoundWithData* fstBoundStateCreate(FstBound type, FstSlice* data);
bool              fstBoundWithDataExceededBy(FstBoundWithData* bound, FstSlice* slice);
bool              fstBoundWithDataIsEmpty(FstBoundWithData* bound);
bool              fstBoundWithDataIsIncluded(FstBoundWithData* bound);

typedef struct FstOutput {
  bool   null;
  Output out;
} FstOutput;

/*
 *
 * UnFinished node and helper function
 * TODO: simple function name
 */
typedef struct FstUnFinishedNodes {
  SArray* stack;  // <FstBuilderNodeUnfinished> } FstUnFinishedNodes;
} FstUnFinishedNodes;

#define FST_UNFINISHED_NODES_LEN(nodes) taosArrayGetSize(nodes->stack)

FstUnFinishedNodes* fstUnFinishedNodesCreate();
void                fstUnFinishedNodesDestroy(FstUnFinishedNodes* node);
void                fstUnFinishedNodesPushEmpty(FstUnFinishedNodes* nodes, bool isFinal);
void                fstUnFinishedNodesSetRootOutput(FstUnFinishedNodes* node, Output out);
void                fstUnFinishedNodesTopLastFreeze(FstUnFinishedNodes* node, CompiledAddr addr);
void                fstUnFinishedNodesAddSuffix(FstUnFinishedNodes* node, FstSlice bs, Output out);
uint64_t            fstUnFinishedNodesFindCommPrefix(FstUnFinishedNodes* node, FstSlice bs);
FstBuilderNode*     fstUnFinishedNodesPopRoot(FstUnFinishedNodes* nodes);
FstBuilderNode*     fstUnFinishedNodesPopFreeze(FstUnFinishedNodes* nodes, CompiledAddr addr);
FstBuilderNode*     fstUnFinishedNodesPopEmpty(FstUnFinishedNodes* nodes);

uint64_t fstUnFinishedNodesFindCommPrefixAndSetOutput(FstUnFinishedNodes* node, FstSlice bs, Output in, Output* out);

typedef struct FstBuilder {
  FstCountingWriter*  wrt;         // The FST raw data is written directly to `wtr`.
  FstUnFinishedNodes* unfinished;  // The stack of unfinished nodes
  FstRegistry*        registry;    // A map of finished nodes.
  FstSlice            last;        // The last word added
  CompiledAddr        lastAddr;    // The address of the last compiled node
  uint64_t            len;         // num of keys added
} FstBuilder;

FstBuilder* fstBuilderCreate(void* w, FstType ty);

void         fstBuilderDestroy(FstBuilder* b);
void         fstBuilderInsertOutput(FstBuilder* b, FstSlice bs, Output in);
bool         fstBuilderInsert(FstBuilder* b, FstSlice bs, Output in);
void         fstBuilderCompileFrom(FstBuilder* b, uint64_t istate);
void*        fstBuilerIntoInner(FstBuilder* b);
void         fstBuilderFinish(FstBuilder* b);
OrderType    fstBuilderCheckLastKey(FstBuilder* b, FstSlice bs, bool ckDup);
CompiledAddr fstBuilderCompile(FstBuilder* b, FstBuilderNode* bn);

typedef struct FstTransitions {
  FstNode* node;
  FstRange range;
} FstTransitions;

// FstState and relation function

typedef struct FstState {
  State   state;
  uint8_t val;
} FstState;

FstState fstStateCreateFrom(FstSlice* data, CompiledAddr addr);
FstState fstStateCreate(State state);

// compile
void fstStateCompileForOneTransNext(FstCountingWriter* w, CompiledAddr addr, uint8_t inp);
void fstStateCompileForOneTrans(FstCountingWriter* w, CompiledAddr addr, FstTransition* trn);
void fstStateCompileForAnyTrans(FstCountingWriter* w, CompiledAddr addr, FstBuilderNode* node);

// set_comm_input
void fstStateSetCommInput(FstState* state, uint8_t inp);

// comm_input
uint8_t fstStateCommInput(FstState* state, bool* null);

// input_len

uint64_t fstStateInputLen(FstState* state);

// end_addr
uint64_t fstStateEndAddrForOneTransNext(FstState* state, FstSlice* data);
uint64_t fstStateEndAddrForOneTrans(FstState* state, FstSlice* data, PackSizes sizes);
uint64_t fstStateEndAddrForAnyTrans(FstState* state, uint64_t version, FstSlice* date, PackSizes sizes,
                                    uint64_t nTrans);
// input
uint8_t fstStateInput(FstState* state, FstNode* node);
uint8_t fstStateInputForAnyTrans(FstState* state, FstNode* node, uint64_t i);

// trans_addr
CompiledAddr fstStateTransAddr(FstState* state, FstNode* node);
CompiledAddr fstStateTransAddrForAnyTrans(FstState* state, FstNode* node, uint64_t i);

// sizes
PackSizes fstStateSizes(FstState* state, FstSlice* data);
// Output
Output fstStateOutput(FstState* state, FstNode* node);
Output fstStateOutputForAnyTrans(FstState* state, FstNode* node, uint64_t i);

// anyTrans specify function

void fstStateSetFinalState(FstState* state, bool yes);
bool fstStateIsFinalState(FstState* state);
void fstStateSetStateNtrans(FstState* state, uint8_t n);
// state_ntrans
uint8_t  fstStateStateNtrans(FstState* state, bool* null);
uint64_t fstStateTotalTransSize(FstState* state, uint64_t version, PackSizes size, uint64_t nTrans);
uint64_t fstStateTransIndexSize(FstState* state, uint64_t version, uint64_t nTrans);
uint64_t fstStateNtransLen(FstState* state);
uint64_t fstStateNtrans(FstState* state, FstSlice* slice);
Output   fstStateFinalOutput(FstState* state, uint64_t version, FstSlice* date, PackSizes sizes, uint64_t nTrans);
uint64_t fstStateFindInput(FstState* state, FstNode* node, uint8_t b, bool* null);

#define FST_STATE_ONE_TRNAS_NEXT(node) (node->state.state == OneTransNext)
#define FST_STATE_ONE_TRNAS(node) (node->state.state == OneTrans)
#define FST_STATE_ANY_TRANS(node) (node->state.state == AnyTrans)
#define FST_STATE_EMPTY_FINAL(node) (node->state.state == EmptyFinal)

typedef struct FstLastTransition {
  uint8_t inp;
  Output  out;
} FstLastTransition;

/*
 * FstBuilderNodeUnfinished and helper function
 * TODO: simple function name
 */
typedef struct FstBuilderNodeUnfinished {
  FstBuilderNode*    node;
  FstLastTransition* last;
} FstBuilderNodeUnfinished;

void fstBuilderNodeUnfinishedLastCompiled(FstBuilderNodeUnfinished* node, CompiledAddr addr);

void fstBuilderNodeUnfinishedAddOutputPrefix(FstBuilderNodeUnfinished* node, Output out);

/*
 * FstNode and helper function
 */
typedef struct FstNode {
  FstSlice     data;
  uint64_t     version;
  FstState     state;
  CompiledAddr start;
  CompiledAddr end;
  bool         isFinal;
  uint64_t     nTrans;
  PackSizes    sizes;
  Output       finalOutput;
} FstNode;

// If this node is final and has a terminal output value, then it is,  returned.
// Otherwise, a zero output is returned
#define FST_NODE_FINAL_OUTPUT(node) node->finalOutput
// Returns true if and only if this node corresponds to a final or "match",
// state in the finite state transducer.
#define FST_NODE_IS_FINAL(node) node->isFinal
// Returns the number of transitions in this node, The maximum number of
// transitions is 256.
#define FST_NODE_LEN(node) node->nTrans
// Returns true if and only if this node has zero transitions.
#define FST_NODE_IS_EMPTYE(node) (node->nTrans == 0)
// Return the address of this node.
#define FST_NODE_ADDR(node) node->start

FstNode* fstNodeCreate(int64_t version, CompiledAddr addr, FstSlice* data);
void     fstNodeDestroy(FstNode* fstNode);

FstTransitions  fstNodeTransitionIter(FstNode* node);
FstTransitions* fstNodeTransitions(FstNode* node);
bool            fstNodeGetTransitionAt(FstNode* node, uint64_t i, FstTransition* res);
bool            fstNodeGetTransitionAddrAt(FstNode* node, uint64_t i, CompiledAddr* res);
bool            fstNodeFindInput(FstNode* node, uint8_t b, uint64_t* res);

bool fstNodeCompile(FstNode* node, void* w, CompiledAddr lastAddr, CompiledAddr addr, FstBuilderNode* builderNode);

FstSlice fstNodeAsSlice(FstNode* node);

// ops

typedef struct FstIndexedValue {
  uint64_t index;
  uint64_t value;
} FstIndexedValue;

FstLastTransition* fstLastTransitionCreate(uint8_t inp, Output out);
void               fstLastTransitionDestroy(FstLastTransition* trn);

typedef struct FstMeta {
  uint64_t     version;
  CompiledAddr rootAddr;
  FstType      ty;
  uint64_t     len;
  uint32_t     checkSum;
} FstMeta;

typedef struct Fst {
  FstMeta*        meta;
  FstSlice*       data;  //
  FstNode*        root;  //
  TdThreadMutex mtx;
} Fst;

// refactor simple function

Fst* fstCreate(FstSlice* data);
void fstDestroy(Fst* fst);

bool              fstGet(Fst* fst, FstSlice* b, Output* out);
FstNode*          fstGetNode(Fst* fst, CompiledAddr);
FstNode*          fstGetRoot(Fst* fst);
FstType           fstGetType(Fst* fst);
CompiledAddr      fstGetRootAddr(Fst* fst);
Output            fstEmptyFinalOutput(Fst* fst, bool* null);
FstStreamBuilder* fstSearch(Fst* fst, AutomationCtx* ctx);

FstStreamWithStateBuilder* fstSearchWithState(Fst* fst, AutomationCtx* ctx);
// into stream to expand later
StreamWithState* streamBuilderIntoStream(FstStreamBuilder* sb);

bool fstVerify(Fst* fst);

// refactor this function
bool fstBuilderNodeCompileTo(FstBuilderNode* b, FstCountingWriter* wrt, CompiledAddr lastAddr, CompiledAddr startAddr);

typedef struct StreamState {
  FstNode*  node;
  uint64_t  trans;
  FstOutput out;
  void*     autState;
} StreamState;

void streamStateDestroy(void* s);

typedef struct StreamWithState {
  Fst*              fst;
  AutomationCtx*    aut;
  SArray*           inp;
  FstOutput         emptyOutput;
  SArray*           stack;  // <StreamState>
  FstBoundWithData* endAt;
} StreamWithState;

typedef struct StreamWithStateResult {
  FstSlice  data;
  FstOutput out;
  void*     state;
} StreamWithStateResult;

StreamWithStateResult* swsResultCreate(FstSlice* data, FstOutput fOut, void* state);
void                   swsResultDestroy(StreamWithStateResult* result);

typedef void* (*StreamCallback)(void*);
StreamWithState* streamWithStateCreate(Fst* fst, AutomationCtx* automation, FstBoundWithData* min,
                                       FstBoundWithData* max);

void streamWithStateDestroy(StreamWithState* sws);

bool streamWithStateSeekMin(StreamWithState* sws, FstBoundWithData* min);

StreamWithStateResult* streamWithStateNextWith(StreamWithState* sws, StreamCallback callback);

FstStreamBuilder* fstStreamBuilderCreate(Fst* fst, AutomationCtx* aut);

void fstStreamBuilderDestroy(FstStreamBuilder* b);
// set up bound range
// refator, simple code by marco

FstStreamBuilder* fstStreamBuilderRange(FstStreamBuilder* b, FstSlice* val, RangeType type);

#ifdef __cplusplus
}
#endif

#endif
