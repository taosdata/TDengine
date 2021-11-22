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


#include "tarray.h"
#include "index_fst_util.h"
#include "index_fst_registry.h"


typedef struct FstNode FstNode;
#define OUTPUT_PREFIX(a, b) ((a) > (b) ? (b) : (a) 


typedef struct FstRange {
  uint64_t start;
  uint64_t end;
} FstRange;


typedef enum { OneTransNext, OneTrans, AnyTrans, EmptyFinal} State;
typedef enum { Included, Excluded, Unbounded} FstBound; 

typedef uint32_t CheckSummer;


/*
 * 
 * UnFinished node and helper function
 * TODO: simple function name 
 */
typedef struct FstUnFinishedNodes {
  SArray *stack; // <FstBuilderNodeUnfinished> } FstUnFinishedNodes; 
} FstUnFinishedNodes;

#define FST_UNFINISHED_NODES_LEN(nodes) taosArrayGetSize(nodes->stack) 

FstUnFinishedNodes *FstUnFinishedNodesCreate();  
void fstUnFinishedNodesPushEmpty(FstUnFinishedNodes *nodes, bool isFinal);
FstBuilderNode *fstUnFinishedNodesPopRoot(FstUnFinishedNodes *nodes);
FstBuilderNode *fstUnFinishedNodesPopFreeze(FstUnFinishedNodes *nodes, CompiledAddr addr);
FstBuilderNode *fstUnFinishedNodesPopEmpty(FstUnFinishedNodes *nodes);
void fstUnFinishedNodesSetRootOutput(FstUnFinishedNodes *node, Output out); 
void fstUnFinishedNodesTopLastFreeze(FstUnFinishedNodes *node, CompiledAddr addr);
void fstUnFinishedNodesAddSuffix(FstUnFinishedNodes *node, FstSlice bs, Output out);
uint64_t fstUnFinishedNodesFindCommPrefix(FstUnFinishedNodes *node, FstSlice bs);
uint64_t FstUnFinishedNodesFindCommPreifxAndSetOutput(FstUnFinishedNodes *node, FstSlice bs, Output in, Output *out);

typedef struct FstCountingWriter {
  void*    wtr;  // wrap any writer that counts and checksum bytes written 
  uint64_t count; 
  CheckSummer summer;  
} FstCountingWriter;

typedef struct FstBuilder {
  FstCountingWriter  wtr;         // The FST raw data is written directly to `wtr`.  
  FstUnFinishedNodes *unfinished; // The stack of unfinished nodes   
  FstRegistry        registry;    // A map of finished nodes.        
  SArray*            last;        // The last word added 
  CompiledAddr       lastAddr;    // The address of the last compiled node  
  uint64_t           len;         // num of keys added
} FstBuilder;


typedef struct FstTransitions {
  FstNode    *node; 
  FstRange   range;   
} FstTransitions;



typedef struct FstLastTransition {
  uint8_t inp;
  Output  out;
} FstLastTransition;

/* 
 * FstBuilderNodeUnfinished and helper function
 * TODO: simple function name 
 */
typedef struct FstBuilderNodeUnfinished {
  FstBuilderNode *node; 
  FstLastTransition* last; 
} FstBuilderNodeUnfinished;

void fstBuilderNodeUnfinishedLastCompiled(FstBuilderNodeUnfinished *node, CompiledAddr addr);
void fstBuilderNodeUnfinishedAddOutputPrefix(FstBuilderNodeUnfinished *node, CompiledAddr addr);

/*
 * FstNode and helper function  
 */
typedef struct FstNode {
  FstSlice     data;
  uint64_t     version; 
  State        state;
  CompiledAddr start; 
  CompiledAddr end;  
  bool         isFinal;
  uint64_t     nTrans;
  PackSizes    sizes;
  Output      finalOutput;
} FstNode;

// If this node is final and has a terminal output value, then it is,  returned. Otherwise, a zero output is returned 
#define FST_NODE_FINAL_OUTPUT(node) node->finalOutput
// Returns true if and only if this node corresponds to a final or "match", state in the finite state transducer.
#define FST_NODE_IS_FINAL(node) node->isFinal
// Returns the number of transitions in this node, The maximum number of transitions is 256.
#define FST_NODE_LEN(node) node->nTrans
// Returns true if and only if this node has zero transitions.
#define FST_NODE_IS_EMPTYE(node) (node->nTrans == 0)
// Return the address of this node.
#define FST_NODE_ADDR(node) node->start 

FstNode *fstNodeCreate(int64_t version, CompiledAddr addr, FstSlice *data);
FstTransitions fstNodeTransitionIter(FstNode *node);
FstTransitions* fstNodeTransitions(FstNode *node);
bool fstNodeGetTransitionAt(FstNode *node, uint64_t i, FstTransition *res);
bool fstNodeGetTransitionAddrAt(FstNode *node, uint64_t i, CompiledAddr *res);
bool fstNodeFindInput(FstNode *node, uint8_t b, uint64_t *res);
bool fstNodeCompile(FstNode *node, void *w, CompiledAddr lastAddr, CompiledAddr addr, FstBuilderNode *builderNode); 
FstSlice  fstNodeAsSlice(FstNode *node); 



typedef struct FstMeta {
  uint64_t     version;
  CompiledAddr rootAddr;  
  FstType      ty;
  uint64_t     len;
  uint32_t     checkSum;
} FstMeta;

typedef struct Fst {
  FstMeta meta; 
  void    *data;  //  
} Fst;

// ops  

typedef struct FstIndexedValue {
  uint64_t index;
  uint64_t value;
} FstIndexedValue;





#endif
