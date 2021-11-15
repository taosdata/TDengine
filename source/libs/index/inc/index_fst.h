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

#ifndef _INDEX_FST_H_
#define _INDEX_FST_H_
#include "index_fst.h"
#include "tarray.h"

typedef FstType      uint64_t;
typedef CompiledAddr uint64_t;
typedef Output       uint64_t;  
typedef PackSizes    uint8_t;


//A sentinel value used to indicate an empty final state
const CompileAddr EMPTY_ADDRESS  = 0;
/// A sentinel value used to indicate an invalid state.
const CompileAddr NONE_ADDRESS   = 1;

// This version number is written to every finite state transducer created by
// this crate. When a finite state transducer is read, its version number is
// checked against this value.
const uint64_t    version        = 3;
// The threshold (in number of transitions) at which an index is created for                                   
// a node's transitions. This speeds up lookup time at the expense of FST size 

const uint64_t TRANS_INDEX_THRESHOLD = 32;

typedef struct FstRange {
  uint64_t start;
  uint64_t end;
} FstRange;

enum State { OneTransNext, OneTrans, AnyTrans, EmptyFinal};
enum FstBound { Included, Excluded, Unbounded}; 

typedef struct CheckSummer {
  uint32_t sum;
};


typedef struct FstBuilder {
  FstCountingWriter  wtr;       // The FST raw data is written directly to `wtr`.  
  FstUnFinishedNodes unfinished // The stack of unfinished nodes   
  Registry           registry   // A map of finished nodes.        
  SArray*            last       // The last word added 
  CompiledAddr       lastAddr   // The address of the last compiled node  
  uint64_t           len        // num of keys added
} FstBuilder;

typedef struct FstCountingWriter {
  void*    wtr;  // wrap any writer that counts and checksum bytes written 
  uint64_t count; 
  CheckSummer summer;  
};




typedef struct FstTransition {
  uint8_t      inp;  //The byte input associated with this transition.
  Output       out;  //The output associated with this transition 
  CompiledAddr addr; //The address of the node that this transition points to
} FstTransition;

typedef struct FstTransitions {
  FstNode    *node; 
  FstRange   range;   
} FstTransitions;

typedef struct FstUnFinishedNodes {
  SArray *stack; // <FstBuilderNodeUnfinished>
} FstUnFinishedNodes; 

typedef struct FstBuilderNode {
  bool isFinal; 
  Output finalOutput;  
  SArray *trans;  // <FstTransition>
} FstBuilderNode; 



typedef struct FstLastTransition {
  uint8_t inp;
  Output  out;
} FstLastTransition;

typedef struct FstBuilderNodeUnfinished {
  FstBuilderNode    node; 
  FstLastTransition last; 
} FstBuilderNodeUnfinished;

typedef struct FstNode {
  uint8_t*     data;
  uint64_t     version; 
  State        state;
  CompiledAddr start; 
  CompiledAddr end;  
  bool         isFinal;
  uint64_t     nTrans;
  PackSizes    sizes;
  Output      finalOutput;
} FstNode;

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
};

// ops 

typedef struct FstIndexedValue {
  uint64_t index;
  uint64_t value;
};

// relate to Regist 
typedef struct FstRegistry {
  SArray *table;      // <Registtry cell>
  uint64_t tableSize; // num of rows
  uint64_t mruSize;   // num of columns    
} FstRegistry; 

typedef struct FstRegistryCache {
  SArray *cells;  //  <RegistryCell>
} FstRegistryCache;

typedef struct FstRegistryCell {
  CompiledAddr addr; 
  FstBuilderNode *node; 
} FstRegistryCell;

enum FstRegistryEntry {Found, NotFound, Rejected};

FstNode *fstNodeCreate(int64_t version, CompiledAddr addr, uint8_t *data);
FstTransitions fstNodeTransitionIter(FstNode *node);
FstTransition  fstNodeGetTransitionAt(FstNode *node, uint64_t i);
CompiledAddr   fstNodeGetTransitionAddr(FstNode *node, uint64_t i);
int64_t        fstNodeFindInput(FstNode *node, int8_t b);
Output         fstNodeGetFinalOutput(FstNode *node);
void*          fstNodeCompile(FstNode *node, void *w, CompiledAddr lastAddr, CompiledArr addr, FstBuilderNode *builderNode); 




#endif
