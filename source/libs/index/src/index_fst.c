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

#include "index_fst.h"


FstUnFinishedNodes *fstUnFinishedNodesCreate() {
  FstUnFinishedNodes *nodes = malloc(sizeof(FstUnFinishedNodes));
  if (nodes == NULL) { return NULL; }

  nodes->stack = (SArray *)taosArrayInit(64, sizeof(FstBuilderNodeUnfinished));
  fstUnFinishedNodesPushEmpty(nodes, false);
  return nodes;
}
void fstUnFinishedNodesPushEmpty(FstUnFinishedNodes *nodes, bool isFinal) {
  FstBuilderNode *node = malloc(sizeof(FstBuilderNode));
  node->isFinal     = isFinal;
  node->finalOutput = 0;
  node->trans       = NULL;

  FstBuilderNodeUnfinished un = {.node = node, .last = NULL}; 
  taosArrayPush(nodes->stack, &un);
  
}
FstBuilderNode *fstUnFinishedNodesPopRoot(FstUnFinishedNodes *nodes) {
  assert(taosArrayGetSize(nodes->stack) == 1);

  FstBuilderNodeUnfinished *un = taosArrayPop(nodes->stack);
  assert(un->last == NULL); 
  return un->node;  
}

FstBuilderNode *fstUnFinishedNodesPopFreeze(FstUnFinishedNodes *nodes, CompiledAddr addr) {
  FstBuilderNodeUnfinished *un = taosArrayPop(nodes->stack);
  fstBuilderNodeUnfinishedLastCompiled(un, addr);
  free(un->last); // TODO add func FstLastTransitionFree()
  return un->node; 
}

FstBuilderNode *fstUnFinishedNodesPopEmpty(FstUnFinishedNodes *nodes) {
  FstBuilderNodeUnfinished *un = taosArrayPop(nodes->stack);
  assert(un->last == NULL); 
  return un->node;  
  
}
void fstUnFinishedNodesSetRootOutput(FstUnFinishedNodes *nodes, Output out) {
  FstBuilderNodeUnfinished *un = taosArrayGet(nodes->stack, 0);
  un->node->isFinal     = true;
  un->node->finalOutput = out;
  //un->node->trans       = NULL;  
} 
void fstUnFinishedNodesTopLastFreeze(FstUnFinishedNodes *nodes, CompiledAddr addr) {
  size_t sz = taosArrayGetSize(nodes->stack) - 1; 
  FstBuilderNodeUnfinished *un = taosArrayGet(nodes->stack, sz);
  fstBuilderNodeUnfinishedLastCompiled(un, addr);
}
void fstUnFinishedNodesAddSuffix(FstUnFinishedNodes *nodes, FstSlice bs, Output out) {
  FstSlice *s = &bs;
  if (s->data == NULL || s->dLen == 0 || s->start > s->end) {
    return;
  }
  size_t sz = taosArrayGetSize(nodes->stack) - 1; 
  FstBuilderNodeUnfinished *un = taosArrayGet(nodes->stack, sz);
  assert(un->last == NULL);

  
  FstLastTransition *trn = malloc(sizeof(FstLastTransition)); 
  trn->inp = s->data[s->start]; 
  trn->out = out;
  
  un->last = trn; 

  for (uint64_t i = s->start; i <= s->end; i++) {
    FstBuilderNode *n = malloc(sizeof(FstBuilderNode));
    n->isFinal     = false;
    n->finalOutput = 0;
    n->trans       = NULL;
    
    FstLastTransition *trn = malloc(sizeof(FstLastTransition)); 
    trn->inp = s->data[i];
    trn->out = out; 

    FstBuilderNodeUnfinished un = {.node = n, .last = trn}; 
    taosArrayPush(nodes->stack, &un); 
  }
  fstUnFinishedNodesPushEmpty(nodes, true);  
}


uint64_t fstUnFinishedNodesFindCommPrefix(FstUnFinishedNodes *node, FstSlice bs) {
  FstSlice *s = &bs;

  size_t lsz = (size_t)(s->end - s->start + 1);          // data len 
  size_t ssz = taosArrayGetSize(node->stack);  // stack size
    
  uint64_t count = 0;
  for (size_t i = 0; i < ssz && i < lsz; i++) {
    FstBuilderNodeUnfinished *un = taosArrayGet(node->stack, i); 
    if (un->last->inp == s->data[s->start + i]) {
      count++;
    } else {
      break;
    } 
  }
  return count;
}
uint64_t FstUnFinishedNodesFindCommPrefixAndSetOutput(FstUnFinishedNodes *node, FstSlice bs, Output in, Output *out) {
  FstSlice *s = &bs;

  size_t lsz = (size_t)(s->end - s->start + 1);          // data len 
  size_t ssz = taosArrayGetSize(node->stack);  // stack size

  uint64_t res = 0;
  for (size_t i = 0; i < lsz && i < ssz; i++) {
    FstBuilderNodeUnfinished *un = taosArrayGet(node->stack, i);

    FstLastTransition *last = un->last; 
    if (last->inp == s->data[s->start + i]) {
      uint64_t commPrefix = last->out;      
      uint64_t addPrefix  = last->out - commPrefix; 
      out = out - commPrefix; 
      last->out = commPrefix;
      if (addPrefix != 0) {
        fstBuilderNodeUnfinishedAddOutputPrefix(un, addPrefix);  
      }
    } else {
      break;
    }
  }   
  return res;
} 

// fst node function 



FstNode *fstNodeCreate(int64_t version, CompiledAddr addr, FstSlice *slice) {
  FstNode *n = (FstNode *)malloc(sizeof(FstNode)); 
  if (n == NULL) { return NULL; }

  if (addr == EMPTY_ADDRESS) {
     n->data    = fstSliceCreate(NULL, 0);   
     n->version = version;
     n->state   = EmptyFinal;  
     n->start   = EMPTY_ADDRESS;
     n->end     = EMPTY_ADDRESS;  
     n->isFinal = true; 
     n->nTrans  = 0;
     n->sizes   = 0;  
     n->finalOutput = 0;  
   }  
   uint8_t v = slice->data[addr]; 
   uint8_t s = (v & 0b11000000) >> 6;
   if (s == 0b11) { // oneTransNext
     n->data    = fstSliceCopy(slice, 0, addr);     
     n->version = version;
     n->state   = OneTransNext;  
     n->start   = addr;
     n->end     = addr; //? s.end_addr(data); 
     n->isFinal = false;
     n->sizes   = 0;
     n->nTrans  = 0;
     n->finalOutput = 0;
   } else if (v == 0b10) { // oneTrans 
     uint64_t sz; // fetch sz from addr
     n->data    = fstSliceCopy(slice, 0, addr); 
     n->version = version; 
     n->state   = OneTrans; 
     n->start   = addr;
     n->end     = addr; // s.end_addr(data, sz);
     n->isFinal = false; 
     n->nTrans  = 1; 
     n->sizes   = sz;   
     n->finalOutput = 0; 
   } else {   // anyTrans
     uint64_t sz;    // s.sizes(data)
     uint32_t nTrans; // s.ntrans(data)  
     n->data    = *slice; 
     n->version = version;
     n->state   = AnyTrans;
     n->start   = addr;
     n->end     = addr; // s.end_addr(version, data, sz, ntrans);
     n->isFinal = false; // s.is_final_state();
     n->nTrans  = nTrans;
     n->sizes   = sz;
     n->finalOutput = 0; // s.final_output(version, data, sz, ntrans);
   } 
   return n; 
}
FstTransitions* fstNodeTransitions(FstNode *node) {
  FstTransitions *t = malloc(sizeof(FstTransitions));
  if (NULL == t) {
    return NULL; 
  }
  FstRange range = {.start = 0, .end = FST_NODE_LEN(node)};
  t->node  = node;
  t->range = range;
  return t; 
} 
bool fstNodeGetTransitionAt(FstNode *node, uint64_t i, FstTransition *res) {
  bool s = true;
  if (node->state == OneTransNext) {

  } else if (node->state == OneTrans) {
    
  } else if (node->state == AnyTrans) {

  } else {
    s = false;
  }
  return s;
} 

bool fstNodeGetTransitionAddrAt(FstNode *node, uint64_t i, CompiledAddr *res) {
  bool s = true;
  if (node->state == OneTransNext) {

  } else if (node->state == OneTrans) {

  } else if (node->state == AnyTrans) {

  } else if (node->state == EmptyFinal){
    s = false;
  }
  return s;
}

bool fstNodeFindInput(FstNode *node, uint8_t b, uint64_t *res) {
  bool s = true;
  uint8_t input; // s.input
  if (node->state == OneTransNext) {
    if (b == input) { *res = 0; } 
    else { return s ; }
  } else if (node->state == OneTrans) {
    if (b == input) { *res = 0; }
    else {return s;}
  } else if (node->state == AnyTrans) {
     
  } else if (node->state == EmptyFinal) {
    s = false;
  } 
  return s;
} 

bool fstNodeCompile(FstNode *node, void *w, CompiledAddr lastAddr, CompiledAddr addr, FstBuilderNode *builderNode) {
  size_t sz = taosArrayGetSize(builderNode->trans);  
  assert(sz < 256);
  if (sz == 0 && builderNode->isFinal && builderNode->finalOutput == 0) {
    return true; 
  } else if (sz != 1 || builderNode->isFinal) {
    // AnyTrans->Compile(w, addr, node);
  } else {
    FstTransition *tran = taosArrayGet(builderNode->trans, 0);   
    if (tran->addr == lastAddr && tran->out == 0) {
       //OneTransNext::compile(w, lastAddr, tran->inp);
       return true;
    } else {
      //OneTrans::Compile(w, lastAddr, *tran);
       return true;
    } 
  } 
  return true; 
} 




FstBuilder *fstBuilderCreate(void *w, FstType ty) {
  FstBuilder *b = malloc(sizeof(FstBuilder));  
  if (NULL == b) { return b; }

  FstCountingWriter wtr = {.wtr = w, .count = 0, .summer = 0};  
  b->wtr = wtr;
  b->unfinished = malloc(sizeof(FstUnFinishedNodes));   
  return b;
  
}
FstSlice fstNodeAsSlice(FstNode *node) {
  FstSlice *slice = &node->data; 
  FstSlice s = fstSliceCopy(slice, slice->end, slice->dLen - 1);   
  return s; 
}


