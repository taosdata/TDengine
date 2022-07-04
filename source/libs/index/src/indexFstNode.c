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
#include "indexFstNode.h"

FstBuilderNode* fstBuilderNodeDefault() {
  FstBuilderNode* bn = taosMemoryMalloc(sizeof(FstBuilderNode));
  bn->isFinal = false;
  bn->finalOutput = 0;
  bn->trans = taosArrayInit(16, sizeof(FstTransition));
  return bn;
}
void fstBuilderNodeDestroy(FstBuilderNode* node) {
  if (node == NULL) {
    return;
  }

  taosArrayDestroy(node->trans);
  taosMemoryFree(node);
}

bool fstBuilderNodeEqual(FstBuilderNode* n1, FstBuilderNode* n2) {
  if (n1 == n2) {
    return true;
  }
  if (n1 == NULL || n2 == NULL) {
    return false;
  }

  if (n1->isFinal != n2->isFinal || n1->finalOutput != n2->finalOutput) {
    return false;
  }
  size_t s1 = n1->trans ? taosArrayGetSize(n1->trans) : 0;
  size_t s2 = n2->trans ? taosArrayGetSize(n2->trans) : 0;
  if (s1 != s2) {
    return false;
  }
  for (size_t i = 0; i < s1; i++) {
    FstTransition* t1 = taosArrayGet(n1->trans, i);
    FstTransition* t2 = taosArrayGet(n2->trans, i);
    if (t1->inp != t2->inp || t1->out != t2->out || t1->addr != t2->addr) {
      return false;
    }
  }

  return true;
}
FstBuilderNode* fstBuilderNodeClone(FstBuilderNode* src) {
  FstBuilderNode* node = taosMemoryMalloc(sizeof(FstBuilderNode));
  if (node == NULL) {
    return NULL;
  }

  //
  size_t  sz = taosArrayGetSize(src->trans);
  SArray* trans = taosArrayInit(sz, sizeof(FstTransition));

  for (size_t i = 0; i < sz; i++) {
    FstTransition* tran = taosArrayGet(src->trans, i);
    taosArrayPush(trans, tran);
  }

  node->trans = trans;
  node->isFinal = src->isFinal;
  node->finalOutput = src->finalOutput;
  return node;
}
// not destroy src, User's bussiness
void fstBuilderNodeCloneFrom(FstBuilderNode* dst, FstBuilderNode* src) {
  if (dst == NULL || src == NULL) {
    return;
  }

  dst->isFinal = src->isFinal;
  dst->finalOutput = src->finalOutput;

  // release free  avoid mem leak
  taosArrayDestroy(dst->trans);
  size_t sz = taosArrayGetSize(src->trans);
  dst->trans = taosArrayInit(sz, sizeof(FstTransition));
  for (size_t i = 0; i < sz; i++) {
    FstTransition* trn = taosArrayGet(src->trans, i);
    taosArrayPush(dst->trans, trn);
  }
}

// bool fstBuilderNodeCompileTo(FstBuilderNode *b, IdxFile *wrt, CompiledAddr lastAddr, CompiledAddr
// startAddr) {

// size_t sz = taosArrayGetSize(b->trans);
// assert(sz < 256);
// if (FST_BUILDER_NODE_IS_FINAL(b)
//    && FST_BUILDER_NODE_TRANS_ISEMPTY(b)
//    && FST_BUILDER_NODE_FINALOUTPUT_ISZERO(b)) {
//  return true;
//} else if (sz != 1 || b->isFinal) {
//  // AnyTrans->Compile(w, addr, node);
//} else {
//  FstTransition *tran = taosArrayGet(b->trans, 0);
//  if (tran->addr == lastAddr && tran->out == 0) {
//     //OneTransNext::compile(w, lastAddr, tran->inp);
//     return true;
//  } else {
//    //OneTrans::Compile(w, lastAddr, *tran);
//     return true;
//  }
//}
// return true;
//}
