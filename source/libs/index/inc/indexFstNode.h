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

#ifndef __INDEX_FST_NODE_H__
#define __INDEX_FST_NODE_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "indexFstFile.h"
#include "indexFstUtil.h"
#include "indexInt.h"

#define FST_BUILDER_NODE_IS_FINAL(bn)           (bn->isFinal)
#define FST_BUILDER_NODE_TRANS_ISEMPTY(bn)      (taosArrayGetSize(bn->trans) == 0)
#define FST_BUILDER_NODE_FINALOUTPUT_ISZERO(bn) (bn->finalOutput == 0)

typedef struct FstTransition {
  uint8_t      inp;   // The byte input associated with this transition.
  Output       out;   // The output associated with this transition
  CompiledAddr addr;  // The address of the node that this transition points to
} FstTransition;

typedef struct FstBuilderNode {
  bool    isFinal;
  Output  finalOutput;
  SArray* trans;  // <FstTransition>
} FstBuilderNode;

FstBuilderNode* fstBuilderNodeDefault();

FstBuilderNode* fstBuilderNodeClone(FstBuilderNode* src);

void fstBuilderNodeCloneFrom(FstBuilderNode* dst, FstBuilderNode* src);

// bool fstBuilderNodeCompileTo(FstBuilderNode *b, IdxFile' *wrt,
// CompiledAddr lastAddr, CompiledAddr startAddr);
bool fstBuilderNodeEqual(FstBuilderNode* n1, FstBuilderNode* n2);

void fstBuilderNodeDestroy(FstBuilderNode* node);

#ifdef __cplusplus
}
#endif

#endif
