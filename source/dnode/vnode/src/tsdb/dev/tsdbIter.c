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

#include "inc/tsdbIter.h"

// STsdbIter ================
struct STsdbIter {
  struct {
    bool noMoreData;
  } ctx[1];
  SRowInfo    row[1];
  SRBTreeNode node[1];
  SBlockData  bData[1];
  int32_t     iRow;
  // TODO
};

int32_t tsdbIterNext(STsdbIter *iter) {
  // TODO
  return 0;
}

static int32_t tsdbIterSkipTableData(STsdbIter *iter) {
  // TODO
  return 0;
}

static int32_t tsdbIterCmprFn(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  STsdbIter *iter1 = TCONTAINER_OF(n1, STsdbIter, node);
  STsdbIter *iter2 = TCONTAINER_OF(n2, STsdbIter, node);
  return tRowInfoCmprFn(&iter1->row, &iter2->row);
}

// SIterMerger ================
struct SIterMerger {
  STsdbIter *iter;
  SRBTree    iterTree[1];
};

int32_t tsdbIterMergerInit(const TTsdbIterArray *iterArray, SIterMerger **merger) {
  STsdbIter *iter;

  merger[0] = taosMemoryCalloc(1, sizeof(*merger[0]));
  if (!merger[0]) return TSDB_CODE_OUT_OF_MEMORY;

  tRBTreeCreate(merger[0]->iterTree, tsdbIterCmprFn);
  TARRAY2_FOREACH(iterArray, iter) {
    if (iter->ctx->noMoreData) continue;
    SRBTreeNode *node = tRBTreePut(merger[0]->iterTree, iter->node);
    ASSERT(node);
  }

  return tsdbIterMergerNext(merger[0]);
}

int32_t tsdbIterMergerClear(SIterMerger **merger) {
  taosMemoryFree(merger[0]);
  return 0;
}

int32_t tsdbIterMergerNext(SIterMerger *merger) {
  int32_t      code;
  int32_t      c;
  SRBTreeNode *node;

  if (merger->iter) {
    code = tsdbIterNext(merger->iter);
    if (code) return code;

    if (merger->iter->ctx->noMoreData) {
      merger->iter = NULL;
    } else if ((node = tRBTreeMin(merger->iterTree))) {
      c = tsdbIterCmprFn(merger->iter->node, node);
      ASSERT(c);
      if (c > 0) {
        node = tRBTreePut(merger->iterTree, merger->iter->node);
        merger->iter = NULL;
        ASSERT(node);
      }
    }
  }

  if (!merger->iter && (node = tRBTreeDropMin(merger->iterTree))) {
    merger->iter = TCONTAINER_OF(node, STsdbIter, node);
  }

  return 0;
}

SRowInfo *tsdbIterMergerGet(SIterMerger *merger) { return merger->iter ? merger->iter->row : NULL; }

int32_t tsdbIterMergerSkipTableData(SIterMerger *merger, const TABLEID *tbid) {
  int32_t      code;
  int32_t      c;
  SRBTreeNode *node;

  while (merger->iter && tbid->suid == merger->iter->row->suid && tbid->uid == merger->iter->row->uid) {
    int32_t code = tsdbIterSkipTableData(merger->iter);
    if (code) return code;

    if (merger->iter->ctx->noMoreData) {
      merger->iter = NULL;
      c = tsdbIterCmprFn(merger->iter->node, node);
      ASSERT(c);
      if (c > 0) {
        node = tRBTreePut(merger->iterTree, merger->iter->node);
        merger->iter = NULL;
        ASSERT(node);
      }
    }

    if (!merger->iter && (node = tRBTreeDropMin(merger->iterTree))) {
      merger->iter = TCONTAINER_OF(node, STsdbIter, node);
    }
  }
  return 0;
}