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

#include "tsdbIter.h"

// STsdbIter ================
struct STsdbIter {
  EIterType type;
  bool      noMoreData;
  bool      filterByVersion;
  int64_t   range[2];
  union {
    SRowInfo    row[1];
    STombRecord record[1];
  };
  SRBTreeNode node[1];
  union {
    struct {
      SSttFileReader     *reader;
      const TSttBlkArray *sttBlkArray;
      int32_t             sttBlkArrayIdx;
      SBlockData          blockData[1];
      int32_t             blockDataIdx;
    } sttData[1];
    struct {
      SDataFileReader     *reader;
      const TBrinBlkArray *brinBlkArray;
      int32_t              brinBlkArrayIdx;
      SBrinBlock           brinBlock[1];
      int32_t              brinBlockIdx;
      SBlockData           blockData[1];
      int32_t              blockDataIdx;
    } dataData[1];
    struct {
      SMemTable  *memt;
      TSDBKEY     from[1];
      SRBTreeIter iter[1];
      STbData    *tbData;
      STbDataIter tbIter[1];
    } memtData[1];
    struct {
      SSttFileReader      *reader;
      const TTombBlkArray *tombBlkArray;
      int32_t              tombBlkArrayIdx;
      STombBlock           tombBlock[1];
      int32_t              tombBlockIdx;
    } sttTomb[1];
    struct {
      SDataFileReader     *reader;
      const TTombBlkArray *tombBlkArray;
      int32_t              tombBlkArrayIdx;
      STombBlock           tombBlock[1];
      int32_t              tombBlockIdx;
    } dataTomb[1];
    struct {
      SMemTable  *memt;
      SRBTreeIter rbtIter[1];
      STbData    *tbData;
      SDelData   *delData;
    } memtTomb[1];
  };
};

static int32_t tsdbSttIterNext(STsdbIter *iter, const TABLEID *tbid) {
  while (!iter->noMoreData) {
    for (; iter->sttData->blockDataIdx < iter->sttData->blockData->nRow; iter->sttData->blockDataIdx++) {
      int64_t version = iter->sttData->blockData->aVersion[iter->sttData->blockDataIdx];

      if (iter->filterByVersion && (version < iter->range[0] || version > iter->range[1])) {
        continue;
      }

      iter->row->suid = iter->sttData->blockData->suid;
      iter->row->uid = iter->sttData->blockData->uid ? iter->sttData->blockData->uid
                                                     : iter->sttData->blockData->aUid[iter->sttData->blockDataIdx];

      if (tbid && iter->row->suid == tbid->suid && iter->row->uid == tbid->uid) {
        continue;
      }

      iter->row->row = tsdbRowFromBlockData(iter->sttData->blockData, iter->sttData->blockDataIdx);
      iter->sttData->blockDataIdx++;
      goto _exit;
    }

    if (iter->sttData->sttBlkArrayIdx >= TARRAY2_SIZE(iter->sttData->sttBlkArray)) {
      iter->noMoreData = true;
      break;
    }

    for (; iter->sttData->sttBlkArrayIdx < TARRAY2_SIZE(iter->sttData->sttBlkArray); iter->sttData->sttBlkArrayIdx++) {
      const SSttBlk *sttBlk = TARRAY2_GET_PTR(iter->sttData->sttBlkArray, iter->sttData->sttBlkArrayIdx);

      if (iter->filterByVersion && (sttBlk->maxVer < iter->range[0] || sttBlk->minVer > iter->range[1])) {
        continue;
      }

      if (tbid && tbid->suid == sttBlk->suid && tbid->uid == sttBlk->minUid && tbid->uid == sttBlk->maxUid) {
        continue;
      }

      int32_t code = tsdbSttFileReadBlockData(iter->sttData->reader, sttBlk, iter->sttData->blockData);
      if (code) return code;

      iter->sttData->blockDataIdx = 0;
      iter->sttData->sttBlkArrayIdx++;
      break;
    }
  }

_exit:
  return 0;
}

static int32_t tsdbDataIterNext(STsdbIter *iter, const TABLEID *tbid) {
  int32_t code;

  while (!iter->noMoreData) {
    for (;;) {
      // SBlockData
      for (; iter->dataData->blockDataIdx < iter->dataData->blockData->nRow; iter->dataData->blockDataIdx++) {
        int64_t version = iter->dataData->blockData->aVersion[iter->dataData->blockDataIdx];
        if (iter->filterByVersion && (version < iter->range[0] || version > iter->range[1])) {
          continue;
        }

        if (tbid && tbid->suid == iter->dataData->blockData->suid && tbid->uid == iter->dataData->blockData->uid) {
          iter->dataData->blockDataIdx = iter->dataData->blockData->nRow;
          break;
        }

        iter->row->row = tsdbRowFromBlockData(iter->dataData->blockData, iter->dataData->blockDataIdx);
        iter->dataData->blockDataIdx++;
        goto _exit;
      }

      // SBrinBlock
      if (iter->dataData->brinBlockIdx >= BRIN_BLOCK_SIZE(iter->dataData->brinBlock)) {
        break;
      }

      for (; iter->dataData->brinBlockIdx < BRIN_BLOCK_SIZE(iter->dataData->brinBlock);
           iter->dataData->brinBlockIdx++) {
        SBrinRecord record[1];
        tBrinBlockGet(iter->dataData->brinBlock, iter->dataData->brinBlockIdx, record);

        if (iter->filterByVersion && (record->maxVer < iter->range[0] || record->minVer > iter->range[1])) {
          continue;
        }

        if (tbid && tbid->suid == record->suid && tbid->uid == record->uid) {
          continue;
        }

        iter->row->suid = record->suid;
        iter->row->uid = record->uid;

        code = tsdbDataFileReadBlockData(iter->dataData->reader, record, iter->dataData->blockData);
        if (code) return code;

        iter->dataData->blockDataIdx = 0;
        iter->dataData->brinBlockIdx++;
        break;
      }
    }

    if (iter->dataData->brinBlkArrayIdx >= TARRAY2_SIZE(iter->dataData->brinBlkArray)) {
      iter->noMoreData = true;
      break;
    }

    for (; iter->dataData->brinBlkArrayIdx < TARRAY2_SIZE(iter->dataData->brinBlkArray);
         iter->dataData->brinBlkArrayIdx++) {
      const SBrinBlk *brinBlk = TARRAY2_GET_PTR(iter->dataData->brinBlkArray, iter->dataData->brinBlkArrayIdx);

      if (iter->filterByVersion && (brinBlk->maxVer < iter->range[0] || brinBlk->minVer > iter->range[1])) {
        continue;
      }

      if (tbid && tbid->uid == brinBlk->minTbid.uid && tbid->uid == brinBlk->maxTbid.uid) {
        continue;
      }

      code = tsdbDataFileReadBrinBlock(iter->dataData->reader, brinBlk, iter->dataData->brinBlock);
      if (code) return code;

      iter->dataData->brinBlockIdx = 0;
      iter->dataData->brinBlkArrayIdx++;
      break;
    }
  }

_exit:
  return 0;
}

static int32_t tsdbMemTableIterNext(STsdbIter *iter, const TABLEID *tbid) {
  SRBTreeNode *node;

  while (!iter->noMoreData) {
    for (TSDBROW *row; iter->memtData->tbData && (row = tsdbTbDataIterGet(iter->memtData->tbIter));) {
      if (tbid && tbid->suid == iter->memtData->tbData->suid && tbid->uid == iter->memtData->tbData->uid) {
        iter->memtData->tbData = NULL;
        break;
      }

      if (iter->filterByVersion) {
        int64_t version = TSDBROW_VERSION(row);
        if (version < iter->range[0] || version > iter->range[1]) {
          continue;
        }
      }

      iter->row->row = row[0];

      tsdbTbDataIterNext(iter->memtData->tbIter);
      goto _exit;
    }

    for (;;) {
      node = tRBTreeIterNext(iter->memtData->iter);
      if (!node) {
        iter->noMoreData = true;
        goto _exit;
      }

      iter->memtData->tbData = TCONTAINER_OF(node, STbData, rbtn);
      if (tbid && tbid->suid == iter->memtData->tbData->suid && tbid->uid == iter->memtData->tbData->uid) {
        continue;
      } else {
        iter->row->suid = iter->memtData->tbData->suid;
        iter->row->uid = iter->memtData->tbData->uid;
        tsdbTbDataIterOpen(iter->memtData->tbData, iter->memtData->from, 0, iter->memtData->tbIter);
        break;
      }
    }
  }

_exit:
  return 0;
}

static int32_t tsdbDataTombIterNext(STsdbIter *iter, const TABLEID *tbid) {
  while (!iter->noMoreData) {
    for (; iter->dataTomb->tombBlockIdx < TOMB_BLOCK_SIZE(iter->dataTomb->tombBlock); iter->dataTomb->tombBlockIdx++) {
      iter->record->suid = TARRAY2_GET(iter->dataTomb->tombBlock->suid, iter->dataTomb->tombBlockIdx);
      iter->record->uid = TARRAY2_GET(iter->dataTomb->tombBlock->uid, iter->dataTomb->tombBlockIdx);
      iter->record->version = TARRAY2_GET(iter->dataTomb->tombBlock->version, iter->dataTomb->tombBlockIdx);

      if (iter->filterByVersion && (iter->record->version < iter->range[0] || iter->record->version > iter->range[1])) {
        continue;
      }

      if (tbid && iter->record->suid == tbid->suid && iter->record->uid == tbid->uid) {
        continue;
      }

      iter->record->skey = TARRAY2_GET(iter->dataTomb->tombBlock->skey, iter->dataTomb->tombBlockIdx);
      iter->record->ekey = TARRAY2_GET(iter->dataTomb->tombBlock->ekey, iter->dataTomb->tombBlockIdx);
      iter->dataTomb->tombBlockIdx++;
      goto _exit;
    }

    if (iter->dataTomb->tombBlkArrayIdx >= TARRAY2_SIZE(iter->dataTomb->tombBlkArray)) {
      iter->noMoreData = true;
      goto _exit;
    }

    for (; iter->dataTomb->tombBlkArrayIdx < TARRAY2_SIZE(iter->dataTomb->tombBlkArray);
         iter->dataTomb->tombBlkArrayIdx++) {
      const STombBlk *tombBlk = TARRAY2_GET_PTR(iter->dataTomb->tombBlkArray, iter->dataTomb->tombBlkArrayIdx);

      if (tbid && tbid->suid == tombBlk->minTbid.suid && tbid->uid == tombBlk->minTbid.uid &&
          tbid->suid == tombBlk->maxTbid.suid && tbid->uid == tombBlk->maxTbid.uid) {
        continue;
      }

      int32_t code = tsdbDataFileReadTombBlock(iter->dataTomb->reader, tombBlk, iter->dataTomb->tombBlock);
      if (code) return code;

      iter->dataTomb->tombBlockIdx = 0;
      iter->dataTomb->tombBlkArrayIdx++;
      break;
    }
  }

_exit:
  return 0;
}

static int32_t tsdbMemTombIterNext(STsdbIter *iter, const TABLEID *tbid) {
  while (!iter->noMoreData) {
    for (; iter->memtTomb->delData;) {
      if (tbid && tbid->uid == iter->memtTomb->tbData->uid) {
        iter->memtTomb->delData = NULL;
        break;
      }

      if (iter->filterByVersion &&
          (iter->memtTomb->delData->version < iter->range[0] || iter->memtTomb->delData->version > iter->range[1])) {
        continue;
      }

      iter->record->suid = iter->memtTomb->tbData->suid;
      iter->record->uid = iter->memtTomb->tbData->uid;
      iter->record->version = iter->memtTomb->delData->version;
      iter->record->skey = iter->memtTomb->delData->sKey;
      iter->record->ekey = iter->memtTomb->delData->eKey;

      iter->memtTomb->delData = iter->memtTomb->delData->pNext;
      goto _exit;
    }

    for (;;) {
      SRBTreeNode *node = tRBTreeIterNext(iter->memtTomb->rbtIter);
      if (node == NULL) {
        iter->noMoreData = true;
        goto _exit;
      }

      iter->memtTomb->tbData = TCONTAINER_OF(node, STbData, rbtn);
      if (tbid && tbid->uid == iter->memtTomb->tbData->uid) {
        continue;
      } else {
        iter->memtTomb->delData = iter->memtTomb->tbData->pHead;
        break;
      }
    }
  }

_exit:
  return 0;
}

static int32_t tsdbSttIterOpen(STsdbIter *iter) {
  int32_t code;

  code = tsdbSttFileReadSttBlk(iter->sttData->reader, &iter->sttData->sttBlkArray);
  if (code) return code;

  if (TARRAY2_SIZE(iter->sttData->sttBlkArray) == 0) {
    iter->noMoreData = true;
    return 0;
  }

  iter->sttData->sttBlkArrayIdx = 0;
  code = tBlockDataCreate(iter->sttData->blockData);
  if (code) return code;
  iter->sttData->blockDataIdx = 0;

  return tsdbSttIterNext(iter, NULL);
}

static int32_t tsdbDataIterOpen(STsdbIter *iter) {
  int32_t code;

  // SBrinBlk
  code = tsdbDataFileReadBrinBlk(iter->dataData->reader, &iter->dataData->brinBlkArray);
  if (code) return code;

  if (TARRAY2_SIZE(iter->dataData->brinBlkArray) == 0) {
    iter->noMoreData = true;
    return 0;
  }

  iter->dataData->brinBlkArrayIdx = 0;

  // SBrinBlock
  tBrinBlockInit(iter->dataData->brinBlock);
  iter->dataData->brinBlockIdx = 0;

  // SBlockData
  code = tBlockDataCreate(iter->dataData->blockData);
  if (code) return code;
  iter->dataData->blockDataIdx = 0;

  return tsdbDataIterNext(iter, NULL);
}

static int32_t tsdbMemTableIterOpen(STsdbIter *iter) {
  if (iter->memtData->memt->nRow == 0) {
    iter->noMoreData = true;
    return 0;
  }

  iter->memtData->iter[0] = tRBTreeIterCreate(iter->memtData->memt->tbDataTree, 1);
  return tsdbMemTableIterNext(iter, NULL);
}

static int32_t tsdbSttIterClose(STsdbIter *iter) {
  tBlockDataDestroy(iter->sttData->blockData);
  return 0;
}

static int32_t tsdbDataTombIterOpen(STsdbIter *iter) {
  int32_t code;

  code = tsdbDataFileReadTombBlk(iter->dataTomb->reader, &iter->dataTomb->tombBlkArray);
  if (code) return code;

  if (TARRAY2_SIZE(iter->dataTomb->tombBlkArray) == 0) {
    iter->noMoreData = true;
    return 0;
  }
  iter->dataTomb->tombBlkArrayIdx = 0;

  tTombBlockInit(iter->dataTomb->tombBlock);
  iter->dataTomb->tombBlockIdx = 0;

  return tsdbDataTombIterNext(iter, NULL);
}

static int32_t tsdbMemTombIterOpen(STsdbIter *iter) {
  int32_t code;

  if (iter->memtTomb->memt->nDel == 0) {
    iter->noMoreData = true;
    return 0;
  }

  iter->memtTomb->rbtIter[0] = tRBTreeIterCreate(iter->memtTomb->memt->tbDataTree, 1);
  return tsdbMemTombIterNext(iter, NULL);
}

static int32_t tsdbDataIterClose(STsdbIter *iter) {
  tBrinBlockDestroy(iter->dataData->brinBlock);
  tBlockDataDestroy(iter->dataData->blockData);
  return 0;
}

static int32_t tsdbMemTableIterClose(STsdbIter *iter) { return 0; }

static int32_t tsdbSttTombIterNext(STsdbIter *iter, const TABLEID *tbid) {
  while (!iter->noMoreData) {
    for (; iter->sttTomb->tombBlockIdx < TOMB_BLOCK_SIZE(iter->sttTomb->tombBlock); iter->sttTomb->tombBlockIdx++) {
      iter->record->suid = TARRAY2_GET(iter->sttTomb->tombBlock->suid, iter->sttTomb->tombBlockIdx);
      iter->record->uid = TARRAY2_GET(iter->sttTomb->tombBlock->uid, iter->sttTomb->tombBlockIdx);
      iter->record->version = TARRAY2_GET(iter->sttTomb->tombBlock->version, iter->sttTomb->tombBlockIdx);

      if (iter->filterByVersion && (iter->record->version < iter->range[0] || iter->record->version > iter->range[1])) {
        continue;
      }

      if (tbid && iter->record->suid == tbid->suid && iter->record->uid == tbid->uid) {
        continue;
      }

      iter->record->skey = TARRAY2_GET(iter->sttTomb->tombBlock->skey, iter->sttTomb->tombBlockIdx);
      iter->record->ekey = TARRAY2_GET(iter->sttTomb->tombBlock->ekey, iter->sttTomb->tombBlockIdx);
      iter->sttTomb->tombBlockIdx++;
      goto _exit;
    }

    if (iter->sttTomb->tombBlkArrayIdx >= TARRAY2_SIZE(iter->sttTomb->tombBlkArray)) {
      iter->noMoreData = true;
      goto _exit;
    }

    for (; iter->sttTomb->tombBlkArrayIdx < TARRAY2_SIZE(iter->sttTomb->tombBlkArray);
         iter->sttTomb->tombBlkArrayIdx++) {
      const STombBlk *tombBlk = TARRAY2_GET_PTR(iter->sttTomb->tombBlkArray, iter->sttTomb->tombBlkArrayIdx);

      if (iter->filterByVersion && (tombBlk->maxVer < iter->range[0] || tombBlk->minVer > iter->range[1])) {
        continue;
      }

      if (tbid && tbid->suid == tombBlk->minTbid.suid && tbid->uid == tombBlk->minTbid.uid &&
          tbid->suid == tombBlk->maxTbid.suid && tbid->uid == tombBlk->maxTbid.uid) {
        continue;
      }

      int32_t code = tsdbSttFileReadTombBlock(iter->sttTomb->reader, tombBlk, iter->sttTomb->tombBlock);
      if (code) return code;

      iter->sttTomb->tombBlockIdx = 0;
      iter->sttTomb->tombBlkArrayIdx++;
      break;
    }
  }

_exit:
  return 0;
}

static int32_t tsdbSttTombIterOpen(STsdbIter *iter) {
  int32_t code;

  code = tsdbSttFileReadTombBlk(iter->sttTomb->reader, &iter->sttTomb->tombBlkArray);
  if (code) return code;

  if (TARRAY2_SIZE(iter->sttTomb->tombBlkArray) == 0) {
    iter->noMoreData = true;
    return 0;
  }

  iter->sttTomb->tombBlkArrayIdx = 0;
  tTombBlockInit(iter->sttTomb->tombBlock);
  iter->sttTomb->tombBlockIdx = 0;

  return tsdbSttTombIterNext(iter, NULL);
}

int32_t tsdbIterOpen(const STsdbIterConfig *config, STsdbIter **iter) {
  int32_t code;

  iter[0] = taosMemoryCalloc(1, sizeof(*iter[0]));
  if (iter[0] == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  iter[0]->type = config->type;
  iter[0]->noMoreData = false;
  iter[0]->filterByVersion = config->filterByVersion;
  if (iter[0]->filterByVersion) {
    iter[0]->range[0] = config->verRange[0];
    iter[0]->range[1] = config->verRange[1];
  }

  switch (config->type) {
    case TSDB_ITER_TYPE_STT:
      iter[0]->sttData->reader = config->sttReader;
      code = tsdbSttIterOpen(iter[0]);
      break;
    case TSDB_ITER_TYPE_DATA:
      iter[0]->dataData->reader = config->dataReader;
      code = tsdbDataIterOpen(iter[0]);
      break;
    case TSDB_ITER_TYPE_MEMT:
      iter[0]->memtData->memt = config->memt;
      iter[0]->memtData->from[0] = config->from[0];
      code = tsdbMemTableIterOpen(iter[0]);
      break;
    case TSDB_ITER_TYPE_STT_TOMB:
      iter[0]->sttTomb->reader = config->sttReader;
      code = tsdbSttTombIterOpen(iter[0]);
      break;
    case TSDB_ITER_TYPE_DATA_TOMB:
      iter[0]->dataTomb->reader = config->dataReader;
      code = tsdbDataTombIterOpen(iter[0]);
      break;
    case TSDB_ITER_TYPE_MEMT_TOMB:
      iter[0]->memtTomb->memt = config->memt;
      code = tsdbMemTombIterOpen(iter[0]);
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      ASSERTS(false, "Not implemented");
  }

  if (code) {
    taosMemoryFree(iter[0]);
    iter[0] = NULL;
  }
  return code;
}

static int32_t tsdbSttTombIterClose(STsdbIter *iter) {
  tTombBlockDestroy(iter->sttTomb->tombBlock);
  return 0;
}

static int32_t tsdbDataTombIterClose(STsdbIter *iter) {
  tTombBlockDestroy(iter->dataTomb->tombBlock);
  return 0;
}

int32_t tsdbIterClose(STsdbIter **iter) {
  switch (iter[0]->type) {
    case TSDB_ITER_TYPE_STT:
      tsdbSttIterClose(iter[0]);
      break;
    case TSDB_ITER_TYPE_DATA:
      tsdbDataIterClose(iter[0]);
      break;
    case TSDB_ITER_TYPE_MEMT:
      tsdbMemTableIterClose(iter[0]);
      break;
    case TSDB_ITER_TYPE_STT_TOMB:
      tsdbSttTombIterClose(iter[0]);
      break;
    case TSDB_ITER_TYPE_DATA_TOMB:
      tsdbDataTombIterClose(iter[0]);
      break;
    case TSDB_ITER_TYPE_MEMT_TOMB:
      break;
    default:
      ASSERT(false);
  }
  taosMemoryFree(iter[0]);
  iter[0] = NULL;
  return 0;
}

int32_t tsdbIterNext(STsdbIter *iter) {
  switch (iter->type) {
    case TSDB_ITER_TYPE_STT:
      return tsdbSttIterNext(iter, NULL);
    case TSDB_ITER_TYPE_DATA:
      return tsdbDataIterNext(iter, NULL);
    case TSDB_ITER_TYPE_MEMT:
      return tsdbMemTableIterNext(iter, NULL);
    case TSDB_ITER_TYPE_STT_TOMB:
      return tsdbSttTombIterNext(iter, NULL);
    case TSDB_ITER_TYPE_DATA_TOMB:
      return tsdbDataTombIterNext(iter, NULL);
    case TSDB_ITER_TYPE_MEMT_TOMB:
      return tsdbMemTombIterNext(iter, NULL);
    default:
      ASSERT(false);
  }
  return 0;
}

static int32_t tsdbIterSkipTableData(STsdbIter *iter, const TABLEID *tbid) {
  switch (iter->type) {
    case TSDB_ITER_TYPE_STT:
      return tsdbSttIterNext(iter, tbid);
    case TSDB_ITER_TYPE_DATA:
      return tsdbDataIterNext(iter, tbid);
    case TSDB_ITER_TYPE_MEMT:
      return tsdbMemTableIterNext(iter, tbid);
    case TSDB_ITER_TYPE_STT_TOMB:
      return tsdbSttTombIterNext(iter, tbid);
    case TSDB_ITER_TYPE_DATA_TOMB:
      return tsdbDataTombIterNext(iter, tbid);
    case TSDB_ITER_TYPE_MEMT_TOMB:
      return tsdbMemTombIterNext(iter, tbid);
    default:
      ASSERT(false);
  }
  return 0;
}

static int32_t tsdbIterCmprFn(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  STsdbIter *iter1 = TCONTAINER_OF(n1, STsdbIter, node);
  STsdbIter *iter2 = TCONTAINER_OF(n2, STsdbIter, node);
  return tRowInfoCmprFn(&iter1->row, &iter2->row);
}

static int32_t tsdbTombIterCmprFn(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  STsdbIter *iter1 = TCONTAINER_OF(n1, STsdbIter, node);
  STsdbIter *iter2 = TCONTAINER_OF(n2, STsdbIter, node);

  if (iter1->record->suid < iter2->record->suid) {
    return -1;
  } else if (iter1->record->suid > iter2->record->suid) {
    return 1;
  }

  if (iter1->record->uid < iter2->record->uid) {
    return -1;
  } else if (iter1->record->uid > iter2->record->uid) {
    return 1;
  }

  if (iter1->record->version < iter2->record->version) {
    return -1;
  } else if (iter1->record->version > iter2->record->version) {
    return 1;
  }

  return 0;
}

// SIterMerger ================
struct SIterMerger {
  bool       isTomb;
  STsdbIter *iter;
  SRBTree    iterTree[1];
};

int32_t tsdbIterMergerOpen(const TTsdbIterArray *iterArray, SIterMerger **merger, bool isTomb) {
  STsdbIter   *iter;
  SRBTreeNode *node;

  merger[0] = taosMemoryCalloc(1, sizeof(*merger[0]));
  if (merger[0] == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  merger[0]->isTomb = isTomb;
  if (isTomb) {
    tRBTreeCreate(merger[0]->iterTree, tsdbTombIterCmprFn);
  } else {
    tRBTreeCreate(merger[0]->iterTree, tsdbIterCmprFn);
  }
  TARRAY2_FOREACH(iterArray, iter) {
    if (iter->noMoreData) continue;
    node = tRBTreePut(merger[0]->iterTree, iter->node);
    ASSERT(node);
  }

  return tsdbIterMergerNext(merger[0]);
}

int32_t tsdbIterMergerClose(SIterMerger **merger) {
  if (merger[0]) {
    taosMemoryFree(merger[0]);
    merger[0] = NULL;
  }
  return 0;
}

int32_t tsdbIterMergerNext(SIterMerger *merger) {
  int32_t      code;
  int32_t      c;
  SRBTreeNode *node;

  if (merger->iter) {
    code = tsdbIterNext(merger->iter);
    if (code) return code;

    if (merger->iter->noMoreData) {
      merger->iter = NULL;
    } else if ((node = tRBTreeMin(merger->iterTree))) {
      c = merger->iterTree->cmprFn(merger->iter->node, node);
      ASSERT(c);
      if (c > 0) {
        node = tRBTreePut(merger->iterTree, merger->iter->node);
        ASSERT(node);
        merger->iter = NULL;
      }
    }
  }

  if (merger->iter == NULL && (node = tRBTreeDropMin(merger->iterTree))) {
    merger->iter = TCONTAINER_OF(node, STsdbIter, node);
  }

  return 0;
}

SRowInfo *tsdbIterMergerGetData(SIterMerger *merger) {
  ASSERT(!merger->isTomb);
  return merger->iter ? merger->iter->row : NULL;
}

STombRecord *tsdbIterMergerGetTombRecord(SIterMerger *merger) {
  ASSERT(merger->isTomb);
  return merger->iter ? merger->iter->record : NULL;
}

int32_t tsdbIterMergerSkipTableData(SIterMerger *merger, const TABLEID *tbid) {
  int32_t      code;
  int32_t      c;
  SRBTreeNode *node;

  while (merger->iter && tbid->suid == merger->iter->row->suid && tbid->uid == merger->iter->row->uid) {
    int32_t code = tsdbIterSkipTableData(merger->iter, tbid);
    if (code) return code;

    if (merger->iter->noMoreData) {
      merger->iter = NULL;
    } else if ((node = tRBTreeMin(merger->iterTree))) {
      c = merger->iterTree->cmprFn(merger->iter->node, node);
      ASSERT(c);
      if (c > 0) {
        node = tRBTreePut(merger->iterTree, merger->iter->node);
        ASSERT(node);
        merger->iter = NULL;
      }
    }

    if (!merger->iter && (node = tRBTreeDropMin(merger->iterTree))) {
      merger->iter = TCONTAINER_OF(node, STsdbIter, node);
    }
  }

  return 0;
}