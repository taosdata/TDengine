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

#include "tsdb.h"

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t    code = 0;
  SLRUCache *pCache = NULL;
  size_t     cfgCapacity = 1024 * 1024;  // TODO: get cfg from tsdb config

  pCache = taosLRUCacheInit(cfgCapacity, -1, .5);
  if (pCache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  taosLRUCacheSetStrictCapacity(pCache, true);

_err:
  pTsdb->lruCache = pCache;
  return code;
}

void tsdbCloseCache(SLRUCache *pCache) {
  if (pCache) {
    taosLRUCacheEraseUnrefEntries(pCache);

    taosLRUCacheCleanup(pCache);
  }
}

static void getTableCacheKey(tb_uid_t uid, const char *cacheType, char *key, int *len) {
  int keyLen = 0;

  snprintf(key, 30, "%" PRIi64 "%s", uid, cacheType);
  *len = strlen(key);
}

static void deleteTableCacheLastrow(const void *key, size_t keyLen, void *value) { taosMemoryFree(value); }

int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, tb_uid_t uid, STSRow *row) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    cacheRow = (STSRow *)taosLRUCacheValue(pCache, h);
    if (row->ts >= cacheRow->ts) {
      if (row->ts > cacheRow->ts) {
        tdRowCpy(cacheRow, row);
      }
    }
  } else {
    cacheRow = tdRowDup(row);

    _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
    LRUStatus           status =
        taosLRUCacheInsert(pCache, key, keyLen, cacheRow, TD_ROW_LEN(cacheRow), deleter, NULL, TAOS_LRU_PRIORITY_LOW);
    if (status != TAOS_LRU_STATUS_OK) {
      code = -1;
    }
  }

  return code;
}

static tb_uid_t getTableSuidByUid(tb_uid_t uid, STsdb *pTsdb) {
  tb_uid_t suid = 0;

  SMetaReader mr = {0};
  metaReaderInit(&mr, pTsdb->pVnode->pMeta, 0);
  if (metaGetTableEntryByUid(&mr, uid) < 0) {
    metaReaderClear(&mr);  // table not esist
    return 0;
  }

  if (mr.me.type == TSDB_CHILD_TABLE) {
    suid = mr.me.ctbEntry.suid;
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {
    suid = 0;
  } else {
    suid = 0;
  }

  metaReaderClear(&mr);

  return suid;
}
/*
static int32_t getMemLastRow(SMemTable *mem, tb_uid_t suid, tb_uid_t uid, STSRow
**ppRow) { int32_t code = 0;

  if (mem) {
    STbData *pMem = NULL;
    STbDataIter* iter;              // mem buffer skip list iterator

    tsdbGetTbDataFromMemTable(mem, suid, uid, &pMem);
    if (pMem != NULL) {
      tsdbTbDataIterCreate(pMem, NULL, 1, &iter);

      if (iter != NULL) {
        TSDBROW *row = tsdbTbDataIterGet(iter);

        tsdbTbDataIterDestroy(iter);
      }
    }
  } else {
    *ppRow = NULL;
  }

  return code;
}
*/
static int32_t getTableDelDataFromDelIdx(SDelFReader *pDelReader, SDelIdx *pDelIdx, SArray *aDelData) {
  int32_t code = 0;

  SMapData delDataMap;
  SDelData delData;

  if (pDelIdx) {
    tMapDataReset(&delDataMap);

    code = tsdbReadDelData(pDelReader, pDelIdx, &delDataMap, NULL);
    if (code) goto _err;

    for (int32_t iDelData = 0; iDelData < delDataMap.nItem; ++iDelData) {
      code = tMapDataGetItemByIdx(&delDataMap, iDelData, &delData, tGetDelData);
      if (code) goto _err;

      taosArrayPush(aDelData, &delData);
    }
  }

_err:
  return code;
}

static int32_t getTableDelDataFromTbData(STbData *pTbData, SArray *aDelData) {
  int32_t   code = 0;
  SDelData *pDelData = pTbData ? pTbData->pHead : NULL;

  for (; pDelData; pDelData = pDelData->pNext) {
    taosArrayPush(aDelData, pDelData);
  }

  return code;
}

static int32_t getTableDelData(STbData *pMem, STbData *pIMem, SDelFReader *pDelReader, SDelIdx *pDelIdx,
                               SArray *aDelData) {
  int32_t code = 0;

  if (pMem) {
    code = getTableDelDataFromTbData(pMem, aDelData);
    if (code) goto _err;
  }

  if (pIMem) {
    code = getTableDelDataFromTbData(pIMem, aDelData);
    if (code) goto _err;
  }

  if (pDelIdx) {
    code = getTableDelDataFromDelIdx(pDelReader, pDelIdx, aDelData);
    if (code) goto _err;
  }

_err:
  return code;
}

static int32_t getTableDelSkyline(STbData *pMem, STbData *pIMem, SDelFReader *pDelReader, SDelIdx *pDelIdx,
                                  SArray *aSkyline) {
  int32_t code = 0;

  SArray *aDelData = taosArrayInit(32, sizeof(SDelData));
  code = getTableDelData(pMem, pIMem, pDelReader, pDelIdx, aDelData);
  if (code) goto _err;

  size_t nDelData = taosArrayGetSize(aDelData);
  code = tsdbBuildDeleteSkyline(aDelData, 0, (int32_t)(nDelData - 1), aSkyline);
  if (code) goto _err;

  taosArrayDestroy(aDelData);

_err:
  return code;
}

static int32_t getTableDelIdx(SDelFReader *pDelFReader, tb_uid_t suid, tb_uid_t uid, SDelIdx *pDelIdx) {
  int32_t code = 0;

  SMapData delIdxMap;
  SDelIdx  idx = {.suid = suid, .uid = uid};

  tMapDataReset(&delIdxMap);
  code = tsdbReadDelIdx(pDelFReader, &delIdxMap, NULL);
  if (code) goto _err;

  code = tMapDataSearch(&delIdxMap, &idx, tGetDelIdx, tCmprDelIdx, pDelIdx);
  if (code) goto _err;

_err:
  return code;
}
#if 0
static int32_t mergeLastRowFileSet(STbDataIter *iter, STbDataIter *iiter, SDFileSet *pFileSet, SArray *pSkyline,
                                   STsdb *pTsdb, STSRow **ppLastRow) {
  int32_t code = 0;

  TSDBROW *pMemRow = NULL;
  TSDBROW *pIMemRow = NULL;
  TSDBKEY  memKey = TSDBKEY_MIN;
  TSDBKEY  imemKey = TSDBKEY_MIN;

  if (iter != NULL) {
    pMemRow = tsdbTbDataIterGet(iter);
    if (pMemRow) {
      memKey = tsdbRowKey(pMemRow);
    }
  }

  if (iter != NULL) {
    pIMemRow = tsdbTbDataIterGet(iiter);
    if (pIMemRow) {
      imemKey = tsdbRowKey(pIMemRow);
    }
  }

  SDataFReader *pDataFReader;
  code = tsdbDataFReaderOpen(&pDataFReader, pTsdb, pFileSet);
  if (code) goto _err;

  SMapData blockIdxMap;
  tMapDataReset(&blockIdxMap);
  code = tsdbReadBlockIdx(pDataFReader, &blockIdxMap, NULL);
  if (code) goto _err;

  SBlockIdx blockIdx = {0};
  tBlockIdxReset(&blockIdx);
  code = tMapDataSearch(&blockIdxMap, pBlockIdx, tGetBlockIdx, tCmprBlockIdx, &blockIdx);
  if (code) goto _err;

  SMapData blockMap = {0};
  tMapDataReset(&blockMap);
  code = tsdbReadBlock(pDataFReader, &blockIdx, &blockMap, NULL);
  if (code) goto _err;

  int32_t nBlock = blockMap.nItem;
  for (int32_t iBlock = nBlock - 1; iBlock >= 0; --iBlock) {
    SBlock     block = {0};
    SBlockData blockData = {0};

    tBlockReset(&block);
    tBlockDataReset(&blockData);

    tMapDataGetItemByIdx(&blockMap, iBlock, &block, tGetBlock);

    code = tsdbReadBlockData(pDataFReader, &blockIdx, &block, &blockData, NULL, 0, NULL, NULL);
    if (code) goto _err;

    int32_t nRow = blockData.nRow;
    for (int32_t iRow = nRow - 1; iRow >= 0; --iRow) {
      TSDBROW row = tsdbRowFromBlockData(&blockData, iRow);

      TSDBKEY key = tsdbRowKey(&row);
      if (pMemRow != NULL && pIMemRow != NULL) {
        int32_t c = tsdbKeyCmprFn(memKey, imemKey);
        if (c < 0) {
        } else if (c > 0) {
        } else {
        }
      } else if (pMemRow != NULL) {
        pMemRow = tsdbTbDataIterGet(iter);

      } else if (pIMemRow != NULL) {
      } else {
        if (!tsdbKeyDeleted(key, pSkyline)) {
          *ppLastRow = buildTsrowFromTsdbrow(&row);
          goto _done;
        } else {
          continue;
        }
      }
      // select current row if outside delete area
      STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);
    }
  }

_done:
  tsdbDataFReaderClose(&pDataFReader);

  return code;

_err:
  return code;
}
#endif
typedef enum SFSNEXTROWSTATES {
  SFSNEXTROW_FS,
  SFSNEXTROW_FILESET,
  SFSNEXTROW_BLOCKDATA,
  SFSNEXTROW_BLOCKROW
} SFSNEXTROWSTATES;

typedef struct SFSNextRowIter {
  SFSNEXTROWSTATES state;         // [input]
  STsdb           *pTsdb;         // [input]
  SBlockIdx       *pBlockIdxExp;  // [input]
  int32_t          nFileSet;
  int32_t          iFileSet;
  SArray          *aDFileSet;
  SDataFReader    *pDataFReader;
  SMapData         blockIdxMap;
  SBlockIdx        blockIdx;
  SMapData         blockMap;
  int32_t          nBlock;
  int32_t          iBlock;
  SBlock           block;
  SBlockData       blockData;
  int32_t          nRow;
  int32_t          iRow;
  TSDBROW          row;
} SFSNextRowIter;

static int32_t getNextRowFromFS(void *iter, TSDBROW **ppRow) {
  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  int32_t         code = 0;

  switch (state->state) {
    case SFSNEXTROW_FS:
      state->aDFileSet = state->pTsdb->fs->cState->aDFileSet;
      state->nFileSet = taosArrayGetSize(state->aDFileSet);
      state->iFileSet = state->nFileSet;

    case SFSNEXTROW_FILESET: {
      SDFileSet *pFileSet = NULL;
      if (--state->iFileSet >= 0) {
        pFileSet = (SDFileSet *)taosArrayGet(state->aDFileSet, state->iFileSet);
      } else {
        *ppRow = NULL;
        return code;
      }

      code = tsdbDataFReaderOpen(&state->pDataFReader, state->pTsdb, pFileSet);
      if (code) goto _err;

      tMapDataReset(&state->blockIdxMap);
      code = tsdbReadBlockIdx(state->pDataFReader, &state->blockIdxMap, NULL);
      if (code) goto _err;

      tBlockIdxReset(&state->blockIdx);
      code = tMapDataSearch(&state->blockIdxMap, state->pBlockIdxExp, tGetBlockIdx, tCmprBlockIdx, &state->blockIdx);
      if (code) goto _err;

      tMapDataReset(&state->blockMap);
      code = tsdbReadBlock(state->pDataFReader, &state->blockIdx, &state->blockMap, NULL);
      if (code) goto _err;

      state->nBlock = state->blockMap.nItem;
      state->iBlock = state->nBlock - 1;
    }
    case SFSNEXTROW_BLOCKDATA:
      if (state->iBlock >= 0) {
        SBlock block = {0};

        tBlockReset(&block);
        tBlockDataReset(&state->blockData);

        tMapDataGetItemByIdx(&state->blockMap, state->iBlock, &block, tGetBlock);
        code = tsdbReadBlockData(state->pDataFReader, &state->blockIdx, &block, &state->blockData, NULL, 0, NULL, NULL);
        if (code) goto _err;

        state->nRow = state->blockData.nRow;
        state->iRow = state->nRow - 1;

        state->state = SFSNEXTROW_BLOCKROW;
      }
    case SFSNEXTROW_BLOCKROW:
      if (state->iRow >= 0) {
        state->row = tsdbRowFromBlockData(&state->blockData, state->iRow);
        *ppRow = &state->row;

        if (--state->iRow < 0) {
          state->state = SFSNEXTROW_BLOCKDATA;
          if (--state->iBlock < 0) {
            tsdbDataFReaderClose(&state->pDataFReader);

            state->state = SFSNEXTROW_FILESET;
          }
        }
      }

      return code;
    default:
      ASSERT(0);
      break;
  }

_err:
  *ppRow = NULL;
  return code;
}

typedef enum SMEMNEXTROWSTATES {
  SMEMNEXTROW_ENTER,
  SMEMNEXTROW_NEXT,
} SMEMNEXTROWSTATES;

typedef struct SMemNextRowIter {
  SMEMNEXTROWSTATES state;
  STbData          *pMem;  // [input]
  STbDataIter       iter;  // mem buffer skip list iterator
} SMemNextRowIter;

static int32_t getNextRowFromMem(void *iter, TSDBROW **ppRow) {
  SMemNextRowIter *state = (SMemNextRowIter *)iter;
  int32_t          code = 0;

  switch (state->state) {
    case SMEMNEXTROW_ENTER: {
      if (state->pMem != NULL) {
        tsdbTbDataIterOpen(state->pMem, NULL, 1, &state->iter);

        TSDBROW *pMemRow = tsdbTbDataIterGet(&state->iter);
        if (pMemRow) {
          *ppRow = pMemRow;
          state->state = SMEMNEXTROW_NEXT;

          return code;
        }
      }

      *ppRow = NULL;

      return code;
    }
    case SMEMNEXTROW_NEXT:
      if (tsdbTbDataIterNext(&state->iter)) {
        *ppRow = tsdbTbDataIterGet(&state->iter);

        return code;
      } else {
        *ppRow = NULL;

        return code;
      }
    default:
      ASSERT(0);
      break;
  }

_err:
  *ppRow = NULL;
  return code;
}

static STSRow *tsRowFromTsdbRow(TSDBROW *pRow) {
  // TODO: new tsrow from tsdbrow
  STSRow *ret = NULL;
  if (pRow->type == 0) {
    return pRow->pTSRow;
  } else {
  }

  return ret;
}

typedef int32_t (*_next_row_fn_t)(void *iter, TSDBROW **ppRow);

typedef struct TsdbNextRowState {
  TSDBROW       *pRow;
  bool           stop;
  bool           next;
  void          *iter;
  _next_row_fn_t nextRowFn;
} TsdbNextRowState;

static int32_t mergeLastRow(tb_uid_t uid, STsdb *pTsdb, STSRow **ppRow) {
  int32_t code = 0;

  tb_uid_t suid = getTableSuidByUid(uid, pTsdb);

  STbData *pMem = NULL;
  if (pTsdb->mem) {
    tsdbGetTbDataFromMemTable(pTsdb->mem, suid, uid, &pMem);
  }

  STbData *pIMem = NULL;
  if (pTsdb->imem) {
    tsdbGetTbDataFromMemTable(pTsdb->imem, suid, uid, &pIMem);
  }

  *ppRow = NULL;

  SDelFReader *pDelFReader;
  SDelFile    *pDelFile = tsdbFSStateGetDelFile(pTsdb->fs->cState);
  code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb, NULL);
  if (code) goto _err;

  SDelIdx delIdx;
  code = getTableDelIdx(pDelFReader, suid, uid, &delIdx);
  if (code) goto _err;

  SArray *pSkyline = taosArrayInit(32, sizeof(TSDBKEY));
  code = getTableDelSkyline(pMem, pIMem, pDelFReader, &delIdx, pSkyline);
  if (code) goto _err;

  int iSkyline = taosArrayGetSize(pSkyline) - 1;

  tsdbDelFReaderClose(pDelFReader);

  SBlockIdx idx = {.suid = suid, .uid = uid};

  SFSNextRowIter fsState = {0};
  fsState.state = SFSNEXTROW_FS;
  fsState.pTsdb = pTsdb;
  fsState.pBlockIdxExp = &idx;

  SMemNextRowIter memState = {0};
  SMemNextRowIter imemState = {0};
  TSDBROW         memRow, imemRow, fsRow;

  TsdbNextRowState input[3] = {{&memRow, true, false, &memState, getNextRowFromMem},
                               {&imemRow, true, false, &imemState, getNextRowFromMem},
                               {&fsRow, false, true, &fsState, getNextRowFromFS}};

  if (pMem) {
    memState.pMem = pMem;
    memState.state = SMEMNEXTROW_ENTER;
    input[0].stop = false;
    input[0].next = true;
  }
  if (pIMem) {
    imemState.pMem = pIMem;
    imemState.state = SMEMNEXTROW_ENTER;
    input[1].stop = false;
    input[1].next = true;
  }

  do {
    for (int i = 0; i < 3; ++i) {
      if (input[i].next && !input[i].stop) {
        code = input[i].nextRowFn(input[i].iter, &input[i].pRow);
        if (code) goto _err;

        if (input[i].pRow == NULL) {
          input[i].stop = true;
          input[i].next = false;
        }
      }
    }

    if (input[0].stop && input[1].stop && input[2].stop) {
      break;
    }

    // select maxpoint(s) from mem, imem, fs
    TSDBROW *max[3] = {0};
    int      iMax[3] = {-1, -1, -1};
    int      nMax = 0;
    for (int i = 0; i < 3; ++i) {
      if (input[i].pRow != NULL) {
        TSDBKEY key = TSDBROW_KEY(input[i].pRow);
        TSDBKEY maxKey = TSDBROW_KEY(max[nMax]);

        // merging & deduplicating on client side
        if (maxKey.ts <= key.ts) {
          if (maxKey.ts < key.ts) {
            nMax = 0;
          }

          iMax[nMax] = i;
          max[nMax++] = input[i].pRow;
        }
      }
    }

    // delete detection
    TSDBROW *merge[3] = {0};
    int      nMerge = 0;
    for (int i = 0; i < nMax; ++i) {
      TSDBKEY maxKey = TSDBROW_KEY(max[i]);

      bool deleted = false;
      // bool deleted = tsdbKeyDeleted(maxKey, pSkyline, &iSkyline);
      if (!deleted) {
        merge[nMerge++] = max[i];
      } else {
        input[iMax[i]].next = true;
      }
    }

    // merge if nMerge > 1
    if (nMerge > 0) {
      if (nMerge == 1) {
        *ppRow = tsRowFromTsdbRow(merge[nMerge]);
      } else {
        // merge 2 or 3 rows
        SRowMerger merger = {0};

        STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);

        tRowMergerInit(&merger, merge[0], pTSchema);
        for (int i = 1; i < nMerge; ++i) {
          tRowMerge(&merger, merge[i]);
        }
        tRowMergerGetRow(&merger, ppRow);
        tRowMergerClear(&merger);
      }
    }
  } while (*ppRow == NULL);

  return code;

_err:
  tsdbError("vgId:%d merge last_row failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbCacheGetLastrow(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, STSRow **ppRow) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    *ppRow = (STSRow *)taosLRUCacheValue(pCache, h);
  } else {
    STSRow *pRow = NULL;
    code = mergeLastRow(uid, pTsdb, &pRow);
    // if table's empty or error, return code of -1
    if (code < 0 || pRow == NULL) {
      return -1;
    }
  }

  return code;
}

int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    taosLRUCacheRelease(pCache, h, true);
    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t
    // keyLen);
  }

  return code;
}
