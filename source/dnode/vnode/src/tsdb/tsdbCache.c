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
  // TODO: get cfg from vnode config: pTsdb->pVnode->config.lruCapacity
  size_t cfgCapacity = 1024 * 1024;

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

static int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    STSRow *pRow = (STSRow *)taosLRUCacheValue(pCache, h);
    if (pRow->ts <= eKey) {
      taosLRUCacheRelease(pCache, h, true);
    } else {
      taosLRUCacheRelease(pCache, h, false);
    }

    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}

static int32_t tsdbCacheDeleteLast(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "l", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    // clear last cache anyway, no matter where eKey ends.
    taosLRUCacheRelease(pCache, h, true);

    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}

int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, STsdb *pTsdb, tb_uid_t uid, STSRow *row, bool dup) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    cacheRow = (STSRow *)taosLRUCacheValue(pCache, h);
    if (row->ts >= cacheRow->ts) {
      if (row->ts == cacheRow->ts) {
        STSRow    *mergedRow;
        SRowMerger merger = {0};
        STSchema  *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);

        tRowMergerInit(&merger, &tsdbRowFromTSRow(0, cacheRow), pTSchema);

        tRowMerge(&merger, &tsdbRowFromTSRow(1, row));

        tRowMergerGetRow(&merger, &mergedRow);
        tRowMergerClear(&merger);

        taosMemoryFreeClear(pTSchema);

        row = mergedRow;
        dup = false;
      }

      if (TD_ROW_LEN(row) <= TD_ROW_LEN(cacheRow)) {
        tdRowCpy(cacheRow, row);
        if (!dup) {
          taosMemoryFree(row);
        }

        taosLRUCacheRelease(pCache, h, false);
      } else {
        taosLRUCacheRelease(pCache, h, true);
        // tsdbCacheDeleteLastrow(pCache, uid, TSKEY_MAX);

        _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
        LRUStatus status = taosLRUCacheInsert(pCache, key, keyLen, cacheRow, TD_ROW_LEN(cacheRow), deleter, NULL,
                                              TAOS_LRU_PRIORITY_LOW);
        if (status != TAOS_LRU_STATUS_OK) {
          code = -1;
        }
        /* tsdbCacheInsertLastrow(pCache, uid, row, dup); */
      }
    }
  } else {
    if (dup) {
      cacheRow = tdRowDup(row);
    } else {
      cacheRow = row;
    }

    _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
    LRUStatus           status =
        taosLRUCacheInsert(pCache, key, keyLen, cacheRow, TD_ROW_LEN(cacheRow), deleter, NULL, TAOS_LRU_PRIORITY_LOW);
    if (status != TAOS_LRU_STATUS_OK) {
      code = -1;
    }
  }

  return code;
}

int32_t tsdbCacheInsertLast(SLRUCache *pCache, tb_uid_t uid, STSRow *row) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  ((void)(row));

  getTableCacheKey(uid, "l", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    // clear last cache anyway, lazy load when get last lookup
    taosLRUCacheRelease(pCache, h, true);
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

static int32_t getTableDelDataFromDelIdx(SDelFReader *pDelReader, SDelIdx *pDelIdx, SArray *aDelData) {
  int32_t code = 0;

  // SMapData delDataMap;
  // SDelData delData;

  if (pDelIdx) {
    // tMapDataReset(&delDataMap);

    // code = tsdbReadDelData(pDelReader, pDelIdx, &delDataMap, NULL);
    code = tsdbReadDelData(pDelReader, pDelIdx, aDelData, NULL);
    if (code) goto _err;
    /*
    for (int32_t iDelData = 0; iDelData < delDataMap.nItem; ++iDelData) {
      code = tMapDataGetItemByIdx(&delDataMap, iDelData, &delData, tGetDelData);
      if (code) goto _err;

      taosArrayPush(aDelData, &delData);
    }
    */
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
  if (nDelData > 0) {
    code = tsdbBuildDeleteSkyline(aDelData, 0, (int32_t)(nDelData - 1), aSkyline);
    if (code) goto _err;
  }

  taosArrayDestroy(aDelData);

_err:
  return code;
}

static int32_t getTableDelIdx(SDelFReader *pDelFReader, tb_uid_t suid, tb_uid_t uid, SDelIdx *pDelIdx) {
  int32_t code = 0;

  // SMapData delIdxMap;
  SArray *pDelIdxArray = taosArrayInit(32, sizeof(SDelIdx));
  SDelIdx idx = {.suid = suid, .uid = uid};

  // tMapDataReset(&delIdxMap);
  //  code = tsdbReadDelIdx(pDelFReader, &delIdxMap, NULL);
  code = tsdbReadDelIdx(pDelFReader, pDelIdxArray, NULL);
  if (code) goto _err;

  // code = tMapDataSearch(&delIdxMap, &idx, tGetDelIdx, tCmprDelIdx, pDelIdx);
  pDelIdx = taosArraySearch(pDelIdxArray, &idx, tCmprDelIdx, TD_EQ);
  if (code) goto _err;

_err:
  return code;
}

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
  SArray          *aBlockIdx;
  // SMapData         blockIdxMap;
  //  SBlockIdx  blockIdx;
  SBlockIdx *pBlockIdx;
  SMapData   blockMap;
  int32_t    nBlock;
  int32_t    iBlock;
  SBlock     block;
  SBlockData blockData;
  int32_t    nRow;
  int32_t    iRow;
  TSDBROW    row;
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

      // tMapDataReset(&state->blockIdxMap);
      // code = tsdbReadBlockIdx(state->pDataFReader, &state->blockIdxMap, NULL);
      if (!state->aBlockIdx) {
        state->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
      } else {
        taosArrayClear(state->aBlockIdx);
      }
      code = tsdbReadBlockIdx(state->pDataFReader, state->aBlockIdx, NULL);
      if (code) goto _err;

      /* if (state->pBlockIdx) { */
      /*   tBlockIdxReset(state->blockIdx); */
      /* } */
      /* tBlockIdxReset(state->blockIdx); */
      /* code = tMapDataSearch(&state->blockIdxMap, state->pBlockIdxExp, tGetBlockIdx, tCmprBlockIdx,
       * &state->blockIdx);
       */
      state->pBlockIdx = taosArraySearch(state->aBlockIdx, state->pBlockIdxExp, tCmprBlockIdx, TD_EQ);
      if (code) goto _err;

      tMapDataReset(&state->blockMap);
      code = tsdbReadBlock(state->pDataFReader, state->pBlockIdx, &state->blockMap, NULL);
      /* code = tsdbReadBlock(state->pDataFReader, &state->blockIdx, &state->blockMap, NULL); */
      if (code) goto _err;

      state->nBlock = state->blockMap.nItem;
      state->iBlock = state->nBlock - 1;

      tBlockDataInit(&state->blockData);
    }
    case SFSNEXTROW_BLOCKDATA:
      if (state->iBlock >= 0) {
        SBlock block = {0};

        tBlockReset(&block);
        tBlockDataReset(&state->blockData);

        tMapDataGetItemByIdx(&state->blockMap, state->iBlock, &block, tGetBlock);
        /* code = tsdbReadBlockData(state->pDataFReader, &state->blockIdx, &block, &state->blockData, NULL, NULL); */
        code = tsdbReadBlockData(state->pDataFReader, state->pBlockIdx, &block, &state->blockData, NULL, NULL);
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
            if (state->aBlockIdx) {
              taosArrayDestroy(state->aBlockIdx);
            }
            tBlockDataClear(&state->blockData);

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
  if (state->pDataFReader) {
    tsdbDataFReaderClose(&state->pDataFReader);
  }
  if (state->aBlockIdx) {
    taosArrayDestroy(state->aBlockIdx);
  }
  tBlockDataClear(&state->blockData);

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

static int32_t tsRowFromTsdbRow(STSchema *pTSchema, TSDBROW *pRow, STSRow **ppRow) {
  int32_t code = 0;

  SColVal *pColVal = &(SColVal){0};

  if (pRow->type == 0) {
    *ppRow = tdRowDup(pRow->pTSRow);
  } else {
    SArray *pArray = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal));
    if (pArray == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    TSDBKEY   key = TSDBROW_KEY(pRow);
    STColumn *pTColumn = &pTSchema->columns[0];
    *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = key.ts});

    if (taosArrayPush(pArray, pColVal) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    for (int16_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
      tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
      if (taosArrayPush(pArray, pColVal) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
    }

    code = tdSTSRowNew(pArray, pTSchema, ppRow);
    if (code) goto _exit;
  }

_exit:
  return code;
}

static bool tsdbKeyDeleted(TSDBKEY *key, SArray *pSkyline, int *iSkyline) {
  bool deleted = false;
  while (*iSkyline > 0) {
    TSDBKEY *pItemBack = (TSDBKEY *)taosArrayGet(pSkyline, *iSkyline);
    TSDBKEY *pItemFront = (TSDBKEY *)taosArrayGet(pSkyline, *iSkyline - 1);

    if (key->ts > pItemBack->ts) {
      return false;
    } else if (key->ts >= pItemFront->ts && key->ts <= pItemBack->ts) {
      if ((key->version <= pItemFront->version || key->ts == pItemBack->ts && key->version <= pItemBack->version)) {
        return true;
      } else {
        return false;
      }
    } else {
      if (*iSkyline > 1) {
        --*iSkyline;
      } else {
        return false;
      }
    }
  }

  return deleted;
}

typedef int32_t (*_next_row_fn_t)(void *iter, TSDBROW **ppRow);

typedef struct TsdbNextRowState {
  TSDBROW       *pRow;
  bool           stop;
  bool           next;
  void          *iter;
  _next_row_fn_t nextRowFn;
} TsdbNextRowState;

static int32_t mergeLastRow(tb_uid_t uid, STsdb *pTsdb, bool *dup, STSRow **ppRow) {
  int32_t code = 0;

  STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);
  int16_t   nCol = pTSchema->numOfCols;
  SArray   *pColArray = taosArrayInit(nCol, sizeof(SColVal));

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

  SArray *pSkyline = taosArrayInit(32, sizeof(TSDBKEY));

  SDelIdx delIdx;

  SDelFile *pDelFile = tsdbFSStateGetDelFile(pTsdb->fs->cState);
  if (pDelFile) {
    SDelFReader *pDelFReader;

    code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb, NULL);
    if (code) goto _err;

    code = getTableDelIdx(pDelFReader, suid, uid, &delIdx);
    if (code) goto _err;

    code = getTableDelSkyline(pMem, pIMem, pDelFReader, &delIdx, pSkyline);
    if (code) goto _err;

    tsdbDelFReaderClose(pDelFReader);
  } else {
    code = getTableDelSkyline(pMem, pIMem, NULL, NULL, pSkyline);
    if (code) goto _err;
  }

  int iSkyline = taosArrayGetSize(pSkyline) - 1;

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

  int16_t nilColCount = nCol - 1;  // count of null & none cols
  int     iCol = 0;                // index of first nil col index from left to right
  bool    setICol = false;

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
    TSKEY    maxKey = TSKEY_MIN;

    for (int i = 0; i < 3; ++i) {
      if (!input[i].stop && input[i].pRow != NULL) {
        TSDBKEY key = TSDBROW_KEY(input[i].pRow);

        // merging & deduplicating on client side
        if (maxKey <= key.ts) {
          if (maxKey < key.ts) {
            nMax = 0;
            maxKey = key.ts;
          }

          iMax[nMax] = i;
          max[nMax++] = input[i].pRow;
        }
      }
    }

    // delete detection
    TSDBROW *merge[3] = {0};
    // int      iMerge[3] = {-1, -1, -1};
    int nMerge = 0;
    for (int i = 0; i < nMax; ++i) {
      TSDBKEY maxKey = TSDBROW_KEY(max[i]);

      // bool deleted = false;
      bool deleted = tsdbKeyDeleted(&maxKey, pSkyline, &iSkyline);
      if (!deleted) {
        // iMerge[nMerge] = i;
        merge[nMerge++] = max[i];
      }

      input[iMax[i]].next = deleted;
    }

    // merge if nMerge > 1
    if (nMerge > 0) {
      *dup = false;

      if (nMerge == 1) {
        code = tsRowFromTsdbRow(pTSchema, merge[nMerge - 1], ppRow);
        if (code) goto _err;
      } else {
        // merge 2 or 3 rows
        SRowMerger merger = {0};

        tRowMergerInit(&merger, merge[0], pTSchema);
        for (int i = 1; i < nMerge; ++i) {
          tRowMerge(&merger, merge[i]);
        }
        tRowMergerGetRow(&merger, ppRow);
        tRowMergerClear(&merger);
      }
    }

  } while (*ppRow == NULL);

  taosMemoryFreeClear(pTSchema);

  return code;
_err:
  taosMemoryFreeClear(pTSchema);
  tsdbError("vgId:%d merge last_row failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t mergeLast(tb_uid_t uid, STsdb *pTsdb, STSRow **ppRow) {
  int32_t code = 0;

  STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);
  int16_t   nCol = pTSchema->numOfCols;
  SArray   *pColArray = taosArrayInit(nCol, sizeof(SColVal));

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

  SArray *pSkyline = taosArrayInit(32, sizeof(TSDBKEY));

  SDelIdx delIdx;

  SDelFile *pDelFile = tsdbFSStateGetDelFile(pTsdb->fs->cState);
  if (pDelFile) {
    SDelFReader *pDelFReader;

    code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb, NULL);
    if (code) goto _err;

    code = getTableDelIdx(pDelFReader, suid, uid, &delIdx);
    if (code) goto _err;

    code = getTableDelSkyline(pMem, pIMem, pDelFReader, &delIdx, pSkyline);
    if (code) goto _err;

    tsdbDelFReaderClose(pDelFReader);
  } else {
    code = getTableDelSkyline(pMem, pIMem, NULL, NULL, pSkyline);
    if (code) goto _err;
  }

  int iSkyline = taosArrayGetSize(pSkyline) - 1;

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

  int16_t nilColCount = nCol - 1;  // count of null & none cols
  int     iCol = 0;                // index of first nil col index from left to right
  bool    setICol = false;

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
    TSKEY    maxKey = TSKEY_MIN;

    for (int i = 0; i < 3; ++i) {
      if (!input[i].stop && input[i].pRow != NULL) {
        TSDBKEY key = TSDBROW_KEY(input[i].pRow);

        // merging & deduplicating on client side
        if (maxKey <= key.ts) {
          if (maxKey < key.ts) {
            nMax = 0;
            maxKey = key.ts;
          }

          iMax[nMax] = i;
          max[nMax++] = input[i].pRow;
        }
      }
    }

    // delete detection
    TSDBROW *merge[3] = {0};
    int      iMerge[3] = {-1, -1, -1};
    int      nMerge = 0;
    for (int i = 0; i < nMax; ++i) {
      TSDBKEY maxKey = TSDBROW_KEY(max[i]);

      // bool deleted = false;
      bool deleted = tsdbKeyDeleted(&maxKey, pSkyline, &iSkyline);
      if (!deleted) {
        iMerge[nMerge] = iMax[i];
        merge[nMerge++] = max[i];
      }

      input[iMax[i]].next = deleted;
    }

    // merge if nMerge > 1
    if (nMerge > 0) {
      if (nMerge == 1) {
        code = tsRowFromTsdbRow(pTSchema, merge[nMerge - 1], ppRow);
        if (code) goto _err;
      } else {
        // merge 2 or 3 rows
        SRowMerger merger = {0};

        tRowMergerInit(&merger, merge[0], pTSchema);
        for (int i = 1; i < nMerge; ++i) {
          tRowMerge(&merger, merge[i]);
        }
        tRowMergerGetRow(&merger, ppRow);
        tRowMergerClear(&merger);
      }
    }

    if (iCol == 0) {
      STColumn *pTColumn = &pTSchema->columns[0];
      SColVal  *pColVal = &(SColVal){0};

      *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = maxKey});

      if (taosArrayPush(pColArray, pColVal) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }

      ++iCol;

      setICol = false;
      for (int16_t i = iCol; i < nCol; ++i) {
        // tsdbRowGetColVal(*ppRow, pTSchema, i, pColVal);
        tTSRowGetVal(*ppRow, pTSchema, i, pColVal);
        if (taosArrayPush(pColArray, pColVal) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }

        if (pColVal->isNull || pColVal->isNone) {
          for (int j = 0; j < nMerge; ++j) {
            SColVal jColVal = {0};
            tsdbRowGetColVal(merge[j], pTSchema, i, &jColVal);
            if (jColVal.isNull || jColVal.isNone) {
              input[iMerge[j]].next = true;
            }
          }
          if (!setICol) {
            iCol = i;
            setICol = true;
          }
        } else {
          --nilColCount;
        }
      }

      if (*ppRow) {
        taosMemoryFreeClear(*ppRow);
      }

      continue;
    }

    setICol = false;
    for (int16_t i = iCol; i < nCol; ++i) {
      SColVal colVal = {0};
      tTSRowGetVal(*ppRow, pTSchema, i, &colVal);

      SColVal *tColVal = (SColVal *)taosArrayGet(pColArray, i);

      if (!colVal.isNone && !colVal.isNull) {
        if (tColVal->isNull || tColVal->isNone) {
          taosArraySet(pColArray, i, &colVal);
          --nilColCount;
        }
      } else {
        if ((tColVal->isNull || tColVal->isNone) && !setICol) {
          iCol = i;
          setICol = true;

          for (int j = 0; j < nMerge; ++j) {
            SColVal jColVal = {0};
            tsdbRowGetColVal(merge[j], pTSchema, i, &jColVal);
            if (jColVal.isNull || jColVal.isNone) {
              input[iMerge[j]].next = true;
            }
          }
        }
      }
    }

    if (*ppRow) {
      taosMemoryFreeClear(*ppRow);
    }
  } while (nilColCount > 0);

  // if () new ts row from pColArray if non empty
  if (taosArrayGetSize(pColArray) == nCol) {
    code = tdSTSRowNew(pColArray, pTSchema, ppRow);
    if (code) goto _err;
  }
  taosArrayDestroy(pColArray);
  taosMemoryFreeClear(pTSchema);

  return code;
_err:
  taosArrayDestroy(pColArray);
  taosMemoryFreeClear(pTSchema);
  tsdbError("vgId:%d merge last_row failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbCacheGetLastrowH(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, LRUHandle **handle) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    //*ppRow = (STSRow *)taosLRUCacheValue(pCache, h);
  } else {
    STSRow *pRow = NULL;
    bool    dup = false;
    code = mergeLastRow(uid, pTsdb, &dup, &pRow);
    // if table's empty or error, return code of -1
    if (code < 0 || pRow == NULL) {
      if (!dup) {
        taosMemoryFree(pRow);
      }

      *handle = NULL;
      return 0;
    }

    tsdbCacheInsertLastrow(pCache, pTsdb, uid, pRow, dup);
    h = taosLRUCacheLookup(pCache, key, keyLen);
    //*ppRow = (STSRow *)taosLRUCacheValue(pCache, h);
  }

  *handle = h;

  return code;
}

int32_t tsdbCacheGetLastH(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, LRUHandle **handle) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "l", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    //*ppRow = (STSRow *)taosLRUCacheValue(pCache, h);

  } else {
    STSRow *pRow = NULL;
    code = mergeLast(uid, pTsdb, &pRow);
    // if table's empty or error, return code of -1
    if (code < 0 || pRow == NULL) {
      *handle = NULL;
      return 0;
    }

    _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
    LRUStatus           status =
        taosLRUCacheInsert(pCache, key, keyLen, pRow, TD_ROW_LEN(pRow), deleter, NULL, TAOS_LRU_PRIORITY_LOW);
    if (status != TAOS_LRU_STATUS_OK) {
      code = -1;
    }
    /* tsdbCacheInsertLast(pCache, uid, pRow); */
    h = taosLRUCacheLookup(pCache, key, keyLen);
    //*ppRow = (STSRow *)taosLRUCacheValue(pCache, h);
  }

  *handle = h;

  return code;
}

int32_t tsdbCacheDelete(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    STSRow *pRow = (STSRow *)taosLRUCacheValue(pCache, h);
    if (pRow->ts <= eKey) {
      taosLRUCacheRelease(pCache, h, true);
    } else {
      taosLRUCacheRelease(pCache, h, false);
    }

    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  getTableCacheKey(uid, "l", key, &keyLen);
  h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    // clear last cache anyway, no matter where eKey ends.
    taosLRUCacheRelease(pCache, h, true);

    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}

int32_t tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h) {
  int32_t code = 0;

  taosLRUCacheRelease(pCache, h, false);

  return code;
}
