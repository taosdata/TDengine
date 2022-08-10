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

typedef struct {
  TSKEY   ts;
  SColVal colVal;
} SLastCol;

int32_t tsdbOpenCache(STsdb *pTsdb) {
  int32_t    code = 0;
  SLRUCache *pCache = NULL;
  size_t     cfgCapacity = pTsdb->pVnode->config.cacheLastSize * 1024 * 1024;

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

static void getTableCacheKey(tb_uid_t uid, int cacheType, char *key, int *len) {
  if (cacheType == 0) {  // last_row
    *(uint64_t *)key = (uint64_t)uid;
  } else {  // last
    *(uint64_t *)key = ((uint64_t)uid) | 0x8000000000000000;
  }

  *len = sizeof(uint64_t);
}

static void deleteTableCacheLastrow(const void *key, size_t keyLen, void *value) { taosMemoryFree(value); }

static void deleteTableCacheLast(const void *key, size_t keyLen, void *value) { taosArrayDestroy(value); }

int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;

  char key[32] = {0};
  int  keyLen = 0;

  // getTableCacheKey(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
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

int32_t tsdbCacheDeleteLast(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;

  char key[32] = {0};
  int  keyLen = 0;

  // getTableCacheKey(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    bool    invalidate = false;
    int16_t nCol = taosArrayGetSize(pLast);

    for (int16_t iCol = 0; iCol < nCol; ++iCol) {
      SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
      if (eKey >= tTsVal->ts) {
        invalidate = true;
        break;
      }
    }

    if (invalidate) {
      taosLRUCacheRelease(pCache, h, true);
    } else {
      taosLRUCacheRelease(pCache, h, false);
    }
    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}

int32_t tsdbCacheDelete(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKey(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
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

  // getTableCacheKey(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    bool    invalidate = false;
    int16_t nCol = taosArrayGetSize(pLast);

    for (int16_t iCol = 0; iCol < nCol; ++iCol) {
      SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
      if (eKey >= tTsVal->ts) {
        invalidate = true;
        break;
      }
    }

    if (invalidate) {
      taosLRUCacheRelease(pCache, h, true);
    } else {
      taosLRUCacheRelease(pCache, h, false);
    }
    // void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}

int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, STsdb *pTsdb, tb_uid_t uid, STSRow *row, bool dup) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKey(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    cacheRow = (STSRow *)taosLRUCacheValue(pCache, h);
    if (row->ts >= cacheRow->ts) {
      if (row->ts == cacheRow->ts) {
        STSRow    *mergedRow = NULL;
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
        if (dup) {
          cacheRow = tdRowDup(row);
        } else {
          cacheRow = row;
        }
        _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
        LRUStatus status = taosLRUCacheInsert(pCache, key, keyLen, cacheRow, TD_ROW_LEN(cacheRow), deleter, NULL,
                                              TAOS_LRU_PRIORITY_LOW);
        if (status != TAOS_LRU_STATUS_OK) {
          code = -1;
        }
        /* tsdbCacheInsertLastrow(pCache, uid, row, dup); */
      }
    }
  } /*else {
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
    }*/

  return code;
}

int32_t tsdbCacheInsertLast(SLRUCache *pCache, tb_uid_t uid, STSRow *row, STsdb *pTsdb) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKey(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);
    TSKEY     keyTs = row->ts;
    bool      invalidate = false;

    SArray *pLast = (SArray *)taosLRUCacheValue(pCache, h);
    int16_t nCol = taosArrayGetSize(pLast);
    int16_t iCol = 0;

    SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
    if (keyTs > tTsVal->ts) {
      STColumn *pTColumn = &pTSchema->columns[0];
      SColVal   tColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = keyTs});

      taosArraySet(pLast, iCol, &(SLastCol){.ts = keyTs, .colVal = tColVal});
    }

    for (++iCol; iCol < nCol; ++iCol) {
      SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLast, iCol);
      if (keyTs >= tTsVal->ts) {
        SColVal *tColVal = &tTsVal->colVal;

        SColVal colVal = {0};
        tTSRowGetVal(row, pTSchema, iCol, &colVal);
        if (colVal.isNone || colVal.isNull) {
          if (keyTs == tTsVal->ts && !tColVal->isNone && !tColVal->isNull) {
            invalidate = true;

            break;
          }
        } else {
          taosArraySet(pLast, iCol, &(SLastCol){.ts = keyTs, .colVal = colVal});
        }
      }
    }

    taosMemoryFreeClear(pTSchema);

    taosLRUCacheRelease(pCache, h, invalidate);

    // clear last cache anyway, lazy load when get last lookup
    // taosLRUCacheRelease(pCache, h, true);
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

  if (pDelIdx) {
    code = tsdbReadDelData(pDelReader, pDelIdx, aDelData, NULL);
  }

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
  SArray *aDelData = NULL;

  aDelData = taosArrayInit(32, sizeof(SDelData));
  code = getTableDelData(pMem, pIMem, pDelReader, pDelIdx, aDelData);
  if (code) goto _err;

  size_t nDelData = taosArrayGetSize(aDelData);
  if (nDelData > 0) {
    code = tsdbBuildDeleteSkyline(aDelData, 0, (int32_t)(nDelData - 1), aSkyline);
    if (code) goto _err;
  }

  if (aDelData) {
    taosArrayDestroy(aDelData);
  }
_err:
  return code;
}

static int32_t getTableDelIdx(SDelFReader *pDelFReader, tb_uid_t suid, tb_uid_t uid, SDelIdx *pDelIdx) {
  int32_t code = 0;
  SArray *pDelIdxArray = NULL;

  // SMapData delIdxMap;
  pDelIdxArray = taosArrayInit(32, sizeof(SDelIdx));
  SDelIdx idx = {.suid = suid, .uid = uid};

  // tMapDataReset(&delIdxMap);
  //  code = tsdbReadDelIdx(pDelFReader, &delIdxMap, NULL);
  code = tsdbReadDelIdx(pDelFReader, pDelIdxArray, NULL);
  if (code) goto _err;

  // code = tMapDataSearch(&delIdxMap, &idx, tGetDelIdx, tCmprDelIdx, pDelIdx);
  SDelIdx *pIdx = taosArraySearch(pDelIdxArray, &idx, tCmprDelIdx, TD_EQ);
  if (code) goto _err;

  *pDelIdx = *pIdx;

  if (pDelIdxArray) {
    taosArrayDestroy(pDelIdxArray);
  }
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
  SBlockIdx       *pBlockIdx;
  SMapData         blockMap;
  int32_t          nBlock;
  int32_t          iBlock;
  SBlock           block;
  SBlockData       blockData;
  SBlockData      *pBlockData;
  int32_t          nRow;
  int32_t          iRow;
  TSDBROW          row;
} SFSNextRowIter;

static int32_t getNextRowFromFS(void *iter, TSDBROW **ppRow) {
  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  int32_t         code = 0;

  switch (state->state) {
    case SFSNEXTROW_FS:
      // state->aDFileSet = state->pTsdb->pFS->cState->aDFileSet;
      state->nFileSet = taosArrayGetSize(state->aDFileSet);
      state->iFileSet = state->nFileSet;

      state->pBlockData = NULL;

    case SFSNEXTROW_FILESET: {
      SDFileSet *pFileSet = NULL;
    _next_fileset:
      if (--state->iFileSet >= 0) {
        pFileSet = (SDFileSet *)taosArrayGet(state->aDFileSet, state->iFileSet);
      } else {
        // tBlockDataClear(&state->blockData, 1);
        if (state->pBlockData) {
          tBlockDataClear(state->pBlockData, 1);
          state->pBlockData = NULL;
        }

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
      /* } */
      /* code = tMapDataSearch(&state->blockIdxMap, state->pBlockIdxExp, tGetBlockIdx, tCmprBlockIdx,
       * &state->blockIdx);
       */
      state->pBlockIdx = taosArraySearch(state->aBlockIdx, state->pBlockIdxExp, tCmprBlockIdx, TD_EQ);
      if (code) goto _err;

      if (!state->pBlockIdx) {
        goto _next_fileset;
      }

      tMapDataReset(&state->blockMap);
      code = tsdbReadBlock(state->pDataFReader, state->pBlockIdx, &state->blockMap, NULL);
      /* code = tsdbReadBlock(state->pDataFReader, &state->blockIdx, &state->blockMap, NULL); */
      if (code) goto _err;

      state->nBlock = state->blockMap.nItem;
      state->iBlock = state->nBlock - 1;

      if (!state->pBlockData) {
        state->pBlockData = &state->blockData;

        tBlockDataInit(&state->blockData);
      }
    }
    case SFSNEXTROW_BLOCKDATA:
      if (state->iBlock >= 0) {
        SBlock block = {0};

        tBlockReset(&block);
        // tBlockDataReset(&state->blockData);
        tBlockDataReset(state->pBlockData);

        tMapDataGetItemByIdx(&state->blockMap, state->iBlock, &block, tGetBlock);
        /* code = tsdbReadBlockData(state->pDataFReader, &state->blockIdx, &block, &state->blockData, NULL, NULL); */
        code = tsdbReadBlockData(state->pDataFReader, state->pBlockIdx, &block, state->pBlockData, NULL, NULL);
        if (code) goto _err;

        state->nRow = state->blockData.nRow;
        state->iRow = state->nRow - 1;

        state->state = SFSNEXTROW_BLOCKROW;
      }
    case SFSNEXTROW_BLOCKROW:
      if (state->iRow >= 0) {
        state->row = tsdbRowFromBlockData(state->pBlockData, state->iRow);
        *ppRow = &state->row;

        if (--state->iRow < 0) {
          state->state = SFSNEXTROW_BLOCKDATA;
          if (--state->iBlock < 0) {
            tsdbDataFReaderClose(&state->pDataFReader);
            state->pDataFReader = NULL;

            if (state->aBlockIdx) {
              taosArrayDestroy(state->aBlockIdx);
              state->aBlockIdx = NULL;
            }

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
    state->pDataFReader = NULL;
  }
  if (state->aBlockIdx) {
    taosArrayDestroy(state->aBlockIdx);
    state->aBlockIdx = NULL;
  }
  if (state->pBlockData) {
    // tBlockDataClear(&state->blockData, 1);
    tBlockDataClear(state->pBlockData, 1);
    state->pBlockData = NULL;
  }

  *ppRow = NULL;

  return code;
}

int32_t clearNextRowFromFS(void *iter) {
  int32_t code = 0;

  SFSNextRowIter *state = (SFSNextRowIter *)iter;
  if (!state) {
    return code;
  }

  if (state->pDataFReader) {
    tsdbDataFReaderClose(&state->pDataFReader);
    state->pDataFReader = NULL;
  }
  if (state->aBlockIdx) {
    taosArrayDestroy(state->aBlockIdx);
    state->aBlockIdx = NULL;
  }
  if (state->pBlockData) {
    // tBlockDataClear(&state->blockData, 1);
    tBlockDataClear(state->pBlockData, 1);
    state->pBlockData = NULL;
  }

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
  // bool              iterOpened;
  // TSDBROW          *curRow;
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

/* static int32_t tsRowFromTsdbRow(STSchema *pTSchema, TSDBROW *pRow, STSRow **ppRow) { */
/*   int32_t code = 0; */

/*   SColVal *pColVal = &(SColVal){0}; */

/*   if (pRow->type == 0) { */
/*     *ppRow = tdRowDup(pRow->pTSRow); */
/*   } else { */
/*     SArray *pArray = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal)); */
/*     if (pArray == NULL) { */
/*       code = TSDB_CODE_OUT_OF_MEMORY; */
/*       goto _exit; */
/*     } */

/*     TSDBKEY   key = TSDBROW_KEY(pRow); */
/*     STColumn *pTColumn = &pTSchema->columns[0]; */
/*     *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = key.ts}); */

/*     if (taosArrayPush(pArray, pColVal) == NULL) { */
/*       code = TSDB_CODE_OUT_OF_MEMORY; */
/*       goto _exit; */
/*     } */

/*     for (int16_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) { */
/*       tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal); */
/*       if (taosArrayPush(pArray, pColVal) == NULL) { */
/*         code = TSDB_CODE_OUT_OF_MEMORY; */
/*         goto _exit; */
/*       } */
/*     } */

/*     code = tdSTSRowNew(pArray, pTSchema, ppRow); */
/*     if (code) goto _exit; */
/*   } */

/* _exit: */
/*   return code; */
/* } */

static bool tsdbKeyDeleted(TSDBKEY *key, SArray *pSkyline, int64_t *iSkyline) {
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
typedef int32_t (*_next_row_clear_fn_t)(void *iter);

typedef struct {
  TSDBROW             *pRow;
  bool                 stop;
  bool                 next;
  void                *iter;
  _next_row_fn_t       nextRowFn;
  _next_row_clear_fn_t nextRowClearFn;
} TsdbNextRowState;

typedef struct {
  SArray *pSkyline;
  int64_t iSkyline;

  SBlockIdx       idx;
  SMemNextRowIter memState;
  SMemNextRowIter imemState;
  SFSNextRowIter  fsState;
  TSDBROW         memRow, imemRow, fsRow;

  TsdbNextRowState input[3];
  STsdbReadSnap   *pReadSnap;
  STsdb           *pTsdb;
} CacheNextRowIter;

static int32_t nextRowIterOpen(CacheNextRowIter *pIter, tb_uid_t uid, STsdb *pTsdb) {
  int code = 0;

  tb_uid_t suid = getTableSuidByUid(uid, pTsdb);

  tsdbTakeReadSnap(pTsdb, &pIter->pReadSnap);

  STbData *pMem = NULL;
  if (pIter->pReadSnap->pMem) {
    pMem = tsdbGetTbDataFromMemTable(pIter->pReadSnap->pMem, suid, uid);
  }

  STbData *pIMem = NULL;
  if (pIter->pReadSnap->pIMem) {
    pMem = tsdbGetTbDataFromMemTable(pIter->pReadSnap->pIMem, suid, uid);
  }

  pIter->pTsdb = pTsdb;

  pIter->pSkyline = taosArrayInit(32, sizeof(TSDBKEY));

  SDelIdx delIdx;

  SDelFile *pDelFile = pIter->pReadSnap->fs.pDelFile;
  if (pDelFile) {
    SDelFReader *pDelFReader;

    code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb, NULL);
    if (code) goto _err;

    code = getTableDelIdx(pDelFReader, suid, uid, &delIdx);
    if (code) goto _err;

    code = getTableDelSkyline(pMem, pIMem, pDelFReader, &delIdx, pIter->pSkyline);
    if (code) goto _err;

    tsdbDelFReaderClose(&pDelFReader);
  } else {
    code = getTableDelSkyline(pMem, pIMem, NULL, NULL, pIter->pSkyline);
    if (code) goto _err;
  }

  pIter->iSkyline = taosArrayGetSize(pIter->pSkyline) - 1;

  pIter->idx = (SBlockIdx){.suid = suid, .uid = uid};

  pIter->fsState.state = SFSNEXTROW_FS;
  pIter->fsState.pTsdb = pTsdb;
  pIter->fsState.aDFileSet = pIter->pReadSnap->fs.aDFileSet;
  pIter->fsState.pBlockIdxExp = &pIter->idx;

  pIter->input[0] = (TsdbNextRowState){&pIter->memRow, true, false, &pIter->memState, getNextRowFromMem, NULL};
  pIter->input[1] = (TsdbNextRowState){&pIter->imemRow, true, false, &pIter->imemState, getNextRowFromMem, NULL};
  pIter->input[2] =
      (TsdbNextRowState){&pIter->fsRow, false, true, &pIter->fsState, getNextRowFromFS, clearNextRowFromFS};

  if (pMem) {
    pIter->memState.pMem = pMem;
    pIter->memState.state = SMEMNEXTROW_ENTER;
    pIter->input[0].stop = false;
    pIter->input[0].next = true;
  }

  if (pIMem) {
    pIter->imemState.pMem = pIMem;
    pIter->imemState.state = SMEMNEXTROW_ENTER;
    pIter->input[1].stop = false;
    pIter->input[1].next = true;
  }

  return code;
_err:
  return code;
}

static int32_t nextRowIterClose(CacheNextRowIter *pIter) {
  int code = 0;

  for (int i = 0; i < 3; ++i) {
    if (pIter->input[i].nextRowClearFn) {
      pIter->input[i].nextRowClearFn(pIter->input[i].iter);
    }
  }

  if (pIter->pSkyline) {
    taosArrayDestroy(pIter->pSkyline);
  }

  tsdbUntakeReadSnap(pIter->pTsdb, pIter->pReadSnap);

  return code;
_err:
  return code;
}

// iterate next row non deleted backward ts, version (from high to low)
static int32_t nextRowIterGet(CacheNextRowIter *pIter, TSDBROW **ppRow) {
  int code = 0;

  for (int i = 0; i < 3; ++i) {
    if (pIter->input[i].next && !pIter->input[i].stop) {
      code = pIter->input[i].nextRowFn(pIter->input[i].iter, &pIter->input[i].pRow);
      if (code) goto _err;

      if (pIter->input[i].pRow == NULL) {
        pIter->input[i].stop = true;
        pIter->input[i].next = false;
      }
    }
  }

  if (pIter->input[0].stop && pIter->input[1].stop && pIter->input[2].stop) {
    *ppRow = NULL;
    return code;
  }

  // select maxpoint(s) from mem, imem, fs
  TSDBROW *max[3] = {0};
  int      iMax[3] = {-1, -1, -1};
  int      nMax = 0;
  TSKEY    maxKey = TSKEY_MIN;

  for (int i = 0; i < 3; ++i) {
    if (!pIter->input[i].stop && pIter->input[i].pRow != NULL) {
      TSDBKEY key = TSDBROW_KEY(pIter->input[i].pRow);

      // merging & deduplicating on client side
      if (maxKey <= key.ts) {
        if (maxKey < key.ts) {
          nMax = 0;
          maxKey = key.ts;
        }

        iMax[nMax] = i;
        max[nMax++] = pIter->input[i].pRow;
      }
    }
  }

  // delete detection
  TSDBROW *merge[3] = {0};
  int      iMerge[3] = {-1, -1, -1};
  int      nMerge = 0;
  for (int i = 0; i < nMax; ++i) {
    TSDBKEY maxKey = TSDBROW_KEY(max[i]);

    bool deleted = tsdbKeyDeleted(&maxKey, pIter->pSkyline, &pIter->iSkyline);
    if (!deleted) {
      iMerge[nMerge] = iMax[i];
      merge[nMerge++] = max[i];
    }

    pIter->input[iMax[i]].next = deleted;
  }

  if (nMerge > 0) {
    pIter->input[iMerge[0]].next = true;

    *ppRow = merge[0];
  } else {
    *ppRow = NULL;
  }

  return code;
_err:
  return code;
}

static int32_t mergeLastRow(tb_uid_t uid, STsdb *pTsdb, bool *dup, STSRow **ppRow) {
  int32_t code = 0;

  STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);
  int16_t   nCol = pTSchema->numOfCols;
  int16_t   iCol = 0;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  SArray   *pColArray = taosArrayInit(nCol, sizeof(SColVal));
  SColVal  *pColVal = &(SColVal){0};

  TSKEY lastRowTs = TSKEY_MAX;

  CacheNextRowIter iter = {0};
  nextRowIterOpen(&iter, uid, pTsdb);

  do {
    TSDBROW *pRow = NULL;
    nextRowIterGet(&iter, &pRow);

    if (!pRow) {
      break;
    }

    if (lastRowTs == TSKEY_MAX) {
      lastRowTs = TSDBROW_TS(pRow);
      STColumn *pTColumn = &pTSchema->columns[0];

      *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = lastRowTs});
      if (taosArrayPush(pColArray, pColVal) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }

      for (iCol = 1; iCol < nCol; ++iCol) {
        tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);

        if (taosArrayPush(pColArray, pColVal) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }

        if (pColVal->isNone && !setNoneCol) {
          noneCol = iCol;
          setNoneCol = true;
        }
      }
      if (!setNoneCol) {
        // goto build the result ts row
        break;
      } else {
        continue;
      }
    }

    if ((TSDBROW_TS(pRow) < lastRowTs)) {
      // goto build the result ts row
      break;
    }

    // merge into pColArray
    setNoneCol = false;
    for (iCol = noneCol; iCol < nCol; ++iCol) {
      // high version's column value
      SColVal *tColVal = (SColVal *)taosArrayGet(pColArray, iCol);

      tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
      if (tColVal->isNone && !pColVal->isNone) {
        taosArraySet(pColArray, iCol, pColVal);
      } else if (tColVal->isNone && pColVal->isNone && !setNoneCol) {
        noneCol = iCol;
        setNoneCol = true;
      }
    }
  } while (setNoneCol);

  // build the result ts row here
  *dup = false;
  if (taosArrayGetSize(pColArray) == nCol) {
    code = tdSTSRowNew(pColArray, pTSchema, ppRow);
    if (code) goto _err;
  } else {
    *ppRow = NULL;
  }

  nextRowIterClose(&iter);
  taosArrayDestroy(pColArray);
  taosMemoryFreeClear(pTSchema);
  return code;

_err:
  nextRowIterClose(&iter);
  taosArrayDestroy(pColArray);
  taosMemoryFreeClear(pTSchema);
  return code;
}

static int32_t mergeLast(tb_uid_t uid, STsdb *pTsdb, SArray **ppLastArray) {
  int32_t code = 0;

  STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, uid, -1);
  int16_t   nCol = pTSchema->numOfCols;
  int16_t   iCol = 0;
  int16_t   noneCol = 0;
  bool      setNoneCol = false;
  SArray   *pColArray = taosArrayInit(nCol, sizeof(SLastCol));
  SColVal  *pColVal = &(SColVal){0};

  TSKEY lastRowTs = TSKEY_MAX;

  CacheNextRowIter iter = {0};
  nextRowIterOpen(&iter, uid, pTsdb);

  do {
    TSDBROW *pRow = NULL;
    nextRowIterGet(&iter, &pRow);

    if (!pRow) {
      break;
    }

    TSKEY rowTs = TSDBROW_TS(pRow);

    if (lastRowTs == TSKEY_MAX) {
      lastRowTs = rowTs;
      STColumn *pTColumn = &pTSchema->columns[0];

      *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, (SValue){.ts = lastRowTs});
      if (taosArrayPush(pColArray, &(SLastCol){.ts = lastRowTs, .colVal = *pColVal}) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }

      for (iCol = 1; iCol < nCol; ++iCol) {
        tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);

        if (taosArrayPush(pColArray, &(SLastCol){.ts = lastRowTs, .colVal = *pColVal}) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }

        if ((pColVal->isNone || pColVal->isNull) && !setNoneCol) {
          noneCol = iCol;
          setNoneCol = true;
        }
      }
      if (!setNoneCol) {
        // goto build the result ts row
        break;
      } else {
        continue;
      }
    }

    // merge into pColArray
    setNoneCol = false;
    for (iCol = noneCol; iCol < nCol; ++iCol) {
      // high version's column value
      SColVal *tColVal = (SColVal *)taosArrayGet(pColArray, iCol);

      tsdbRowGetColVal(pRow, pTSchema, iCol, pColVal);
      if ((tColVal->isNone || tColVal->isNull) && (!pColVal->isNone && !pColVal->isNull)) {
        taosArraySet(pColArray, iCol, &(SLastCol){.ts = rowTs, .colVal = *pColVal});
      } else if ((tColVal->isNone || tColVal->isNull) && (pColVal->isNone || pColVal->isNull) && !setNoneCol) {
        noneCol = iCol;
        setNoneCol = true;
      }
    }
  } while (setNoneCol);

  // build the result ts row here
  if (taosArrayGetSize(pColArray) <= 0) {
    *ppLastArray = NULL;
    taosArrayDestroy(pColArray);
  } else {
    *ppLastArray = pColArray;
  }

  nextRowIterClose(&iter);
  taosMemoryFreeClear(pTSchema);
  return code;

_err:
  nextRowIterClose(&iter);
  taosMemoryFreeClear(pTSchema);
  return code;
}

int32_t tsdbCacheGetLastrowH(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, LRUHandle **handle) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  //  getTableCacheKeyS(uid, "lr", key, &keyLen);
  getTableCacheKey(uid, 0, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
  } else {
    STSRow *pRow = NULL;
    bool    dup = false;  // which is always false for now
    code = mergeLastRow(uid, pTsdb, &dup, &pRow);
    // if table's empty or error, return code of -1
    if (code < 0 || pRow == NULL) {
      if (!dup && pRow) {
        taosMemoryFree(pRow);
      }

      *handle = NULL;
      return 0;
    }

    _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
    LRUStatus           status =
        taosLRUCacheInsert(pCache, key, keyLen, pRow, TD_ROW_LEN(pRow), deleter, NULL, TAOS_LRU_PRIORITY_LOW);
    if (status != TAOS_LRU_STATUS_OK) {
      code = -1;
    }

    h = taosLRUCacheLookup(pCache, key, keyLen);
  }

  *handle = h;

  return code;
}

int32_t tsdbCacheLastArray2Row(SArray *pLastArray, STSRow **ppRow, STSchema *pTSchema) {
  int32_t code = 0;
  int16_t nCol = taosArrayGetSize(pLastArray);
  SArray *pColArray = taosArrayInit(nCol, sizeof(SColVal));

  for (int16_t iCol = 0; iCol < nCol; ++iCol) {
    SLastCol *tTsVal = (SLastCol *)taosArrayGet(pLastArray, iCol);
    SColVal  *tColVal = &tTsVal->colVal;
    taosArrayPush(pColArray, tColVal);
  }

  code = tdSTSRowNew(pColArray, pTSchema, ppRow);
  if (code) goto _err;

  taosArrayDestroy(pColArray);

  return code;

_err:
  taosArrayDestroy(pColArray);

  return code;
}

int32_t tsdbCacheGetLastH(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, LRUHandle **handle) {
  int32_t code = 0;
  char    key[32] = {0};
  int     keyLen = 0;

  // getTableCacheKeyS(uid, "l", key, &keyLen);
  getTableCacheKey(uid, 1, key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
  } else {
    SArray *pLastArray = NULL;
    code = mergeLast(uid, pTsdb, &pLastArray);
    // if table's empty or error, return code of -1
    // if (code < 0 || pRow == NULL) {
    if (code < 0 || pLastArray == NULL) {
      *handle = NULL;
      return 0;
    }

    _taos_lru_deleter_t deleter = deleteTableCacheLast;
    LRUStatus           status =
        taosLRUCacheInsert(pCache, key, keyLen, pLastArray, pLastArray->capacity, deleter, NULL, TAOS_LRU_PRIORITY_LOW);
    if (status != TAOS_LRU_STATUS_OK) {
      code = -1;
    }

    h = taosLRUCacheLookup(pCache, key, keyLen);
  }

  *handle = h;

  return code;
}

int32_t tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h) {
  int32_t code = 0;

  taosLRUCacheRelease(pCache, h, false);

  return code;
}

void tsdbCacheSetCapacity(SVnode *pVnode, size_t capacity) {
  taosLRUCacheSetCapacity(pVnode->pTsdb->lruCache, capacity);
}

size_t tsdbCacheGetCapacity(SVnode *pVnode) { return taosLRUCacheGetCapacity(pVnode->pTsdb->lruCache); }
