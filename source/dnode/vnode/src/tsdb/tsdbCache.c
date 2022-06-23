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
  int32_t code = 0;
  SLRUCache *pCache = NULL;
  size_t cfgCapacity = 1024 * 1024; // TODO: get cfg from tsdb config

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

  snprintf(key, 30, "%"PRIi64 "%s", uid, cacheType);
  *len = strlen(key);
}

static void deleteTableCacheLastrow(const void *key, size_t keyLen, void *value) {
  taosMemoryFree(value);
}

int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, tb_uid_t uid, STSRow *row) {
  int32_t code = 0;
  STSRow *cacheRow = NULL;
  char key[32] = {0};
  int keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    cacheRow = (STSRow *) taosLRUCacheValue(pCache, h);
    if (row->ts >= cacheRow->ts) {
      if (row->ts > cacheRow->ts) {
	tdRowCpy(cacheRow, row);
      }
    }
  } else {
    cacheRow = tdRowDup(row);

    _taos_lru_deleter_t deleter = deleteTableCacheLastrow;
    LRUStatus status = taosLRUCacheInsert(pCache, key, keyLen, cacheRow, TD_ROW_LEN(cacheRow),
					  deleter, NULL, TAOS_LRU_PRIORITY_LOW);
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
    metaReaderClear(&mr); // table not esist
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
static int32_t getMemLastRow(SMemTable *mem, tb_uid_t suid, tb_uid_t uid, STSRow **ppRow) {
  int32_t code = 0;

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
  int32_t code = 0;
  SDelData *pDelData = pTbData ? pTbData->pHead : NULL;

  for (; pDelData; pDelData = pDelData->pNext) {
    taosArrayPush(aDelData, pDelData);
  }

  return code;
}

static int32_t getTableDelData(STbData *pMem, STbData *pIMem, SDelFReader *pDelReader, SDelIdx *pDelIdx, SArray *aDelData) {
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

static int32_t getTableDelSkyline(STbData *pMem, STbData *pIMem, SDelFReader *pDelReader, SDelIdx *pDelIdx, SArray *aSkyline) {
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

static int32_t mergeLastRowFileSet(STbDataIter *iter, STbDataIter *iiter, SDFileSet *pFileSet,
				   SArray *pSkyline,
				   STsdb *pTsdb,
				   STSRow **pLastRow) {
  int32_t code = 0;

  TSDBROW *pMemRow = NULL;
  TSDBROW *pIMemRow = NULL;

  if (iter != NULL) {
    pMemRow = tsdbTbDataIterGet(iter);
  }

  if (iter != NULL) {
    pIMemRow = tsdbTbDataIterGet(iiter);
  }

  SDataFReader *pDataFReader;
  code = tsdbDataFReaderOpen(&pDataFReader, pTsdb, pFileSet);
  if (code) goto _err;

  SMapData blockIdxMap;
  tMapDataReset(&blockIdxMap);
  code = tsdbReadBlockIdx(pDataFReader, &blockIdxMap, NULL);
  if (code) goto _err;

  SBlockData *pBlockData;

  tsdbDataFReaderClose(pDataFReader);

_err:
  return code;
}

static int32_t mergeLastRow(tb_uid_t uid, STsdb *pTsdb, STSRow **ppRow) {
  int32_t code = 0;

  tb_uid_t suid = getTableSuidByUid(uid, pTsdb);

  STbData *pMem = NULL;
  STbData *pIMem = NULL;
  STbDataIter iter;              // mem buffer skip list iterator
  STbDataIter iiter;             // imem buffer skip list iterator

  if (pTsdb->mem) {
    tsdbGetTbDataFromMemTable(pTsdb->mem, suid, uid, &pMem);
    if (pMem != NULL) {
      tsdbTbDataIterOpen(pMem, NULL, 1, &iter);
    }
  }

  if (pTsdb->imem) {
    tsdbGetTbDataFromMemTable(pTsdb->imem, suid, uid, &pIMem);
    if (pIMem != NULL) {
      tsdbTbDataIterOpen(pIMem, NULL, 1, &iiter);
    }
  }

  *ppRow = NULL;

  SDelFReader *pDelFReader;
  //code = tsdbDelFReaderOpen(&pDelFReader, pTsdb->fs->cState->pDelFile, pTsdb, NULL);
  if (code) goto _err;

  SDelIdx delIdx;
  code = getTableDelIdx(pDelFReader, suid, uid, &delIdx);
  if (code) goto _err;

  SArray *pSkyline = taosArrayInit(32, sizeof(TSDBKEY));
  code = getTableDelSkyline(pMem, pIMem, pDelFReader, &delIdx, pSkyline);
  if (code) goto _err;
  /*
  SFSIter fsiter;
  bool fsHasNext = false;

  tsdbFSIterOpen(pTsdb->fs, TSDB_FS_ITER_BACKWARD, &fsiter);
  do {
  */
    SDFileSet *pFileSet = NULL;
    //pFileSet = tsdbFSIterGet(fsiter);
    
    code = mergeLastRowFileSet(&iter, &iiter, pFileSet, pSkyline, pTsdb, ppRow);
    if (code < 0) {
      goto _err;
    }

    if (*ppRow != NULL) {
      //break;
    }
  /*    
  } while (fsHasNext = tsdbFSIterNext(fsiter))
  */

  tsdbDelFReaderClose(pDelFReader);

  return code;

_err:
  tsdbError("vgId:%d merge last_row failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbCacheGetLastrow(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, STSRow **ppRow) {
  int32_t code = 0;
  char key[32] = {0};
  int keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    *ppRow = (STSRow *) taosLRUCacheValue(pCache, h);
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
  char key[32] = {0};
  int keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    taosLRUCacheRelease(pCache, h, true);
    //void taosLRUCacheErase(SLRUCache * cache, const void *key, size_t keyLen);
  }

  return code;
}
