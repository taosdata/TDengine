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

int32_t tsdbCacheGetLastrow(SLRUCache *pCache, tb_uid_t uid, STSRow **ppRow) {
  int32_t code = 0;
  char key[32] = {0};
  int keyLen = 0;

  getTableCacheKey(uid, "lr", key, &keyLen);
  LRUHandle *h = taosLRUCacheLookup(pCache, key, keyLen);
  if (h) {
    *ppRow = (STSRow *) taosLRUCacheValue(pCache, h);
  } else {
    // TODO: load lastrow from mem, imem, and files
    // if table's empty, return code of -1
    code = -1;
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
