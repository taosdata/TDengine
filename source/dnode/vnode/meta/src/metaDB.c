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

#include "meta.h"
#include "metaDef.h"

int metaOpenDB(SMeta *pMeta) {
  char *             err = NULL;
  rocksdb_options_t *pOpts;

  pOpts = rocksdb_options_create();
  if (pOpts == NULL) {
    // TODO: handle error
    return -1;
  }

  // Create LRU cache
  if (pMeta->options.lruCacheSize) {
    pMeta->metaDB.pCache = rocksdb_cache_create_lru(pMeta->options.lruCacheSize);
    if (pMeta->metaDB.pCache == NULL) {
      // TODO: handle error
      return -1;
    }

    rocksdb_options_set_row_cache(pOpts, pMeta->metaDB.pCache);
  }

  // Open raw data DB
  pMeta->metaDB.pDB = rocksdb_open(pOpts, "db", &err);
  if (pMeta->metaDB.pDB == NULL) {
    // TODO: handle error
    return -1;
  }

  // Open index DB
  pMeta->metaDB.pIdx = rocksdb_open(pOpts, "index", &err);
  if (pMeta->metaDB.pIdx == NULL) {
    // TODO: handle error
    rocksdb_close(pMeta->metaDB.pDB);
    return -1;
  }

  return 0;
}

void metaCloseDB(SMeta *pMeta) { /* TODO */
  // Close index DB
  if (pMeta->metaDB.pIdx) {
    rocksdb_close(pMeta->metaDB.pIdx);
  }

  // Close raw data DB
  if (pMeta->metaDB.pDB) {
    rocksdb_close(pMeta->metaDB.pDB);
  }

  // Destroy cache
  if (pMeta->metaDB.pCache) {
    rocksdb_cache_destroy(pMeta->metaDB.pCache);
  }
}