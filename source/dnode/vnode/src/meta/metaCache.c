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

#include "vnode.h"
#include "metaDef.h"

struct SMetaCache {
  // TODO
};

int metaOpenCache(SMeta *pMeta) {
  // TODO
  // if (pMeta->options.lruSize) {
  //   pMeta->pCache = rocksdb_cache_create_lru(pMeta->options.lruSize);
  //   if (pMeta->pCache == NULL) {
  //     // TODO: handle error
  //     return -1;
  //   }
  // }

  return 0;
}

void metaCloseCache(SMeta *pMeta) {
  // if (pMeta->pCache) {
  //   rocksdb_cache_destroy(pMeta->pCache);
  //   pMeta->pCache = NULL;
  // }
}