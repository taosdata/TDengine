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

#include "metaDef.h"

struct SMetaIdx {
  /* data */
};

int metaOpenIdx(SMeta *pMeta) {
#if 0
  char               idxDir[128];  // TODO
  char *             err = NULL;
  rocksdb_options_t *options = rocksdb_options_create();

  // TODO
  sprintf(idxDir, "%s/index", pMeta->path);

  if (pMeta->pCache) {
    rocksdb_options_set_row_cache(options, pMeta->pCache);
  }
  rocksdb_options_set_create_if_missing(options, 1);

  pMeta->pIdx = rocksdb_open(options, idxDir, &err);
  if (pMeta->pIdx == NULL) {
    // TODO: handle error
    rocksdb_options_destroy(options);
    return -1;
  }

  rocksdb_options_destroy(options);
#endif

  return 0;
}

void metaCloseIdx(SMeta *pMeta) { /* TODO */
#if 0
  if (pMeta->pIdx) {
    rocksdb_close(pMeta->pIdx);
    pMeta->pIdx = NULL;
  }
#endif
}

int metaSaveTableToIdx(SMeta *pMeta, const STbCfg *pTbOptions) {
  // TODO
  return 0;
}

int metaRemoveTableFromIdx(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  return 0;
}