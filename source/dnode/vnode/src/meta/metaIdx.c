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

#ifdef USE_INVERTED_INDEX
#include "index.h"
#endif
#include "meta.h"

struct SMetaIdx {
#ifdef USE_INVERTED_INDEX
  SIndex *pIdx;
#endif
  /* data */
#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif
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

#ifdef USE_INVERTED_INDEX
  // SIndexOpts opts;
  // if (indexOpen(&opts, pMeta->path, &pMeta->pIdx->pIdx) != 0) {
  //  return -1;
  //}

#endif
  return 0;
}

#ifdef BUILD_NO_CALL
void metaCloseIdx(SMeta *pMeta) { /* TODO */
#if 0
  if (pMeta->pIdx) {
    rocksdb_close(pMeta->pIdx);
    pMeta->pIdx = NULL;
  }
#endif

#ifdef USE_INVERTED_INDEX
  // SIndexOpts opts;
  // if (indexClose(pMeta->pIdx->pIdx) != 0) {
  //  return -1;
  //}
  // return 0;

#endif
}

int metaSaveTableToIdx(SMeta *pMeta, const STbCfg *pTbCfg) {
#ifdef USE_INVERTED_INDEX
  // if (pTbCfgs->type == META_CHILD_TABLE) {
  //  char    buf[8] = {0};
  //  int16_t colId = (kvRowColIdx(pTbCfg->ctbCfg.pTag))[0].colId;
  //  sprintf(buf, "%d", colId);  // colname

  //  char *pTagVal = (char *)tdGetKVRowValOfCol(pTbCfg->ctbCfg.pTag, (kvRowColIdx(pTbCfg->ctbCfg.pTag))[0].colId);

  //  tb_uid_t         suid = pTbCfg->ctbCfg.suid;  // super id
  //  tb_uid_t         tuid = 0;                    // child table uid
  //  SIndexMultiTerm *terms = indexMultiTermCreate();
  //  SIndexTerm      *term =
  //      indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_BINARY, buf, strlen(buf), pTagVal, strlen(pTagVal), tuid);
  //  indexMultiTermAdd(terms, term);

  //  int ret = indexPut(pMeta->pIdx->pIdx, terms);
  //  indexMultiTermDestroy(terms);
  //  return ret;
  //} else {
  //  return DB_DONOTINDEX;
  //}
#endif
  // TODO
  return 0;
}

int metaRemoveTableFromIdx(SMeta *pMeta, tb_uid_t uid) {
#ifdef USE_INVERTED_INDEX

#endif
  // TODO
  return 0;
}
#endif