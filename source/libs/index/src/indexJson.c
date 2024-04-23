/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include "index.h"
#include "indexInt.h"

int indexJsonOpen(SIndexJsonOpts *opts, const char *path, SIndexJson **index) {
  // handle
  return indexOpen(opts, path, index);
}
int indexJsonPut(SIndexJson *index, SIndexJsonMultiTerm *terms, uint64_t uid) {
  for (int i = 0; i < taosArrayGetSize(terms); i++) {
    SIndexJsonTerm *p = taosArrayGetP(terms, i);
    if (p->colType == TSDB_DATA_TYPE_BOOL) {
      p->colType = TSDB_DATA_TYPE_INT;
    } else if (p->colType == TSDB_DATA_TYPE_VARBINARY || p->colType == TSDB_DATA_TYPE_NCHAR ||
               p->colType == TSDB_DATA_TYPE_BINARY || p->colType == TSDB_DATA_TYPE_GEOMETRY) {
      // p->colType = TSDB_DATA_TYPE_NCHAR;
    } else {
      p->colType = TSDB_DATA_TYPE_DOUBLE;
    }
    IDX_TYPE_ADD_EXTERN_TYPE((p->colType), TSDB_DATA_TYPE_JSON);
  }
  // handle put
  return indexPut(index, terms, uid);
}

int indexJsonSearch(SIndexJson *index, SIndexJsonMultiTermQuery *tq, SArray *result) {
  SArray *terms = tq->query;
  for (int i = 0; i < taosArrayGetSize(terms); i++) {
    SIndexJsonTerm *p = taosArrayGetP(terms, i);
    if (p->colType == TSDB_DATA_TYPE_BOOL) {
      p->colType = TSDB_DATA_TYPE_INT;
    } else if (p->colType == TSDB_DATA_TYPE_VARBINARY || p->colType == TSDB_DATA_TYPE_NCHAR ||
               p->colType == TSDB_DATA_TYPE_BINARY || p->colType == TSDB_DATA_TYPE_GEOMETRY) {
      // p->colType = TSDB_DATA_TYPE_NCHAR;
    } else {
      p->colType = TSDB_DATA_TYPE_DOUBLE;
    }
    IDX_TYPE_ADD_EXTERN_TYPE(p->colType, TSDB_DATA_TYPE_JSON);
  }
  // handle search
  return indexSearch(index, tq, result);
}

void indexJsonClose(SIndexJson *index) {
  // handle close
  return indexClose(index);
}
