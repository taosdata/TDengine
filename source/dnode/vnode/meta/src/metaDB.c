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
  /* TODO */
  pMeta->metaDB.pDB = tkvOpen(NULL, "db");
  if (pMeta->metaDB.pDB == NULL) {
    //   TODO
    return -1;
  }

  pMeta->metaDB.pIdx = tkvOpen(NULL, "index");
  if (pMeta->metaDB.pIdx == NULL) {
    /* TODO */
    return -1;
  }

  { /* TODO: for cache*/
  }

  return 0;
}

void metaCloseDB(SMeta *pMeta) { /* TODO */
  {
    // TODO: clear cache
  }

  if (pMeta->metaDB.pIdx) {
    tkvClose(pMeta->metaDB.pIdx);
  }

  if (pMeta->metaDB.pDB) {
    tkvClose(pMeta->metaDB.pIdx);
  }
}