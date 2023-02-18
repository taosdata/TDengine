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

#include "mndIndex.h"
#include "mndIndexComm.h"
#include "mndSma.h"

static void *mndGetIdx(SMnode *pMnode, char *name, int type) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIdx = sdbAcquire(pSdb, type, name);
  if (pIdx == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = 0;
  }
  return pIdx;
}

int mndAcquireGlobalIdx(SMnode *pMnode, char *name, int type, SSIdx *idx) {
  SSmaObj *pSma = mndGetIdx(pMnode, name, SDB_SMA);
  SIdxObj *pIdx = mndGetIdx(pMnode, name, SDB_IDX);

  terrno = 0;

  if (pSma == NULL && pIdx == NULL) return 0;

  if (pSma != NULL) {
    if (type == SDB_SMA) {
      idx->type = SDB_SMA;
      idx->pIdx = pSma;
    } else {
      mndReleaseSma(pMnode, pSma);
      terrno = TSDB_CODE_MND_SMA_ALREADY_EXIST;
      return -1;
    }
  } else {
    if (type == SDB_IDX) {
      idx->type = SDB_IDX;
      idx->pIdx = pIdx;
    } else {
      mndReleaseIdx(pMnode, pIdx);
      terrno = TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST;
      return -1;
    }
  }
  return 0;
}
