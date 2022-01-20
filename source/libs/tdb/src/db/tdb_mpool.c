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

#include "tdb_mpool.h"

int tdbMPoolOpen(TDB_MPOOL **mpp, uint64_t cachesize, pgsize_t pgsize) {
  TDB_MPOOL *mp;
  size_t     tsize;
  MP_PAGE *  pagep;

  // check parameters
  if (!TDB_IS_PGSIZE_VLD(pgsize)) {
    tdbError("invalid page size");
    return -1;
  }

  // allocate handle
  mp = (TDB_MPOOL *)calloc(1, sizeof(*mp));
  if (mp == NULL) {
    tdbError("failed to malloc memory pool handle");
    return -1;
  }

  // initialize the handle
  mp->cachesize = cachesize;
  mp->pgsize = pgsize;
  mp->npages = cachesize / pgsize;
  mp->pages = (MP_PAGE *)calloc(mp->npages, MP_PAGE_SIZE(pgsize));
  if (mp->pages == NULL) {
    tdbError("failed to malloc memory pool pages");
    free(mp);
    return -1;
  }

  TD_DLIST_INIT(&(mp->freeList));

  mp->nbucket = mp->npages;
  mp->hashtab = (MP_PAGE_LIST *)calloc(mp->nbucket, sizeof(MP_PAGE_LIST));
  if (mp->hashtab == NULL) {
    tdbError("failed to malloc memory pool hash table");
    free(mp->pages);
    free(mp);
    return -1;
  }

  for (int i = 0; i < mp->npages; i++) {
    pagep = (MP_PAGE *)MP_PAGE_AT(mp, i);
    TD_DLIST_APPEND(&mp->freeList, pagep);
  }

  // return
  *mpp = mp;
  return 0;
}

int tdbMPoolClose(TDB_MPOOL *mp) {
  // TODO
  return 0;
}