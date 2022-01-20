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

static int  tdbGnrtFileID(const char *fname, uint8_t *fileid);
static void tdbMpoolRegFile(TDB_MPOOL *mp, TDB_MPFILE *mpf);

int tdbMPoolOpen(TDB_MPOOL **mpp, uint64_t cachesize, pgsize_t pgsize) {
  TDB_MPOOL *mp = NULL;
  size_t     tsize;
  pg_t *     pagep;

  // check parameters
  if (!TDB_IS_PGSIZE_VLD(pgsize)) {
    tdbError("invalid page size");
    return -1;
  }

  // allocate handle
  mp = (TDB_MPOOL *)calloc(1, sizeof(*mp));
  if (mp == NULL) {
    tdbError("failed to malloc memory pool handle");
    goto _err;
  }

  // initialize the handle
  mp->cachesize = cachesize;
  mp->pgsize = pgsize;
  mp->npages = cachesize / pgsize;

  TD_DLIST_INIT(&mp->freeList);

  mp->pages = (pg_t **)calloc(mp->npages, sizeof(pg_t *));
  if (mp->pages == NULL) {
    tdbError("failed to malloc memory pool pages");
    goto _err;
  }

  for (frame_id_t i = 0; i < mp->npages; i++) {
    mp->pages[i] = (pg_t *)calloc(1, MP_PAGE_SIZE(pgsize));
    if (mp->pages[i] == NULL) {
      goto _err;
    }

    taosInitRWLatch(&mp->pages[i]->rwLatch);
    mp->pages[i]->frameid = i;
    mp->pages[i]->pgid = TDB_IVLD_PGID;

    // TODO: add the new page to the free list
    // TD_DLIST_APPEND(&mp->freeList, mp->pages[i]);
  }

#define PGTAB_FACTOR 1.0
  mp->pgtab.nbucket = mp->npages / PGTAB_FACTOR;
  mp->pgtab.hashtab = (pg_list_t *)calloc(mp->pgtab.nbucket, sizeof(pg_list_t));
  if (mp->pgtab.hashtab == NULL) {
    tdbError("failed to malloc memory pool hash table");
    goto _err;
  }

  // return
  *mpp = mp;
  return 0;

_err:
  tdbMPoolClose(mp);
  *mpp = NULL;
  return -1;
}

int tdbMPoolClose(TDB_MPOOL *mp) {
  if (mp) {
    tfree(mp->pgtab.hashtab);
    if (mp->pages) {
      for (int i = 0; i < mp->npages; i++) {
        tfree(mp->pages[i]);
      }

      free(mp->pages);
    }

    free(mp);
  }
  return 0;
}

int tdbMPoolFileOpen(TDB_MPFILE **mpfp, const char *fname, TDB_MPOOL *mp) {
  TDB_MPFILE *mpf;

  if ((mpf = (TDB_MPFILE *)calloc(1, sizeof(*mpf))) == NULL) {
    return -1;
  }

  mpf->fd = -1;
  mpf->mp = mp;

  if ((mpf->fname = strdup(fname)) == NULL) {
    goto _err;
  }

  if ((mpf->fd = open(fname, O_CREAT | O_RDWR, 0755)) < 0) {
    goto _err;
  }

  if (tdbGnrtFileID(fname, mpf->fileid) < 0) {
    goto _err;
  }

  // Register current MPF to MP
  tdbMpoolRegFile(mp, mpf);

  *mpfp = mpf;
  return 0;

_err:
  tdbMPoolFileClose(mpf);
  *mpfp = NULL;
  return -1;
}

int tdbMPoolFileClose(TDB_MPFILE *mpf) {
  if (mpf) {
    if (mpf->fd > 0) {
      close(mpf->fd);
    }
    tfree(mpf->fname);
    free(mpf);
  }
  return 0;
}

int tdbMPoolFileGet(TDB_MPFILE *mpf, pgno_t pgid, void *addr) {
  pg_t *     pagep;
  TDB_MPOOL *mp;

  mp = mpf->mp;

  // get page in the cache
  if (pagep) {
    // page is found in the page table
    // todo: pin the page and return
    *(void **)addr = pagep->data;
    return 0;
  } else {
    // page not found
    pagep = TD_DLIST_HEAD(&mp->freeList);
    if (pagep) {
      // TD_DLIST_POP(&(mp->freeList), pagep);
    } else {
      // no page found in the freelist, need to evict
      // pagep = tdbMpoolEvict(mp);
      if (pagep) {
      } else {
        // TODO: Cannot find a page to evict
      }
    }
  }

  return 0;
}

int tdbMPoolFilePut(TDB_MPOOL *mpf, pgno_t pgid, void *addr) {
  // TODO
  return 0;
}

static int tdbGnrtFileID(const char *fname, uint8_t *fileid) {
  struct stat statbuf;

  if (stat(fname, &statbuf) < 0) {
    return -1;
  }

  memset(fileid, 0, TDB_FILE_ID_LEN);

  ((uint64_t *)fileid)[0] = (uint64_t)statbuf.st_ino;
  ((uint64_t *)fileid)[1] = (uint64_t)statbuf.st_dev;
  ((uint64_t *)fileid)[2] = rand();

  return 0;
}

static void tdbMpoolRegFile(TDB_MPOOL *mp, TDB_MPFILE *mpf) {
  // TODO
}