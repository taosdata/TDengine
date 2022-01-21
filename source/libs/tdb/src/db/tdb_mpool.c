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

static int         tdbGnrtFileID(const char *fname, uint8_t *fileid);
static void        tdbMPoolRegFile(TDB_MPOOL *mp, TDB_MPFILE *mpf);
static void        tdbMPoolUnregFile(TDB_MPOOL *mp, TDB_MPFILE *mpf);
static TDB_MPFILE *tdbMPoolGetFile(TDB_MPOOL *mp, uint8_t *fileid);
static int         tdbMPoolFileReadPage(TDB_MPFILE *mpf, pgno_t pgno, void *p);
static int         tdbMPoolFileWritePage(TDB_MPFILE *mpf, pgno_t pgno, const void *p);

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

  mp->pages = (pg_t *)calloc(mp->npages, sizeof(pg_t));
  if (mp->pages == NULL) {
    tdbError("failed to malloc memory pool pages");
    goto _err;
  }

  for (frame_id_t i = 0; i < mp->npages; i++) {
    mp->pages[i].p = malloc(pgsize);
    if (mp->pages[i].p == NULL) {
      goto _err;
    }

    taosInitRWLatch(&mp->pages[i].rwLatch);
    mp->pages[i].frameid = i;
    mp->pages[i].pgid = TDB_IVLD_PGID;

    // add new page to the free list
    TD_DLIST_APPEND_WITH_FIELD(&(mp->freeList), &(mp->pages[i]), free);
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
        tfree(mp->pages[i].p);
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
  tdbMPoolRegFile(mp, mpf);

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

#define MPF_GET_PAGE_BUCKETID(fileid, pgno, nbuckets) \
  ({                                                  \
    uint64_t *tmp = (uint64_t *)fileid;               \
    (tmp[0] + tmp[1] + tmp[2] + (pgno)) % (nbuckets); \
  })

int tdbMPoolFileNewPage(TDB_MPFILE *mpf, pgno_t *pgno, void *addr) {
  // TODO
  return 0;
}

int tdbMPoolFileFreePage(TDB_MPOOL *mpf, pgno_t *pgno, void *addr) {
  // TODO
  return 0;
}

int tdbMPoolFileGetPage(TDB_MPFILE *mpf, pgno_t pgno, void *addr) {
  pg_t *     pagep;
  TDB_MPOOL *mp;
  pg_list_t *pglist;

  mp = mpf->mp;

  // check if the page already in pool
  pglist = mp->pgtab.hashtab + MPF_GET_PAGE_BUCKETID(mpf->fileid, pgno, mp->pgtab.nbucket);
  pagep = TD_DLIST_HEAD(pglist);
  while (pagep) {
    if (memcmp(mpf->fileid, pagep->pgid.fileid, TDB_FILE_ID_LEN) == 0 && pgno == pagep->pgid.pgno) {
      break;
    }

    pagep = TD_DLIST_NODE_NEXT_WITH_FIELD(pagep, hash);
  }

  if (pagep) {
    // page is found
    // todo: pin the page and return
    *(void **)addr = pagep->p;
    return 0;
  }

  // page not found
  pagep = TD_DLIST_HEAD(&mp->freeList);
  if (pagep) {
    // has free page
    TD_DLIST_POP_WITH_FIELD(&(mp->freeList), pagep, free);
  } else {
    // no free page available
    // pagep = tdbMpoolEvict(mp);
    if (pagep) {
    } else {
      // TODO: Cannot find a page to evict
    }
  }

  if (pagep == NULL) {
    // no available container page
    return -1;
  }

  // load page from the disk if a container page is available
  // TODO: load the page from the disk
  if (tdbMPoolFileReadPage(mpf, pgno, pagep->p) < 0) {
    return -1;
  }

  memcpy(pagep->pgid.fileid, mpf->fileid, TDB_FILE_ID_LEN);
  pagep->pgid.pgno = pgno;
  pagep->dirty = 0;
  pagep->pinRef = 1;

  // add current page to page table
  TD_DLIST_APPEND_WITH_FIELD(pglist, pagep, hash);

  return 0;
}

int tdbMPoolFilePutPage(TDB_MPOOL *mpf, pgno_t pgno, void *addr) {
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

#define MPF_GET_BUCKETID(fileid)                   \
  ({                                               \
    uint64_t *tmp = (uint64_t *)fileid;            \
    (tmp[0] + tmp[1] + tmp[2]) % MPF_HASH_BUCKETS; \
  })

static void tdbMPoolRegFile(TDB_MPOOL *mp, TDB_MPFILE *mpf) {
  mpf_bucket_t *bktp;

  bktp = mp->mpfht.buckets + MPF_GET_BUCKETID(mpf->fileid);

  taosWLockLatch(&(bktp->latch));

  TD_DLIST_APPEND_WITH_FIELD(bktp, mpf, node);

  taosWUnLockLatch(&(bktp->latch));

  mpf->mp = mp;
}

static TDB_MPFILE *tdbMPoolGetFile(TDB_MPOOL *mp, uint8_t *fileid) {
  TDB_MPFILE *  mpf = NULL;
  mpf_bucket_t *bktp;

  bktp = mp->mpfht.buckets + MPF_GET_BUCKETID(fileid);

  taosRLockLatch(&(bktp->latch));

  mpf = TD_DLIST_HEAD(bktp);
  while (mpf) {
    if (memcmp(fileid, mpf->fileid, TDB_FILE_ID_LEN) == 0) {
      break;
    }

    mpf = TD_DLIST_NODE_NEXT_WITH_FIELD(mpf, node);
  }

  taosRUnLockLatch(&(bktp->latch));

  return mpf;
}

static void tdbMPoolUnregFile(TDB_MPOOL *mp, TDB_MPFILE *mpf) {
  mpf_bucket_t *bktp;
  TDB_MPFILE *  tmpf;

  if (mpf->mp == NULL) return;

  ASSERT(mpf->mp == mp);

  bktp = mp->mpfht.buckets + MPF_GET_BUCKETID(mpf->fileid);

  taosWLockLatch(&(bktp->latch));

  tmpf = TD_DLIST_HEAD(bktp);

  while (tmpf) {
    if (memcmp(mpf->fileid, tmpf->fileid, TDB_FILE_ID_LEN) == 0) {
      TD_DLIST_POP_WITH_FIELD(bktp, tmpf, node);
      break;
    }

    tmpf = TD_DLIST_NODE_NEXT_WITH_FIELD(tmpf, node);
  }

  taosWUnLockLatch(&(bktp->latch));

  ASSERT(tmpf == mpf);
}

static int tdbMPoolFileReadPage(TDB_MPFILE *mpf, pgno_t pgno, void *p) {
  pgsize_t   pgsize;
  TDB_MPOOL *mp;
  off_t      offset;
  size_t     rsize;

  mp = mpf->mp;
  pgsize = mp->pgsize;
  offset = pgno * pgsize;

  // TODO: use loop to read all data
  rsize = pread(mpf->fd, p, pgsize, offset);
  // TODO: error handle

  return 0;
}

static int tdbMPoolFileWritePage(TDB_MPFILE *mpf, pgno_t pgno, const void *p) {
  pgsize_t   pgsize;
  TDB_MPOOL *mp;
  off_t      offset;

  mp = mpf->mp;
  pgsize = mp->pgsize;
  offset = pgno * pgsize;

  lseek(mpf->fd, offset, SEEK_SET);
  // TODO: handle error

  write(mpf->fd, p, pgsize);
  // TODO: handle error

  return 0;
}