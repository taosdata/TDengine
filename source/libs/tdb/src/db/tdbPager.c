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

#include "tdbInt.h"
/*
#pragma pack(push, 1)
typedef struct {
  u8    hdrString[16];
  u16   pageSize;
  SPgno freePage;
  u32   nFreePages;
  u8    reserved[102];
} SFileHdr;
#pragma pack(pop)

TDB_STATIC_ASSERT(sizeof(SFileHdr) == 128, "Size of file header is not correct");
*/
struct hashset_st {
  size_t  nbits;
  size_t  mask;
  size_t  capacity;
  size_t *items;
  size_t  nitems;
  double  load_factor;
};

static const unsigned int prime = 39;
static const unsigned int prime2 = 5009;

hashset_t hashset_create(void) {
  hashset_t set = tdbOsCalloc(1, sizeof(struct hashset_st));
  if (!set) {
    return NULL;
  }

  set->nbits = 4;
  set->capacity = (size_t)(1 << set->nbits);
  set->items = tdbOsCalloc(set->capacity, sizeof(size_t));
  if (!set->items) {
    tdbOsFree(set);
    return NULL;
  }
  set->mask = set->capacity - 1;
  set->nitems = 0;

  set->load_factor = 0.75;

  return set;
}

void hashset_destroy(hashset_t set) {
  if (set) {
    tdbOsFree(set->items);
    tdbOsFree(set);
  }
}

int hashset_add_member(hashset_t set, void *item) {
  size_t value = (size_t)item;
  size_t h;

  if (value == 0) {
    return -1;
  }

  for (h = set->mask & (prime * value); set->items[h] != 0; h = set->mask & (h + prime2)) {
    if (set->items[h] == value) {
      return 0;
    }
  }

  set->items[h] = value;
  ++set->nitems;
  return 1;
}

int hashset_add(hashset_t set, void *item) {
  int ret = hashset_add_member(set, item);

  size_t old_capacity = set->capacity;
  if (set->nitems >= (double)old_capacity * set->load_factor) {
    size_t *old_items = set->items;
    ++set->nbits;
    set->capacity = (size_t)(1 << set->nbits);
    set->mask = set->capacity - 1;

    set->items = tdbOsCalloc(set->capacity, sizeof(size_t));
    if (!set->items) {
      return -1;
    }

    set->nitems = 0;
    for (size_t i = 0; i < old_capacity; ++i) {
      hashset_add_member(set, (void *)old_items[i]);
    }
    tdbOsFree(old_items);
  }

  return ret;
}

int hashset_remove(hashset_t set, void *item) {
  size_t value = (size_t)item;

  for (size_t h = set->mask & (prime * value); set->items[h] != 0; h = set->mask & (h + prime2)) {
    if (set->items[h] == value) {
      set->items[h] = 0;
      --set->nitems;
      return 1;
    }
  }

  return 0;
}

int hashset_contains(hashset_t set, void *item) {
  size_t value = (size_t)item;

  for (size_t h = set->mask & (prime * value); set->items[h] != 0; h = set->mask & (h + prime2)) {
    if (set->items[h] == value) {
      return 1;
    }
  }

  return 0;
}

#define TDB_PAGE_INITIALIZED(pPage) ((pPage)->pPager != NULL)

static int tdbPagerInitPage(SPager *pPager, SPage *pPage, int (*initPage)(SPage *, void *, int), void *arg,
                            u8 loadPage);
static int tdbPagerWritePageToJournal(SPager *pPager, SPage *pPage);
static int tdbPagerPWritePageToDB(SPager *pPager, SPage *pPage);

static FORCE_INLINE int32_t pageCmpFn(const SRBTreeNode *lhs, const SRBTreeNode *rhs) {
  SPage *pPageL = (SPage *)(((uint8_t *)lhs) - offsetof(SPage, node));
  SPage *pPageR = (SPage *)(((uint8_t *)rhs) - offsetof(SPage, node));

  SPgno pgnoL = TDB_PAGE_PGNO(pPageL);
  SPgno pgnoR = TDB_PAGE_PGNO(pPageR);

  if (pgnoL < pgnoR) {
    return -1;
  } else if (pgnoL > pgnoR) {
    return 1;
  } else {
    return 0;
  }
}

int tdbPagerOpen(SPCache *pCache, const char *fileName, SPager **ppPager) {
  uint8_t *pPtr;
  SPager  *pPager;
  int      fsize;
  int      zsize;
  int      ret;

  *ppPager = NULL;

  fsize = strlen(fileName);
  zsize = sizeof(*pPager)  /* SPager */
          + fsize + 1      /* dbFileName */
          + fsize + 8 + 1; /* jFileName */
  pPtr = (uint8_t *)tdbOsCalloc(1, zsize);
  if (pPtr == NULL) {
    return -1;
  }

  pPager = (SPager *)pPtr;
  pPtr += sizeof(*pPager);
  // pPager->dbFileName
  pPager->dbFileName = (char *)pPtr;
  memcpy(pPager->dbFileName, fileName, fsize);
  pPager->dbFileName[fsize] = '\0';
  pPtr += fsize + 1;
  // pPager->jFileName
  pPager->jFileName = (char *)pPtr;
  memcpy(pPager->jFileName, fileName, fsize);
  memcpy(pPager->jFileName + fsize, "-journal", 8);
  pPager->jFileName[fsize + 8] = '\0';
  // pPager->pCache
  pPager->pCache = pCache;

  pPager->fd = tdbOsOpen(pPager->dbFileName, TDB_O_CREAT | TDB_O_RDWR, 0755);
  if (TDB_FD_INVALID(pPager->fd)) {
    // if (pPager->fd < 0) {
    return -1;
  }

  ret = tdbGnrtFileID(pPager->fd, pPager->fid, false);
  if (ret < 0) {
    return -1;
  }

  // pPager->jfd = -1;
  pPager->pageSize = tdbPCacheGetPageSize(pCache);
  // pPager->dbOrigSize
  ret = tdbGetFileSize(pPager->fd, pPager->pageSize, &(pPager->dbOrigSize));
  pPager->dbFileSize = pPager->dbOrigSize;

  tdbTrace("pager/open reset dirty tree: %p", &pPager->rbt);
  tRBTreeCreate(&pPager->rbt, pageCmpFn);

  *ppPager = pPager;
  return 0;
}

int tdbPagerClose(SPager *pPager) {
  if (pPager) {
    /*
    if (pPager->inTran) {
      tdbOsClose(pPager->jfd);
    }
    */
    tdbOsClose(pPager->fd);
    tdbOsFree(pPager);
  }
  return 0;
}

int tdbPagerWrite(SPager *pPager, SPage *pPage) {
  int     ret;
  SPage **ppPage;

  if (pPage->isDirty) return 0;

  // ref page one more time so the page will not be release
  tdbRefPage(pPage);
  tdbTrace("pager/mdirty page %p/%d/%d", pPage, TDB_PAGE_PGNO(pPage), pPage->id);

  // Set page as dirty
  pPage->isDirty = 1;

  tdbTrace("tdb/pager-write: put page: %p %d to dirty tree: %p", pPage, TDB_PAGE_PGNO(pPage), &pPager->rbt);
  tRBTreePut(&pPager->rbt, (SRBTreeNode *)pPage);

  // Write page to journal if neccessary
  if (TDB_PAGE_PGNO(pPage) <= pPager->dbOrigSize &&
      (pPager->pActiveTxn->jPageSet == NULL ||
       !hashset_contains(pPager->pActiveTxn->jPageSet, (void *)((long)TDB_PAGE_PGNO(pPage))))) {
    ret = tdbPagerWritePageToJournal(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to journal since %s", tstrerror(terrno));
      return -1;
    }

    if (pPager->pActiveTxn->jPageSet) {
      hashset_add(pPager->pActiveTxn->jPageSet, (void *)((long)TDB_PAGE_PGNO(pPage)));
    }
  }

  return 0;
}

int tdbPagerBegin(SPager *pPager, TXN *pTxn) {
  /*
  if (pPager->inTran) {
    return 0;
  }
  */
  // Open the journal
  char jTxnFileName[TDB_FILENAME_LEN];
  sprintf(jTxnFileName, "%s.%" PRId64, pPager->jFileName, pTxn->txnId);
  pTxn->jfd = tdbOsOpen(jTxnFileName, TDB_O_CREAT | TDB_O_RDWR, 0755);
  if (TDB_FD_INVALID(pTxn->jfd)) {
    tdbError("failed to open file due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pTxn->jPageSet = hashset_create();

  pPager->pActiveTxn = pTxn;

  tdbDebug("pager/begin: %p, %d/%d, txnId:%" PRId64, pPager, pPager->dbOrigSize, pPager->dbFileSize, pTxn->txnId);

  // TODO: write the size of the file
  /*
  pPager->inTran = 1;
  */
  return 0;
}
/*
int tdbPagerCancelDirty(SPager *pPager, SPage *pPage, TXN *pTxn) {
  SRBTreeNode *pNode = tRBTreeGet(&pPager->rbt, (SRBTreeNode *)pPage);
  if (pNode) {
    pPage->isDirty = 0;

    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    if (pTxn->jPageSet) {
      hashset_remove(pTxn->jPageSet, (void *)((long)TDB_PAGE_PGNO(pPage)));
    }

    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

  return 0;
}
*/
int tdbPagerCommit(SPager *pPager, TXN *pTxn) {
  SPage *pPage;
  int    ret;

  // sync the journal file
  ret = tdbOsFSync(pTxn->jfd);
  if (ret < 0) {
    tdbError("failed to fsync: %s. jFileName:%s, %" PRId64, strerror(errno), pPager->jFileName, pTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // loop to write the dirty pages to file
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;

    if (pPage->nOverflow != 0) {
      tdbError("tdb/pager-commit: %p, pPage: %p, ovfl: %d, commit page failed.", pPager, pPage, pPage->nOverflow);
      return -1;
    }

    ret = tdbPagerPWritePageToDB(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to db since %s", tstrerror(terrno));
      return -1;
    }
  }

  tdbDebug("pager/commit: %p, %d/%d, txnId:%" PRId64, pPager, pPager->dbOrigSize, pPager->dbFileSize, pTxn->txnId);

  pPager->dbOrigSize = pPager->dbFileSize;

  // release the page
  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;

    pPage->isDirty = 0;

    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    if (pTxn->jPageSet) {
      hashset_remove(pTxn->jPageSet, (void *)((long)TDB_PAGE_PGNO(pPage)));
    }

    tdbTrace("tdb/pager-commit: remove page: %p %d from dirty tree: %p", pPage, TDB_PAGE_PGNO(pPage), &pPager->rbt);

    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

  tdbTrace("tdb/pager-commit reset dirty tree: %p", &pPager->rbt);
  tRBTreeCreate(&pPager->rbt, pageCmpFn);

  // sync the db file
  if (tdbOsFSync(pPager->fd) < 0) {
    tdbError("failed to fsync fd due to %s. file:%s", strerror(errno), pPager->dbFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tdbPagerPostCommit(SPager *pPager, TXN *pTxn) {
  char jTxnFileName[TDB_FILENAME_LEN];
  sprintf(jTxnFileName, "%s.%" PRId64, pPager->jFileName, pTxn->txnId);

  // remove the journal file
  if (tdbOsClose(pTxn->jfd) < 0) {
    tdbError("failed to close jfd: %s. file:%s, %" PRId64, strerror(errno), pPager->jFileName, pTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tdbOsRemove(jTxnFileName) < 0 && errno != ENOENT) {
    tdbError("failed to remove file due to %s. file:%s", strerror(errno), jTxnFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // pPager->inTran = 0;

  tdbDebug("pager/post-commit:%p, %d/%d", pPager, pPager->dbOrigSize, pPager->dbFileSize);

  return 0;
}

int tdbPagerPrepareAsyncCommit(SPager *pPager, TXN *pTxn) {
  SPage *pPage;
  SPgno  maxPgno = pPager->dbOrigSize;
  int    ret;

  // sync the journal file
  ret = tdbOsFSync(pTxn->jfd);
  if (ret < 0) {
    tdbError("failed to fsync jfd: %s. jfile:%s, %" PRId64, strerror(errno), pPager->jFileName, pTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // loop to write the dirty pages to file
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    if (pPage->isLocal) continue;

    SPgno pgno = TDB_PAGE_PGNO(pPage);
    if (pgno > maxPgno) {
      maxPgno = pgno;
    }
    ret = tdbPagerPWritePageToDB(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to db since %s", tstrerror(terrno));
      return -1;
    }
  }

  tdbTrace("tdbttl commit:%p, %d/%d", pPager, pPager->dbOrigSize, pPager->dbFileSize);
  pPager->dbOrigSize = maxPgno;
  //  pPager->dbOrigSize = pPager->dbFileSize;

  // release the page
  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    if (pPage->isLocal) continue;
    pPage->isDirty = 0;

    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

  /*
  tdbTrace("reset dirty tree: %p", &pPager->rbt);
  tRBTreeCreate(&pPager->rbt, pageCmpFn);

  // sync the db file
  if (tdbOsFSync(pPager->fd) < 0) {
    tdbError("failed to fsync fd due to %s. file:%s", strerror(errno), pPager->dbFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  */
  return 0;
}

// recovery dirty pages
int tdbPagerAbort(SPager *pPager, TXN *pTxn) {
  SPage *pPage;
  int    pgIdx;
  SPgno  journalSize = 0;
  int    ret;

  if (pTxn->jfd == 0) {
    // txn is commited
    return 0;
  }

  // sync the journal file
  ret = tdbOsFSync(pTxn->jfd);
  if (ret < 0) {
    tdbError("failed to fsync jfd: %s. jfile:%s, %" PRId64, strerror(errno), pPager->jFileName, pTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tdb_fd_t jfd = pTxn->jfd;

  ret = tdbGetFileSize(jfd, pPager->pageSize, &journalSize);
  if (ret < 0) {
    return -1;
  }

  if (tdbOsLSeek(jfd, 0L, SEEK_SET) < 0) {
    tdbError("failed to lseek jfd due to %s. file:%s, offset:0", strerror(errno), pPager->dbFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  u8 *pageBuf = tdbOsCalloc(1, pPager->pageSize);
  if (pageBuf == NULL) {
    return -1;
  }

  tdbDebug("pager/abort: %p, %d/%d, txnId:%" PRId64, pPager, pPager->dbOrigSize, pPager->dbFileSize, pTxn->txnId);

  for (int pgIndex = 0; pgIndex < journalSize; ++pgIndex) {
    // read pgno & the page from journal
    SPgno pgno;

    int ret = tdbOsRead(jfd, &pgno, sizeof(pgno));
    if (ret < 0) {
      tdbOsFree(pageBuf);
      return -1;
    }

    tdbTrace("pager/abort: restore pgno:%d,", pgno);

    tdbPCacheInvalidatePage(pPager->pCache, pPager, pgno);

    ret = tdbOsRead(jfd, pageBuf, pPager->pageSize);
    if (ret < 0) {
      tdbOsFree(pageBuf);
      return -1;
    }

    i64 offset = pPager->pageSize * (pgno - 1);
    if (tdbOsLSeek(pPager->fd, offset, SEEK_SET) < 0) {
      tdbError("failed to lseek fd due to %s. file:%s, offset:%" PRId64, strerror(errno), pPager->dbFileName, offset);
      terrno = TAOS_SYSTEM_ERROR(errno);
      tdbOsFree(pageBuf);
      return -1;
    }

    ret = tdbOsWrite(pPager->fd, pageBuf, pPager->pageSize);
    if (ret < 0) {
      tdbError("failed to write buf due to %s. file: %s, bufsize:%d", strerror(errno), pPager->dbFileName,
               pPager->pageSize);
      terrno = TAOS_SYSTEM_ERROR(errno);
      tdbOsFree(pageBuf);
      return -1;
    }
  }

  if (tdbOsFSync(pPager->fd) < 0) {
    tdbError("failed to fsync fd due to %s. dbfile:%s", strerror(errno), pPager->dbFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    tdbOsFree(pageBuf);
    return -1;
  }

  tdbOsFree(pageBuf);

  // 3, release the dirty pages
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    SPgno pgno = TDB_PAGE_PGNO(pPage);

    tdbTrace("pager/abort: drop dirty pgno:%d,", pgno);

    pPage->isDirty = 0;

    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    hashset_remove(pTxn->jPageSet, (void *)((long)TDB_PAGE_PGNO(pPage)));
    tdbPCacheMarkFree(pPager->pCache, pPage);
    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

  tdbTrace("pager/abort: reset dirty tree: %p", &pPager->rbt);
  tRBTreeCreate(&pPager->rbt, pageCmpFn);

  // 4, remove the journal file
  if (tdbOsClose(pTxn->jfd) < 0) {
    tdbError("failed to close jfd: %s. file:%s, %" PRId64, strerror(errno), pPager->jFileName, pTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  char jTxnFileName[TDB_FILENAME_LEN];
  sprintf(jTxnFileName, "%s.%" PRId64, pPager->jFileName, pTxn->txnId);

  if (tdbOsRemove(jTxnFileName) < 0 && errno != ENOENT) {
    tdbError("failed to remove file due to %s. file:%s", strerror(errno), jTxnFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // pPager->inTran = 0;

  return 0;
}

int tdbPagerFlushPage(SPager *pPager, TXN *pTxn) {
  SPage *pPage;
  i32    nRef;
  SPgno  maxPgno = pPager->dbOrigSize;
  int    ret;

  // loop to write the dirty pages to file
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    nRef = tdbGetPageRef(pPage);
    if (nRef > 1) {
      continue;
    }

    SPgno pgno = TDB_PAGE_PGNO(pPage);
    if (pgno > maxPgno) {
      maxPgno = pgno;
    }
    ret = tdbPagerPWritePageToDB(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to db since %s", tstrerror(terrno));
      return -1;
    }

    tdbTrace("tdb/flush:%p, pgno:%d, %d/%d/%d", pPager, pgno, pPager->dbOrigSize, pPager->dbFileSize, maxPgno);
    pPager->dbOrigSize = maxPgno;

    pPage->isDirty = 0;

    tdbTrace("pager/flush drop page: %p, pgno:%d, from dirty tree: %p", pPage, TDB_PAGE_PGNO(pPage), &pPager->rbt);
    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    tdbPCacheRelease(pPager->pCache, pPage, pTxn);

    break;
  }

  tdbDebug("pager/flush: %p, %d/%d, txnId:%" PRId64, pPager, pPager->dbOrigSize, pPager->dbFileSize, pTxn->txnId);

  /*
  tdbTrace("tdb/flush:%p, %d/%d/%d", pPager, pPager->dbOrigSize, pPager->dbFileSize, maxPgno);
  pPager->dbOrigSize = maxPgno;

  // release the page
  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    nRef = tdbGetPageRef(pPage);
    if (nRef > 1) {
      continue;
    }

    pPage->isDirty = 0;

    tdbTrace("pager/flush drop page: %p %d from dirty tree: %p", pPage, TDB_PAGE_PGNO(pPage), &pPager->rbt);
    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }
  */
  return 0;
}

static int tdbPagerAllocPage(SPager *pPager, SPgno *ppgno, TXN *pTxn);

int tdbPagerFetchPage(SPager *pPager, SPgno *ppgno, SPage **ppPage, int (*initPage)(SPage *, void *, int), void *arg,
                      TXN *pTxn) {
  SPage *pPage;
  SPgid  pgid;
  int    ret;
  SPgno  pgno;
  u8     loadPage;

  pgno = *ppgno;
  loadPage = 1;

  // alloc new page
  if (pgno == 0) {
    loadPage = 0;
    ret = tdbPagerAllocPage(pPager, &pgno, pTxn);
    if (ret < 0) {
      tdbError("tdb/pager: %p, ret: %d pgno: %" PRIu32 ", alloc page failed.", pPager, ret, pgno);
      return -1;
    }
  }

  if (pgno == 0) {
    tdbError("tdb/pager: %p, ret: %d pgno: %" PRIu32 ", alloc page failed.", pPager, ret, pgno);
    return -1;
  }

  // fetch a page container
  memcpy(&pgid, pPager->fid, TDB_FILE_ID_LEN);
  pgid.pgno = pgno;
  while ((pPage = tdbPCacheFetch(pPager->pCache, &pgid, pTxn)) == NULL) {
    tdbPagerFlushPage(pPager, pTxn);
  }

  tdbTrace("tdbttl fetch pager:%p", pPage->pPager);
  // init page if need
  if (!TDB_PAGE_INITIALIZED(pPage)) {
    ret = tdbPagerInitPage(pPager, pPage, initPage, arg, loadPage);
    if (ret < 0) {
      tdbError("tdb/pager: %p, pPage: %p, init page failed.", pPager, pPage);
      return -1;
    }
  }

  // printf("thread %" PRId64 " pager fetch page %d pgno %d ppage %p\n", taosGetSelfPthreadId(), pPage->id,
  //        TDB_PAGE_PGNO(pPage), pPage);

  if (!TDB_PAGE_INITIALIZED(pPage)) {
    tdbError("tdb/pager: %p, pPage: %p, fetch page uninited.", pPager, pPage);
    return -1;
  }
  if (pPage->pPager != pPager) {
    tdbError("tdb/pager: %p/%p, fetch page failed.", pPager, pPage->pPager);
    return -1;
  }

  *ppgno = pgno;
  *ppPage = pPage;
  return 0;
}

void tdbPagerReturnPage(SPager *pPager, SPage *pPage, TXN *pTxn) {
  tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  // printf("thread %" PRId64 " pager retun page %d pgno %d ppage %p\n", taosGetSelfPthreadId(), pPage->id,
  //        TDB_PAGE_PGNO(pPage), pPage);
}

int tdbPagerInsertFreePage(SPager *pPager, SPage *pPage, TXN *pTxn) {
  int   code = 0;
  SPgno pgno = TDB_PAGE_PGNO(pPage);

  if (pPager->frps) {
    taosArrayPush(pPager->frps, &pgno);
    pPage->pPager = NULL;
    return code;
  }

  pPager->frps = taosArrayInit(8, sizeof(SPgno));
  // memset(pPage->pData, 0, pPage->pageSize);
  tdbTrace("tdb/insert-free-page: tbc recycle page: %d.", pgno);
  // printf("tdb/insert-free-page: tbc recycle page: %d.\n", pgno);
  code = tdbTbInsert(pPager->pEnv->pFreeDb, &pgno, sizeof(pgno), NULL, 0, pTxn);
  if (code < 0) {
    tdbError("tdb/insert-free-page: tb insert failed with ret: %d.", code);
    taosArrayDestroy(pPager->frps);
    pPager->frps = NULL;
    return -1;
  }

  while (TARRAY_SIZE(pPager->frps) > 0) {
    pgno = *(SPgno *)taosArrayPop(pPager->frps);

    code = tdbTbInsert(pPager->pEnv->pFreeDb, &pgno, sizeof(pgno), NULL, 0, pTxn);
    if (code < 0) {
      tdbError("tdb/insert-free-page: tb insert failed with ret: %d.", code);
      taosArrayDestroy(pPager->frps);
      pPager->frps = NULL;
      return -1;
    }
  }

  taosArrayDestroy(pPager->frps);
  pPager->frps = NULL;

  pPage->pPager = NULL;

  return code;
}

static int tdbPagerRemoveFreePage(SPager *pPager, SPgno *pPgno, TXN *pTxn) {
  int  code = 0;
  TBC *pCur;

  if (!pPager->pEnv->pFreeDb) {
    return code;
  }

  if (pPager->frps) {
    return code;
  }

  code = tdbTbcOpen(pPager->pEnv->pFreeDb, &pCur, pTxn);
  if (code < 0) {
    return 0;
  }

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    tdbError("tdb/remove-free-page: moveto first failed with ret: %d.", code);
    tdbTbcClose(pCur);
    return 0;
  }

  void *pKey = NULL;
  int   nKey = 0;

  code = tdbTbcGet(pCur, (const void **)&pKey, &nKey, NULL, NULL);
  if (code < 0) {
    // tdbError("tdb/remove-free-page: tbc get failed with ret: %d.", code);
    tdbTbcClose(pCur);
    return 0;
  }

  *pPgno = *(SPgno *)pKey;
  tdbTrace("tdb/remove-free-page: tbc get page: %d.", *pPgno);
  // printf("tdb/remove-free-page: tbc get page: %d.\n", *pPgno);

  code = tdbTbcDelete(pCur);
  if (code < 0) {
    tdbError("tdb/remove-free-page: tbc delete failed with ret: %d.", code);
    tdbTbcClose(pCur);
    return 0;
  }
  tdbTbcClose(pCur);
  return 0;
}

static int tdbPagerAllocFreePage(SPager *pPager, SPgno *ppgno, TXN *pTxn) {
  // Allocate a page from the free list
  return tdbPagerRemoveFreePage(pPager, ppgno, pTxn);
}

static int tdbPagerAllocNewPage(SPager *pPager, SPgno *ppgno) {
  *ppgno = ++pPager->dbFileSize;
  // tdbError("tdb/alloc-new-page: %d.", *ppgno);
  return 0;
}

static int tdbPagerAllocPage(SPager *pPager, SPgno *ppgno, TXN *pTxn) {
  int ret;

  *ppgno = 0;

  // Try to allocate from the free list of the pager
  ret = tdbPagerAllocFreePage(pPager, ppgno, pTxn);
  if (ret < 0) {
    return -1;
  }

  if (*ppgno != 0) return 0;

  // Allocate the page by extending the pager
  ret = tdbPagerAllocNewPage(pPager, ppgno);
  if (ret < 0) {
    return -1;
  }

  if (*ppgno == 0) {
    tdbError("tdb/pager:%p, alloc new page failed.", pPager);
    return -1;
  }
  return 0;
}

static int tdbPagerInitPage(SPager *pPager, SPage *pPage, int (*initPage)(SPage *, void *, int), void *arg,
                            u8 loadPage) {
  int   ret;
  int   lcode;
  int   nLoops;
  i64   nRead = 0;
  SPgno pgno = 0;
  int   init = 0;

  lcode = TDB_TRY_LOCK_PAGE(pPage);
  if (lcode == P_LOCK_SUCC) {
    if (TDB_PAGE_INITIALIZED(pPage)) {
      TDB_UNLOCK_PAGE(pPage);
      return 0;
    }

    pgno = TDB_PAGE_PGNO(pPage);

    tdbTrace("tdb/pager:%p, pgno:%d, loadPage:%d, size:%d", pPager, pgno, loadPage, pPager->dbOrigSize);
    if (loadPage && pgno <= pPager->dbOrigSize) {
      init = 1;

      nRead = tdbOsPRead(pPager->fd, pPage->pData, pPage->pageSize, ((i64)pPage->pageSize) * (pgno - 1));
      tdbTrace("tdb/pager:%p, pgno:%d, nRead:%" PRId64, pPager, pgno, nRead);
      if (nRead < pPage->pageSize) {
        tdbError("tdb/pager:%p, pgno:%d, nRead:%" PRId64 "pgSize:%" PRId32, pPager, pgno, nRead, pPage->pageSize);
        TDB_UNLOCK_PAGE(pPage);
        return -1;
      }
    } else {
      init = 0;
    }

    ret = (*initPage)(pPage, arg, init);
    if (ret < 0) {
      tdbError("tdb/pager:%p, pgno:%d, nRead:%" PRId64 "pgSize:%" PRId32 " init page failed.", pPager, pgno, nRead,
               pPage->pageSize);
      TDB_UNLOCK_PAGE(pPage);
      return -1;
    }

    pPage->pPager = pPager;

    TDB_UNLOCK_PAGE(pPage);
  } else if (lcode == P_LOCK_BUSY) {
    nLoops = 0;
    for (;;) {
      if (TDB_PAGE_INITIALIZED(pPage)) break;
      nLoops++;
      if (nLoops > 1000) {
        sched_yield();
        nLoops = 0;
      }
    }
  } else {
    tdbError("tdb/pager:%p, pgno:%d, nRead:%" PRId64 "pgSize:%" PRId32 " lock page failed.", pPager, pgno, nRead,
             pPage->pageSize);
    return -1;
  }

  return 0;
}

// ---------------------------- Journal manipulation
static int tdbPagerWritePageToJournal(SPager *pPager, SPage *pPage) {
  int   ret;
  SPgno pgno;

  pgno = TDB_PAGE_PGNO(pPage);

  ret = tdbOsWrite(pPager->pActiveTxn->jfd, &pgno, sizeof(pgno));
  if (ret < 0) {
    tdbError("failed to write pgno due to %s. file:%s, pgno:%u, txnId:%" PRId64, strerror(errno), pPager->jFileName,
             pgno, pPager->pActiveTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  ret = tdbOsWrite(pPager->pActiveTxn->jfd, pPage->pData, pPage->pageSize);
  if (ret < 0) {
    tdbError("failed to write page data due to %s. file:%s, pageSize:%d, txnId:%" PRId64, strerror(errno),
             pPager->jFileName, pPage->pageSize, pPager->pActiveTxn->txnId);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}
/*
static int tdbPagerWritePageToDB(SPager *pPager, SPage *pPage) {
  i64 offset;
  int ret;

  offset = (i64)pPage->pageSize * (TDB_PAGE_PGNO(pPage) - 1);
  if (tdbOsLSeek(pPager->fd, offset, SEEK_SET) < 0) {
    tdbError("failed to lseek due to %s. file:%s, offset:%" PRId64, strerror(errno), pPager->dbFileName, offset);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  ret = tdbOsWrite(pPager->fd, pPage->pData, pPage->pageSize);
  if (ret < 0) {
    tdbError("failed to write page data due to %s. file:%s, pageSize:%d", strerror(errno), pPager->dbFileName,
             pPage->pageSize);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}
*/
static int tdbPagerPWritePageToDB(SPager *pPager, SPage *pPage) {
  i64 offset;
  int ret;

  offset = (i64)pPage->pageSize * (TDB_PAGE_PGNO(pPage) - 1);

  ret = tdbOsPWrite(pPager->fd, pPage->pData, pPage->pageSize, offset);
  if (ret < 0) {
    tdbError("failed to pwrite page data due to %s. file:%s, pageSize:%d", strerror(errno), pPager->dbFileName,
             pPage->pageSize);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int tdbPagerRestore(SPager *pPager, const char *jFileName) {
  int   ret = 0;
  SPgno journalSize = 0;
  u8   *pageBuf = NULL;

  tdb_fd_t jfd = tdbOsOpen(jFileName, TDB_O_RDWR, 0755);
  if (jfd == NULL) {
    return 0;
  }

  ret = tdbGetFileSize(jfd, pPager->pageSize, &journalSize);
  if (ret < 0) {
    return -1;
  }

  if (tdbOsLSeek(jfd, 0L, SEEK_SET) < 0) {
    tdbError("failed to lseek jfd due to %s. file:%s, offset:0", strerror(errno), pPager->dbFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pageBuf = tdbOsCalloc(1, pPager->pageSize);
  if (pageBuf == NULL) {
    return -1;
  }

  tdbDebug("pager/restore: %p, %d/%d, txnId:%s", pPager, pPager->dbOrigSize, pPager->dbFileSize, jFileName);

  for (int pgIndex = 0; pgIndex < journalSize; ++pgIndex) {
    // read pgno & the page from journal
    SPgno pgno;

    int ret = tdbOsRead(jfd, &pgno, sizeof(pgno));
    if (ret < 0) {
      tdbOsFree(pageBuf);
      return -1;
    }

    tdbTrace("pager/restore: restore pgno:%d,", pgno);

    ret = tdbOsRead(jfd, pageBuf, pPager->pageSize);
    if (ret < 0) {
      tdbOsFree(pageBuf);
      return -1;
    }

    i64 offset = pPager->pageSize * (pgno - 1);
    if (tdbOsLSeek(pPager->fd, offset, SEEK_SET) < 0) {
      tdbError("failed to lseek fd due to %s. file:%s, offset:%" PRId64, strerror(errno), pPager->dbFileName, offset);
      terrno = TAOS_SYSTEM_ERROR(errno);
      tdbOsFree(pageBuf);
      return -1;
    }

    ret = tdbOsWrite(pPager->fd, pageBuf, pPager->pageSize);
    if (ret < 0) {
      tdbError("failed to write buf due to %s. file: %s, bufsize:%d", strerror(errno), pPager->dbFileName,
               pPager->pageSize);
      terrno = TAOS_SYSTEM_ERROR(errno);
      tdbOsFree(pageBuf);
      return -1;
    }
  }

  if (tdbOsFSync(pPager->fd) < 0) {
    tdbError("failed to fsync fd due to %s. dbfile:%s", strerror(errno), pPager->dbFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    tdbOsFree(pageBuf);
    return -1;
  }

  tdbOsFree(pageBuf);

  if (tdbOsClose(jfd) < 0) {
    tdbError("failed to close jfd due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tdbOsRemove(jFileName) < 0 && errno != ENOENT) {
    tdbError("failed to remove file due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int32_t txnIdCompareDesc(const void *pLeft, const void *pRight) {
  int64_t lhs = *(int64_t *)pLeft;
  int64_t rhs = *(int64_t *)pRight;
  return lhs > rhs ? -1 : 1;
}

int tdbPagerRestoreJournals(SPager *pPager) {
  tdbDirEntryPtr pDirEntry;
  tdbDirPtr      pDir = taosOpenDir(pPager->pEnv->dbName);
  if (pDir == NULL) {
    tdbError("failed to open %s since %s", pPager->pEnv->dbName, strerror(errno));
    return -1;
  }

  SArray *pTxnList = taosArrayInit(16, sizeof(int64_t));

  while ((pDirEntry = tdbReadDir(pDir)) != NULL) {
    char *name = tdbDirEntryBaseName(tdbGetDirEntryName(pDirEntry));
    if (strncmp(TDB_MAINDB_NAME "-journal", name, 16) == 0) {
      int64_t txnId = -1;
      sscanf(name, TDB_MAINDB_NAME "-journal.%" PRId64, &txnId);
      taosArrayPush(pTxnList, &txnId);
    }
  }
  taosArraySort(pTxnList, txnIdCompareDesc);
  for (int i = 0; i < TARRAY_SIZE(pTxnList); ++i) {
    int64_t *pTxnId = taosArrayGet(pTxnList, i);
    char     jname[TD_PATH_MAX] = {0};
    int      dirLen = strlen(pPager->pEnv->dbName);
    memcpy(jname, pPager->pEnv->dbName, dirLen);
    jname[dirLen] = '/';
    sprintf(jname + dirLen + 1, TDB_MAINDB_NAME "-journal.%" PRId64, *pTxnId);
    if (tdbPagerRestore(pPager, jname) < 0) {
      taosArrayDestroy(pTxnList);
      tdbCloseDir(&pDir);

      tdbError("failed to restore file due to %s. jFileName:%s", strerror(errno), jname);
      return -1;
    }
  }

  taosArrayDestroy(pTxnList);
  tdbCloseDir(&pDir);

  return 0;
}

int tdbPagerRollback(SPager *pPager) {
  tdbDirEntryPtr pDirEntry;
  tdbDirPtr      pDir = taosOpenDir(pPager->pEnv->dbName);
  if (pDir == NULL) {
    tdbError("failed to open %s since %s", pPager->pEnv->dbName, strerror(errno));
    return -1;
  }

  while ((pDirEntry = tdbReadDir(pDir)) != NULL) {
    char *name = tdbDirEntryBaseName(tdbGetDirEntryName(pDirEntry));

    if (strncmp(TDB_MAINDB_NAME "-journal", name, 16) == 0) {
      char jname[TD_PATH_MAX] = {0};
      int  dirLen = strlen(pPager->pEnv->dbName);
      memcpy(jname, pPager->pEnv->dbName, dirLen);
      jname[dirLen] = '/';
      memcpy(jname + dirLen + 1, name, strlen(name));
      if (tdbOsRemove(jname) < 0 && errno != ENOENT) {
        tdbCloseDir(&pDir);

        tdbError("failed to remove file due to %s. jFileName:%s", strerror(errno), name);
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
  }

  tdbCloseDir(&pDir);

  return 0;
}
