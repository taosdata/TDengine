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

#define TDB_PAGE_INITIALIZED(pPage) ((pPage)->pPager != NULL)

static int tdbPagerInitPage(SPager *pPager, SPage *pPage, int (*initPage)(SPage *, void *, int), void *arg,
                            u8 loadPage);
static int tdbPagerWritePageToJournal(SPager *pPager, SPage *pPage);
static int tdbPagerWritePageToDB(SPager *pPager, SPage *pPage);

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

  tRBTreeCreate(&pPager->rbt, pageCmpFn);

  *ppPager = pPager;
  return 0;
}

int tdbPagerClose(SPager *pPager) {
  if (pPager) {
    if (pPager->inTran) {
      tdbOsClose(pPager->jfd);
    }
    tdbOsClose(pPager->fd);
    tdbOsFree(pPager);
  }
  return 0;
}
/*
int tdbPagerOpenDB(SPager *pPager, SPgno *ppgno, bool toCreate, SBTree *pBt) {
  SPgno  pgno;
  SPage *pPage;
  int    ret;

  if (pPager->dbOrigSize > 0) {
    pgno = 1;
  } else {
    pgno = 0;
  }

  {
    // TODO: try to search the main DB to get the page number
    // pgno = 0;
  }

  if (pgno == 0 && toCreate) {
    // allocate a new child page
    TXN txn;
    tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, 0);

    pPager->inTran = 1;

    SBtreeInitPageArg zArg;
    zArg.flags = 0x1 | 0x2;  // root leaf node;
    zArg.pBt = pBt;
    ret = tdbPagerFetchPage(pPager, &pgno, &pPage, tdbBtreeInitPage, &zArg, &txn);
    if (ret < 0) {
      return -1;
    }

    //    ret = tdbPagerAllocPage(pPager, &pPage, &pgno);
    // if (ret < 0) {
    //  return -1;
    //}

    // TODO: Need to zero the page

    ret = tdbPagerWrite(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page since %s", terrstr());
      return -1;
    }

    tdbTxnClose(&txn);
  }

  *ppgno = pgno;
  return 0;
}
*/
int tdbPagerWrite(SPager *pPager, SPage *pPage) {
  int     ret;
  SPage **ppPage;

  ASSERT(pPager->inTran);
#if 0
  if (pPager->inTran == 0) {
    ret = tdbPagerBegin(pPager);
    if (ret < 0) {
      return -1;
    }
  }
#endif

  if (pPage->isDirty) return 0;

  // ref page one more time so the page will not be release
  tdbRefPage(pPage);
  tdbDebug("pcache/mdirty page %p/%d/%d", pPage, TDB_PAGE_PGNO(pPage), pPage->id);

  // Set page as dirty
  pPage->isDirty = 1;
  /*
  // Add page to dirty list(TODO: NOT use O(n^2) algorithm)
  for (ppPage = &pPager->pDirty; (*ppPage) && TDB_PAGE_PGNO(*ppPage) < TDB_PAGE_PGNO(pPage);
       ppPage = &((*ppPage)->pDirtyNext)) {
  }

  if (*ppPage && TDB_PAGE_PGNO(*ppPage) == TDB_PAGE_PGNO(pPage)) {
    tdbUnrefPage(pPage);

    return 0;
  }

  ASSERT(*ppPage == NULL || TDB_PAGE_PGNO(*ppPage) > TDB_PAGE_PGNO(pPage));
  pPage->pDirtyNext = *ppPage;
  *ppPage = pPage;
  */
  tRBTreePut(&pPager->rbt, (SRBTreeNode *)pPage);

  // Write page to journal if neccessary
  if (TDB_PAGE_PGNO(pPage) <= pPager->dbOrigSize) {
    ret = tdbPagerWritePageToJournal(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to journal since %s", tstrerror(terrno));
      return -1;
    }
  }

  return 0;
}

int tdbPagerBegin(SPager *pPager, TXN *pTxn) {
  if (pPager->inTran) {
    return 0;
  }

  // Open the journal
  pPager->jfd = tdbOsOpen(pPager->jFileName, TDB_O_CREAT | TDB_O_RDWR, 0755);
  if (TDB_FD_INVALID(pPager->jfd)) {
    tdbError("failed to open file due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // TODO: write the size of the file

  pPager->inTran = 1;

  return 0;
}

int tdbPagerCommit(SPager *pPager, TXN *pTxn) {
  SPage *pPage;
  int    ret;

  // sync the journal file
  ret = tdbOsFSync(pPager->jfd);
  if (ret < 0) {
    tdbError("failed to fsync jfd due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // loop to write the dirty pages to file
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    ret = tdbPagerWritePageToDB(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to db since %s", tstrerror(terrno));
      return -1;
    }
  }

  tdbTrace("tdbttl commit:%p, %d/%d", pPager, pPager->dbOrigSize, pPager->dbFileSize);
  pPager->dbOrigSize = pPager->dbFileSize;

  // release the page
  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;

    pPage->isDirty = 0;

    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

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
  // remove the journal file
  if (tdbOsClose(pPager->jfd) < 0) {
    tdbError("failed to close jfd due to %s. file:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tdbOsRemove(pPager->jFileName) < 0 && errno != ENOENT) {
    tdbError("failed to remove file due to %s. file:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pPager->inTran = 0;

  return 0;
}

int tdbPagerPrepareAsyncCommit(SPager *pPager, TXN *pTxn) {
  SPage *pPage;
  int    ret;

  // sync the journal file
  ret = tdbOsFSync(pPager->jfd);
  if (ret < 0) {
    tdbError("failed to fsync jfd due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  // loop to write the dirty pages to file
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;
    if (pPage->isLocal) continue;
    ret = tdbPagerWritePageToDB(pPager, pPage);
    if (ret < 0) {
      tdbError("failed to write page to db since %s", tstrerror(terrno));
      return -1;
    }
  }

  tdbTrace("tdbttl commit:%p, %d/%d", pPager, pPager->dbOrigSize, pPager->dbFileSize);
  pPager->dbOrigSize = pPager->dbFileSize;

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

  // 0, sync the journal file
  ret = tdbOsFSync(pPager->jfd);
  if (ret < 0) {
    tdbError("failed to fsync jfd due to %s. file:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tdb_fd_t jfd = tdbOsOpen(pPager->jFileName, TDB_O_RDWR, 0755);
  if (jfd == NULL) {
    return -1;
  }

  ret = tdbGetFileSize(jfd, pPager->pageSize, &journalSize);
  if (ret < 0) {
    return -1;
  }

  // 1, read pages from jounal file
  // 2, write original pages to buffered ones

  /* TODO: reset the buffered pages instead of releasing them
  // loop to reset the dirty pages from file
  for (pgIdx = 0, pPage = pPager->pDirty; pPage != NULL && pgIndex < journalSize; pPage = pPage->pDirtyNext, ++pgIdx) {
    // read pgno & the page from journal
    SPgno pgno;

    int ret = tdbOsRead(jfd, &pgno, sizeof(pgno));
    if (ret < 0) {
      return -1;
    }

    ret = tdbOsRead(jfd, pageBuf, pPager->pageSize);
    if (ret < 0) {
      return -1;
    }
  }
  */
  // 3, release the dirty pages
  SRBTreeIter  iter = tRBTreeIterCreate(&pPager->rbt, 1);
  SRBTreeNode *pNode = NULL;
  while ((pNode = tRBTreeIterNext(&iter)) != NULL) {
    pPage = (SPage *)pNode;

    pPage->isDirty = 0;

    tRBTreeDrop(&pPager->rbt, (SRBTreeNode *)pPage);
    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

  tRBTreeCreate(&pPager->rbt, pageCmpFn);

  // 4, remove the journal file
  tdbOsClose(pPager->jfd);
  (void)tdbOsRemove(pPager->jFileName);
  pPager->inTran = 0;

  return 0;
}

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
    ret = tdbPagerAllocPage(pPager, &pgno);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  ASSERT(pgno > 0);

  // fetch a page container
  memcpy(&pgid, pPager->fid, TDB_FILE_ID_LEN);
  pgid.pgno = pgno;
  pPage = tdbPCacheFetch(pPager->pCache, &pgid, pTxn);
  if (pPage == NULL) {
    ASSERT(0);
    return -1;
  }

  tdbTrace("tdbttl fetch pager:%p", pPage->pPager);
  // init page if need
  if (!TDB_PAGE_INITIALIZED(pPage)) {
    ret = tdbPagerInitPage(pPager, pPage, initPage, arg, loadPage);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  // printf("thread %" PRId64 " pager fetch page %d pgno %d ppage %p\n", taosGetSelfPthreadId(), pPage->id,
  //        TDB_PAGE_PGNO(pPage), pPage);

  ASSERT(TDB_PAGE_INITIALIZED(pPage));
  ASSERT(pPage->pPager == pPager);

  *ppgno = pgno;
  *ppPage = pPage;
  return 0;
}

void tdbPagerReturnPage(SPager *pPager, SPage *pPage, TXN *pTxn) {
  tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  // printf("thread %" PRId64 " pager retun page %d pgno %d ppage %p\n", taosGetSelfPthreadId(), pPage->id,
  //        TDB_PAGE_PGNO(pPage), pPage);
}

static int tdbPagerAllocFreePage(SPager *pPager, SPgno *ppgno) {
  // TODO: Allocate a page from the free list
  return 0;
}

static int tdbPagerAllocNewPage(SPager *pPager, SPgno *ppgno) {
  *ppgno = ++pPager->dbFileSize;
  return 0;
}

int tdbPagerAllocPage(SPager *pPager, SPgno *ppgno) {
  int ret;

  *ppgno = 0;

  // Try to allocate from the free list of the pager
  ret = tdbPagerAllocFreePage(pPager, ppgno);
  if (ret < 0) {
    return -1;
  }

  if (*ppgno != 0) return 0;

  // Allocate the page by extending the pager
  ret = tdbPagerAllocNewPage(pPager, ppgno);
  if (ret < 0) {
    return -1;
  }

  ASSERT(*ppgno != 0);

  return 0;
}

static int tdbPagerInitPage(SPager *pPager, SPage *pPage, int (*initPage)(SPage *, void *, int), void *arg,
                            u8 loadPage) {
  int   ret;
  int   lcode;
  int   nLoops;
  i64   nRead;
  SPgno pgno;
  int   init = 0;

  lcode = TDB_TRY_LOCK_PAGE(pPage);
  if (lcode == P_LOCK_SUCC) {
    if (TDB_PAGE_INITIALIZED(pPage)) {
      TDB_UNLOCK_PAGE(pPage);
      return 0;
    }

    pgno = TDB_PAGE_PGNO(pPage);

    tdbTrace("tdbttl init pager:%p, pgno:%d, loadPage:%d, size:%d", pPager, pgno, loadPage, pPager->dbOrigSize);
    if (loadPage && pgno <= pPager->dbOrigSize) {
      init = 1;

      nRead = tdbOsPRead(pPager->fd, pPage->pData, pPage->pageSize, ((i64)pPage->pageSize) * (pgno - 1));
      tdbTrace("tdbttl pager:%p, pgno:%d, nRead:%" PRId64, pPager, pgno, nRead);
      if (nRead < pPage->pageSize) {
        ASSERT(0);
        return -1;
      }
    } else {
      init = 0;
    }

    ret = (*initPage)(pPage, arg, init);
    if (ret < 0) {
      ASSERT(0);
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
    ASSERT(0);
    return -1;
  }

  return 0;
}

// ---------------------------- Journal manipulation
static int tdbPagerWritePageToJournal(SPager *pPager, SPage *pPage) {
  int   ret;
  SPgno pgno;

  pgno = TDB_PAGE_PGNO(pPage);

  ret = tdbOsWrite(pPager->jfd, &pgno, sizeof(pgno));
  if (ret < 0) {
    tdbError("failed to write pgno due to %s. file:%s, pgno:%u", strerror(errno), pPager->jFileName, pgno);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  ret = tdbOsWrite(pPager->jfd, pPage->pData, pPage->pageSize);
  if (ret < 0) {
    tdbError("failed to write page data due to %s. file:%s, pageSize:%ld", strerror(errno), pPager->jFileName,
             (long)pPage->pageSize);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}
/*
struct TdFile {
  TdThreadRwlock rwlock;
  int            refId;
  int            fd;
  FILE          *fp;
} TdFile;
*/
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

  // pwrite(pPager->fd->fd, pPage->pData, pPage->pageSize, offset);
  return 0;
}

int tdbPagerRestore(SPager *pPager, SBTree *pBt) {
  int   ret = 0;
  SPgno journalSize = 0;
  u8   *pageBuf = NULL;

  tdb_fd_t jfd = tdbOsOpen(pPager->jFileName, TDB_O_RDWR, 0755);
  if (jfd == NULL) {
    return 0;
  }

  ret = tdbGetFileSize(jfd, pPager->pageSize, &journalSize);
  if (ret < 0) {
    return -1;
  }

  pageBuf = tdbOsCalloc(1, pPager->pageSize);
  if (pageBuf == NULL) {
    return -1;
  }

  for (int pgIndex = 0; pgIndex < journalSize; ++pgIndex) {
    // read pgno & the page from journal
    SPgno pgno;

    int ret = tdbOsRead(jfd, &pgno, sizeof(pgno));
    if (ret < 0) {
      tdbOsFree(pageBuf);
      return -1;
    }

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

  if (tdbOsRemove(pPager->jFileName) < 0 && errno != ENOENT) {
    tdbError("failed to remove file due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tdbPagerRollback(SPager *pPager) {
  if (tdbOsRemove(pPager->jFileName) < 0 && errno != ENOENT) {
    tdbError("failed to remove file due to %s. jFileName:%s", strerror(errno), pPager->jFileName);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}
