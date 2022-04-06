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

typedef struct __attribute__((__packed__)) {
  u8    hdrString[16];
  u16   pageSize;
  SPgno freePage;
  u32   nFreePages;
  u8    reserved[102];
} SFileHdr;

TDB_STATIC_ASSERT(sizeof(SFileHdr) == 128, "Size of file header is not correct");

#define TDB_PAGE_INITIALIZED(pPage) ((pPage)->pPager != NULL)

static int tdbPagerInitPage(SPager *pPager, SPage *pPage, int (*initPage)(SPage *, void *), void *arg, u8 loadPage);
static int tdbPagerWritePageToJournal(SPager *pPager, SPage *pPage);
static int tdbPagerWritePageToDB(SPager *pPager, SPage *pPage);

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
  if (pPager->fd < 0) {
    return -1;
  }

  ret = tdbGnrtFileID(pPager->dbFileName, pPager->fid, false);
  if (ret < 0) {
    return -1;
  }

  // pPager->jfd = -1;
  pPager->pageSize = tdbPCacheGetPageSize(pCache);
  // pPager->dbOrigSize
  ret = tdbGetFileSize(pPager->fd, pPager->pageSize, &(pPager->dbOrigSize));

  *ppPager = pPager;
  return 0;
}

int tdbPagerClose(SPager *pPager) {
  // TODO
  return 0;
}

int tdbPagerOpenDB(SPager *pPager, SPgno *ppgno, bool toCreate) {
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

      // if (pgno == 0 && toCreate) {
      //   ret = tdbPagerAllocPage(pPager, &pPage, &pgno);
      //   if (ret < 0) {
      //     return -1;
      //   }

      //   // TODO: Need to zero the page

      //   ret = tdbPagerWrite(pPager, pPage);
      //   if (ret < 0) {
      //     return -1;
      //   }
      // }

      *ppgno = pgno;
  return 0;
}

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
  TDB_REF_PAGE(pPage);

  // Set page as dirty
  pPage->isDirty = 1;

  // Add page to dirty list(TODO: NOT use O(n^2) algorithm)
  for (ppPage = &pPager->pDirty; (*ppPage) && TDB_PAGE_PGNO(*ppPage) < TDB_PAGE_PGNO(pPage);
       ppPage = &((*ppPage)->pDirtyNext)) {
  }
  ASSERT(*ppPage == NULL || TDB_PAGE_PGNO(*ppPage) > TDB_PAGE_PGNO(pPage));
  pPage->pDirtyNext = *ppPage;
  *ppPage = pPage;

  // Write page to journal if neccessary
  if (TDB_PAGE_PGNO(pPage) <= pPager->dbOrigSize) {
    ret = tdbPagerWritePageToJournal(pPager, pPage);
    if (ret < 0) {
      ASSERT(0);
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
  if (pPager->jfd < 0) {
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
    // TODO
    ASSERT(0);
    return 0;
  }

  // loop to write the dirty pages to file
  for (pPage = pPager->pDirty; pPage; pPage = pPage->pDirtyNext) {
    // TODO: update the page footer
    ret = tdbPagerWritePageToDB(pPager, pPage);
    if (ret < 0) {
      ASSERT(0);
      return -1;
    }
  }

  // release the page
  for (pPage = pPager->pDirty; pPage; pPage = pPager->pDirty) {
    pPager->pDirty = pPage->pDirtyNext;
    pPage->pDirtyNext = NULL;

    pPage->isDirty = 0;

    tdbPCacheRelease(pPager->pCache, pPage, pTxn);
  }

  // sync the db file
  tdbOsFSync(pPager->fd);

  // remote the journal file
  tdbOsClose(pPager->jfd);
  tdbOsRemove(pPager->jFileName);
  pPager->dbOrigSize = pPager->dbFileSize;
  pPager->inTran = 0;

  return 0;
}

int tdbPagerFetchPage(SPager *pPager, SPgno pgno, SPage **ppPage, int (*initPage)(SPage *, void *), void *arg,
                      TXN *pTxn) {
  SPage *pPage;
  SPgid  pgid;
  int    ret;

  // Fetch a page container from the page cache
  memcpy(&pgid, pPager->fid, TDB_FILE_ID_LEN);
  pgid.pgno = pgno;
  pPage = tdbPCacheFetch(pPager->pCache, &pgid, pTxn);
  if (pPage == NULL) {
    return -1;
  }

  // Initialize the page if need
  if (!TDB_PAGE_INITIALIZED(pPage)) {
    ret = tdbPagerInitPage(pPager, pPage, initPage, arg, 1);
    if (ret < 0) {
      return -1;
    }
  }

  ASSERT(TDB_PAGE_INITIALIZED(pPage));
  ASSERT(pPage->pPager == pPager);

  *ppPage = pPage;
  return 0;
}

int tdbPagerNewPage(SPager *pPager, SPgno *ppgno, SPage **ppPage, int (*initPage)(SPage *, void *), void *arg,
                    TXN *pTxn) {
  int    ret;
  SPage *pPage;
  SPgid  pgid;

  // Allocate a page number
  ret = tdbPagerAllocPage(pPager, ppgno);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  ASSERT(*ppgno != 0);

  // Fetch a page container from the page cache
  memcpy(&pgid, pPager->fid, TDB_FILE_ID_LEN);
  pgid.pgno = *ppgno;
  pPage = tdbPCacheFetch(pPager->pCache, &pgid, pTxn);
  if (pPage == NULL) {
    ASSERT(0);
    return -1;
  }

  ASSERT(!TDB_PAGE_INITIALIZED(pPage));

  // Initialize the page if need
  ret = tdbPagerInitPage(pPager, pPage, initPage, arg, 0);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  ASSERT(TDB_PAGE_INITIALIZED(pPage));
  ASSERT(pPage->pPager == pPager);

  *ppPage = pPage;
  return 0;
}

void tdbPagerReturnPage(SPager *pPager, SPage *pPage, TXN *pTxn) { tdbPCacheRelease(pPager->pCache, pPage, pTxn); }

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

static int tdbPagerInitPage(SPager *pPager, SPage *pPage, int (*initPage)(SPage *, void *), void *arg, u8 loadPage) {
  int ret;
  int lcode;
  int nLoops;
  i64 nRead;

  lcode = TDB_TRY_LOCK_PAGE(pPage);
  if (lcode == P_LOCK_SUCC) {
    if (TDB_PAGE_INITIALIZED(pPage)) {
      TDB_UNLOCK_PAGE(pPage);
      return 0;
    }

    if (loadPage) {
      nRead = tdbOsPRead(pPager->fd, pPage->pData, pPage->pageSize, ((i64)pPage->pageSize) * TDB_PAGE_PGNO(pPage));
      if (nRead < 0) {
        // TODO
        ASSERT(0);
        return -1;
      } else if (nRead < pPage->pageSize) {
        // TODO
        ASSERT(0);
        return -1;
      }
    }

    ret = (*initPage)(pPage, arg);
    if (ret < 0) {
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
    return -1;
  }

  ret = tdbOsWrite(pPager->jfd, pPage->pData, pPage->pageSize);
  if (ret < 0) {
    return -1;
  }

  return 0;
}

static int tdbPagerWritePageToDB(SPager *pPager, SPage *pPage) {
  i64 offset;
  int ret;

  offset = pPage->pageSize * TDB_PAGE_PGNO(pPage);
  if (tdbOsLSeek(pPager->fd, offset, SEEK_SET) < 0) {
    ASSERT(0);
    return -1;
  }

  ret = tdbOsWrite(pPager->fd, pPage->pData, pPage->pageSize);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  return 0;
}