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

#ifndef _TD_TDB_INTERNAL_H_
#define _TD_TDB_INTERNAL_H_

#include "tdb.h"

#include "tlog.h"
#include "trbtree.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
extern int32_t tdbDebugFlag;

#define tdbFatal(...) do { if (tdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TDB FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define tdbError(...) do { if (tdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TDB ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define tdbWarn(...)  do { if (tdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TDB WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define tdbInfo(...)  do { if (tdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TDB ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define tdbDebug(...) do { if (tdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TDB ", DEBUG_DEBUG, tdbDebugFlag, __VA_ARGS__); }} while(0)
#define tdbTrace(...) do { if (tdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TDB ", DEBUG_TRACE, tdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef int8_t   i8;
typedef int16_t  i16;
typedef int32_t  i32;
typedef int64_t  i64;
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

// SPgno
typedef u32 SPgno;

#include "tdbOs.h"
#include "tdbUtil.h"

// p must be u8 *
#define TDB_GET_U24(p) ((p)[0] * 65536 + *(u16 *)((p) + 1))
#define TDB_PUT_U24(p, v)       \
  do {                          \
    int tv = (v);               \
    (p)[1] = tv & 0xff;         \
    (p)[2] = (tv >> 8) & 0xff;  \
    (p)[0] = (tv >> 16) & 0xff; \
  } while (0)

// fileid
#define TDB_FILE_ID_LEN 24

// SPgid
typedef struct {
  uint8_t fileid[TDB_FILE_ID_LEN];
  SPgno   pgno;
} SPgid;

// pgsz_t
#define TDB_MIN_PGSIZE       512       // 512B
#define TDB_MAX_PGSIZE       16777216  // 16M
#define TDB_DEFAULT_PGSIZE   4096
#define TDB_IS_PGSIZE_VLD(s) (((s) >= TDB_MIN_PGSIZE) && ((s) <= TDB_MAX_PGSIZE))

// dbname
#define TDB_MAX_DBNAME_LEN 24

#define TDB_VARIANT_LEN ((int)-1)

#define TDB_JOURNAL_NAME "tdb.journal"

#define TDB_FILENAME_LEN 128

#define BTREE_MAX_DEPTH 20

#define TDB_FLAG_IS(flags, flag)     ((flags) == (flag))
#define TDB_FLAG_HAS(flags, flag)    (((flags) & (flag)) != 0)
#define TDB_FLAG_NO(flags, flag)     ((flags) & (flag) == 0)
#define TDB_FLAG_ADD(flags, flag)    ((flags) | (flag))
#define TDB_FLAG_REMOVE(flags, flag) ((flags) & (~(flag)))

typedef struct SPager  SPager;
typedef struct SPCache SPCache;
typedef struct SPage   SPage;

// transaction

#define TDB_TXN_IS_WRITE(PTXN)            ((PTXN)->flags & TDB_TXN_WRITE)
#define TDB_TXN_IS_READ(PTXN)             (!TDB_TXN_IS_WRITE(PTXN))
#define TDB_TXN_IS_READ_UNCOMMITTED(PTXN) ((PTXN)->flags & TDB_TXN_READ_UNCOMMITTED)

// tdbEnv.c ====================================
void    tdbEnvAddPager(TDB *pEnv, SPager *pPager);
void    tdbEnvRemovePager(TDB *pEnv, SPager *pPager);
SPager *tdbEnvGetPager(TDB *pEnv, const char *fname);

// tdbBtree.c ====================================
typedef struct SBTree SBTree;
typedef struct SBTC   SBTC;
typedef struct SBtInfo {
  SPgno root;
  int   nLevel;
  int   nData;
} SBtInfo;

#define TDB_CELLD_F_NIL 0x0
#define TDB_CELLD_F_KEY 0x1
#define TDB_CELLD_F_VAL 0x2

#define TDB_CELLDECODER_SET_FREE_NIL(pCellDecoder) ((pCellDecoder)->freeKV = TDB_CELLD_F_NIL)
#define TDB_CELLDECODER_CLZ_FREE_KEY(pCellDecoder) ((pCellDecoder)->freeKV &= ~TDB_CELLD_F_KEY)
#define TDB_CELLDECODER_CLZ_FREE_VAL(pCellDecoder) ((pCellDecoder)->freeKV &= ~TDB_CELLD_F_VAL)
#define TDB_CELLDECODER_SET_FREE_KEY(pCellDecoder) ((pCellDecoder)->freeKV |= TDB_CELLD_F_KEY)
#define TDB_CELLDECODER_SET_FREE_VAL(pCellDecoder) ((pCellDecoder)->freeKV |= TDB_CELLD_F_VAL)

#define TDB_CELLDECODER_FREE_KEY(pCellDecoder) ((pCellDecoder)->freeKV & TDB_CELLD_F_KEY)
#define TDB_CELLDECODER_FREE_VAL(pCellDecoder) ((pCellDecoder)->freeKV & TDB_CELLD_F_VAL)

typedef struct {
  int     kLen;
  u8     *pKey;
  int     vLen;
  u8     *pVal;
  SPgno   pgno;
  u8     *pBuf;
  u8      freeKV;
  SArray *ofps;
} SCellDecoder;

struct SBTC {
  SBTree      *pBt;
  i8           iPage;
  SPage       *pPage;
  int          idx;
  int          idxStack[BTREE_MAX_DEPTH + 1];
  SPage       *pgStack[BTREE_MAX_DEPTH + 1];
  SCellDecoder coder;
  TXN         *pTxn;
  i8           freeTxn;
};

// SBTree
int tdbBtreeOpen(int keyLen, int valLen, SPager *pFile, char const *tbname, SPgno pgno, tdb_cmpr_fn_t kcmpr, TDB *pEnv,
                 SBTree **ppBt);
int tdbBtreeClose(SBTree *pBt);
int tdbBtreeInsert(SBTree *pBt, const void *pKey, int kLen, const void *pVal, int vLen, TXN *pTxn);
int tdbBtreeDelete(SBTree *pBt, const void *pKey, int kLen, TXN *pTxn);
int tdbBtreeUpsert(SBTree *pBt, const void *pKey, int nKey, const void *pData, int nData, TXN *pTxn);
int tdbBtreeGet(SBTree *pBt, const void *pKey, int kLen, void **ppVal, int *vLen);
int tdbBtreePGet(SBTree *pBt, const void *pKey, int kLen, void **ppKey, int *pkLen, void **ppVal, int *vLen);

typedef struct {
  u8      flags;
  SBTree *pBt;
} SBtreeInitPageArg;

int tdbBtreeInitPage(SPage *pPage, void *arg, int init);

// SBTC
int tdbBtcOpen(SBTC *pBtc, SBTree *pBt, TXN *pTxn);
int tdbBtcClose(SBTC *pBtc);
int tdbBtcIsValid(SBTC *pBtc);
int tdbBtcMoveTo(SBTC *pBtc, const void *pKey, int kLen, int *pCRst);
int tdbBtcMoveToFirst(SBTC *pBtc);
int tdbBtcMoveToLast(SBTC *pBtc);
int tdbBtcMoveToNext(SBTC *pBtc);
int tdbBtcMoveToPrev(SBTC *pBtc);
int tdbBtreeNext(SBTC *pBtc, void **ppKey, int *kLen, void **ppVal, int *vLen);
int tdbBtreePrev(SBTC *pBtc, void **ppKey, int *kLen, void **ppVal, int *vLen);
int tdbBtcGet(SBTC *pBtc, const void **ppKey, int *kLen, const void **ppVal, int *vLen);
int tdbBtcDelete(SBTC *pBtc);
int tdbBtcUpsert(SBTC *pBtc, const void *pKey, int kLen, const void *pData, int nData, int insert);

// tdbPager.c ====================================

int  tdbPagerOpen(SPCache *pCache, const char *fileName, SPager **ppPager);
int  tdbPagerClose(SPager *pPager);
int  tdbPagerOpenDB(SPager *pPager, SPgno *ppgno, bool toCreate, SBTree *pBt);
int  tdbPagerWrite(SPager *pPager, SPage *pPage);
int  tdbPagerBegin(SPager *pPager, TXN *pTxn);
int  tdbPagerCommit(SPager *pPager, TXN *pTxn);
int  tdbPagerPostCommit(SPager *pPager, TXN *pTxn);
int  tdbPagerPrepareAsyncCommit(SPager *pPager, TXN *pTxn);
int  tdbPagerAbort(SPager *pPager, TXN *pTxn);
int  tdbPagerFetchPage(SPager *pPager, SPgno *ppgno, SPage **ppPage, int (*initPage)(SPage *, void *, int), void *arg,
                       TXN *pTxn);
void tdbPagerReturnPage(SPager *pPager, SPage *pPage, TXN *pTxn);
int  tdbPagerInsertFreePage(SPager *pPager, SPage *pPage, TXN *pTxn);
// int  tdbPagerAllocPage(SPager *pPager, SPgno *ppgno);
int tdbPagerRestoreJournals(SPager *pPager);
int tdbPagerRollback(SPager *pPager);

// tdbPCache.c ====================================
#define TDB_PCACHE_PAGE    \
  u8           isAnchor;   \
  u8           isLocal;    \
  u8           isDirty;    \
  u8           isFree;     \
  volatile i32 nRef;       \
  i32          id;         \
  SPage       *pFreeNext;  \
  SPage       *pHashNext;  \
  SPage       *pLruNext;   \
  SPage       *pLruPrev;   \
  SPage       *pDirtyNext; \
  SPager      *pPager;     \
  SPgid        pgid;

// For page ref

int    tdbPCacheOpen(int pageSize, int cacheSize, SPCache **ppCache);
int    tdbPCacheClose(SPCache *pCache);
int    tdbPCacheAlter(SPCache *pCache, int32_t nPage);
SPage *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, TXN *pTxn);
void   tdbPCacheRelease(SPCache *pCache, SPage *pPage, TXN *pTxn);
void   tdbPCacheMarkFree(SPCache *pCache, SPage *pPage);
void   tdbPCacheInvalidatePage(SPCache *pCache, SPager *pPager, SPgno pgno);
int    tdbPCacheGetPageSize(SPCache *pCache);

// tdbPage.c ====================================
typedef u8 SCell;

// PAGE APIS implemented
typedef struct {
  int szOffset;
  int szPageHdr;
  int szFreeCell;
  // cell number
  int (*getCellNum)(SPage *);
  void (*setCellNum)(SPage *, int);
  // cell content offset
  int (*getCellBody)(SPage *);
  void (*setCellBody)(SPage *, int);
  // first free cell offset (0 means no free cells)
  int (*getCellFree)(SPage *);
  void (*setCellFree)(SPage *, int);
  // total free bytes
  int (*getFreeBytes)(SPage *);
  void (*setFreeBytes)(SPage *, int);
  // cell offset at idx
  int (*getCellOffset)(SPage *, int);
  void (*setCellOffset)(SPage *, int, int);
  // free cell info
  void (*getFreeCellInfo)(SCell *pCell, int *szCell, int *nxOffset);
  void (*setFreeCellInfo)(SCell *pCell, int szCell, int nxOffset);
} SPageMethods;

#pragma pack(push, 1)

// Page footer
typedef struct {
  u8 cksm[4];
} SPageFtr;
#pragma pack(pop)

struct SPage {
  SRBTreeNode    node;  // must be the first field for pageCmpFn to work
  tdb_spinlock_t lock;
  int            pageSize;
  u8            *pData;
  SPageMethods  *pPageMethods;
  // Fields below used by pager and am
  u8       *pPageHdr;
  u8       *pCellIdx;
  u8       *pFreeStart;
  u8       *pFreeEnd;
  SPageFtr *pPageFtr;
  int       nOverflow;
  SCell    *apOvfl[4];
  int       aiOvfl[4];
  int       kLen;  // key length of the page, -1 for unknown
  int       vLen;  // value length of the page, -1 for unknown
  int       maxLocal;
  int       minLocal;
  int (*xCellSize)(const SPage *, SCell *, int, TXN *pTxn, SBTree *pBt);
  // Fields used by SPCache
  TDB_PCACHE_PAGE
};

static inline i32 tdbRefPage(SPage *pPage) {
  i32 nRef = atomic_add_fetch_32(&((pPage)->nRef), 1);
  // tdbTrace("ref page %p/%d, nRef %d", pPage, pPage->id, nRef);
  return nRef;
}

static inline i32 tdbUnrefPage(SPage *pPage) {
  i32 nRef = atomic_sub_fetch_32(&((pPage)->nRef), 1);
  // tdbTrace("unref page %p/%d, nRef %d", pPage, pPage->id, nRef);
  return nRef;
}

#define tdbGetPageRef(pPage) atomic_load_32(&((pPage)->nRef))

// For page lock
#define P_LOCK_SUCC 0
#define P_LOCK_BUSY 1
#define P_LOCK_FAIL -1

static inline int tdbTryLockPage(tdb_spinlock_t *pLock) {
  int ret = tdbSpinlockTrylock(pLock);
  if (ret == 0) {
    return P_LOCK_SUCC;
  } else if (ret == EBUSY) {
    return P_LOCK_BUSY;
  } else {
    ASSERT(0);
    return P_LOCK_FAIL;
  }
}

#define TDB_INIT_PAGE_LOCK(pPage)    tdbSpinlockInit(&((pPage)->lock), 0)
#define TDB_DESTROY_PAGE_LOCK(pPage) tdbSpinlockDestroy(&((pPage)->lock))
#define TDB_LOCK_PAGE(pPage)         tdbSpinlockLock(&((pPage)->lock))
#define TDB_UNLOCK_PAGE(pPage)       tdbSpinlockUnlock(&((pPage)->lock))
#define TDB_TRY_LOCK_PAGE(pPage)     tdbTryLockPage(&((pPage)->lock))

// APIs
#define TDB_PAGE_TOTAL_CELLS(pPage) ((pPage)->nOverflow + (pPage)->pPageMethods->getCellNum(pPage))
#define TDB_PAGE_USABLE_SIZE(pPage) ((u8 *)(pPage)->pPageFtr - (pPage)->pCellIdx)
#define TDB_PAGE_FREE_SIZE(pPage)   (*(pPage)->pPageMethods->getFreeBytes)(pPage)
#define TDB_PAGE_PGNO(pPage)        ((pPage)->pgid.pgno)
#define TDB_BYTES_CELL_TAKEN(pPage, pCell) \
  ((*(pPage)->xCellSize)(pPage, pCell, 0, NULL, NULL) + (pPage)->pPageMethods->szOffset)
#define TDB_PAGE_OFFSET_SIZE(pPage) ((pPage)->pPageMethods->szOffset)

int  tdbPageCreate(int pageSize, SPage **ppPage, void *(*xMalloc)(void *, size_t), void *arg);
int  tdbPageDestroy(SPage *pPage, void (*xFree)(void *arg, void *ptr), void *arg);
void tdbPageZero(SPage *pPage, u8 szAmHdr, int (*xCellSize)(const SPage *, SCell *, int, TXN *, SBTree *pBt));
void tdbPageInit(SPage *pPage, u8 szAmHdr, int (*xCellSize)(const SPage *, SCell *, int, TXN *, SBTree *pBt));
int  tdbPageInsertCell(SPage *pPage, int idx, SCell *pCell, int szCell, u8 asOvfl);
int  tdbPageDropCell(SPage *pPage, int idx, TXN *pTxn, SBTree *pBt);
int  tdbPageUpdateCell(SPage *pPage, int idx, SCell *pCell, int szCell, TXN *pTxn, SBTree *pBt);
void tdbPageCopy(SPage *pFromPage, SPage *pToPage, int copyOvflCells);
int  tdbPageCapacity(int pageSize, int amHdrSize);

static inline SCell *tdbPageGetCell(SPage *pPage, int idx) {
  SCell *pCell;
  int    iOvfl;
  int    lidx;

  ASSERT(idx >= 0 && idx < TDB_PAGE_TOTAL_CELLS(pPage));

  iOvfl = 0;
  for (; iOvfl < pPage->nOverflow; iOvfl++) {
    if (pPage->aiOvfl[iOvfl] == idx) {
      pCell = pPage->apOvfl[iOvfl];
      return pCell;
    } else if (pPage->aiOvfl[iOvfl] > idx) {
      break;
    }
  }

  lidx = idx - iOvfl;
  ASSERT(lidx >= 0 && lidx < pPage->pPageMethods->getCellNum(pPage));
  pCell = pPage->pData + pPage->pPageMethods->getCellOffset(pPage, lidx);

  return pCell;
}

#define USE_MAINDB

#ifdef USE_MAINDB
#define TDB_MAINDB_NAME "main.tdb"
#define TDB_FREEDB_NAME "_free.db"
#endif

struct STDB {
  char    *dbName;
  char    *jnName;
  int      jfd;
  SPCache *pCache;
  SPager  *pgrList;
  int      nPager;
  int      nPgrHash;
  SPager **pgrHash;
#ifdef USE_MAINDB
  TTB *pMainDb;
  TTB *pFreeDb;
#endif
  int64_t txnId;
};

struct SPager {
  char    *dbFileName;
  char    *jFileName;
  int      pageSize;
  uint8_t  fid[TDB_FILE_ID_LEN];
  tdb_fd_t fd;
  SPCache *pCache;
  SPgno    dbFileSize;
  SPgno    dbOrigSize;
  // SPage   *pDirty;
  SRBTree rbt;
  // u8        inTran;
  TXN    *pActiveTxn;
  SArray *ofps;
  SArray *frps;
  SPager *pNext;      // used by TDB
  SPager *pHashNext;  // used by TDB
#ifdef USE_MAINDB
  TDB *pEnv;
#endif
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_INTERNAL_H_*/
