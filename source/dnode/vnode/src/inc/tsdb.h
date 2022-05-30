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

#ifndef _TD_VNODE_TSDB_H_
#define _TD_VNODE_TSDB_H_

#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// tsdbDebug ================
// clang-format off
#define tsdbFatal(...) do { if (tsdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TSDB FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define tsdbError(...) do { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TSDB ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define tsdbWarn(...)  do { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TSDB WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define tsdbInfo(...)  do { if (tsdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TSDB ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define tsdbDebug(...) do { if (tsdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSDB ", DEBUG_DEBUG, tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define tsdbTrace(...) do { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TSDB ", DEBUG_TRACE, tsdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

// tsdbMemTable ================
typedef struct STbData       STbData;
typedef struct STsdbMemTable STsdbMemTable;
typedef struct SMergeInfo    SMergeInfo;
typedef struct STable        STable;

int  tsdbMemTableCreate(STsdb *pTsdb, STsdbMemTable **ppMemTable);
void tsdbMemTableDestroy(STsdb *pTsdb, STsdbMemTable *pMemTable);
int  tsdbLoadDataFromCache(STsdb *pTsdb, STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead,
                           SDataCols *pCols, TKEY *filterKeys, int nFilterKeys, bool keepDup, SMergeInfo *pMergeInfo);

// tsdbCommit ================

// tsdbFS ================
typedef struct STsdbFS STsdbFS;

// tsdbSma ================
typedef struct SSmaEnv  SSmaEnv;
typedef struct SSmaEnvs SSmaEnvs;

// structs
typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

#define TSDB_DATA_DIR_LEN 6  // adapt accordingly
struct STsdb {
  char          *path;
  SVnode        *pVnode;
  TdThreadMutex  mutex;
  char           dir[TSDB_DATA_DIR_LEN];
  bool           repoLocked;
  STsdbKeepCfg   keepCfg;
  STsdbMemTable *mem;
  STsdbMemTable *imem;
  SRtn           rtn;
  STsdbFS       *fs;
};

#if 1  // ======================================

struct STable {
  uint64_t  tid;
  uint64_t  uid;
  STSchema *pSchema;
};

#define TABLE_TID(t) (t)->tid
#define TABLE_UID(t) (t)->uid

int tsdbPrepareCommit(STsdb *pTsdb);
typedef enum {
  TSDB_FILE_HEAD = 0,  // .head
  TSDB_FILE_DATA,      // .data
  TSDB_FILE_LAST,      // .last
  TSDB_FILE_SMAD,      // .smad(Block-wise SMA)
  TSDB_FILE_SMAL,      // .smal(Block-wise SMA)
  TSDB_FILE_MAX,       //
  TSDB_FILE_META,      // meta
} E_TSDB_FILE_T;

typedef struct {
  uint32_t magic;
  uint32_t fver;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;
  uint64_t tombSize;
} SDFInfo;

typedef struct {
  SDFInfo   info;
  STfsFile  f;
  TdFilePtr pFile;
  uint8_t   state;
} SDFile;

typedef struct {
  int      fid;
  int8_t   state;  // -128~127
  uint8_t  ver;    // 0~255, DFileSet version
  uint16_t reserve;
  SDFile   files[TSDB_FILE_MAX];
} SDFileSet;

struct STbData {
  tb_uid_t   uid;
  TSKEY      keyMin;
  TSKEY      keyMax;
  int64_t    minVer;
  int64_t    maxVer;
  int64_t    nrows;
  SSkipList *pData;
};

struct STsdbMemTable {
  SVBufPool *pPool;
  T_REF_DECLARE()
  SRWLatch   latch;
  TSKEY      keyMin;
  TSKEY      keyMax;
  int64_t    minVer;
  int64_t    maxVer;
  int64_t    nRow;
  SSkipList *pSlIdx;  // SSkiplist<STbData>
  SHashObj  *pHashIdx;
};

typedef struct {
  uint32_t version;       // Commit version from 0 to increase
  int64_t  totalPoints;   // total points
  int64_t  totalStorage;  // Uncompressed total storage
} STsdbFSMeta;

// ==================
typedef struct {
  STsdbFSMeta meta;  // FS meta
  SArray     *df;    // data file array
  SArray     *sf;    // sma data file array    v2f1900.index_name_1
} SFSStatus;

struct STsdbFS {
  TdThreadRwlock lock;

  SFSStatus *cstatus;        // current status
  SHashObj  *metaCache;      // meta cache
  SHashObj  *metaCacheComp;  // meta cache for compact
  bool       intxn;
  SFSStatus *nstatus;  // new status
};

#define REPO_ID(r)        TD_VID((r)->pVnode)
#define REPO_CFG(r)       (&(r)->pVnode->config.tsdbCfg)
#define REPO_KEEP_CFG(r)  (&(r)->keepCfg)
#define REPO_FS(r)        ((r)->fs)
#define REPO_META(r)      ((r)->pVnode->pMeta)
#define REPO_TFS(r)       ((r)->pVnode->pTfs)
#define IS_REPO_LOCKED(r) ((r)->repoLocked)

int tsdbLockRepo(STsdb *pTsdb);
int tsdbUnlockRepo(STsdb *pTsdb);

static FORCE_INLINE STSchema *tsdbGetTableSchemaImpl(STsdb *pTsdb, STable *pTable, bool lock, bool copy,
                                                     int32_t version) {
  if ((version != -1) && (schemaVersion(pTable->pSchema) != version)) {
    taosMemoryFreeClear(pTable->pSchema);
    pTable->pSchema = metaGetTbTSchema(REPO_META(pTsdb), pTable->uid, version);
  }

  return pTable->pSchema;
}

// tsdbMemTable.h
struct SMergeInfo {
  int   rowsInserted;
  int   rowsUpdated;
  int   rowsDeleteSucceed;
  int   rowsDeleteFailed;
  int   nOperations;
  TSKEY keyFirst;
  TSKEY keyLast;
};

static void  *taosTMalloc(size_t size);
static void  *taosTCalloc(size_t nmemb, size_t size);
static void  *taosTRealloc(void *ptr, size_t size);
static void  *taosTZfree(void *ptr);
static size_t taosTSizeof(void *ptr);
static void   taosTMemset(void *ptr, int c);

static FORCE_INLINE STSRow *tsdbNextIterRow(SSkipListIterator *pIter) {
  if (pIter == NULL) return NULL;

  SSkipListNode *node = tSkipListIterGet(pIter);
  if (node == NULL) return NULL;

  return (STSRow *)SL_GET_NODE_DATA(node);
}

static FORCE_INLINE TSKEY tsdbNextIterKey(SSkipListIterator *pIter) {
  STSRow *row = tsdbNextIterRow(pIter);
  if (row == NULL) return TSDB_DATA_TIMESTAMP_NULL;

  return TD_ROW_KEY(row);
}

// tsdbReadImpl
typedef struct SReadH SReadH;

typedef struct {
  uint32_t len;
  uint32_t offset;
  uint32_t hasLast : 2;
  uint32_t numOfBlocks : 30;
  uint64_t uid;
  TSKEY    maxKey;
} SBlockIdx;

#ifdef TD_REFACTOR_3
typedef struct {
  int64_t last : 1;
  int64_t offset : 63;
  int32_t algorithm : 8;
  int32_t numOfRows : 24;
  int32_t len;
  int32_t keyLen;  // key column length, keyOffset = offset+sizeof(SBlockData)+sizeof(SBlockCol)*numOfCols
  int16_t numOfSubBlocks;
  int16_t numOfCols;  // not including timestamp column
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SBlock;

#else

typedef enum {
  TSDB_SBLK_VER_0 = 0,
  TSDB_SBLK_VER_MAX,
} ESBlockVer;

#define SBlockVerLatest TSDB_SBLK_VER_0

typedef struct {
  uint8_t  last : 1;
  uint8_t  hasDupKey : 1;  // 0: no dup TS key, 1: has dup TS key(since supporting Multi-Version)
  uint8_t  blkVer : 6;
  uint8_t  numOfSubBlocks;
  col_id_t numOfCols;    // not including timestamp column
  uint32_t len;          // data block length
  uint32_t keyLen : 20;  // key column length, keyOffset = offset+sizeof(SBlockData)+sizeof(SBlockCol)*numOfCols
  uint32_t algorithm : 4;
  uint32_t reserve : 8;
  col_id_t numOfBSma;
  uint16_t numOfRows;
  int64_t  offset;
  uint64_t aggrStat : 1;
  uint64_t aggrOffset : 63;
  TSKEY    keyFirst;
  TSKEY    keyLast;
} SBlockV0;

#define SBlock SBlockV0  // latest SBlock definition

static FORCE_INLINE bool tsdbIsSupBlock(SBlock *pBlock) { return pBlock->numOfSubBlocks == 1; }
static FORCE_INLINE bool tsdbIsSubBlock(SBlock *pBlock) { return pBlock->numOfSubBlocks == 0; }

#endif

typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  tid;
  uint64_t uid;
  SBlock   blocks[];
} SBlockInfo;

#ifdef TD_REFACTOR_3
typedef struct {
  int16_t  colId;
  uint16_t bitmap : 1;  // 0: no bitmap if all rows are NORM, 1: has bitmap if has NULL/NORM rows
  uint16_t reserve : 15;
  int32_t  len;
  uint32_t type : 8;
  uint32_t offset : 24;
  int64_t  sum;
  int64_t  max;
  int64_t  min;
  int16_t  maxIndex;
  int16_t  minIndex;
  int16_t  numOfNull;
  uint8_t  offsetH;
  char     padding[1];
} SBlockCol;
#else
typedef struct {
  int16_t  colId;
  uint16_t type : 6;
  uint16_t blen : 10;  // 0 no bitmap if all rows are NORM, > 0 bitmap length
  uint32_t len;        // data length + bitmap length
  uint32_t offset;
} SBlockColV0;

#define SBlockCol SBlockColV0  // latest SBlockCol definition

typedef struct {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SAggrBlkColV0;

#define SAggrBlkCol SAggrBlkColV0  // latest SAggrBlkCol definition

#endif

// Code here just for back-ward compatibility
static FORCE_INLINE void tsdbSetBlockColOffset(SBlockCol *pBlockCol, uint32_t offset) {
#ifdef TD_REFACTOR_3
  pBlockCol->offset = offset & ((((uint32_t)1) << 24) - 1);
  pBlockCol->offsetH = (uint8_t)(offset >> 24);
#else
  pBlockCol->offset = offset;
#endif
}

static FORCE_INLINE uint32_t tsdbGetBlockColOffset(SBlockCol *pBlockCol) {
#ifdef TD_REFACTOR_3
  uint32_t offset1 = pBlockCol->offset;
  uint32_t offset2 = pBlockCol->offsetH;
  return (offset1 | (offset2 << 24));
#else
  return pBlockCol->offset;
#endif
}

typedef struct {
  int32_t   delimiter;  // For recovery usage
  int32_t   numOfCols;  // For recovery usage
  uint64_t  uid;        // For recovery usage
  SBlockCol cols[];
} SBlockData;

typedef void SAggrBlkData;  // SBlockCol cols[];

struct SReadH {
  STsdb        *pRepo;
  SDFileSet     rSet;     // FSET to read
  SArray       *aBlkIdx;  // SBlockIdx array
  STable       *pTable;   // table to read
  SBlockIdx    *pBlkIdx;  // current reading table SBlockIdx
  int           cidx;
  SBlockInfo   *pBlkInfo;
  SBlockData   *pBlkData;      // Block info
  SAggrBlkData *pAggrBlkData;  // Aggregate Block info
  SDataCols    *pDCols[2];
  void         *pBuf;    // buffer
  void         *pCBuf;   // compression buffer
  void         *pExBuf;  // extra buffer
};

#define TSDB_READ_REPO(rh)      ((rh)->pRepo)
#define TSDB_READ_REPO_ID(rh)   REPO_ID(TSDB_READ_REPO(rh))
#define TSDB_READ_FSET(rh)      (&((rh)->rSet))
#define TSDB_READ_TABLE(rh)     ((rh)->pTable)
#define TSDB_READ_TABLE_UID(rh) ((rh)->pTable->uid)
#define TSDB_READ_HEAD_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_HEAD)
#define TSDB_READ_DATA_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_DATA)
#define TSDB_READ_LAST_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_LAST)
#define TSDB_READ_SMAD_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_SMAD)
#define TSDB_READ_SMAL_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_SMAL)
#define TSDB_READ_BUF(rh)       ((rh)->pBuf)
#define TSDB_READ_COMP_BUF(rh)  ((rh)->pCBuf)
#define TSDB_READ_EXBUF(rh)     ((rh)->pExBuf)

#define TSDB_BLOCK_STATIS_SIZE(ncols, blkVer) \
  (sizeof(SBlockData) + sizeof(SBlockColV##blkVer) * (ncols) + sizeof(TSCKSUM))

static FORCE_INLINE size_t tsdbBlockStatisSize(int nCols, uint32_t blkVer) {
  switch (blkVer) {
    case TSDB_SBLK_VER_0:
    default:
      return TSDB_BLOCK_STATIS_SIZE(nCols, 0);
  }
}

#define TSDB_BLOCK_AGGR_SIZE(ncols, blkVer) (sizeof(SAggrBlkColV##blkVer) * (ncols) + sizeof(TSCKSUM))

static FORCE_INLINE size_t tsdbBlockAggrSize(int nCols, uint32_t blkVer) {
  switch (blkVer) {
    case TSDB_SBLK_VER_0:
    default:
      return TSDB_BLOCK_AGGR_SIZE(nCols, 0);
  }
}

int  tsdbInitReadH(SReadH *pReadh, STsdb *pRepo);
void tsdbDestroyReadH(SReadH *pReadh);
int  tsdbSetAndOpenReadFSet(SReadH *pReadh, SDFileSet *pSet);
void tsdbCloseAndUnsetFSet(SReadH *pReadh);
int  tsdbLoadBlockIdx(SReadH *pReadh);
int  tsdbSetReadTable(SReadH *pReadh, STable *pTable);
int  tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget);
int  tsdbLoadBlockData(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlockInfo);
int tsdbLoadBlockDataCols(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo, const int16_t *colIds, int numOfColsIds,
                          bool mergeBitmap);
int tsdbLoadBlockStatis(SReadH *pReadh, SBlock *pBlock);
int tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx);
void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx);
void  tsdbGetBlockStatis(SReadH *pReadh, SColumnDataAgg *pStatis, int numOfCols, SBlock *pBlock);

static FORCE_INLINE int tsdbMakeRoom(void **ppBuf, size_t size) {
  void  *pBuf = *ppBuf;
  size_t tsize = taosTSizeof(pBuf);

  if (tsize < size) {
    if (tsize == 0) tsize = 1024;

    while (tsize < size) {
      tsize *= 2;
    }

    *ppBuf = taosTRealloc(pBuf, tsize);
    if (*ppBuf == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}

// tsdbMemory
static FORCE_INLINE void *taosTMalloc(size_t size) {
  if (size <= 0) return NULL;

  void *ret = taosMemoryMalloc(size + sizeof(size_t));
  if (ret == NULL) return NULL;

  *(size_t *)ret = size;

  return (void *)((char *)ret + sizeof(size_t));
}

static FORCE_INLINE void *taosTCalloc(size_t nmemb, size_t size) {
  size_t tsize = nmemb * size;
  void  *ret = taosTMalloc(tsize);
  if (ret == NULL) return NULL;

  taosTMemset(ret, 0);
  return ret;
}

static FORCE_INLINE size_t taosTSizeof(void *ptr) { return (ptr) ? (*(size_t *)((char *)ptr - sizeof(size_t))) : 0; }

static FORCE_INLINE void taosTMemset(void *ptr, int c) { memset(ptr, c, taosTSizeof(ptr)); }

static FORCE_INLINE void *taosTRealloc(void *ptr, size_t size) {
  if (ptr == NULL) return taosTMalloc(size);

  if (size <= taosTSizeof(ptr)) return ptr;

  void  *tptr = (void *)((char *)ptr - sizeof(size_t));
  size_t tsize = size + sizeof(size_t);
  void  *tptr1 = taosMemoryRealloc(tptr, tsize);
  if (tptr1 == NULL) return NULL;
  tptr = tptr1;

  *(size_t *)tptr = size;

  return (void *)((char *)tptr + sizeof(size_t));
}

static FORCE_INLINE void *taosTZfree(void *ptr) {
  if (ptr) {
    taosMemoryFree((void *)((char *)ptr - sizeof(size_t)));
  }
  return NULL;
}

// tsdbCommit

void tsdbGetRtnSnap(STsdb *pRepo, SRtn *pRtn);

static FORCE_INLINE int TSDB_KEY_FID(TSKEY key, int32_t days, int8_t precision) {
  if (key < 0) {
    return (int)((key + 1) / tsTickPerMin[precision] / days - 1);
  } else {
    return (int)((key / tsTickPerMin[precision] / days));
  }
}

static FORCE_INLINE int tsdbGetFidLevel(int fid, SRtn *pRtn) {
  if (fid >= pRtn->maxFid) {
    return 0;
  } else if (fid >= pRtn->midFid) {
    return 1;
  } else if (fid >= pRtn->minFid) {
    return 2;
  } else {
    return -1;
  }
}

// tsdbFile
#define TSDB_FILE_HEAD_SIZE  512
#define TSDB_FILE_DELIMITER  0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC 0xFFFFFFFF
#define TSDB_IVLD_FID        INT_MIN
#define TSDB_FILE_STATE_OK   0
#define TSDB_FILE_STATE_BAD  1

#define TSDB_FILE_INFO(tf)         (&((tf)->info))
#define TSDB_FILE_F(tf)            (&((tf)->f))
#define TSDB_FILE_PFILE(tf)        ((tf)->pFile)
#define TSDB_FILE_FULL_NAME(tf)    (TSDB_FILE_F(tf)->aname)
#define TSDB_FILE_OPENED(tf)       (TSDB_FILE_PFILE(tf) != NULL)
#define TSDB_FILE_CLOSED(tf)       (!TSDB_FILE_OPENED(tf))
#define TSDB_FILE_SET_CLOSED(f)    (TSDB_FILE_PFILE(f) = NULL)
#define TSDB_FILE_LEVEL(tf)        (TSDB_FILE_F(tf)->did.level)
#define TSDB_FILE_ID(tf)           (TSDB_FILE_F(tf)->did.id)
#define TSDB_FILE_DID(tf)          (TSDB_FILE_F(tf)->did)
#define TSDB_FILE_REL_NAME(tf)     (TSDB_FILE_F(tf)->rname)
#define TSDB_FILE_ABS_NAME(tf)     (TSDB_FILE_F(tf)->aname)
#define TSDB_FILE_FSYNC(tf)        taosFsyncFile(TSDB_FILE_PFILE(tf))
#define TSDB_FILE_STATE(tf)        ((tf)->state)
#define TSDB_FILE_SET_STATE(tf, s) ((tf)->state = (s))
#define TSDB_FILE_IS_OK(tf)        (TSDB_FILE_STATE(tf) == TSDB_FILE_STATE_OK)
#define TSDB_FILE_IS_BAD(tf)       (TSDB_FILE_STATE(tf) == TSDB_FILE_STATE_BAD)

typedef int32_t TSDB_FILE_T;
typedef enum {
  TSDB_FS_VER_0 = 0,
  TSDB_FS_VER_MAX,
} ETsdbFsVer;

#define TSDB_LATEST_FVER    TSDB_FS_VER_0  // latest version for DFile
#define TSDB_LATEST_SFS_VER TSDB_FS_VER_0  // latest version for 'current' file

static FORCE_INLINE uint32_t tsdbGetDFSVersion(TSDB_FILE_T fType) {  // latest version for DFile
  switch (fType) {
    case TSDB_FILE_HEAD:  // .head
    case TSDB_FILE_DATA:  // .data
    case TSDB_FILE_LAST:  // .last
    case TSDB_FILE_SMAD:  // .smad(Block-wise SMA)
    case TSDB_FILE_SMAL:  // .smal(Block-wise SMA)
    default:
      return TSDB_LATEST_FVER;
  }
}

void  tsdbInitDFile(STsdb *pRepo, SDFile *pDFile, SDiskID did, int fid, uint32_t ver, TSDB_FILE_T ftype);
void  tsdbInitDFileEx(SDFile *pDFile, SDFile *pODFile);
int   tsdbEncodeSDFile(void **buf, SDFile *pDFile);
void *tsdbDecodeSDFile(STsdb *pRepo, void *buf, SDFile *pDFile);
int   tsdbCreateDFile(STsdb *pRepo, SDFile *pDFile, bool updateHeader, TSDB_FILE_T fType);
int   tsdbUpdateDFileHeader(SDFile *pDFile);
int   tsdbLoadDFileHeader(SDFile *pDFile, SDFInfo *pInfo);
int   tsdbParseDFilename(const char *fname, int *vid, int *fid, TSDB_FILE_T *ftype, uint32_t *version);

static FORCE_INLINE void tsdbSetDFileInfo(SDFile *pDFile, SDFInfo *pInfo) { pDFile->info = *pInfo; }

static FORCE_INLINE int tsdbOpenDFile(SDFile *pDFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pDFile));

  pDFile->pFile = taosOpenFile(TSDB_FILE_FULL_NAME(pDFile), flags);
  if (pDFile->pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static FORCE_INLINE void tsdbCloseDFile(SDFile *pDFile) {
  if (TSDB_FILE_OPENED(pDFile)) {
    taosCloseFile(&pDFile->pFile);
    TSDB_FILE_SET_CLOSED(pDFile);
  }
}

static FORCE_INLINE int64_t tsdbSeekDFile(SDFile *pDFile, int64_t offset, int whence) {
  // ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t loffset = taosLSeekFile(TSDB_FILE_PFILE(pDFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

static FORCE_INLINE int64_t tsdbWriteDFile(SDFile *pDFile, void *buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nwrite = taosWriteFile(pDFile->pFile, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nwrite;
}

static FORCE_INLINE void tsdbUpdateDFileMagic(SDFile *pDFile, void *pCksm) {
  pDFile->info.magic = taosCalcChecksum(pDFile->info.magic, (uint8_t *)(pCksm), sizeof(TSCKSUM));
}

static FORCE_INLINE int tsdbAppendDFile(SDFile *pDFile, void *buf, int64_t nbyte, int64_t *offset) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t toffset;

  if ((toffset = tsdbSeekDFile(pDFile, 0, SEEK_END)) < 0) {
    return -1;
  }

  ASSERT(pDFile->info.size == toffset);

  if (offset) {
    *offset = toffset;
  }

  if (tsdbWriteDFile(pDFile, buf, nbyte) < 0) {
    return -1;
  }

  pDFile->info.size += nbyte;

  return (int)nbyte;
}

static FORCE_INLINE int tsdbRemoveDFile(SDFile *pDFile) { return tfsRemoveFile(TSDB_FILE_F(pDFile)); }

static FORCE_INLINE int64_t tsdbReadDFile(SDFile *pDFile, void *buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nread = taosReadFile(pDFile->pFile, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

static FORCE_INLINE int tsdbCopyDFile(SDFile *pSrc, SDFile *pDest) {
  if (tfsCopyFile(TSDB_FILE_F(pSrc), TSDB_FILE_F(pDest)) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tsdbSetDFileInfo(pDest, TSDB_FILE_INFO(pSrc));
  return 0;
}

// =============== SDFileSet

typedef struct {
  int      fid;
  int8_t   state;
  uint8_t  ver;
  uint16_t reserve;
#if 0
  SDFInfo   info;
#endif
  STfsFile  f;
  TdFilePtr pFile;

} SSFile;  // files split by days with fid

#define TSDB_LATEST_FSET_VER 0

#define TSDB_FSET_FID(s)        ((s)->fid)
#define TSDB_FSET_STATE(s)      ((s)->state)
#define TSDB_FSET_VER(s)        ((s)->ver)
#define TSDB_DFILE_IN_SET(s, t) ((s)->files + (t))
#define TSDB_FSET_LEVEL(s)      TSDB_FILE_LEVEL(TSDB_DFILE_IN_SET(s, 0))
#define TSDB_FSET_ID(s)         TSDB_FILE_ID(TSDB_DFILE_IN_SET(s, 0))
#define TSDB_FSET_SET_CLOSED(s)                                                \
  do {                                                                         \
    for (TSDB_FILE_T ftype = TSDB_FILE_HEAD; ftype < TSDB_FILE_MAX; ftype++) { \
      TSDB_FILE_SET_CLOSED(TSDB_DFILE_IN_SET(s, ftype));                       \
    }                                                                          \
  } while (0);
#define TSDB_FSET_FSYNC(s)                                                     \
  do {                                                                         \
    for (TSDB_FILE_T ftype = TSDB_FILE_HEAD; ftype < TSDB_FILE_MAX; ftype++) { \
      TSDB_FILE_FSYNC(TSDB_DFILE_IN_SET(s, ftype));                            \
    }                                                                          \
  } while (0);

void  tsdbInitDFileSet(STsdb *pRepo, SDFileSet *pSet, SDiskID did, int fid, uint32_t ver);
void  tsdbInitDFileSetEx(SDFileSet *pSet, SDFileSet *pOSet);
int   tsdbEncodeDFileSet(void **buf, SDFileSet *pSet);
void *tsdbDecodeDFileSet(STsdb *pRepo, void *buf, SDFileSet *pSet);
int   tsdbEncodeDFileSetEx(void **buf, SDFileSet *pSet);
void *tsdbDecodeDFileSetEx(void *buf, SDFileSet *pSet);
int   tsdbApplyDFileSetChange(SDFileSet *from, SDFileSet *to);
int   tsdbCreateDFileSet(STsdb *pRepo, SDFileSet *pSet, bool updateHeader);
int   tsdbUpdateDFileSetHeader(SDFileSet *pSet);
int   tsdbScanAndTryFixDFileSet(STsdb *pRepo, SDFileSet *pSet);

static FORCE_INLINE void tsdbCloseDFileSet(SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbCloseDFile(TSDB_DFILE_IN_SET(pSet, ftype));
  }
}

static FORCE_INLINE int tsdbOpenDFileSet(SDFileSet *pSet, int flags) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbOpenDFile(TSDB_DFILE_IN_SET(pSet, ftype), flags) < 0) {
      tsdbCloseDFileSet(pSet);
      return -1;
    }
  }
  return 0;
}

static FORCE_INLINE void tsdbRemoveDFileSet(SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    (void)tsdbRemoveDFile(TSDB_DFILE_IN_SET(pSet, ftype));
  }
}

static FORCE_INLINE int tsdbCopyDFileSet(SDFileSet *pSrc, SDFileSet *pDest) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbCopyDFile(TSDB_DFILE_IN_SET(pSrc, ftype), TSDB_DFILE_IN_SET(pDest, ftype)) < 0) {
      tsdbRemoveDFileSet(pDest);
      return -1;
    }
  }

  return 0;
}

static FORCE_INLINE void tsdbGetFidKeyRange(int days, int8_t precision, int fid, TSKEY *minKey, TSKEY *maxKey) {
  *minKey = fid * days * tsTickPerMin[precision];
  *maxKey = *minKey + days * tsTickPerMin[precision] - 1;
}

static FORCE_INLINE bool tsdbFSetIsOk(SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (TSDB_FILE_IS_BAD(TSDB_DFILE_IN_SET(pSet, ftype))) {
      return false;
    }
  }

  return true;
}

// tsdbFS
// ================== TSDB global config
extern bool tsdbForceKeepFile;

// ================== CURRENT file header info
typedef struct {
  uint32_t version;  // Current file system version (relating to code)
  uint32_t len;      // Encode content length (including checksum)
} SFSHeader;

// ================== TSDB File System Meta
#define FS_CURRENT_STATUS(pfs) ((pfs)->cstatus)
#define FS_NEW_STATUS(pfs)     ((pfs)->nstatus)
#define FS_IN_TXN(pfs)         (pfs)->intxn
#define FS_VERSION(pfs)        ((pfs)->cstatus->meta.version)
#define FS_TXN_VERSION(pfs)    ((pfs)->nstatus->meta.version)

typedef struct {
  int        direction;
  uint64_t   version;  // current FS version
  STsdbFS   *pfs;
  int        index;  // used to position next fset when version the same
  int        fid;    // used to seek when version is changed
  SDFileSet *pSet;
} SFSIter;

#define TSDB_FS_ITER_FORWARD  TSDB_ORDER_ASC
#define TSDB_FS_ITER_BACKWARD TSDB_ORDER_DESC

STsdbFS *tsdbNewFS(const STsdbKeepCfg *pCfg);
void    *tsdbFreeFS(STsdbFS *pfs);
int      tsdbOpenFS(STsdb *pRepo);
void     tsdbCloseFS(STsdb *pRepo);
void     tsdbStartFSTxn(STsdb *pRepo, int64_t pointsAdd, int64_t storageAdd);
int      tsdbEndFSTxn(STsdb *pRepo);
int      tsdbEndFSTxnWithError(STsdbFS *pfs);
void     tsdbUpdateFSTxnMeta(STsdbFS *pfs, STsdbFSMeta *pMeta);
// void     tsdbUpdateMFile(STsdbFS *pfs, const SMFile *pMFile);
int tsdbUpdateDFileSet(STsdbFS *pfs, const SDFileSet *pSet);

void       tsdbFSIterInit(SFSIter *pIter, STsdbFS *pfs, int direction);
void       tsdbFSIterSeek(SFSIter *pIter, int fid);
SDFileSet *tsdbFSIterNext(SFSIter *pIter);
int        tsdbLoadMetaCache(STsdb *pRepo, bool recoverMeta);

static FORCE_INLINE int tsdbRLockFS(STsdbFS *pFs) {
  int code = taosThreadRwlockRdlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbWLockFS(STsdbFS *pFs) {
  int code = taosThreadRwlockWrlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbUnLockFS(STsdbFS *pFs) {
  int code = taosThreadRwlockUnlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_H_*/