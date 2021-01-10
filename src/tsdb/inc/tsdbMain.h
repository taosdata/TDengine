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
#ifndef _TD_TSDB_MAIN_H_
#define _TD_TSDB_MAIN_H_

#include "os.h"
#include "hash.h"
#include "tcoding.h"
#include "tglobal.h"
#include "tkvstore.h"
#include "tlist.h"
#include "tlog.h"
#include "tlockfree.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tutil.h"
#include "tchecksum.h"
#include "tfs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STsdbRepo STsdbRepo;

// ================= tsdbLog.h

// ================= OTHERS

#define TAOS_IN_RANGE(key, keyMin, keyLast) (((key) >= (keyMin)) && ((key) <= (keyMax)))

// NOTE: Any file format change must increase this version number by 1
//       Also, implement the convert function
#define TSDB_FILE_VERSION ((uint32_t)0)

// Definitions
// ================= tsdbMeta.c

// ================= tsdbBuffer.c

// ------------------ tsdbMemTable.c
// ================= tsdbFile.c
/* Statistic information of the TSDB file system.
 */
// ================= tsdbStore.c
#define KVSTORE_FILE_VERSION ((uint32_t)0)

typedef int (*iterFunc)(void*, void* cont, int contLen);
typedef void (*afterFunc)(void*);

typedef struct {
  SMFile    f;
  SHashObj* map;
  iterFunc  iFunc;
  afterFunc aFunc;
  void*     appH;
} SKVStore;

#define KVSTORE_MAGIC(s) (s)->f.info.magic

int       tdCreateKVStore(char* fname);
int       tdDestroyKVStore(char* fname);
SKVStore* tdOpenKVStore(char* fname, iterFunc iFunc, afterFunc aFunc, void* appH);
void      tdCloseKVStore(SKVStore* pStore);
int       tdKVStoreStartCommit(SKVStore* pStore);
int       tdUpdateKVStoreRecord(SKVStore* pStore, uint64_t uid, void* cont, int contLen);
int       tdDropKVStoreRecord(SKVStore* pStore, uint64_t uid);
int       tdKVStoreEndCommit(SKVStore* pStore);
void      tsdbGetStoreInfo(char* fname, uint32_t* magic, int64_t* size);

// ================= 
// extern const char* tsdbFileSuffix[];

// minFid <= midFid <= maxFid
// typedef struct {
//   int minFid;  // >= minFid && < midFid, at level 2
//   int midFid;  // >= midFid && < maxFid, at level 1
//   int maxFid;  // >= maxFid, at level 0
// } SFidGroup;

// typedef enum {
//   TSDB_FILE_TYPE_HEAD = 0,
//   TSDB_FILE_TYPE_DATA,
//   TSDB_FILE_TYPE_LAST,
//   TSDB_FILE_TYPE_STAT,
//   TSDB_FILE_TYPE_NHEAD,
//   TSDB_FILE_TYPE_NDATA,
//   TSDB_FILE_TYPE_NLAST,
//   TSDB_FILE_TYPE_NSTAT
// } TSDB_FILE_TYPE;

// #ifndef TDINTERNAL
// #define TSDB_FILE_TYPE_MAX (TSDB_FILE_TYPE_LAST+1)
// #else
// #define TSDB_FILE_TYPE_MAX (TSDB_FILE_TYPE_STAT+1)
// #endif

// typedef struct {
//   uint32_t magic;
//   uint32_t len;
//   uint32_t totalBlocks;
//   uint32_t totalSubBlocks;
//   uint32_t offset;
//   uint64_t size;      // total size of the file
//   uint64_t tombSize;  // unused file size
// } STsdbFileInfo;

// typedef struct {
//   TFILE         file;
//   STsdbFileInfo info;
//   int           fd;
// } SFile;

// typedef struct {
//   int   fileId;
//   int   state; // 0 for health, 1 for problem
//   SFile files[TSDB_FILE_TYPE_MAX];
// } SFileGroup;

// typedef struct {
//   pthread_rwlock_t fhlock;

//   int         maxFGroups;
//   int         nFGroups;
//   SFileGroup* pFGroup;
// } STsdbFileH;

// typedef struct {
//   int         direction;
//   STsdbFileH* pFileH;
//   int         fileId;
//   int         index;
// } SFileGroupIter;

// #define TSDB_FILE_NAME(pFile) ((pFile)->file.aname)
#define TSDB_KEY_FILEID(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
// #define TSDB_MAX_FILE(keep, daysPerFile) ((keep) / (daysPerFile) + 3)
// #define TSDB_MIN_FILE_ID(fh) (fh)->pFGroup[0].fileId
// #define TSDB_MAX_FILE_ID(fh) (fh)->pFGroup[(fh)->nFGroups - 1].fileId
// #define TSDB_IS_FILE_OPENED(f) ((f)->fd > 0)
// #define TSDB_FGROUP_ITER_FORWARD TSDB_ORDER_ASC
// #define TSDB_FGROUP_ITER_BACKWARD TSDB_ORDER_DESC

// STsdbFileH* tsdbNewFileH(STsdbCfg* pCfg);
// void        tsdbFreeFileH(STsdbFileH* pFileH);
// int         tsdbOpenFileH(STsdbRepo* pRepo);
// void        tsdbCloseFileH(STsdbRepo* pRepo, bool isRestart);
// SFileGroup *tsdbCreateFGroup(STsdbRepo *pRepo, int fid, int level);
// void        tsdbInitFileGroupIter(STsdbFileH* pFileH, SFileGroupIter* pIter, int direction);
// void        tsdbSeekFileGroupIter(SFileGroupIter* pIter, int fid);
// SFileGroup* tsdbGetFileGroupNext(SFileGroupIter* pIter);
// int         tsdbOpenFile(SFile* pFile, int oflag);
// void        tsdbCloseFile(SFile* pFile);
// int         tsdbCreateFile(SFile* pFile, STsdbRepo* pRepo, int fid, int type);
// SFileGroup* tsdbSearchFGroup(STsdbFileH* pFileH, int fid, int flags);
// int         tsdbGetFidLevel(int fid, SFidGroup fidg);
// void        tsdbRemoveFilesBeyondRetention(STsdbRepo* pRepo, SFidGroup* pFidGroup);
// int         tsdbUpdateFileHeader(SFile* pFile);
// int         tsdbEncodeSFileInfo(void** buf, const STsdbFileInfo* pInfo);
// void*       tsdbDecodeSFileInfo(void* buf, STsdbFileInfo* pInfo);
// void        tsdbRemoveFileGroup(STsdbRepo* pRepo, SFileGroup* pFGroup);
// int         tsdbLoadFileHeader(SFile* pFile, uint32_t* version);
// void        tsdbGetFileInfoImpl(char* fname, uint32_t* magic, int64_t* size);
// void        tsdbGetFidGroup(STsdbCfg* pCfg, SFidGroup* pFidGroup);
// int         tsdbApplyRetention(STsdbRepo* pRepo, SFidGroup *pFidGroup);

// ================= tsdbMain.c


#include "tsdbReadImpl.h"

#if 0
// ================= tsdbRWHelper.c

typedef enum { TSDB_WRITE_HELPER, TSDB_READ_HELPER } tsdb_rw_helper_t;

typedef struct {
  TSKEY      minKey;
  TSKEY      maxKey;
  SDFileSet  rSet;
  SDFileSet  wSet;
} SHelperFile;

typedef struct {
  uint64_t uid;
  int32_t  tid;
} SHelperTable;

typedef struct {
  SBlockIdx* pIdxArray;
  int       numOfIdx;
  int       curIdx;
} SIdxH;

typedef struct {
  tsdb_rw_helper_t type;

  STsdbRepo* pRepo;
  int8_t     state;
  // For file set usage
  SHelperFile files;
  SIdxH       idxH;
  SBlockIdx    curCompIdx;
  void*       pWIdx;
  // For table set usage
  SHelperTable tableInfo;
  SBlockInfo*   pCompInfo;
  bool         hasOldLastBlock;
  // For block set usage
  SBlockData* pCompData;
  SDataCols* pDataCols[2];
  void*      pBuffer;     // Buffer to hold the whole data block
  void*      compBuffer;  // Buffer for temperary compress/decompress purpose
} SRWHelper;

#define TSDB_HELPER_CLEAR_STATE 0x0        // Clear state
#define TSDB_HELPER_FILE_SET_AND_OPEN 0x1  // File is set
#define TSDB_HELPER_IDX_LOAD 0x2           // SBlockIdx part is loaded
#define TSDB_HELPER_TABLE_SET 0x4          // Table is set
#define TSDB_HELPER_INFO_LOAD 0x8          // SBlockInfo part is loaded
#define TSDB_HELPER_FILE_DATA_LOAD 0x10    // SBlockData part is loaded
#define helperSetState(h, s) (((h)->state) |= (s))
#define helperClearState(h, s) ((h)->state &= (~(s)))
#define helperHasState(h, s) ((((h)->state) & (s)) == (s))
#define blockAtIdx(h, idx) ((h)->pCompInfo->blocks + idx)
#define TSDB_MAX_SUBBLOCKS 8
#define IS_SUB_BLOCK(pBlock) ((pBlock)->numOfSubBlocks == 0)
#define helperType(h) (h)->type
#define helperRepo(h) (h)->pRepo
#define helperState(h) (h)->state
#define TSDB_NLAST_FILE_OPENED(h) ((h)->files.nLastF.fd > 0)
#define helperFileId(h) ((h)->files.fGroup.fileId)
#define helperHeadF(h) (&((h)->files.fGroup.files[TSDB_FILE_TYPE_HEAD]))
#define helperDataF(h) (&((h)->files.fGroup.files[TSDB_FILE_TYPE_DATA]))
#define helperLastF(h) (&((h)->files.fGroup.files[TSDB_FILE_TYPE_LAST]))
#define helperNewHeadF(h) (&((h)->files.nHeadF))
#define helperNewLastF(h) (&((h)->files.nLastF))

int  tsdbInitReadHelper(SRWHelper* pHelper, STsdbRepo* pRepo);
int  tsdbInitWriteHelper(SRWHelper* pHelper, STsdbRepo* pRepo);
void tsdbDestroyHelper(SRWHelper* pHelper);
void tsdbResetHelper(SRWHelper* pHelper);
int  tsdbSetAndOpenHelperFile(SRWHelper* pHelper, SFileGroup* pGroup);
int  tsdbCloseHelperFile(SRWHelper* pHelper, bool hasError, SFileGroup* pGroup);
int  tsdbSetHelperTable(SRWHelper* pHelper, STable* pTable, STsdbRepo* pRepo);
int  tsdbCommitTableData(SRWHelper* pHelper, SCommitIter* pCommitIter, SDataCols* pDataCols, TSKEY maxKey);
int  tsdbMoveLastBlockIfNeccessary(SRWHelper* pHelper);
int  tsdbWriteCompInfo(SRWHelper* pHelper);
int  tsdbWriteCompIdx(SRWHelper* pHelper);
int  tsdbLoadCompIdxImpl(SFile* pFile, uint32_t offset, uint32_t len, void* buffer);
int  tsdbDecodeSBlockIdxImpl(void* buffer, uint32_t len, SBlockIdx** ppCompIdx, int* numOfIdx);
int  tsdbLoadCompIdx(SRWHelper* pHelper, void* target);
int  tsdbLoadCompInfoImpl(SFile* pFile, SBlockIdx* pIdx, SBlockInfo** ppCompInfo);
int  tsdbLoadCompInfo(SRWHelper* pHelper, void* target);
int  tsdbLoadCompData(SRWHelper* phelper, SBlock* pcompblock, void* target);
void tsdbGetDataStatis(SRWHelper* pHelper, SDataStatis* pStatis, int numOfCols);
int  tsdbLoadBlockDataCols(SRWHelper* pHelper, SBlock* pCompBlock, SBlockInfo* pCompInfo, int16_t* colIds,
                           int numOfColIds);
int  tsdbLoadBlockData(SRWHelper* pHelper, SBlock* pCompBlock, SBlockInfo* pCompInfo);

static FORCE_INLINE int compTSKEY(const void* key1, const void* key2) {
  if (*(TSKEY*)key1 > *(TSKEY*)key2) {
    return 1;
  } else if (*(TSKEY*)key1 == *(TSKEY*)key2) {
    return 0;
  } else {
    return -1;
  }
}

#endif

// ================= tsdbScan.c
typedef struct {
  SFileGroup  fGroup;
  int         numOfIdx;
  SBlockIdx*  pCompIdx;
  SBlockInfo* pCompInfo;
  void*       pBuf;
  FILE*       tLogStream;
} STsdbScanHandle;

int              tsdbScanFGroup(STsdbScanHandle* pScanHandle, char* rootDir, int fid);
STsdbScanHandle* tsdbNewScanHandle();
void             tsdbSetScanLogStream(STsdbScanHandle* pScanHandle, FILE* fLogStream);
int              tsdbSetAndOpenScanFile(STsdbScanHandle* pScanHandle, char* rootDir, int fid);
int              tsdbScanSBlockIdx(STsdbScanHandle* pScanHandle);
int              tsdbScanSBlock(STsdbScanHandle* pScanHandle, int idx);
int              tsdbCloseScanFile(STsdbScanHandle* pScanHandle);
void             tsdbFreeScanHandle(STsdbScanHandle* pScanHandle);

// ------------------ tsdbCommitQueue.c
int tsdbScheduleCommit(STsdbRepo *pRepo);

#ifdef __cplusplus
}
#endif

#endif