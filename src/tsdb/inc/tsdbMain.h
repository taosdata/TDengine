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

#include "hash.h"
#include "tcoding.h"
#include "tglobal.h"
#include "tkvstore.h"
#include "tlist.h"
#include "tlog.h"
#include "tref.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int tsdbDebugFlag;

#define tsdbError(...) { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("ERROR TDB ", tsdbDebugFlag, __VA_ARGS__); }}
#define tsdbWarn(...)  { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("WARN TDB ", tsdbDebugFlag, __VA_ARGS__); }}
#define tsdbTrace(...) { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TDB ", tsdbDebugFlag, __VA_ARGS__); }}
#define tsdbPrint(...) { taosPrintLog("TDB ", 255, __VA_ARGS__); }

#define TSDB_MAX_TABLE_SCHEMAS 16
#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F
#define 

// Definitions
// ------------------ tsdbMeta.c
typedef struct STable {
  ETableType type;
  tstr*      name;  // NOTE: there a flexible string here
  STableId   tableId;
  STable*    pSuper;  // super table pointer
  uint8_t    numOfSchemas;
  STSchema   schema[TSDB_MAX_TABLE_SCHEMAS];
  STSchema*  tagSchema;
  SKVRow     tagVal;
  void*      pIndex;         // For TSDB_SUPER_TABLE, it is the skiplist index
  void*      eventHandler;   // TODO
  void*      streamHandler;  // TODO
  TSKEY      lastKey;        // lastkey inserted in this table, initialized as 0, TODO: make a structure
  char*      sql;
  void*      cqhandle;
} STable;

typedef struct {
  pthread_rwlock_t rwLock;

  int32_t   nTables;
  STable**  tables;
  SList*    superList;
  SHashObj* uidMap;
  SKVStore* pStore;
  int       maxRowBytes;
  int       maxCols;
} STsdbMeta;

// ------------------ tsdbBuffer.c
typedef struct {
  int64_t blockId;
  int     offset;
  int     remain;
  char    data[];
} STsdbBufBlock;

typedef struct {
  pthread_cond_t poolNotEmpty;
  int            bufBlockSize;
  int            tBufBlocks;
  int            nBufBlocks;
  int64_t        index;
  SList*         bufBlockList;
} STsdbBufPool;

// ------------------ tsdbMemTable.c
typedef struct {
  uint64_t   uid;
  TSKEY      keyFirst;
  TSKEY      keyLast;
  int64_t    numOfRows;
  SSkipList* pData;
} STableData;

typedef struct {
  T_REF_DECLARE();
  TSKEY        keyFirst;
  TSKEY        keyLast;
  int64_t      numOfRows;
  STableData** tData;
  SList*       actList;
  SList*       bufBlockList;
} SMemTable;

// ------------------ tsdbFile.c
typedef enum { TSDB_FILE_TYPE_HEAD = 0, TSDB_FILE_TYPE_DATA, TSDB_FILE_TYPE_LAST, TSDB_FILE_TYPE_MAX } TSDB_FILE_TYPE;

typedef struct {
} STsdbFileInfo;

typedef struct {
  char*    fname;
  int      fd;
  uint64_t size;
  uint64_t tombSize;
  uint64_t totalBlocks;
  uint64_t totalSubBlocks;
} SFile;

typedef struct {
  int fileId;
  SFile headF;
  SFile dataF;
  SFile lastF;
} SFileGroup;

typedef struct {
  int         maxFGroups;
  int         nFGroups;
  SFileGroup* pFGroup;
} STsdbFileH;

typedef struct {
  int         numOfFGroups;
  SFileGroup *base;
  SFileGroup *pFileGroup;
  int         direction;
} SFileGroupIter;

// ------------------ tsdbRWHelper.c
typedef struct {
  uint32_t len;
  uint32_t offset;
  uint32_t padding;
  uint32_t hasLast : 2;
  uint32_t numOfBlocks : 30;
  uint64_t uid;
  TSKEY    maxKey;
} SCompIdx;

typedef struct {
  int64_t last : 1;
  int64_t offset : 63;
  int32_t algorithm : 8;
  int32_t numOfRows : 24;
  int32_t sversion;
  int32_t len;
  int16_t numOfSubBlocks;
  int16_t numOfCols;
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SCompBlock;

typedef struct {
  int32_t    delimiter;  // For recovery usage
  int32_t    checksum;   // TODO: decide if checksum logic in this file or make it one API
  uint64_t   uid;
  SCompBlock blocks[];
} SCompInfo;

typedef struct {
  int16_t colId;
  int16_t len;
  int32_t type : 8;
  int32_t offset : 24;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  char    padding[2];
} SCompCol;

typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  numOfCols;  // For recovery usage
  uint64_t uid;        // For recovery usage
  SCompCol cols[];
} SCompData;

typedef enum { TSDB_WRITE_HELPER, TSDB_READ_HELPER } tsdb_rw_helper_t;

typedef struct {
  tsdb_rw_helper_t type;  // helper type

  int    maxTables;
  int    maxRowSize;
  int    maxRows;
  int    maxCols;
  int    minRowsPerFileBlock;
  int    maxRowsPerFileBlock;
  int8_t compress;
} SHelperCfg;

typedef struct {
  int fid;
  TSKEY minKey;
  TSKEY maxKey;
  // For read/write purpose
  SFile headF;
  SFile dataF;
  SFile lastF;
  // For write purpose only
  SFile nHeadF;
  SFile nLastF;
} SHelperFile;

typedef struct {
  uint64_t uid;
  int32_t  tid;
  int32_t  sversion;
} SHelperTable;

typedef struct {
  // Global configuration
  SHelperCfg config;

  int8_t state;

  // For file set usage
  SHelperFile files;
  SCompIdx *  pCompIdx;

  // For table set usage
  SHelperTable tableInfo;
  SCompInfo *  pCompInfo;
  bool         hasOldLastBlock;

  // For block set usage
  SCompData *pCompData;
  SDataCols *pDataCols[2];

  void *pBuffer;  // Buffer to hold the whole data block
  void *compBuffer;   // Buffer for temperary compress/decompress purpose
} SRWHelper;

// ------------------ tsdbMain.c
typedef struct {
  int8_t          state;

  char*           rootDir;
  STsdbCfg        config;
  STsdbAppH       appH;
  STsdbStat       stat;
  STsdbMeta*      tsdbMeta;
  STsdbBufPool*   pPool;
  SMemTable*      mem;
  SMemTable*      imem;
  STsdbFileH*     tsdbFileH;
  pthread_mutex_t mutex;
  int             commit;
  pthread_t       commitThread;
} STsdbRepo;

// Operations
// ------------------ tsdbMeta.c
#define TABLE_TYPE(t) (t)->type
#define TABLE_NAME(t) (t)->name
#define TABLE_CHAR_NAME(t) TABLE_NAME(t)->data
#define TALBE_UID(t) (t)->tableId.uid
#define TABLE_TID(t) (t)->tableId.tid
#define TABLE_SUID(t) (t)->superUid
#define TABLE_LASTKEY(t) (t)->lastKey

STsdbMeta* tsdbNewMeta(STsdbCfg* pCfg);
void       tsdbFreeMeta(STsdbMeta* pMeta);

// ------------------ tsdbBuffer.c
STsdbBufPool* tsdbNewBufPool();
void          tsdbFreeBufPool(STsdbBufPool* pBufPool);
int           tsdbOpenBufPool(STsdbRepo* pRepo);
int           tsdbOpenBufPool(STsdbRepo* pRepo);
SListNode*    tsdbAllocBufBlockFromPool(STsdbRepo* pRepo);

// ------------------ tsdbMemTable.c

// ------------------ tsdbFile.c
#define TSDB_KEY_FILEID(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
#define TSDB_MAX_FILE(keep, daysPerFile) ((keep) / (daysPerFile) + 3)
#define TSDB_MIN_FILE_ID(fh) (fh)->pFGroup[0].fileId
#define TSDB_MAX_FILE_ID(fh) (fh)->pFGroup[(fh)->nFGroups - 1].fileId
#define TSDB_FGROUP_ITER_FORWARD TSDB_ORDER_ASC
#define TSDB_FGROUP_ITER_BACKWARD TSDB_ORDER_DESC

STsdbFileH*   tsdbNewFileH(STsdbCfg* pCfg);
void          tsdbFreeFileH(STsdbFileH* pFileH);

// ------------------ tsdbRWHelper.c
#define TSDB_MAX_SUBBLOCKS 8
#define IS_SUB_BLOCK(pBlock) ((pBlock)->numOfSubBlocks == 0)

// ------------------ tsdbMain.c
#define REPO_ID(r) (r)->config.tsdbId

char* tsdbGetMetaFileName(char* rootDir);
int   tsdbLockRepo(STsdbRepo* pRepo);
int   tsdbUnlockRepo(STsdbRepo* pRepo);
void* tsdbCommitData(void* arg);

#if 0


// TSDB repository definition

typedef struct {
  int32_t  totalLen;
  int32_t  len;
  SDataRow row;
} SSubmitBlkIter;

// SSubmitMsg Iterator
typedef struct {
  int32_t     totalLen;
  int32_t     len;
  SSubmitBlk *pBlock;
} SSubmitMsgIter;

// --------- Helper state
#define TSDB_HELPER_CLEAR_STATE 0x0        // Clear state
#define TSDB_HELPER_FILE_SET_AND_OPEN 0x1  // File is set
#define TSDB_HELPER_IDX_LOAD 0x2           // SCompIdx part is loaded
#define TSDB_HELPER_TABLE_SET 0x4          // Table is set
#define TSDB_HELPER_INFO_LOAD 0x8          // SCompInfo part is loaded
#define TSDB_HELPER_FILE_DATA_LOAD 0x10    // SCompData part is loaded

#define TSDB_HELPER_TYPE(h) ((h)->config.type)

#define helperSetState(h, s) (((h)->state) |= (s))
#define helperClearState(h, s) ((h)->state &= (~(s)))
#define helperHasState(h, s) ((((h)->state) & (s)) == (s))
#define blockAtIdx(h, idx) ((h)->pCompInfo->blocks + idx)

int  tsdbInitReadHelper(SRWHelper *pHelper, STsdbRepo *pRepo);
int  tsdbInitWriteHelper(SRWHelper *pHelper, STsdbRepo *pRepo);
void tsdbDestroyHelper(SRWHelper *pHelper);
void tsdbResetHelper(SRWHelper *pHelper);

// --------- For set operations
int tsdbSetAndOpenHelperFile(SRWHelper *pHelper, SFileGroup *pGroup);
void tsdbSetHelperTable(SRWHelper *pHelper, STable *pTable, STsdbRepo *pRepo);
int  tsdbCloseHelperFile(SRWHelper *pHelper, bool hasError);

// --------- For read operations
int  tsdbLoadCompIdx(SRWHelper *pHelper, void *target);
int  tsdbLoadCompInfo(SRWHelper *pHelper, void *target);
int  tsdbLoadCompData(SRWHelper *pHelper, SCompBlock *pCompBlock, void *target);
int  tsdbLoadBlockDataCols(SRWHelper *pHelper, SDataCols *pDataCols, int blkIdx, int16_t *colIds, int numOfColIds);
int  tsdbLoadBlockData(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *target);
void tsdbGetDataStatis(SRWHelper *pHelper, SDataStatis *pStatis, int numOfCols);

// --------- For write operations
int tsdbWriteDataBlock(SRWHelper *pHelper, SDataCols *pDataCols);
int tsdbMoveLastBlockIfNeccessary(SRWHelper *pHelper);
int tsdbWriteCompInfo(SRWHelper *pHelper);
int tsdbWriteCompIdx(SRWHelper *pHelper);

// --------- Other functions need to further organize
void      tsdbFitRetention(STsdbRepo *pRepo);
int       tsdbAlterCacheTotalBlocks(STsdbRepo *pRepo, int totalBlocks);
void      tsdbAdjustCacheBlocks(STsdbCache *pCache);
int32_t   tsdbGetMetaFileName(char *rootDir, char *fname);
int       tsdbUpdateFileHeader(SFile *pFile, uint32_t version);
int       tsdbUpdateTable(STsdbMeta *pMeta, STable *pTable, STableCfg *pCfg);
int       tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable);
int       tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable);
STSchema *tsdbGetTableSchemaByVersion(STsdbMeta *pMeta, STable *pTable, int16_t version);
STSchema *tsdbGetTableSchema(STsdbMeta *pMeta, STable *pTable);

#define DEFAULT_TAG_INDEX_COLUMN 0  // skip list built based on the first column of tags

int compFGroupKey(const void *key, const void *fgroup);
#endif

#ifdef __cplusplus
}
#endif

#endif