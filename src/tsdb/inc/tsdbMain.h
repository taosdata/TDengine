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

#include "tglobal.h"
#include "tlist.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tutil.h"
#include "tlog.h"
#include "tcoding.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int tsdbDebugFlag;

#define tsdbError(...)                                      \
  if (tsdbDebugFlag & DEBUG_ERROR) {                        \
    taosPrintLog("ERROR TDB ", tsdbDebugFlag, __VA_ARGS__); \
  }
#define tsdbWarn(...)                                      \
  if (tsdbDebugFlag & DEBUG_WARN) {                        \
    taosPrintLog("WARN TDB ", tsdbDebugFlag, __VA_ARGS__); \
  }
#define tsdbTrace(...)                                \
  if (tsdbDebugFlag & DEBUG_TRACE) {                  \
    taosPrintLog("TDB ", tsdbDebugFlag, __VA_ARGS__); \
  }
#define tsdbPrint(...) \
  { taosPrintLog("TDB ", 255, __VA_ARGS__); }

// ------------------------------ TSDB META FILE INTERFACES ------------------------------
#define TSDB_META_FILE_NAME "meta"
#define TSDB_META_HASH_FRACTION 1.1

typedef int (*iterFunc)(void *, void *cont, int contLen);
typedef void (*afterFunc)(void *);

typedef struct {
  int       fd;        // File descriptor
  int       nDel;      // number of deletions
  int       tombSize;  // deleted size
  int64_t   size;      // Total file size
  void *    map;       // Map from uid ==> position
  iterFunc  iFunc;
  afterFunc aFunc;
  void *    appH;
} SMetaFile;

SMetaFile *tsdbInitMetaFile(char *rootDir, int32_t maxTables, iterFunc iFunc, afterFunc aFunc, void *appH);
int32_t    tsdbInsertMetaRecord(SMetaFile *mfh, uint64_t uid, void *cont, int32_t contLen);
int32_t    tsdbDeleteMetaRecord(SMetaFile *mfh, uint64_t uid);
int32_t    tsdbUpdateMetaRecord(SMetaFile *mfh, uint64_t uid, void *cont, int32_t contLen);
void       tsdbCloseMetaFile(SMetaFile *mfh);

// ------------------------------ TSDB META INTERFACES ------------------------------
#define IS_CREATE_STABLE(pCfg) ((pCfg)->tagValues != NULL)

typedef struct {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int32_t numOfRows;
  void *  pData;
} SMemTable;

// ---------- TSDB TABLE DEFINITION
typedef struct STable {
  int8_t         type;
  STableId       tableId;
  uint64_t       superUid;  // Super table UID
  int32_t        sversion;
  STSchema *     schema;
  STSchema *     tagSchema;
  SDataRow       tagVal;
  SMemTable *    mem;
  SMemTable *    imem;
  void *         pIndex;         // For TSDB_SUPER_TABLE, it is the skiplist index
  void *         eventHandler;   // TODO
  void *         streamHandler;  // TODO
  TSKEY          lastKey;        // lastkey inserted in this table, initialized as 0, TODO: make a structure
  struct STable *next;           // TODO: remove the next
  struct STable *prev;
  tstr *         name;           // NOTE: there a flexible string here
} STable;

#define TSDB_GET_TABLE_LAST_KEY(tb) ((tb)->lastKey)

void *  tsdbEncodeTable(STable *pTable, int *contLen);
STable *tsdbDecodeTable(void *cont, int contLen);
void    tsdbFreeEncode(void *cont);

// ---------- TSDB META HANDLE DEFINITION
typedef struct {
  int32_t maxTables;  // Max number of tables

  int32_t nTables;  // Tables created

  STable **tables;  // table array

  STable *superList;  // super table list TODO: change  it to list container

  void *map;  // table map of (uid ===> table)

  SMetaFile *mfh;  // meta file handle
  int        maxRowBytes;
  int        maxCols;
} STsdbMeta;

// element put in skiplist for each table
typedef struct STableIndexElem {
  STsdbMeta* pMeta;
  STable*    pTable;
} STableIndexElem;

STsdbMeta *tsdbInitMeta(char *rootDir, int32_t maxTables);
int32_t    tsdbFreeMeta(STsdbMeta *pMeta);
STSchema * tsdbGetTableSchema(STsdbMeta *pMeta, STable *pTable);
STSchema * tsdbGetTableTagSchema(STsdbMeta *pMeta, STable *pTable);

// ---- Operation on STable
#define TSDB_TABLE_ID(pTable) ((pTable)->tableId)
#define TSDB_TABLE_UID(pTable) ((pTable)->uid)
#define TSDB_TABLE_NAME(pTable) ((pTable)->tableName)
#define TSDB_TABLE_TYPE(pTable) ((pTable)->type)
#define TSDB_TABLE_SUPER_TABLE_UID(pTable) ((pTable)->stableUid)
#define TSDB_TABLE_IS_SUPER_TABLE(pTable) (TSDB_TABLE_TYPE(pTable) == TSDB_SUPER_TABLE)
#define TSDB_TABLE_TAG_VALUE(pTable) ((pTable)->pTagVal)
#define TSDB_TABLE_CACHE_DATA(pTable) ((pTable)->content.pData)
#define TSDB_SUPER_TABLE_INDEX(pTable) ((pTable)->content.pIndex)

// ---- Operation on SMetaHandle
#define TSDB_NUM_OF_TABLES(pHandle) ((pHandle)->numOfTables)
#define TSDB_NUM_OF_SUPER_TABLES(pHandle) ((pHandle)->numOfSuperTables)
#define TSDB_TABLE_OF_ID(pHandle, id) ((pHandle)->pTables)[id]
#define TSDB_GET_TABLE_OF_NAME(pHandle, name) /* TODO */

STsdbMeta *tsdbGetMeta(TsdbRepoT *pRepo);

STable *tsdbIsValidTableToInsert(STsdbMeta *pMeta, STableId tableId);
// int32_t tsdbInsertRowToTableImpl(SSkipListNode *pNode, STable *pTable);
STable *tsdbGetTableByUid(STsdbMeta *pMeta, uint64_t uid);
char *getTSTupleKey(const void * data);

typedef struct {
  int  blockId;
  int  offset;
  int  remain;
  int  padding;
  char data[];
} STsdbCacheBlock;

typedef struct {
  int64_t index;
  int     numOfCacheBlocks;
  SList * memPool;
} STsdbCachePool;

typedef struct {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int64_t numOfRows;
  SList * list;
} SCacheMem;

typedef struct {
  int              cacheBlockSize;
  int              totalCacheBlocks;
  STsdbCachePool   pool;
  STsdbCacheBlock *curBlock;
  SCacheMem *      mem;
  SCacheMem *      imem;
  TsdbRepoT *      pRepo;
} STsdbCache;

STsdbCache *tsdbInitCache(int cacheBlockSize, int totalBlocks, TsdbRepoT *pRepo);
void        tsdbFreeCache(STsdbCache *pCache);
void *      tsdbAllocFromCache(STsdbCache *pCache, int bytes, TSKEY key);

// ------------------------------ TSDB FILE INTERFACES ------------------------------
#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F

#define tsdbGetKeyFileId(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
#define tsdbGetMaxNumOfFiles(keep, daysPerFile) ((keep) / (daysPerFile) + 3)

typedef enum {
  TSDB_FILE_TYPE_HEAD = 0,  // .head file type
  TSDB_FILE_TYPE_DATA,      // .data file type
  TSDB_FILE_TYPE_LAST,      // .last file type
  TSDB_FILE_TYPE_MAX
} TSDB_FILE_TYPE;

#define IS_VALID_TSDB_FILE_TYPE(type) ((type) >= TSDB_FILE_TYPE_HEAD && (type) < TSDB_FILE_TYPE_MAX)

extern const char *tsdbFileSuffix[];

typedef struct {
  uint32_t offset;
  uint32_t len;
  uint64_t size;      // total size of the file
  uint64_t tombSize;  // unused file size
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
} STsdbFileInfo;

void *tsdbEncodeSFileInfo(void *buf, const STsdbFileInfo *pInfo);
void *tsdbDecodeSFileInfo(void *buf, STsdbFileInfo *pInfo);

typedef struct {
  int       fd;
  char      fname[128];
  STsdbFileInfo info;
} SFile;

#define TSDB_IS_FILE_OPENED(f) ((f)->fd != -1)

typedef struct {
  int32_t fileId;
  SFile   files[TSDB_FILE_TYPE_MAX];
} SFileGroup;

// TSDB file handle
typedef struct {
  int maxFGroups;
  int numOfFGroups;

  SFileGroup *fGroup;
} STsdbFileH;

#define TSDB_MIN_FILE_ID(fh) (fh)->fGroup[0].fileId
#define TSDB_MAX_FILE_ID(fh) (fh)->fGroup[(fh)->numOfFGroups - 1].fileId

STsdbFileH *tsdbInitFileH(char *dataDir, STsdbCfg *pCfg);
void        tsdbCloseFileH(STsdbFileH *pFileH);
int         tsdbCreateFile(char *dataDir, int fileId, const char *suffix, SFile *pFile);
SFileGroup *tsdbCreateFGroup(STsdbFileH *pFileH, char *dataDir, int fid, int maxTables);
int         tsdbOpenFile(SFile *pFile, int oflag);
int         tsdbCloseFile(SFile *pFile);
SFileGroup *tsdbOpenFilesForCommit(STsdbFileH *pFileH, int fid);
int         tsdbRemoveFileGroup(STsdbFileH *pFile, int fid);
int         tsdbGetFileName(char *dataDir, int fileId, const char *suffix, char *fname);

#define TSDB_FGROUP_ITER_FORWARD TSDB_ORDER_ASC
#define TSDB_FGROUP_ITER_BACKWARD TSDB_ORDER_DESC

typedef struct {
  int         numOfFGroups;
  SFileGroup *base;
  SFileGroup *pFileGroup;
  int         direction;
} SFileGroupIter;

void        tsdbInitFileGroupIter(STsdbFileH *pFileH, SFileGroupIter *pIter, int direction);
void        tsdbSeekFileGroupIter(SFileGroupIter *pIter, int fid);
SFileGroup *tsdbGetFileGroupNext(SFileGroupIter *pIter);

typedef struct {
  uint32_t len;
  uint32_t offset;
  uint32_t padding;  // For padding purpose
  uint32_t hasLast : 2;
  uint32_t numOfBlocks : 30;
  uint64_t uid;
  TSKEY    maxKey;
} SCompIdx; /* sizeof(SCompIdx) = 28 */

void *tsdbEncodeSCompIdx(void *buf, SCompIdx *pIdx);
void *tsdbDecodeSCompIdx(void *buf, SCompIdx *pIdx);

/**
 * if numOfSubBlocks == 0, then the SCompBlock is a sub-block
 * if numOfSubBlocks >= 1, then the SCompBlock is a super-block
 *    - if numOfSubBlocks == 1, then the SCompBlock refers to the data block, and offset/len refer to
 *      the data block offset and length
 *    - if numOfSubBlocks > 1, then the offset/len refer to the offset of the first sub-block in the
 *      binary
 */
typedef struct {
  int64_t last : 1;          // If the block in data file or last file
  int64_t offset : 63;       // Offset of data block or sub-block index depending on numOfSubBlocks
  int32_t algorithm : 8;     // Compression algorithm
  int32_t numOfRows : 24;    // Number of total points
  int32_t sversion;          // Schema version
  int32_t len;               // Data block length or nothing
  int16_t numOfSubBlocks;    // Number of sub-blocks;
  int16_t numOfCols;
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SCompBlock;

// Maximum number of sub-blocks a super-block can have
#define TSDB_MAX_SUBBLOCKS 8
#define IS_SUPER_BLOCK(pBlock) ((pBlock)->numOfSubBlocks >= 1)
#define IS_SUB_BLOCK(pBlock) ((pBlock)->numOfSubBlocks == 0)

typedef struct {
  int32_t    delimiter;  // For recovery usage
  int32_t    checksum;   // TODO: decide if checksum logic in this file or make it one API
  uint64_t   uid;
  SCompBlock blocks[];
} SCompInfo;

#define TSDB_COMPBLOCK_AT(pCompInfo, idx) ((pCompInfo)->blocks + (idx))
#define TSDB_COMPBLOCK_GET_START_AND_SIZE(pCompInfo, pCompBlock, size) \
  do {                                                                 \
    if (pCompBlock->numOfSubBlocks > 1) {                              \
      pCompBlock = pCompInfo->blocks + pCompBlock->offset;             \
      size = pCompBlock->numOfSubBlocks;                               \
    } else {                                                           \
      size = 1;                                                        \
    }                                                                  \
  } while (0)

// TODO: take pre-calculation into account
typedef struct {
  int16_t colId;  // Column ID
  int16_t len;    // Column length // TODO: int16_t is not enough
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

// TODO: Take recover into account
typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  numOfCols;  // For recovery usage
  uint64_t uid;        // For recovery usage
  SCompCol cols[];
} SCompData;

STsdbFileH *tsdbGetFile(TsdbRepoT *pRepo);

int         tsdbCopyBlockDataInFile(SFile *pOutFile, SFile *pInFile, SCompInfo *pCompInfo, int idx, int isLast,
                                    SDataCols *pCols);
SFileGroup *tsdbSearchFGroup(STsdbFileH *pFileH, int fid);
void tsdbGetKeyRangeOfFileId(int32_t daysPerFile, int8_t precision, int32_t fileId, TSKEY *minKey, TSKEY *maxKey);

// TSDB repository definition
typedef struct STsdbRepo {
  char *rootDir;
  // TSDB configuration
  STsdbCfg config;

  STsdbAppH appH;

  // The meter meta handle of this TSDB repository
  STsdbMeta *tsdbMeta;

  // The cache Handle
  STsdbCache *tsdbCache;

  // The TSDB file handle
  STsdbFileH *tsdbFileH;

  // Disk tier handle for multi-tier storage
  void *diskTier;

  pthread_mutex_t mutex;

  int       commit;
  pthread_t commitThread;

  // A limiter to monitor the resources used by tsdb
  void *limiter;

  int8_t state;

} STsdbRepo;

typedef struct {
  int32_t  totalLen;
  int32_t  len;
  SDataRow row;
} SSubmitBlkIter;

int      tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter);
SDataRow tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter);

#define TSDB_SUBMIT_MSG_HEAD_SIZE sizeof(SSubmitMsg)

// SSubmitMsg Iterator
typedef struct {
  int32_t     totalLen;
  int32_t     len;
  SSubmitBlk *pBlock;
} SSubmitMsgIter;

int         tsdbInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter);
SSubmitBlk *tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter);

int32_t tsdbTriggerCommit(TsdbRepoT *repo);
int32_t tsdbLockRepo(TsdbRepoT *repo);
int32_t tsdbUnLockRepo(TsdbRepoT *repo);

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
void    tsdbFitRetention(STsdbRepo *pRepo);
int     tsdbAlterCacheTotalBlocks(STsdbRepo *pRepo, int totalBlocks);
void    tsdbAdjustCacheBlocks(STsdbCache *pCache);
int32_t tsdbGetMetaFileName(char *rootDir, char *fname);
int     tsdbUpdateFileHeader(SFile *pFile, uint32_t version);

#ifdef __cplusplus
}
#endif

#endif