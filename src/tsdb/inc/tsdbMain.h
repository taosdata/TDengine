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

#ifdef __cplusplus
extern "C" {
#endif

extern int tsdbDebugFlag;

#define tsdbFatal(...) { if (tsdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TDB FATAL ", 255, __VA_ARGS__); }}
#define tsdbError(...) { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TDB ERROR ", 255, __VA_ARGS__); }}
#define tsdbWarn(...)  { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TDB WARN ", 255, __VA_ARGS__); }}
#define tsdbInfo(...)  { if (tsdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TDB ", 255, __VA_ARGS__); }}
#define tsdbDebug(...) { if (tsdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TDB ", tsdbDebugFlag, __VA_ARGS__); }}
#define tsdbTrace(...) { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TDB ", tsdbDebugFlag, __VA_ARGS__); }}

#define TSDB_MAX_TABLE_SCHEMAS 16
#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC 0xFFFFFFFF

#define TAOS_IN_RANGE(key, keyMin, keyLast) (((key) >= (keyMin)) && ((key) <= (keyMax)))

// NOTE: Any file format change must increase this version number by 1
//       Also, implement the convert function
#define TSDB_FILE_VERSION ((uint32_t)0)

// Definitions
// ------------------ tsdbMeta.c
typedef struct STable {
  STableId       tableId;
  ETableType     type;
  tstr*          name;  // NOTE: there a flexible string here
  uint64_t       suid;
  struct STable* pSuper;  // super table pointer
  uint8_t        numOfSchemas;
  STSchema*      schema[TSDB_MAX_TABLE_SCHEMAS];
  STSchema*      tagSchema;
  SKVRow         tagVal;
  SSkipList*     pIndex;         // For TSDB_SUPER_TABLE, it is the skiplist index
  void*          eventHandler;   // TODO
  void*          streamHandler;  // TODO
  TSKEY          lastKey;        // lastkey inserted in this table, initialized as 0, TODO: make a structure
  char*          sql;
  void*          cqhandle;
  SRWLatch       latch;  // TODO: implementa latch functions
  T_REF_DECLARE()
} STable;

typedef struct {
  pthread_rwlock_t rwLock;

  int32_t   nTables;
  int32_t   maxTables;
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
  STable *           pTable;
  SSkipListIterator *pIter;
} SCommitIter;

typedef struct {
  uint64_t   uid;
  TSKEY      keyFirst;
  TSKEY      keyLast;
  int64_t    numOfRows;
  SSkipList* pData;
} STableData;

typedef struct {
  T_REF_DECLARE()
  SRWLatch     latch;
  TSKEY        keyFirst;
  TSKEY        keyLast;
  int64_t      numOfRows;
  int32_t      maxTables;
  STableData** tData;
  SList*       actList;
  SList*       extraBuffList;
  SList*       bufBlockList;
} SMemTable;

enum { TSDB_UPDATE_META, TSDB_DROP_META };

#ifdef WINDOWS
#pragma pack(push ,1) 
typedef struct {
#else
typedef struct __attribute__((packed)){
#endif
  char     act;
  uint64_t uid;
} SActObj;
#ifdef WINDOWS
#pragma pack(pop) 
#endif

typedef struct {
  int  len;
  char cont[];
} SActCont;

// ------------------ tsdbFile.c
extern const char* tsdbFileSuffix[];
typedef enum {
  TSDB_FILE_TYPE_HEAD = 0,
  TSDB_FILE_TYPE_DATA,
  TSDB_FILE_TYPE_LAST,
  TSDB_FILE_TYPE_STAT,
  TSDB_FILE_TYPE_NHEAD,
  TSDB_FILE_TYPE_NDATA,
  TSDB_FILE_TYPE_NLAST,
  TSDB_FILE_TYPE_NSTAT
} TSDB_FILE_TYPE;

#ifndef TDINTERNAL
#define TSDB_FILE_TYPE_MAX (TSDB_FILE_TYPE_LAST+1)
#else
#define TSDB_FILE_TYPE_MAX (TSDB_FILE_TYPE_STAT+1)
#endif

typedef struct {
  uint32_t magic;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;      // total size of the file
  uint64_t tombSize;  // unused file size
} STsdbFileInfo;

typedef struct {
  char  fname[TSDB_FILENAME_LEN];
  int   fd;

  STsdbFileInfo info;
} SFile;

typedef struct {
  int   fileId;
  int   state; // 0 for health, 1 for problem
  SFile files[TSDB_FILE_TYPE_MAX];
} SFileGroup;

typedef struct {
  pthread_rwlock_t fhlock;

  int         maxFGroups;
  int         nFGroups;
  SFileGroup* pFGroup;
} STsdbFileH;

typedef struct {
  int         direction;
  STsdbFileH* pFileH;
  int         fileId;
  int         index;
} SFileGroupIter;

// ------------------ tsdbMain.c
typedef struct {
  int8_t state;

  char*           rootDir;
  STsdbCfg        config;
  STsdbAppH       appH;
  STsdbStat       stat;
  STsdbMeta*      tsdbMeta;
  STsdbBufPool*   pPool;
  SMemTable*      mem;
  SMemTable*      imem;
  STsdbFileH*     tsdbFileH;
  sem_t           readyToCommit;
  pthread_mutex_t mutex;
  bool            repoLocked;
} STsdbRepo;

// ------------------ tsdbRWHelper.c
typedef struct {
  int32_t  tid;
  uint32_t len;
  uint32_t offset;
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
  int32_t len;
  int32_t keyLen;     // key column length, keyOffset = offset+sizeof(SCompData)+sizeof(SCompCol)*numOfCols
  int16_t numOfSubBlocks;
  int16_t numOfCols; // not including timestamp column
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SCompBlock;

typedef struct {
  int32_t    delimiter;  // For recovery usage
  int32_t    tid;
  uint64_t   uid;
  SCompBlock blocks[];
} SCompInfo;

typedef struct {
  int16_t colId;
  int32_t len;
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
  TSKEY      minKey;
  TSKEY      maxKey;
  SFileGroup fGroup;
  SFile      nHeadF;
  SFile      nLastF;
} SHelperFile;

typedef struct {
  uint64_t uid;
  int32_t  tid;
} SHelperTable;

typedef struct {
  SCompIdx* pIdxArray;
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
  SCompIdx    curCompIdx;
  void*       pWIdx;
  // For table set usage
  SHelperTable tableInfo;
  SCompInfo*   pCompInfo;
  bool         hasOldLastBlock;
  // For block set usage
  SCompData* pCompData;
  SDataCols* pDataCols[2];
  void*      pBuffer;     // Buffer to hold the whole data block
  void*      compBuffer;  // Buffer for temperary compress/decompress purpose
} SRWHelper;

typedef struct {
  int   rowsInserted;
  int   rowsUpdated;
  int   rowsDeleteSucceed;
  int   rowsDeleteFailed;
  int   nOperations;
  TSKEY keyFirst;
  TSKEY keyLast;
} SMergeInfo;
// ------------------ tsdbScan.c
typedef struct {
  SFileGroup fGroup;
  int        numOfIdx;
  SCompIdx*  pCompIdx;
  SCompInfo* pCompInfo;
  void*      pBuf;
  FILE*      tLogStream;
} STsdbScanHandle;

// Operations
// ------------------ tsdbMeta.c
#define TSDB_INIT_NTABLES 1024
#define TABLE_TYPE(t) (t)->type
#define TABLE_NAME(t) (t)->name
#define TABLE_CHAR_NAME(t) TABLE_NAME(t)->data
#define TABLE_UID(t) (t)->tableId.uid
#define TABLE_TID(t) (t)->tableId.tid
#define TABLE_SUID(t) (t)->suid
#define TABLE_LASTKEY(t) (t)->lastKey
#define TSDB_META_FILE_MAGIC(m) KVSTORE_MAGIC((m)->pStore)

STsdbMeta* tsdbNewMeta(STsdbCfg* pCfg);
void       tsdbFreeMeta(STsdbMeta* pMeta);
int        tsdbOpenMeta(STsdbRepo* pRepo);
int        tsdbCloseMeta(STsdbRepo* pRepo);
STable*    tsdbGetTableByUid(STsdbMeta* pMeta, uint64_t uid);
STSchema*  tsdbGetTableSchemaByVersion(STable* pTable, int16_t version);
int        tsdbWLockRepoMeta(STsdbRepo* pRepo);
int        tsdbRLockRepoMeta(STsdbRepo* pRepo);
int        tsdbUnlockRepoMeta(STsdbRepo* pRepo);
void       tsdbRefTable(STable* pTable);
void       tsdbUnRefTable(STable* pTable);
void       tsdbUpdateTableSchema(STsdbRepo* pRepo, STable* pTable, STSchema* pSchema, bool insertAct);

static FORCE_INLINE int tsdbCompareSchemaVersion(const void *key1, const void *key2) {
  if (*(int16_t *)key1 < schemaVersion(*(STSchema **)key2)) {
    return -1;
  } else if (*(int16_t *)key1 > schemaVersion(*(STSchema **)key2)) {
    return 1;
  } else {
    return 0;
  }
}

static FORCE_INLINE STSchema* tsdbGetTableSchemaImpl(STable* pTable, bool lock, bool copy, int16_t version) {
  STable*   pDTable = (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) ? pTable->pSuper : pTable;
  STSchema* pSchema = NULL;
  STSchema* pTSchema = NULL;

  if (lock) taosRLockLatch(&(pDTable->latch));
  if (version < 0) {  // get the latest version of schema
    pTSchema = pDTable->schema[pDTable->numOfSchemas - 1];
  } else {  // get the schema with version
    void* ptr = taosbsearch(&version, pDTable->schema, pDTable->numOfSchemas, sizeof(STSchema*),
                            tsdbCompareSchemaVersion, TD_EQ);
    if (ptr == NULL) {
      terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
      goto _exit;
    }
    pTSchema = *(STSchema**)ptr;
  }

  ASSERT(pTSchema != NULL);

  if (copy) {
    if ((pSchema = tdDupSchema(pTSchema)) == NULL) terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  } else {
    pSchema = pTSchema;
  }

_exit:
  if (lock) taosRUnLockLatch(&(pDTable->latch));
  return pSchema;
}

static FORCE_INLINE STSchema* tsdbGetTableSchema(STable* pTable) {
  return tsdbGetTableSchemaImpl(pTable, false, false, -1);
}

static FORCE_INLINE STSchema *tsdbGetTableTagSchema(STable *pTable) {
  if (pTable->type == TSDB_CHILD_TABLE) {  // check child table first
    STable *pSuper = pTable->pSuper;
    if (pSuper == NULL) return NULL;
    return pSuper->tagSchema;
  } else if (pTable->type == TSDB_SUPER_TABLE) {
    return pTable->tagSchema;
  } else {
    return NULL;
  }
}

// ------------------ tsdbBuffer.c
#define TSDB_BUFFER_RESERVE 1024  // Reseve 1K as commit threshold

STsdbBufPool* tsdbNewBufPool();
void          tsdbFreeBufPool(STsdbBufPool* pBufPool);
int           tsdbOpenBufPool(STsdbRepo* pRepo);
void          tsdbCloseBufPool(STsdbRepo* pRepo);
SListNode*    tsdbAllocBufBlockFromPool(STsdbRepo* pRepo);

// ------------------ tsdbMemTable.c
int   tsdbUpdateRowInMem(STsdbRepo* pRepo, SDataRow row, STable* pTable);
int   tsdbRefMemTable(STsdbRepo* pRepo, SMemTable* pMemTable);
int   tsdbUnRefMemTable(STsdbRepo* pRepo, SMemTable* pMemTable);
int   tsdbTakeMemSnapshot(STsdbRepo* pRepo, SMemTable** pMem, SMemTable** pIMem);
void  tsdbUnTakeMemSnapShot(STsdbRepo* pRepo, SMemTable* pMem, SMemTable* pIMem);
void* tsdbAllocBytes(STsdbRepo* pRepo, int bytes);
int   tsdbAsyncCommit(STsdbRepo* pRepo);
int   tsdbLoadDataFromCache(STable* pTable, SSkipListIterator* pIter, TSKEY maxKey, int maxRowsToRead, SDataCols* pCols,
                            TKEY* filterKeys, int nFilterKeys, bool keepDup, SMergeInfo* pMergeInfo);
void* tsdbCommitData(STsdbRepo* pRepo);

static FORCE_INLINE SDataRow tsdbNextIterRow(SSkipListIterator* pIter) {
  if (pIter == NULL) return NULL;

  SSkipListNode* node = tSkipListIterGet(pIter);
  if (node == NULL) return NULL;

  return (SDataRow)SL_GET_NODE_DATA(node);
}

static FORCE_INLINE TSKEY tsdbNextIterKey(SSkipListIterator* pIter) {
  SDataRow row = tsdbNextIterRow(pIter);
  if (row == NULL) return TSDB_DATA_TIMESTAMP_NULL;

  return dataRowKey(row);
}

static FORCE_INLINE TKEY tsdbNextIterTKey(SSkipListIterator* pIter) {
  SDataRow row = tsdbNextIterRow(pIter);
  if (row == NULL) return TKEY_NULL;

  return dataRowTKey(row);
}

static FORCE_INLINE STsdbBufBlock* tsdbGetCurrBufBlock(STsdbRepo* pRepo) {
  ASSERT(pRepo != NULL);
  if (pRepo->mem == NULL) return NULL;

  SListNode* pNode = listTail(pRepo->mem->bufBlockList);
  if (pNode == NULL) return NULL;

  STsdbBufBlock* pBufBlock = NULL;
  tdListNodeGetData(pRepo->mem->bufBlockList, pNode, (void*)(&pBufBlock));

  return pBufBlock;
}

// ------------------ tsdbFile.c
#define TSDB_KEY_FILEID(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
#define TSDB_MAX_FILE(keep, daysPerFile) ((keep) / (daysPerFile) + 3)
#define TSDB_MIN_FILE_ID(fh) (fh)->pFGroup[0].fileId
#define TSDB_MAX_FILE_ID(fh) (fh)->pFGroup[(fh)->nFGroups - 1].fileId
#define TSDB_IS_FILE_OPENED(f) ((f)->fd > 0)
#define TSDB_FGROUP_ITER_FORWARD TSDB_ORDER_ASC
#define TSDB_FGROUP_ITER_BACKWARD TSDB_ORDER_DESC

STsdbFileH* tsdbNewFileH(STsdbCfg* pCfg);
void        tsdbFreeFileH(STsdbFileH* pFileH);
int         tsdbOpenFileH(STsdbRepo* pRepo);
void        tsdbCloseFileH(STsdbRepo* pRepo);
SFileGroup* tsdbCreateFGroupIfNeed(STsdbRepo* pRepo, char* dataDir, int fid);
void        tsdbInitFileGroupIter(STsdbFileH* pFileH, SFileGroupIter* pIter, int direction);
void        tsdbSeekFileGroupIter(SFileGroupIter* pIter, int fid);
SFileGroup* tsdbGetFileGroupNext(SFileGroupIter* pIter);
int         tsdbOpenFile(SFile* pFile, int oflag);
void        tsdbCloseFile(SFile* pFile);
int         tsdbCreateFile(SFile* pFile, STsdbRepo* pRepo, int fid, int type);
SFileGroup* tsdbSearchFGroup(STsdbFileH* pFileH, int fid, int flags);
void        tsdbFitRetention(STsdbRepo* pRepo);
int         tsdbUpdateFileHeader(SFile* pFile);
int         tsdbEncodeSFileInfo(void** buf, const STsdbFileInfo* pInfo);
void*       tsdbDecodeSFileInfo(void* buf, STsdbFileInfo* pInfo);
void        tsdbRemoveFileGroup(STsdbRepo* pRepo, SFileGroup* pFGroup);
int         tsdbLoadFileHeader(SFile* pFile, uint32_t* version);
void        tsdbGetFileInfoImpl(char* fname, uint32_t* magic, int64_t* size);
void        tsdbGetFidKeyRange(int daysPerFile, int8_t precision, int fileId, TSKEY *minKey, TSKEY *maxKey);

// ------------------ tsdbRWHelper.c
#define TSDB_HELPER_CLEAR_STATE 0x0        // Clear state
#define TSDB_HELPER_FILE_SET_AND_OPEN 0x1  // File is set
#define TSDB_HELPER_IDX_LOAD 0x2           // SCompIdx part is loaded
#define TSDB_HELPER_TABLE_SET 0x4          // Table is set
#define TSDB_HELPER_INFO_LOAD 0x8          // SCompInfo part is loaded
#define TSDB_HELPER_FILE_DATA_LOAD 0x10    // SCompData part is loaded
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
int  tsdbDecodeSCompIdxImpl(void* buffer, uint32_t len, SCompIdx** ppCompIdx, int* numOfIdx);
int  tsdbLoadCompIdx(SRWHelper* pHelper, void* target);
int  tsdbLoadCompInfoImpl(SFile* pFile, SCompIdx* pIdx, SCompInfo** ppCompInfo);
int  tsdbLoadCompInfo(SRWHelper* pHelper, void* target);
int  tsdbLoadCompData(SRWHelper* phelper, SCompBlock* pcompblock, void* target);
void tsdbGetDataStatis(SRWHelper* pHelper, SDataStatis* pStatis, int numOfCols);
int  tsdbLoadBlockDataCols(SRWHelper* pHelper, SCompBlock* pCompBlock, SCompInfo* pCompInfo, int16_t* colIds,
                           int numOfColIds);
int  tsdbLoadBlockData(SRWHelper* pHelper, SCompBlock* pCompBlock, SCompInfo* pCompInfo);

static FORCE_INLINE int compTSKEY(const void* key1, const void* key2) {
  if (*(TSKEY*)key1 > *(TSKEY*)key2) {
    return 1;
  } else if (*(TSKEY*)key1 == *(TSKEY*)key2) {
    return 0;
  } else {
    return -1;
  }
}

// ------------------ tsdbMain.c
#define REPO_ID(r) (r)->config.tsdbId
#define IS_REPO_LOCKED(r) (r)->repoLocked
#define TSDB_SUBMIT_MSG_HEAD_SIZE sizeof(SSubmitMsg)

char*       tsdbGetMetaFileName(char* rootDir);
void        tsdbGetDataFileName(char* rootDir, int vid, int fid, int type, char* fname);
int         tsdbLockRepo(STsdbRepo* pRepo);
int         tsdbUnlockRepo(STsdbRepo* pRepo);
char*       tsdbGetDataDirName(char* rootDir);
int         tsdbGetNextMaxTables(int tid);
STsdbMeta*  tsdbGetMeta(TSDB_REPO_T* pRepo);
STsdbFileH* tsdbGetFile(TSDB_REPO_T* pRepo);
int         tsdbCheckCommit(STsdbRepo* pRepo);

// ------------------ tsdbScan.c
int              tsdbScanFGroup(STsdbScanHandle* pScanHandle, char* rootDir, int fid);
STsdbScanHandle* tsdbNewScanHandle();
void             tsdbSetScanLogStream(STsdbScanHandle* pScanHandle, FILE* fLogStream);
int              tsdbSetAndOpenScanFile(STsdbScanHandle* pScanHandle, char* rootDir, int fid);
int              tsdbScanSCompIdx(STsdbScanHandle* pScanHandle);
int              tsdbScanSCompBlock(STsdbScanHandle* pScanHandle, int idx);
int              tsdbCloseScanFile(STsdbScanHandle* pScanHandle);
void             tsdbFreeScanHandle(STsdbScanHandle* pScanHandle);

// ------------------ tsdbCommitQueue.c
int tsdbScheduleCommit(STsdbRepo *pRepo);

#ifdef __cplusplus
}
#endif

#endif