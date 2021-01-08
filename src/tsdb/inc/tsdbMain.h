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
extern int32_t tsdbDebugFlag;

#define tsdbFatal(...) do { if (tsdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TDB FATAL ", 255, __VA_ARGS__); }}     while(0)
#define tsdbError(...) do { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TDB ERROR ", 255, __VA_ARGS__); }}     while(0)
#define tsdbWarn(...)  do { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TDB WARN ", 255, __VA_ARGS__); }}      while(0)
#define tsdbInfo(...)  do { if (tsdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TDB ", 255, __VA_ARGS__); }}           while(0)
#define tsdbDebug(...) do { if (tsdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TDB ", tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define tsdbTrace(...) do { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TDB ", tsdbDebugFlag, __VA_ARGS__); }} while(0)

// ================= OTHERS

#define TAOS_IN_RANGE(key, keyMin, keyLast) (((key) >= (keyMin)) && ((key) <= (keyMax)))

// NOTE: Any file format change must increase this version number by 1
//       Also, implement the convert function
#define TSDB_FILE_VERSION ((uint32_t)0)

// Definitions
// ================= tsdbMeta.c
#define TSDB_MAX_TABLE_SCHEMAS 16

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
  TSKEY          lastKey;
  SDataRow       lastRow;
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

#define TSDB_INIT_NTABLES 1024
#define TABLE_TYPE(t) (t)->type
#define TABLE_NAME(t) (t)->name
#define TABLE_CHAR_NAME(t) TABLE_NAME(t)->data
#define TABLE_UID(t) (t)->tableId.uid
#define TABLE_TID(t) (t)->tableId.tid
#define TABLE_SUID(t) (t)->suid
#define TSDB_META_FILE_MAGIC(m) KVSTORE_MAGIC((m)->pStore)
#define TSDB_RLOCK_TABLE(t) taosRLockLatch(&((t)->latch))
#define TSDB_RUNLOCK_TABLE(t) taosRUnLockLatch(&((t)->latch))
#define TSDB_WLOCK_TABLE(t) taosWLockLatch(&((t)->latch))
#define TSDB_WUNLOCK_TABLE(t) taosWUnLockLatch(&((t)->latch))

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

  if (lock) TSDB_RLOCK_TABLE(pDTable);
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
  if (lock) TSDB_RUNLOCK_TABLE(pDTable);
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

static FORCE_INLINE TSKEY tsdbGetTableLastKeyImpl(STable* pTable) {
  ASSERT(pTable->lastRow == NULL || pTable->lastKey == dataRowKey(pTable->lastRow));
  return pTable->lastKey;
}

// ================= tsdbBuffer.c
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

#define TSDB_BUFFER_RESERVE 1024  // Reseve 1K as commit threshold

STsdbBufPool* tsdbNewBufPool();
void          tsdbFreeBufPool(STsdbBufPool* pBufPool);
int           tsdbOpenBufPool(STsdbRepo* pRepo);
void          tsdbCloseBufPool(STsdbRepo* pRepo);
SListNode*    tsdbAllocBufBlockFromPool(STsdbRepo* pRepo);

// ------------------ tsdbMemTable.c
typedef struct {
  int   rowsInserted;
  int   rowsUpdated;
  int   rowsDeleteSucceed;
  int   rowsDeleteFailed;
  int   nOperations;
  TSKEY keyFirst;
  TSKEY keyLast;
} SMergeInfo;

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

// ================= tsdbFile.c
#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC 0xFFFFFFFF

typedef enum {
  TSDB_FILE_HEAD = 0,
  TSDB_FILE_DATA,
  TSDB_FILE_LAST,
  TSDB_FILE_MAX,
  TSDB_FILE_META,
  TSDB_FILE_MANIFEST
} TSDB_FILE_T;

// For meta file
typedef struct {
  int64_t  size;
  int64_t  tombSize;
  int64_t  nRecords;
  int64_t  nDels;
  uint32_t magic;
} SMFInfo;

typedef struct {
  SMFInfo info;
  TFILE   f;
  int     fd;
} SMFile;

void    tsdbInitMFile(SMFile* pMFile, int vid, int ver, SMFInfo* pInfo);
int     tsdbOpenMFile(SMFile* pMFile, int flags);
void    tsdbCloseMFile(SMFile* pMFile);
int64_t tsdbSeekMFile(SMFile* pMFile, int64_t offset, int whence);
int64_t tsdbWriteMFile(SMFile* pMFile, void* buf, int64_t nbyte);
int64_t tsdbTellMFile(SMFile *pMFile);
int     tsdbEncodeMFile(void** buf, SMFile* pMFile);
void*   tsdbDecodeMFile(void* buf, SMFile* pMFile);

// For .head/.data/.last file
typedef struct {
  uint32_t magic;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;
  uint64_t tombSize;
} SDFInfo;

typedef struct {
  SDFInfo info;
  TFILE   f;
  int     fd;
} SDFile;

void    tsdbInitDFile(SDFile* pDFile, int vid, int fid, int ver, int level, int id, const SDFInfo* pInfo,
                      TSDB_FILE_T ftype);
void    tsdbInitDFileWithOld(SDFile* pDFile, SDFile* pOldDFile);
int     tsdbOpenDFile(SDFile* pDFile, int flags);
void    tsdbCloseDFile(SDFile* pDFile);
int64_t tsdbSeekDFile(SDFile* pDFile, int64_t offset, int whence);
int64_t tsdbWriteDFile(SDFile* pDFile, void* buf, int64_t nbyte);
int64_t tsdbAppendDFile(SDFile* pDFile, void* buf, int64_t nbyte, int64_t* offset);
int64_t tsdbTellDFile(SDFile* pDFile);
int     tsdbEncodeDFile(void** buf, SDFile* pDFile);
void*   tsdbDecodeDFile(void* buf, SDFile* pDFile);
void    tsdbUpdateDFileMagic(SDFile* pDFile, void* pCksm);

typedef struct {
  int    fid;
  int    state;
  SDFile files[TSDB_FILE_MAX];
} SDFileSet;

#define TSDB_FILE_FULL_NAME(f) TFILE_NAME(&((f)->f))
#define TSDB_DFILE_IN_SET(s, t) ((s)->files + (t))

void tsdbInitDFileSet(SDFileSet* pSet, int vid, int fid, int ver, int level, int id);
void tsdbInitDFileSetWithOld(SDFileSet *pSet, SDFileSet *pOldSet);
int  tsdbOpenDFileSet(SDFileSet* pSet, int flags);
void tsdbCloseDFileSet(SDFileSet* pSet);
int  tsdbUpdateDFileSetHeader(SDFileSet* pSet);
int  tsdbCopyDFileSet(SDFileSet* pFromSet, SDFileSet* pToSet);

/* Statistic information of the TSDB file system.
 */
typedef struct {
  int64_t fsversion; // file system version, related to program
  int64_t version;
  int64_t totalPoints;
  int64_t totalStorage;
} STsdbFSMeta;

typedef struct {
  int64_t     version;
  STsdbFSMeta meta;
  SMFile      mf;  // meta file
  SArray*     df;  // data file array
} SFSVer;

typedef struct {
  pthread_rwlock_t lock;

  SFSVer fsv;
} STsdbFS;

typedef struct {
  int        version;  // current FS version
  int        index;
  int        fid;
  SDFileSet* pSet;
} SFSIter;

#define TSDB_FILE_INFO(tf) (&((tf)->info))
#define TSDB_FILE_F(tf) (&((tf)->f)))
#define TSDB_FILE_FD(tf) ((tf)->fd)

int        tsdbOpenFS(STsdbRepo* pRepo);
void       tsdbCloseFS(STsdbRepo* pRepo);
int        tsdbFSNewTxn(STsdbRepo* pRepo);
int        tsdbFSEndTxn(STsdbRepo* pRepo, bool hasError);
int        tsdbUpdateMFile(STsdbRepo* pRepo, SMFile* pMFile);
int        tsdbUpdateDFileSet(STsdbRepo* pRepo, SDFileSet* pSet);
void       tsdbRemoveExpiredDFileSet(STsdbRepo* pRepo, int mfid);
int        tsdbRemoveDFileSet(SDFileSet* pSet);
int        tsdbEncodeMFInfo(void** buf, SMFInfo* pInfo);
void*      tsdbDecodeMFInfo(void* buf, SMFInfo* pInfo);
SDFileSet  tsdbMoveDFileSet(SDFileSet* pOldSet, int to);
int        tsdbInitFSIter(STsdbRepo* pRepo, SFSIter* pIter);
SDFileSet* tsdbFSIterNext(SFSIter* pIter);
int        tsdbCreateDFileSet(int fid, int level, SDFileSet* pSet);

static FORCE_INLINE int tsdbRLockFS(STsdbFS *pFs) {
  int code = pthread_rwlock_rdlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbWLockFS(STsdbFS *pFs) {
  int code = pthread_rwlock_wrlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbUnLockFS(STsdbFS *pFs) {
  int code = pthread_rwlock_unlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

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
void        tsdbGetFidKeyRange(int daysPerFile, int8_t precision, int fileId, TSKEY *minKey, TSKEY *maxKey);
// int         tsdbApplyRetention(STsdbRepo* pRepo, SFidGroup *pFidGroup);

// ================= tsdbMain.c
typedef struct {
  int32_t  totalLen;
  int32_t  len;
  SDataRow row;
} SSubmitBlkIter;

typedef struct {
  int32_t totalLen;
  int32_t len;
  void *  pMsg;
} SSubmitMsgIter;

struct STsdbRepo {
  int8_t state;

  char*           rootDir;
  STsdbCfg        config;
  STsdbAppH       appH;
  STsdbStat       stat;
  STsdbMeta*      tsdbMeta;
  STsdbBufPool*   pPool;
  SMemTable*      mem;
  SMemTable*      imem;
  STsdbFS*        fs;
  sem_t           readyToCommit;
  pthread_mutex_t mutex;
  bool            repoLocked;
  int32_t         code; // Commit code
};

#define REPO_ID(r) (r)->config.tsdbId
#define REPO_CFG(r) (&((r)->config))
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

static FORCE_INLINE STsdbBufBlock* tsdbGetCurrBufBlock(STsdbRepo* pRepo) {
  ASSERT(pRepo != NULL);
  if (pRepo->mem == NULL) return NULL;

  SListNode* pNode = listTail(pRepo->mem->bufBlockList);
  if (pNode == NULL) return NULL;

  STsdbBufBlock* pBufBlock = NULL;
  tdListNodeGetData(pRepo->mem->bufBlockList, pNode, (void*)(&pBufBlock));

  return pBufBlock;
}

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