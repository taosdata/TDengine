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

typedef struct TSDBROW        TSDBROW;
typedef struct TABLEID        TABLEID;
typedef struct TSDBKEY        TSDBKEY;
typedef struct SDelData       SDelData;
typedef struct SDelIdx        SDelIdx;
typedef struct STbData        STbData;
typedef struct SMemTable      SMemTable;
typedef struct STbDataIter    STbDataIter;
typedef struct STable         STable;
typedef struct SMapData       SMapData;
typedef struct SBlockIdx      SBlockIdx;
typedef struct SBlock         SBlock;
typedef struct SBlockStatis   SBlockStatis;
typedef struct SAggrBlkCol    SAggrBlkCol;
typedef struct SColData       SColData;
typedef struct SBlockDataHdr  SBlockDataHdr;
typedef struct SBlockData     SBlockData;
typedef struct SDelFile       SDelFile;
typedef struct STsdbCacheFile STsdbCacheFile;
typedef struct SHeadFile      SHeadFile;
typedef struct SDataFile      SDataFile;
typedef struct SLastFile      SLastFile;
typedef struct SSmaFile       SSmaFile;
typedef struct SDFileSet      SDFileSet;
typedef struct SDataFWriter   SDataFWriter;
typedef struct SDataFReader   SDataFReader;
typedef struct SDelFWriter    SDelFWriter;
typedef struct SDelFReader    SDelFReader;
typedef struct SRowIter       SRowIter;
typedef struct STsdbFS        STsdbFS;
typedef struct SRowMerger     SRowMerger;
typedef struct STsdbFSState   STsdbFSState;
typedef struct STsdbSnapHdr   STsdbSnapHdr;

#define TSDB_MAX_SUBBLOCKS 8
#define TSDB_FHDR_SIZE     512

#define HAS_NONE  ((int8_t)0x1)
#define HAS_NULL  ((int8_t)0x2)
#define HAS_VALUE ((int8_t)0x4)

#define VERSION_MIN 0
#define VERSION_MAX INT64_MAX

#define TSDBKEY_MIN ((TSDBKEY){.ts = TSKEY_MIN, .version = VERSION_MIN})
#define TSDBKEY_MAX ((TSDBKEY){.ts = TSKEY_MAX, .version = VERSION_MAX})

// tsdbUtil.c ==============================================================================================
// TSDBROW
#define TSDBROW_TS(ROW)                       (((ROW)->type == 0) ? (ROW)->pTSRow->ts : (ROW)->pBlockData->aTSKEY[(ROW)->iRow])
#define TSDBROW_VERSION(ROW)                  (((ROW)->type == 0) ? (ROW)->version : (ROW)->pBlockData->aVersion[(ROW)->iRow])
#define TSDBROW_SVERSION(ROW)                 TD_ROW_SVER((ROW)->pTSRow)
#define TSDBROW_KEY(ROW)                      ((TSDBKEY){.version = TSDBROW_VERSION(ROW), .ts = TSDBROW_TS(ROW)})
#define tsdbRowFromTSRow(VERSION, TSROW)      ((TSDBROW){.type = 0, .version = (VERSION), .pTSRow = (TSROW)})
#define tsdbRowFromBlockData(BLOCKDATA, IROW) ((TSDBROW){.type = 1, .pBlockData = (BLOCKDATA), .iRow = (IROW)})
void    tsdbRowGetColVal(TSDBROW *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal);
int32_t tPutTSDBRow(uint8_t *p, TSDBROW *pRow);
int32_t tGetTSDBRow(uint8_t *p, TSDBROW *pRow);
int32_t tsdbRowCmprFn(const void *p1, const void *p2);
// SRowIter
void     tRowIterInit(SRowIter *pIter, TSDBROW *pRow, STSchema *pTSchema);
SColVal *tRowIterNext(SRowIter *pIter);
// SRowMerger
int32_t tRowMergerInit(SRowMerger *pMerger, TSDBROW *pRow, STSchema *pTSchema);
void    tRowMergerClear(SRowMerger *pMerger);
int32_t tRowMerge(SRowMerger *pMerger, TSDBROW *pRow);
int32_t tRowMergerGetRow(SRowMerger *pMerger, STSRow **ppRow);
// TABLEID
int32_t tTABLEIDCmprFn(const void *p1, const void *p2);
// TSDBKEY
int32_t tsdbKeyCmprFn(const void *p1, const void *p2);
#define MIN_TSDBKEY(KEY1, KEY2) ((tsdbKeyCmprFn(&(KEY1), &(KEY2)) < 0) ? (KEY1) : (KEY2))
#define MAX_TSDBKEY(KEY1, KEY2) ((tsdbKeyCmprFn(&(KEY1), &(KEY2)) > 0) ? (KEY1) : (KEY2))
// SBlockCol
int32_t tPutBlockCol(uint8_t *p, void *ph);
int32_t tGetBlockCol(uint8_t *p, void *ph);
int32_t tBlockColCmprFn(const void *p1, const void *p2);
// SBlock
void    tBlockReset(SBlock *pBlock);
int32_t tPutBlock(uint8_t *p, void *ph);
int32_t tGetBlock(uint8_t *p, void *ph);
int32_t tBlockCmprFn(const void *p1, const void *p2);
bool    tBlockHasSma(SBlock *pBlock);
// SBlockIdx
void    tBlockIdxReset(SBlockIdx *pBlockIdx);
int32_t tPutBlockIdx(uint8_t *p, void *ph);
int32_t tGetBlockIdx(uint8_t *p, void *ph);
int32_t tCmprBlockIdx(void const *lhs, void const *rhs);
// SColdata
void    tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn);
void    tColDataReset(SColData *pColData);
void    tColDataClear(void *ph);
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal);
int32_t tColDataGetValue(SColData *pColData, int32_t iRow, SColVal *pColVal);
int32_t tColDataCopy(SColData *pColDataSrc, SColData *pColDataDest);
// SBlockData
#define tBlockDataFirstRow(PBLOCKDATA) tsdbRowFromBlockData(PBLOCKDATA, 0)
#define tBlockDataLastRow(PBLOCKDATA)  tsdbRowFromBlockData(PBLOCKDATA, (PBLOCKDATA)->nRow - 1)
#define tBlockDataFirstKey(PBLOCKDATA) TSDBROW_KEY(&tBlockDataFirstRow(PBLOCKDATA))
#define tBlockDataLastKey(PBLOCKDATA)  TSDBROW_KEY(&tBlockDataLastRow(PBLOCKDATA))
int32_t   tBlockDataInit(SBlockData *pBlockData);
void      tBlockDataReset(SBlockData *pBlockData);
int32_t   tBlockDataSetSchema(SBlockData *pBlockData, STSchema *pTSchema);
void      tBlockDataClearData(SBlockData *pBlockData);
void      tBlockDataClear(SBlockData *pBlockData);
int32_t   tBlockDataAddColData(SBlockData *pBlockData, int32_t iColData, SColData **ppColData);
int32_t   tBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema);
int32_t   tBlockDataMerge(SBlockData *pBlockData1, SBlockData *pBlockData2, SBlockData *pBlockData);
int32_t   tBlockDataCopy(SBlockData *pBlockDataSrc, SBlockData *pBlockDataDest);
SColData *tBlockDataGetColDataByIdx(SBlockData *pBlockData, int32_t idx);
void      tBlockDataGetColData(SBlockData *pBlockData, int16_t cid, SColData **ppColData);
// SDelIdx
int32_t tPutDelIdx(uint8_t *p, void *ph);
int32_t tGetDelIdx(uint8_t *p, void *ph);
int32_t tCmprDelIdx(void const *lhs, void const *rhs);
// SDelData
int32_t tPutDelData(uint8_t *p, void *ph);
int32_t tGetDelData(uint8_t *p, void *ph);
// SMapData
#define tMapDataInit() ((SMapData){0})
void    tMapDataReset(SMapData *pMapData);
void    tMapDataClear(SMapData *pMapData);
int32_t tMapDataPutItem(SMapData *pMapData, void *pItem, int32_t (*tPutItemFn)(uint8_t *, void *));
void    tMapDataGetItemByIdx(SMapData *pMapData, int32_t idx, void *pItem, int32_t (*tGetItemFn)(uint8_t *, void *));
int32_t tMapDataSearch(SMapData *pMapData, void *pSearchItem, int32_t (*tGetItemFn)(uint8_t *, void *),
                       int32_t (*tItemCmprFn)(const void *, const void *), void *pItem);
int32_t tPutMapData(uint8_t *p, SMapData *pMapData);
int32_t tGetMapData(uint8_t *p, SMapData *pMapData);
// other
int32_t tsdbKeyFid(TSKEY key, int32_t minutes, int8_t precision);
void    tsdbFidKeyRange(int32_t fid, int32_t minutes, int8_t precision, TSKEY *minKey, TSKEY *maxKey);
int32_t tsdbBuildDeleteSkyline(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline);
void    tsdbCalcColDataSMA(SColData *pColData, SColumnDataAgg *pColAgg);
// tsdbMemTable ==============================================================================================
// SMemTable
int32_t tsdbMemTableCreate(STsdb *pTsdb, SMemTable **ppMemTable);
void    tsdbMemTableDestroy(SMemTable *pMemTable);
void    tsdbGetTbDataFromMemTable(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid, STbData **ppTbData);
// STbDataIter
int32_t  tsdbTbDataIterCreate(STbData *pTbData, TSDBKEY *pFrom, int8_t backward, STbDataIter **ppIter);
void    *tsdbTbDataIterDestroy(STbDataIter *pIter);
void     tsdbTbDataIterOpen(STbData *pTbData, TSDBKEY *pFrom, int8_t backward, STbDataIter *pIter);
TSDBROW *tsdbTbDataIterGet(STbDataIter *pIter);
bool     tsdbTbDataIterNext(STbDataIter *pIter);
// STbData
int32_t tsdbGetNRowsInTbData(STbData *pTbData);
// tsdbFile.c ==============================================================================================
typedef enum { TSDB_HEAD_FILE = 0, TSDB_DATA_FILE, TSDB_LAST_FILE, TSDB_SMA_FILE } EDataFileT;
void    tsdbDataFileName(STsdb *pTsdb, SDFileSet *pDFileSet, EDataFileT ftype, char fname[]);
bool    tsdbFileIsSame(SDFileSet *pDFileSet1, SDFileSet *pDFileSet2, EDataFileT ftype);
int32_t tsdbUpdateDFileHdr(TdFilePtr pFD, SDFileSet *pSet, EDataFileT ftype);
int32_t tsdbDFileRollback(STsdb *pTsdb, SDFileSet *pSet, EDataFileT ftype);
int32_t tPutDataFileHdr(uint8_t *p, SDFileSet *pSet, EDataFileT ftype);
int32_t tPutDelFile(uint8_t *p, SDelFile *pDelFile);
int32_t tGetDelFile(uint8_t *p, SDelFile *pDelFile);
int32_t tPutDFileSet(uint8_t *p, SDFileSet *pSet);
int32_t tGetDFileSet(uint8_t *p, SDFileSet *pSet);
// SDelFile
void tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile, char fname[]);
// tsdbFS.c ==============================================================================================
int32_t tsdbFSOpen(STsdb *pTsdb, STsdbFS **ppFS);
int32_t tsdbFSClose(STsdbFS *pFS);
int32_t tsdbFSBegin(STsdbFS *pFS);
int32_t tsdbFSCommit(STsdbFS *pFS);
int32_t tsdbFSRollback(STsdbFS *pFS);

int32_t    tsdbFSStateUpsertDelFile(STsdbFSState *pState, SDelFile *pDelFile);
int32_t    tsdbFSStateUpsertDFileSet(STsdbFSState *pState, SDFileSet *pSet);
SDelFile  *tsdbFSStateGetDelFile(STsdbFSState *pState);
SDFileSet *tsdbFSStateGetDFileSet(STsdbFSState *pState, int32_t fid);
// tsdbReaderWriter.c ==============================================================================================
// SDataFWriter
int32_t tsdbDataFWriterOpen(SDataFWriter **ppWriter, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFWriterClose(SDataFWriter **ppWriter, int8_t sync);
int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter);
int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SArray *aBlockIdx, uint8_t **ppBuf);
int32_t tsdbWriteBlock(SDataFWriter *pWriter, SMapData *pMapData, uint8_t **ppBuf, SBlockIdx *pBlockIdx);
int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2,
                           SBlockIdx *pBlockIdx, SBlock *pBlock, int8_t cmprAlg);

SDFileSet *tsdbDataFWriterGetWSet(SDataFWriter *pWriter);
// SDataFReader
int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFReaderClose(SDataFReader **ppReader);
int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx, uint8_t **ppBuf);
int32_t tsdbReadBlock(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *pMapData, uint8_t **ppBuf);
int32_t tsdbReadColData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, int16_t *aColId, int32_t nCol,
                        SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2);
int32_t tsdbReadBlockData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, SBlockData *pBlockData,
                          uint8_t **ppBuf1, uint8_t **ppBuf2);
int32_t tsdbReadBlockSma(SDataFReader *pReader, SBlock *pBlock, SArray *aColumnDataAgg, uint8_t **ppBuf);
// SDelFWriter
int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb);
int32_t tsdbDelFWriterClose(SDelFWriter **ppWriter, int8_t sync);
int32_t tsdbWriteDelData(SDelFWriter *pWriter, SArray *aDelData, uint8_t **ppBuf, SDelIdx *pDelIdx);
int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SArray *aDelIdx, uint8_t **ppBuf);
int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter);
// SDelFReader
int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb, uint8_t **ppBuf);
int32_t tsdbDelFReaderClose(SDelFReader **ppReader);
int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData, uint8_t **ppBuf);
int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx, uint8_t **ppBuf);

#define TSDB_CACHE_NO(c)       ((c).cacheLast == 0)
#define TSDB_CACHE_LAST_ROW(c) (((c).cacheLast & 1) > 0)
#define TSDB_CACHE_LAST(c)     (((c).cacheLast & 2) > 0)

// tsdbCache
int32_t tsdbOpenCache(STsdb *pTsdb);
void    tsdbCloseCache(SLRUCache *pCache);
int32_t tsdbCacheInsertLast(SLRUCache *pCache, tb_uid_t uid, STSRow *row, STsdb *pTsdb);
int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, STsdb *pTsdb, tb_uid_t uid, STSRow *row, bool dup);
int32_t tsdbCacheGetLastH(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, LRUHandle **h);
int32_t tsdbCacheGetLastrowH(SLRUCache *pCache, tb_uid_t uid, STsdb *pTsdb, LRUHandle **h);
int32_t tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h);

int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);
int32_t tsdbCacheDeleteLast(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);
int32_t tsdbCacheDelete(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);

void   tsdbCacheSetCapacity(SVnode *pVnode, size_t capacity);
size_t tsdbCacheGetCapacity(SVnode *pVnode);

int32_t tsdbCacheLastArray2Row(SArray *pLastArray, STSRow **ppRow, STSchema *pSchema);

// structs =======================
typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

struct STsdb {
  char         *path;
  SVnode       *pVnode;
  TdThreadMutex mutex;
  bool          repoLocked;
  STsdbKeepCfg  keepCfg;
  SMemTable    *mem;
  SMemTable    *imem;
  SRtn          rtn;
  STsdbFS      *fs;
  SLRUCache    *lruCache;
};

struct STable {
  uint64_t  suid;
  uint64_t  uid;
  STSchema *pSchema;       // latest schema
  STSchema *pCacheSchema;  // cached cache
};

struct TSDBKEY {
  int64_t version;
  TSKEY   ts;
};

typedef struct SMemSkipListNode SMemSkipListNode;
struct SMemSkipListNode {
  int8_t            level;
  SMemSkipListNode *forwards[0];
};
typedef struct SMemSkipList {
  uint32_t          seed;
  int64_t           size;
  int8_t            maxLevel;
  int8_t            level;
  SMemSkipListNode *pHead;
  SMemSkipListNode *pTail;
} SMemSkipList;

struct SDelDataInfo {
  tb_uid_t suid;
  tb_uid_t uid;
};

struct STbData {
  tb_uid_t     suid;
  tb_uid_t     uid;
  TSKEY        minKey;
  TSKEY        maxKey;
  int64_t      minVersion;
  int64_t      maxVersion;
  int32_t      maxSkmVer;
  SDelData    *pHead;
  SDelData    *pTail;
  SMemSkipList sl;
};

struct SMemTable {
  SRWLatch latch;
  STsdb   *pTsdb;
  int32_t  nRef;
  TSKEY    minKey;
  TSKEY    maxKey;
  int64_t  minVersion;
  int64_t  maxVersion;
  int64_t  nRow;
  int64_t  nDel;
  SArray  *aTbData;  // SArray<STbData*>
};

int tsdbLockRepo(STsdb *pTsdb);
int tsdbUnlockRepo(STsdb *pTsdb);

struct TSDBROW {
  int8_t type;  // 0 for row from tsRow, 1 for row from block data
  union {
    struct {
      int64_t version;
      STSRow *pTSRow;
    };
    struct {
      SBlockData *pBlockData;
      int32_t     iRow;
    };
  };
};

struct SBlockIdx {
  int64_t suid;
  int64_t uid;
  TSKEY   minKey;
  TSKEY   maxKey;
  int64_t minVersion;
  int64_t maxVersion;
  int64_t offset;
  int64_t size;
};

struct SMapData {
  int32_t  nItem;
  int32_t *aOffset;
  int32_t  nData;
  uint8_t *pData;
};

typedef struct {
  int16_t cid;
  int8_t  type;
  int8_t  smaOn;
  int8_t  flag;  // HAS_NONE|HAS_NULL|HAS_VALUE
  int32_t offset;
  int32_t szBitmap;  // bitmap size
  int32_t szOffset;  // size of offset, only for variant-length data type
  int32_t szValue;   // compressed column value size
  int32_t szOrigin;  // original column value size (only save for variant data type)
} SBlockCol;

typedef struct {
  int32_t nRow;
  int8_t  cmprAlg;
  int64_t offset;      // block data offset
  int32_t szBlockCol;  // SBlockCol size
  int32_t szVersion;   // VERSION size
  int32_t szTSKEY;     // TSKEY size
  int32_t szBlock;     // total block size
  int64_t sOffset;     // sma offset
  int32_t nSma;        // sma size
} SSubBlock;

struct SBlock {
  TSDBKEY   minKey;
  TSDBKEY   maxKey;
  int64_t   minVersion;
  int64_t   maxVersion;
  int32_t   nRow;
  int8_t    last;
  int8_t    hasDup;
  int8_t    nSubBlock;
  SSubBlock aSubBlock[TSDB_MAX_SUBBLOCKS];
};

struct SAggrBlkCol {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
};

struct SColData {
  int16_t  cid;
  int8_t   type;
  int8_t   smaOn;
  int32_t  nVal;
  uint8_t  flag;
  uint8_t *pBitMap;
  int32_t *aOffset;
  int32_t  nData;
  uint8_t *pData;
};

struct SBlockData {
  int32_t  nRow;
  int64_t *aVersion;
  TSKEY   *aTSKEY;
  SArray  *aIdx;      // SArray<int32_t>
  SArray  *aColData;  // SArray<SColData>
};

// ================== TSDB global config
extern bool tsdbForceKeepFile;

#define TSDB_FS_ITER_FORWARD  TSDB_ORDER_ASC
#define TSDB_FS_ITER_BACKWARD TSDB_ORDER_DESC

struct TABLEID {
  tb_uid_t suid;
  tb_uid_t uid;
};

struct STbDataIter {
  STbData          *pTbData;
  int8_t            backward;
  SMemSkipListNode *pNode;
  TSDBROW          *pRow;
  TSDBROW           row;
};

struct SDelData {
  int64_t   version;
  TSKEY     sKey;
  TSKEY     eKey;
  SDelData *pNext;
};

struct SDelIdx {
  tb_uid_t suid;
  tb_uid_t uid;
  int64_t  offset;
  int64_t  size;
};

struct SDelFile {
  int64_t commitID;
  int64_t size;
  int64_t offset;
};

#pragma pack(push, 1)
struct SBlockDataHdr {
  uint32_t delimiter;
  int64_t  suid;
  int64_t  uid;
};
#pragma pack(pop)

struct SHeadFile {
  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SDataFile {
  int64_t commitID;
  int64_t size;
};

struct SLastFile {
  int64_t commitID;
  int64_t size;
};

struct SSmaFile {
  int64_t commitID;
  int64_t size;
};

struct SDFileSet {
  SDiskID   diskId;
  int32_t   fid;
  SHeadFile fHead;
  SDataFile fData;
  SLastFile fLast;
  SSmaFile  fSma;
};

struct SRowIter {
  TSDBROW  *pRow;
  STSchema *pTSchema;
  SColVal   colVal;
  int32_t   i;
};
struct SRowMerger {
  STSchema *pTSchema;
  int64_t   version;
  SArray   *pArray;  // SArray<SColVal>
};

struct STsdbFSState {
  SDelFile *pDelFile;
  SArray   *aDFileSet;  // SArray<aDFileSet>
  SDelFile  delFile;
};

struct STsdbFS {
  STsdb         *pTsdb;
  TdThreadRwlock lock;
  int8_t         inTxn;
  STsdbFSState  *cState;
  STsdbFSState  *nState;
};

struct SDelFWriter {
  STsdb    *pTsdb;
  SDelFile  fDel;
  TdFilePtr pWriteH;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_H_*/
