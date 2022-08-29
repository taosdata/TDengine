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
#define tsdbFatal(...) do { if (tsdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TSD FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define tsdbError(...) do { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TSD ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define tsdbWarn(...)  do { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TSD WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define tsdbInfo(...)  do { if (tsdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TSD ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define tsdbDebug(...) do { if (tsdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSD ", DEBUG_DEBUG, tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define tsdbTrace(...) do { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TSD ", DEBUG_TRACE, tsdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef struct TSDBROW       TSDBROW;
typedef struct TABLEID       TABLEID;
typedef struct TSDBKEY       TSDBKEY;
typedef struct SDelData      SDelData;
typedef struct SDelIdx       SDelIdx;
typedef struct STbData       STbData;
typedef struct SMemTable     SMemTable;
typedef struct STbDataIter   STbDataIter;
typedef struct SMapData      SMapData;
typedef struct SBlockIdx     SBlockIdx;
typedef struct SBlock        SBlock;
typedef struct SBlockL       SBlockL;
typedef struct SColData      SColData;
typedef struct SDiskDataHdr  SDiskDataHdr;
typedef struct SBlockData    SBlockData;
typedef struct SDelFile      SDelFile;
typedef struct SHeadFile     SHeadFile;
typedef struct SDataFile     SDataFile;
typedef struct SLastFile     SLastFile;
typedef struct SSmaFile      SSmaFile;
typedef struct SDFileSet     SDFileSet;
typedef struct SDataFWriter  SDataFWriter;
typedef struct SDataFReader  SDataFReader;
typedef struct SDelFWriter   SDelFWriter;
typedef struct SDelFReader   SDelFReader;
typedef struct SRowIter      SRowIter;
typedef struct STsdbFS       STsdbFS;
typedef struct SRowMerger    SRowMerger;
typedef struct STsdbReadSnap STsdbReadSnap;
typedef struct SBlockInfo    SBlockInfo;
typedef struct SSmaInfo      SSmaInfo;
typedef struct SBlockCol     SBlockCol;

#define TSDB_FILE_DLMT     ((uint32_t)0xF00AFA0F)
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
int32_t tRowMergerInit2(SRowMerger *pMerger, STSchema *pResTSchema, TSDBROW *pRow, STSchema *pTSchema);
int32_t tRowMergerAdd(SRowMerger *pMerger, TSDBROW *pRow, STSchema *pTSchema);

int32_t tRowMergerInit(SRowMerger *pMerger, TSDBROW *pRow, STSchema *pTSchema);
void    tRowMergerClear(SRowMerger *pMerger);
int32_t tRowMerge(SRowMerger *pMerger, TSDBROW *pRow);
int32_t tRowMergerGetRow(SRowMerger *pMerger, STSRow **ppRow);
// TABLEID
int32_t tTABLEIDCmprFn(const void *p1, const void *p2);
// TSDBKEY
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
// SBlockL
int32_t tPutBlockL(uint8_t *p, void *ph);
int32_t tGetBlockL(uint8_t *p, void *ph);
// SBlockIdx
int32_t tPutBlockIdx(uint8_t *p, void *ph);
int32_t tGetBlockIdx(uint8_t *p, void *ph);
int32_t tCmprBlockIdx(void const *lhs, void const *rhs);
int32_t tCmprBlockL(void const *lhs, void const *rhs);
// SColdata
void    tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn);
void    tColDataReset(SColData *pColData);
void    tColDataClear(void *ph);
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal);
int32_t tColDataGetValue(SColData *pColData, int32_t iRow, SColVal *pColVal);
int32_t tColDataCopy(SColData *pColDataSrc, SColData *pColDataDest);
int32_t tPutColData(uint8_t *p, SColData *pColData);
int32_t tGetColData(uint8_t *p, SColData *pColData);
// SBlockData
#define tBlockDataFirstRow(PBLOCKDATA) tsdbRowFromBlockData(PBLOCKDATA, 0)
#define tBlockDataLastRow(PBLOCKDATA)  tsdbRowFromBlockData(PBLOCKDATA, (PBLOCKDATA)->nRow - 1)
#define tBlockDataFirstKey(PBLOCKDATA) TSDBROW_KEY(&tBlockDataFirstRow(PBLOCKDATA))
#define tBlockDataLastKey(PBLOCKDATA)  TSDBROW_KEY(&tBlockDataLastRow(PBLOCKDATA))

int32_t   tBlockDataCreate(SBlockData *pBlockData);
void      tBlockDataDestroy(SBlockData *pBlockData, int8_t deepClear);
int32_t   tBlockDataInit(SBlockData *pBlockData, int64_t suid, int64_t uid, STSchema *pTSchema);
int32_t   tBlockDataInitEx(SBlockData *pBlockData, SBlockData *pBlockDataFrom);
void      tBlockDataReset(SBlockData *pBlockData);
int32_t   tBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema, int64_t uid);
void      tBlockDataClear(SBlockData *pBlockData);
SColData *tBlockDataGetColDataByIdx(SBlockData *pBlockData, int32_t idx);
void      tBlockDataGetColData(SBlockData *pBlockData, int16_t cid, SColData **ppColData);
int32_t   tBlockDataCopy(SBlockData *pBlockDataSrc, SBlockData *pBlockDataDest);
int32_t   tBlockDataMerge(SBlockData *pBlockData1, SBlockData *pBlockData2, SBlockData *pBlockData);
int32_t   tBlockDataAddColData(SBlockData *pBlockData, int32_t iColData, SColData **ppColData);
int32_t   tCmprBlockData(SBlockData *pBlockData, int8_t cmprAlg, uint8_t **ppOut, int32_t *szOut, uint8_t *aBuf[],
                         int32_t aBufN[]);
int32_t   tDecmprBlockData(uint8_t *pIn, int32_t szIn, SBlockData *pBlockData, uint8_t *aBuf[]);
// SDiskDataHdr
int32_t tPutDiskDataHdr(uint8_t *p, void *ph);
int32_t tGetDiskDataHdr(uint8_t *p, void *ph);
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
int32_t tsdbFidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int64_t now);
int32_t tsdbBuildDeleteSkyline(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline);
void    tsdbCalcColDataSMA(SColData *pColData, SColumnDataAgg *pColAgg);
int32_t tPutColumnDataAgg(uint8_t *p, SColumnDataAgg *pColAgg);
int32_t tGetColumnDataAgg(uint8_t *p, SColumnDataAgg *pColAgg);
int32_t tsdbCmprData(uint8_t *pIn, int32_t szIn, int8_t type, int8_t cmprAlg, uint8_t **ppOut, int32_t nOut,
                     int32_t *szOut, uint8_t **ppBuf);
int32_t tsdbDecmprData(uint8_t *pIn, int32_t szIn, int8_t type, int8_t cmprAlg, uint8_t **ppOut, int32_t szOut,
                       uint8_t **ppBuf);
int32_t tsdbCmprColData(SColData *pColData, int8_t cmprAlg, SBlockCol *pBlockCol, uint8_t **ppOut, int32_t nOut,
                        uint8_t **ppBuf);
int32_t tsdbDecmprColData(uint8_t *pIn, SBlockCol *pBlockCol, int8_t cmprAlg, int32_t nVal, SColData *pColData,
                          uint8_t **ppBuf);
int32_t tsdbReadAndCheck(TdFilePtr pFD, int64_t offset, uint8_t **ppOut, int32_t size, int8_t toCheck);
// tsdbMemTable ==============================================================================================
// SMemTable
int32_t  tsdbMemTableCreate(STsdb *pTsdb, SMemTable **ppMemTable);
void     tsdbMemTableDestroy(SMemTable *pMemTable);
STbData *tsdbGetTbDataFromMemTable(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid);
void     tsdbRefMemTable(SMemTable *pMemTable);
void     tsdbUnrefMemTable(SMemTable *pMemTable);
SArray  *tsdbMemTableGetTbDataArray(SMemTable *pMemTable);
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

bool    tsdbDelFileIsSame(SDelFile *pDelFile1, SDelFile *pDelFile2);
int32_t tsdbDFileRollback(STsdb *pTsdb, SDFileSet *pSet, EDataFileT ftype);
int32_t tPutHeadFile(uint8_t *p, SHeadFile *pHeadFile);
int32_t tPutDataFile(uint8_t *p, SDataFile *pDataFile);
int32_t tPutLastFile(uint8_t *p, SLastFile *pLastFile);
int32_t tPutSmaFile(uint8_t *p, SSmaFile *pSmaFile);
int32_t tPutDelFile(uint8_t *p, SDelFile *pDelFile);
int32_t tGetDelFile(uint8_t *p, SDelFile *pDelFile);
int32_t tPutDFileSet(uint8_t *p, SDFileSet *pSet);
int32_t tGetDFileSet(uint8_t *p, SDFileSet *pSet);

void tsdbHeadFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SHeadFile *pHeadF, char fname[]);
void tsdbDataFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SDataFile *pDataF, char fname[]);
void tsdbLastFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SLastFile *pLastF, char fname[]);
void tsdbSmaFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SSmaFile *pSmaF, char fname[]);
// SDelFile
void tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile, char fname[]);
// tsdbFS.c ==============================================================================================
int32_t tsdbFSOpen(STsdb *pTsdb);
int32_t tsdbFSClose(STsdb *pTsdb);
int32_t tsdbFSCopy(STsdb *pTsdb, STsdbFS *pFS);
void    tsdbFSDestroy(STsdbFS *pFS);
int32_t tDFileSetCmprFn(const void *p1, const void *p2);
int32_t tsdbFSCommit1(STsdb *pTsdb, STsdbFS *pFS);
int32_t tsdbFSCommit2(STsdb *pTsdb, STsdbFS *pFS);
int32_t tsdbFSRef(STsdb *pTsdb, STsdbFS *pFS);
void    tsdbFSUnref(STsdb *pTsdb, STsdbFS *pFS);

int32_t tsdbFSRollback(STsdbFS *pFS);

int32_t tsdbFSUpsertFSet(STsdbFS *pFS, SDFileSet *pSet);
int32_t tsdbFSUpsertDelFile(STsdbFS *pFS, SDelFile *pDelFile);
// tsdbReaderWriter.c ==============================================================================================
// SDataFWriter
int32_t tsdbDataFWriterOpen(SDataFWriter **ppWriter, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFWriterClose(SDataFWriter **ppWriter, int8_t sync);
int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter);
int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SArray *aBlockIdx);
int32_t tsdbWriteBlock(SDataFWriter *pWriter, SMapData *pMapData, SBlockIdx *pBlockIdx);
int32_t tsdbWriteBlockL(SDataFWriter *pWriter, SArray *aBlockL);
int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlkInfo, SSmaInfo *pSmaInfo,
                           int8_t cmprAlg, int8_t toLast);

int32_t tsdbDFileSetCopy(STsdb *pTsdb, SDFileSet *pSetFrom, SDFileSet *pSetTo);
// SDataFReader
int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFReaderClose(SDataFReader **ppReader);
int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx);
int32_t tsdbReadBlock(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *pMapData);
int32_t tsdbReadBlockL(SDataFReader *pReader, SArray *aBlockL);
int32_t tsdbReadBlockSma(SDataFReader *pReader, SBlock *pBlock, SArray *aColumnDataAgg);
int32_t tsdbReadDataBlock(SDataFReader *pReader, SBlock *pBlock, SBlockData *pBlockData);
int32_t tsdbReadLastBlock(SDataFReader *pReader, SBlockL *pBlockL, SBlockData *pBlockData);
// SDelFWriter
int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb);
int32_t tsdbDelFWriterClose(SDelFWriter **ppWriter, int8_t sync);
int32_t tsdbWriteDelData(SDelFWriter *pWriter, SArray *aDelData, SDelIdx *pDelIdx);
int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SArray *aDelIdx);
int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter);
// SDelFReader
int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb);
int32_t tsdbDelFReaderClose(SDelFReader **ppReader);
int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData);
int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx);
// tsdbRead.c ==============================================================================================
int32_t tsdbTakeReadSnap(STsdb *pTsdb, STsdbReadSnap **ppSnap);
void    tsdbUntakeReadSnap(STsdb *pTsdb, STsdbReadSnap *pSnap);

#define TSDB_CACHE_NO(c)       ((c).cacheLast == 0)
#define TSDB_CACHE_LAST_ROW(c) (((c).cacheLast & 1) > 0)
#define TSDB_CACHE_LAST(c)     (((c).cacheLast & 2) > 0)

// tsdbCache
int32_t tsdbOpenCache(STsdb *pTsdb);
void    tsdbCloseCache(STsdb *pTsdb);
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
struct STsdbFS {
  SDelFile *pDelFile;
  SArray   *aDFileSet;  // SArray<SDFileSet>
};

struct STsdb {
  char          *path;
  SVnode        *pVnode;
  STsdbKeepCfg   keepCfg;
  TdThreadRwlock rwLock;
  SMemTable     *mem;
  SMemTable     *imem;
  STsdbFS        fs;
  SLRUCache     *lruCache;
  TdThreadMutex  lruMutex;
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
  int64_t           size;
  uint32_t          seed;
  int8_t            maxLevel;
  int8_t            level;
  SMemSkipListNode *pHead;
  SMemSkipListNode *pTail;
} SMemSkipList;

struct STbData {
  tb_uid_t     suid;
  tb_uid_t     uid;
  TSKEY        minKey;
  TSKEY        maxKey;
  SDelData    *pHead;
  SDelData    *pTail;
  SMemSkipList sl;
  STbData     *next;
};

struct SMemTable {
  SRWLatch         latch;
  STsdb           *pTsdb;
  SVBufPool       *pPool;
  volatile int32_t nRef;
  TSKEY            minKey;
  TSKEY            maxKey;
  int64_t          nRow;
  int64_t          nDel;
  struct {
    int32_t   nTbData;
    int32_t   nBucket;
    STbData **aBucket;
  };
};

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
  int64_t offset;
  int64_t size;
};

struct SMapData {
  int32_t  nItem;
  int32_t  nData;
  int32_t *aOffset;
  uint8_t *pData;
};

struct SBlockCol {
  int16_t cid;
  int8_t  type;
  int8_t  smaOn;
  int8_t  flag;      // HAS_NONE|HAS_NULL|HAS_VALUE
  int32_t szOrigin;  // original column value size (only save for variant data type)
  int32_t szBitmap;  // bitmap size, 0 only for flag == HAS_VAL
  int32_t szOffset;  // offset size, 0 only for non-variant-length type
  int32_t szValue;   // value size, 0 when flag == (HAS_NULL | HAS_NONE)
  int32_t offset;
};

struct SBlockInfo {
  int64_t offset;  // block data offset
  int32_t szBlock;
  int32_t szKey;
};

struct SSmaInfo {
  int64_t offset;
  int32_t size;
};

struct SBlock {
  TSDBKEY    minKey;
  TSDBKEY    maxKey;
  int64_t    minVer;
  int64_t    maxVer;
  int32_t    nRow;
  int8_t     hasDup;
  int8_t     nSubBlock;
  SBlockInfo aSubBlock[TSDB_MAX_SUBBLOCKS];
  SSmaInfo   smaInfo;
};

struct SBlockL {
  int64_t    suid;
  int64_t    minUid;
  int64_t    maxUid;
  TSKEY      minKey;
  TSKEY      maxKey;
  int64_t    minVer;
  int64_t    maxVer;
  int32_t    nRow;
  SBlockInfo bInfo;
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

// (SBlockData){.suid = 0, .uid = 0}: block data not initialized
// (SBlockData){.suid = suid, .uid = uid}: block data for ONE child table int .data file
// (SBlockData){.suid = suid, .uid = 0}: block data for N child tables int .last file
// (SBlockData){.suid = 0, .uid = uid}: block data for 1 normal table int .last/.data file
struct SBlockData {
  int64_t  suid;      // 0 means normal table block data, otherwise child table block data
  int64_t  uid;       // 0 means block data in .last file, otherwise in .data file
  int32_t  nRow;      // number of rows
  int64_t *aUid;      // uids of each row, only exist in block data in .last file (uid == 0)
  int64_t *aVersion;  // versions of each row
  TSKEY   *aTSKEY;    // timestamp of each row
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

struct SDiskDataHdr {
  uint32_t delimiter;
  uint32_t fmtVer;
  int64_t  suid;
  int64_t  uid;
  int32_t  szUid;
  int32_t  szVer;
  int32_t  szKey;
  int32_t  szBlkCol;
  int32_t  nRow;
  int8_t   cmprAlg;
};

struct SDelFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SHeadFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SDataFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
};

struct SLastFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SSmaFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
};

struct SDFileSet {
  SDiskID    diskId;
  int32_t    fid;
  SHeadFile *pHeadF;
  SDataFile *pDataF;
  SLastFile *pLastF;
  SSmaFile  *pSmaF;
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

struct SDelFWriter {
  STsdb    *pTsdb;
  SDelFile  fDel;
  TdFilePtr pWriteH;

  uint8_t *aBuf[1];
};

struct SDataFWriter {
  STsdb    *pTsdb;
  SDFileSet wSet;

  TdFilePtr pHeadFD;
  TdFilePtr pDataFD;
  TdFilePtr pLastFD;
  TdFilePtr pSmaFD;

  SHeadFile fHead;
  SDataFile fData;
  SLastFile fLast;
  SSmaFile  fSma;

  uint8_t *aBuf[4];
};

struct STsdbReadSnap {
  SMemTable *pMem;
  SMemTable *pIMem;
  STsdbFS    fs;
};

// ========== inline functions ==========
static FORCE_INLINE int32_t tsdbKeyCmprFn(const void *p1, const void *p2) {
  TSDBKEY *pKey1 = (TSDBKEY *)p1;
  TSDBKEY *pKey2 = (TSDBKEY *)p2;

  if (pKey1->ts < pKey2->ts) {
    return -1;
  } else if (pKey1->ts > pKey2->ts) {
    return 1;
  }

  if (pKey1->version < pKey2->version) {
    return -1;
  } else if (pKey1->version > pKey2->version) {
    return 1;
  }

  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_H_*/
