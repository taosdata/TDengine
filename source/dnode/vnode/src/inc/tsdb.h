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

// #include "../tsdb/tsdbFile2.h"
// #include "../tsdb/tsdbMerge.h"
// #include "../tsdb/tsdbSttFileRW.h"
#include "tsimplehash.h"
#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// tsdbDebug ================
// clang-format off
#define tsdbFatal(...) do { if (tsdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TSD FATAL ", DEBUG_FATAL, 255,           __VA_ARGS__); }} while(0)
#define tsdbError(...) do { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TSD ERROR ", DEBUG_ERROR, 255,           __VA_ARGS__); }} while(0)
#define tsdbWarn(...)  do { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TSD WARN  ", DEBUG_WARN,  255,           __VA_ARGS__); }} while(0)
#define tsdbInfo(...)  do { if (tsdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TSD INFO  ", DEBUG_INFO,  255,           __VA_ARGS__); }} while(0)
#define tsdbDebug(...) do { if (tsdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSD DEBUG ", DEBUG_DEBUG, tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define tsdbTrace(...) do { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TSD TRACE ", DEBUG_TRACE, tsdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef struct TSDBROW          TSDBROW;
typedef struct TABLEID          TABLEID;
typedef struct TSDBKEY          TSDBKEY;
typedef struct SDelData         SDelData;
typedef struct SDelIdx          SDelIdx;
typedef struct STbData          STbData;
typedef struct SMemTable        SMemTable;
typedef struct STbDataIter      STbDataIter;
typedef struct SMapData         SMapData;
typedef struct SBlockIdx        SBlockIdx;
typedef struct SDataBlk         SDataBlk;
typedef struct SSttBlk          SSttBlk;
typedef struct SDiskDataHdr     SDiskDataHdr;
typedef struct SBlockData       SBlockData;
typedef struct SDelFile         SDelFile;
typedef struct SHeadFile        SHeadFile;
typedef struct SDataFile        SDataFile;
typedef struct SSttFile         SSttFile;
typedef struct SSmaFile         SSmaFile;
typedef struct SDFileSet        SDFileSet;
typedef struct SDataFWriter     SDataFWriter;
typedef struct SDataFReader     SDataFReader;
typedef struct SDelFWriter      SDelFWriter;
typedef struct SDelFReader      SDelFReader;
typedef struct STSDBRowIter     STSDBRowIter;
typedef struct STsdbFS          STsdbFS;
typedef struct SRowMerger       SRowMerger;
typedef struct STsdbReadSnap    STsdbReadSnap;
typedef struct SBlockInfo       SBlockInfo;
typedef struct SSmaInfo         SSmaInfo;
typedef struct SBlockCol        SBlockCol;
typedef struct SLDataIter       SLDataIter;
typedef struct SDiskCol         SDiskCol;
typedef struct SDiskData        SDiskData;
typedef struct SDiskDataBuilder SDiskDataBuilder;
typedef struct SBlkInfo         SBlkInfo;
typedef struct STsdbDataIter2   STsdbDataIter2;
typedef struct STsdbFilterInfo  STsdbFilterInfo;
typedef struct STFileSystem     STFileSystem;
typedef struct STsdbRowKey      STsdbRowKey;

#define TSDBROW_ROW_FMT ((int8_t)0x0)
#define TSDBROW_COL_FMT ((int8_t)0x1)

#define TSDB_FILE_DLMT ((uint32_t)0xF00AFA0F)
#define TSDB_FHDR_SIZE 512

#define VERSION_MIN 0
#define VERSION_MAX INT64_MAX

#define TSDBKEY_MIN ((TSDBKEY){.ts = TSKEY_MIN, .version = VERSION_MIN})
#define TSDBKEY_MAX ((TSDBKEY){.ts = TSKEY_MAX, .version = VERSION_MAX})

#define TABLE_SAME_SCHEMA(SUID1, UID1, SUID2, UID2) ((SUID1) ? (SUID1) == (SUID2) : (UID1) == (UID2))

#define PAGE_CONTENT_SIZE(PAGE) ((PAGE) - sizeof(TSCKSUM))
#define LOGIC_TO_FILE_OFFSET(LOFFSET, PAGE) \
  ((LOFFSET) / PAGE_CONTENT_SIZE(PAGE) * (PAGE) + (LOFFSET) % PAGE_CONTENT_SIZE(PAGE))
#define FILE_TO_LOGIC_OFFSET(OFFSET, PAGE) ((OFFSET) / (PAGE)*PAGE_CONTENT_SIZE(PAGE) + (OFFSET) % (PAGE))
#define PAGE_OFFSET(PGNO, PAGE)            (((PGNO)-1) * (PAGE))
#define OFFSET_PGNO(OFFSET, PAGE)          ((OFFSET) / (PAGE) + 1)

static FORCE_INLINE int64_t tsdbLogicToFileSize(int64_t lSize, int32_t szPage) {
  int64_t fOffSet = LOGIC_TO_FILE_OFFSET(lSize, szPage);
  int64_t pgno = OFFSET_PGNO(fOffSet, szPage);

  if (fOffSet % szPage == 0) {
    pgno--;
  }

  return pgno * szPage;
}

// tsdbUtil.c ==============================================================================================
// TSDBROW
#define TSDBROW_TS(ROW) (((ROW)->type == TSDBROW_ROW_FMT) ? (ROW)->pTSRow->ts : (ROW)->pBlockData->aTSKEY[(ROW)->iRow])
#define TSDBROW_VERSION(ROW) \
  (((ROW)->type == TSDBROW_ROW_FMT) ? (ROW)->version : (ROW)->pBlockData->aVersion[(ROW)->iRow])
#define TSDBROW_SVERSION(ROW)            ((ROW)->type == TSDBROW_ROW_FMT ? (ROW)->pTSRow->sver : -1)
#define TSDBROW_KEY(ROW)                 ((TSDBKEY){.version = TSDBROW_VERSION(ROW), .ts = TSDBROW_TS(ROW)})
#define tsdbRowFromTSRow(VERSION, TSROW) ((TSDBROW){.type = TSDBROW_ROW_FMT, .version = (VERSION), .pTSRow = (TSROW)})
#define tsdbRowFromBlockData(BLOCKDATA, IROW) \
  ((TSDBROW){.type = TSDBROW_COL_FMT, .pBlockData = (BLOCKDATA), .iRow = (IROW)})

#define TSDBROW_INIT_KEY(_ROW, _KEY)                             \
  {                                                              \
    if ((_ROW)->type == TSDBROW_ROW_FMT) {                       \
      _KEY.version = (_ROW)->version;                            \
      _KEY.ts = (_ROW)->pTSRow->ts;                              \
    } else {                                                     \
      _KEY.version = (_ROW)->pBlockData->aVersion[(_ROW)->iRow]; \
      _KEY.ts = (_ROW)->pBlockData->aTSKEY[(_ROW)->iRow];        \
    }                                                            \
  }

#define tColRowGetKey(_pBlock, _irow, _key)             \
  {                                                     \
    (_key)->ts = (_pBlock)->aTSKEY[(_irow)];            \
    (_key)->numOfPKs = 0;                               \
    if ((_pBlock)->nColData > 0) {                      \
      tColRowGetPrimaryKey((_pBlock), (_irow), (_key)); \
    }                                                   \
  }

void    tsdbRowGetColVal(TSDBROW *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal);
int32_t tsdbRowCompare(const void *p1, const void *p2);
int32_t tsdbRowCompareWithoutVersion(const void *p1, const void *p2);
int32_t tsdbRowKeyCmpr(const STsdbRowKey *key1, const STsdbRowKey *key2);
void    tsdbRowGetKey(TSDBROW *row, STsdbRowKey *key);
void    tColRowGetPrimaryKey(SBlockData *pBlock, int32_t irow, SRowKey *key);

// STSDBRowIter
int32_t  tsdbRowIterOpen(STSDBRowIter *pIter, TSDBROW *pRow, STSchema *pTSchema);
void     tsdbRowClose(STSDBRowIter *pIter);
SColVal *tsdbRowIterNext(STSDBRowIter *pIter);

// SRowMerger
int32_t tsdbRowMergerInit(SRowMerger *pMerger, STSchema *pSchema);
int32_t tsdbRowMergerAdd(SRowMerger *pMerger, TSDBROW *pRow, STSchema *pTSchema);
int32_t tsdbRowMergerGetRow(SRowMerger *pMerger, SRow **ppRow);
void    tsdbRowMergerClear(SRowMerger *pMerger);
void    tsdbRowMergerCleanup(SRowMerger *pMerger);

// TABLEID
int32_t tTABLEIDCmprFn(const void *p1, const void *p2);
// TSDBKEY
#define MIN_TSDBKEY(KEY1, KEY2) ((tsdbKeyCmprFn(&(KEY1), &(KEY2)) < 0) ? (KEY1) : (KEY2))
#define MAX_TSDBKEY(KEY1, KEY2) ((tsdbKeyCmprFn(&(KEY1), &(KEY2)) > 0) ? (KEY1) : (KEY2))
// SBlockCol
int32_t tPutBlockCol(SBuffer *buffer, const SBlockCol *pBlockCol, int32_t ver, uint32_t cmprAlg);
int32_t tGetBlockCol(SBufferReader *br, SBlockCol *pBlockCol, int32_t ver, uint32_t cmprAlg);
int32_t tBlockColCmprFn(const void *p1, const void *p2);
// SDataBlk
void    tDataBlkReset(SDataBlk *pBlock);
int32_t tPutDataBlk(uint8_t *p, void *ph);
int32_t tGetDataBlk(uint8_t *p, void *ph);
int32_t tDataBlkCmprFn(const void *p1, const void *p2);
bool    tDataBlkHasSma(SDataBlk *pDataBlk);
// SSttBlk
int32_t tPutSttBlk(uint8_t *p, void *ph);
int32_t tGetSttBlk(uint8_t *p, void *ph);
// SBlockIdx
int32_t tPutBlockIdx(uint8_t *p, void *ph);
int32_t tGetBlockIdx(uint8_t *p, void *ph);
int32_t tCmprBlockIdx(void const *lhs, void const *rhs);
int32_t tCmprBlockL(void const *lhs, void const *rhs);
// SBlockData
#define tBlockDataFirstRow(PBLOCKDATA)             tsdbRowFromBlockData(PBLOCKDATA, 0)
#define tBlockDataLastRow(PBLOCKDATA)              tsdbRowFromBlockData(PBLOCKDATA, (PBLOCKDATA)->nRow - 1)
#define tBlockDataFirstKey(PBLOCKDATA)             TSDBROW_KEY(&tBlockDataFirstRow(PBLOCKDATA))
#define tBlockDataLastKey(PBLOCKDATA)              TSDBROW_KEY(&tBlockDataLastRow(PBLOCKDATA))
#define tBlockDataGetColDataByIdx(PBLOCKDATA, IDX) (&(PBLOCKDATA)->aColData[IDX])

int32_t tBlockDataCreate(SBlockData *pBlockData);
void    tBlockDataDestroy(SBlockData *pBlockData);
int32_t tBlockDataInit(SBlockData *pBlockData, TABLEID *pId, STSchema *pTSchema, int16_t *aCid, int32_t nCid);
void    tBlockDataReset(SBlockData *pBlockData);
int32_t tBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema, int64_t uid);
int32_t tBlockDataUpdateRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema);
int32_t tBlockDataTryUpsertRow(SBlockData *pBlockData, TSDBROW *pRow, int64_t uid);
int32_t tBlockDataUpsertRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema, int64_t uid);
void    tBlockDataClear(SBlockData *pBlockData);
int32_t tBlockDataCompress(SBlockData *bData, void *pCmprInfo, SBuffer *buffers, SBuffer *assist);
int32_t tBlockDataDecompress(SBufferReader *br, SBlockData *blockData, SBuffer *assist);
int32_t tBlockDataDecompressKeyPart(const SDiskDataHdr *hdr, SBufferReader *br, SBlockData *blockData, SBuffer *assist);
int32_t tBlockDataDecompressColData(const SDiskDataHdr *hdr, const SBlockCol *blockCol, SBufferReader *br,
                                    SBlockData *blockData, SBuffer *assist);

SColData *tBlockDataGetColData(SBlockData *pBlockData, int16_t cid);
int32_t   tBlockDataAddColData(SBlockData *pBlockData, int16_t cid, int8_t type, int8_t cflag, SColData **ppColData);
// SDiskDataHdr
int32_t tPutDiskDataHdr(SBuffer *buffer, const SDiskDataHdr *pHdr);
int32_t tGetDiskDataHdr(SBufferReader *br, SDiskDataHdr *pHdr);
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
int32_t tMapDataCopy(SMapData *pFrom, SMapData *pTo);
void    tMapDataGetItemByIdx(SMapData *pMapData, int32_t idx, void *pItem, int32_t (*tGetItemFn)(uint8_t *, void *));
int32_t tMapDataSearch(SMapData *pMapData, void *pSearchItem, int32_t (*tGetItemFn)(uint8_t *, void *),
                       int32_t (*tItemCmprFn)(const void *, const void *), void *pItem);
int32_t tPutMapData(uint8_t *p, SMapData *pMapData);
int32_t tGetMapData(uint8_t *p, SMapData *pMapData, int32_t *decodedSize);
int32_t tMapDataToArray(SMapData *pMapData, int32_t itemSize, int32_t (*tGetItemFn)(uint8_t *, void *),
                        SArray **ppArray);
// other
int32_t tsdbKeyFid(TSKEY key, int32_t minutes, int8_t precision);
void    tsdbFidKeyRange(int32_t fid, int32_t minutes, int8_t precision, TSKEY *minKey, TSKEY *maxKey);
int32_t tsdbFidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int64_t nowSec);
int32_t tsdbBuildDeleteSkyline(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline);
int32_t tPutColumnDataAgg(SBuffer *buffer, SColumnDataAgg *pColAgg);
int32_t tGetColumnDataAgg(SBufferReader *br, SColumnDataAgg *pColAgg);
int32_t tRowInfoCmprFn(const void *p1, const void *p2);
// tsdbMemTable ==============================================================================================
// SMemTable
int32_t  tsdbMemTableCreate(STsdb *pTsdb, SMemTable **ppMemTable);
void     tsdbMemTableDestroy(SMemTable *pMemTable, bool proactive);
STbData *tsdbGetTbDataFromMemTable(SMemTable *pMemTable, tb_uid_t suid, tb_uid_t uid);
int32_t  tsdbRefMemTable(SMemTable *pMemTable, SQueryNode *pQNode);
void     tsdbUnrefMemTable(SMemTable *pMemTable, SQueryNode *pNode, bool proactive);
// STbDataIter
int32_t tsdbTbDataIterCreate(STbData *pTbData, STsdbRowKey *pFrom, int8_t backward, STbDataIter **ppIter);
void   *tsdbTbDataIterDestroy(STbDataIter *pIter);
void    tsdbTbDataIterOpen(STbData *pTbData, STsdbRowKey *pFrom, int8_t backward, STbDataIter *pIter);
bool    tsdbTbDataIterNext(STbDataIter *pIter);
void    tsdbMemTableCountRows(SMemTable *pMemTable, SSHashObj *pTableMap, int64_t *rowsNum);
int32_t tsdbMemTableSaveToCache(SMemTable *pMemTable, void *func);

// STbData
int32_t tsdbGetNRowsInTbData(STbData *pTbData);
// tsdbFile.c ==============================================================================================
typedef enum { TSDB_HEAD_FILE = 0, TSDB_DATA_FILE, TSDB_LAST_FILE, TSDB_SMA_FILE } EDataFileT;

bool    tsdbDelFileIsSame(SDelFile *pDelFile1, SDelFile *pDelFile2);
int32_t tPutHeadFile(uint8_t *p, SHeadFile *pHeadFile);
int32_t tPutDataFile(uint8_t *p, SDataFile *pDataFile);
int32_t tPutSttFile(uint8_t *p, SSttFile *pSttFile);
int32_t tPutSmaFile(uint8_t *p, SSmaFile *pSmaFile);
int32_t tPutDelFile(uint8_t *p, SDelFile *pDelFile);
int32_t tGetDelFile(uint8_t *p, SDelFile *pDelFile);
int32_t tPutDFileSet(uint8_t *p, SDFileSet *pSet);
int32_t tGetDFileSet(uint8_t *p, SDFileSet *pSet);

void tsdbHeadFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SHeadFile *pHeadF, char fname[]);
void tsdbDataFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SDataFile *pDataF, char fname[]);
void tsdbSttFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SSttFile *pSttF, char fname[]);
void tsdbSmaFileName(STsdb *pTsdb, SDiskID did, int32_t fid, SSmaFile *pSmaF, char fname[]);

// SDelFile
void tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile, char fname[]);
// tsdbFS.c ==============================================================================================
int32_t tsdbFSOpen(STsdb *pTsdb, int8_t rollback);
int32_t tsdbFSClose(STsdb *pTsdb);
void    tsdbGetCurrentFName(STsdb *pTsdb, char *current, char *current_t);
// tsdbReaderWriter.c ==============================================================================================
// SDataFReader
int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet);
void    tsdbDataFReaderClose(SDataFReader **ppReader);
int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx);
int32_t tsdbReadDataBlk(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mDataBlk);
int32_t tsdbReadSttBlk(SDataFReader *pReader, int32_t iStt, SArray *aSttBlk);
// SDelFReader
int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb);
void    tsdbDelFReaderClose(SDelFReader **ppReader);
int32_t tsdbReadDelDatav1(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData, int64_t maxVer);
int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData);
int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx);

// tsdbRead.c ==============================================================================================
int32_t tsdbTakeReadSnap2(STsdbReader *pReader, _query_reseek_func_t reseek, STsdbReadSnap **ppSnap, const char *id);
void    tsdbUntakeReadSnap2(STsdbReader *pReader, STsdbReadSnap *pSnap, bool proactive);
int32_t tsdbGetTableSchema(SMeta *pMeta, int64_t uid, STSchema **pSchema, int64_t *suid);

// tsdbMerge.c ==============================================================================================
typedef struct {
  STsdb  *tsdb;
  int32_t fid;
} SMergeArg;

int32_t tsdbMerge(void *arg);

// tsdbDataIter.c ==============================================================================================
#define TSDB_MEM_TABLE_DATA_ITER 0
#define TSDB_DATA_FILE_DATA_ITER 1
#define TSDB_STT_FILE_DATA_ITER  2
#define TSDB_TOMB_FILE_DATA_ITER 3

#define TSDB_FILTER_FLAG_BY_VERSION           0x1
#define TSDB_FILTER_FLAG_BY_TABLEID           0x2
#define TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE 0x4

#define TSDB_RBTN_TO_DATA_ITER(pNode) ((STsdbDataIter2 *)(((char *)pNode) - offsetof(STsdbDataIter2, rbtn)))
/* open */
int32_t tsdbOpenDataFileDataIter(SDataFReader *pReader, STsdbDataIter2 **ppIter);
int32_t tsdbOpenSttFileDataIter(SDataFReader *pReader, int32_t iStt, STsdbDataIter2 **ppIter);
int32_t tsdbOpenTombFileDataIter(SDelFReader *pReader, STsdbDataIter2 **ppIter);
/* close */
void tsdbCloseDataIter2(STsdbDataIter2 *pIter);
/* cmpr */
int32_t tsdbDataIterCmprFn(const SRBTreeNode *pNode1, const SRBTreeNode *pNode2);
/* next */
int32_t tsdbDataIterNext2(STsdbDataIter2 *pIter, STsdbFilterInfo *pFilterInfo);

// structs =======================
struct STsdbFS {
  SDelFile *pDelFile;
  SArray   *aDFileSet;  // SArray<SDFileSet>
};

typedef struct {
  rocksdb_t                           *db;
  rocksdb_comparator_t                *my_comparator;
  rocksdb_block_based_table_options_t *tableoptions;
  rocksdb_options_t                   *options;
  rocksdb_flushoptions_t              *flushoptions;
  rocksdb_writeoptions_t              *writeoptions;
  rocksdb_readoptions_t               *readoptions;
  rocksdb_writebatch_t                *writebatch;
  TdThreadMutex                        writeBatchMutex;
  int32_t                              sver;
  tb_uid_t                             suid;
  tb_uid_t                             uid;
  STSchema                            *pTSchema;
  SArray                              *ctxArray;
} SRocksCache;

typedef struct {
  STsdb *pTsdb;
  int    flush_count;
} SCacheFlushState;

typedef struct SCompMonitor SCompMonitor;

struct STsdb {
  char                *path;
  SVnode              *pVnode;
  STsdbKeepCfg         keepCfg;
  TdThreadMutex        mutex;
  bool                 bgTaskDisabled;
  SMemTable           *mem;
  SMemTable           *imem;
  STsdbFS              fs;  // old
  SLRUCache           *lruCache;
  TdThreadMutex        lruMutex;
  SLRUCache           *biCache;
  TdThreadMutex        biMutex;
  SLRUCache           *bCache;
  TdThreadMutex        bMutex;
  SLRUCache           *pgCache;
  TdThreadMutex        pgMutex;
  struct STFileSystem *pFS;  // new
  SRocksCache          rCache;
  SCompMonitor        *pCompMonitor;
  struct {
    SVHashTable *ht;
    SArray      *arr;
  } *commitInfo;
};

struct TSDBKEY {
  int64_t version;
  TSKEY   ts;
};

typedef struct SMemSkipListNode SMemSkipListNode;
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
  SRWLatch     lock;
  SDelData    *pHead;
  SDelData    *pTail;
  SMemSkipList sl;
  STbData     *next;
  SRBTreeNode  rbtn[1];
};

struct SMemTable {
  SRWLatch         latch;
  STsdb           *pTsdb;
  SVBufPool       *pPool;
  volatile int32_t nRef;
  int64_t          minVer;
  int64_t          maxVer;
  TSKEY            minKey;
  TSKEY            maxKey;
  int64_t          nRow;
  int64_t          nDel;
  int32_t          nTbData;
  int32_t          nBucket;
  STbData        **aBucket;
  SRBTree          tbDataTree[1];
};

struct TSDBROW {
  int8_t type;  // TSDBROW_ROW_FMT for row from tsRow, TSDBROW_COL_FMT for row from block data
  union {
    struct {
      int64_t version;
      SRow   *pTSRow;
    };
    struct {
      SBlockData *pBlockData;
      int32_t     iRow;
    };
  };
};

struct SMemSkipListNode {
  int8_t            level;
  TSDBROW           row;
  SMemSkipListNode *forwards[0];
};

struct STsdbRowKey {
  SRowKey key;
  int64_t version;
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
  int16_t  cid;
  int8_t   type;
  int8_t   cflag;
  int8_t   flag;      // HAS_NONE|HAS_NULL|HAS_VALUE
  int32_t  szOrigin;  // original column value size (only save for variant data type)
  int32_t  szBitmap;  // bitmap size, 0 only for flag == HAS_VAL
  int32_t  szOffset;  // offset size, 0 only for non-variant-length type
  int32_t  szValue;   // value size, 0 when flag == (HAS_NULL | HAS_NONE)
  int32_t  offset;
  uint32_t alg;
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

struct SBlkInfo {
  int64_t minUid;
  int64_t maxUid;
  TSKEY   minKey;
  TSKEY   maxKey;
  int64_t minVer;
  int64_t maxVer;
  TSDBKEY minTKey;
  TSDBKEY maxTKey;
};

struct SDataBlk {
  TSDBKEY    minKey;
  TSDBKEY    maxKey;
  int64_t    minVer;
  int64_t    maxVer;
  int32_t    nRow;
  int8_t     hasDup;
  int8_t     nSubBlock;
  SBlockInfo aSubBlock[1];
  SSmaInfo   smaInfo;
};

struct SSttBlk {
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

// (SBlockData){.suid = 0, .uid = 0}: block data not initialized
// (SBlockData){.suid = suid, .uid = uid}: block data for ONE child table int .data file
// (SBlockData){.suid = suid, .uid = 0}: block data for N child tables int .last file
// (SBlockData){.suid = 0, .uid = uid}: block data for 1 normal table int .last/.data file
struct SBlockData {
  int64_t   suid;      // 0 means normal table block data, otherwise child table block data
  int64_t   uid;       // 0 means block data in .last file, otherwise in .data file
  int32_t   nRow;      // number of rows
  int64_t  *aUid;      // uids of each row, only exist in block data in .last file (uid == 0)
  int64_t  *aVersion;  // versions of each row
  TSKEY    *aTSKEY;    // timestamp of each row
  int32_t   nColData;
  SColData *aColData;
};

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
  uint32_t cmprAlg;

  // fmtVer == 1
  int8_t    numOfPKs;
  SBlockCol primaryBlockCols[TD_MAX_PK_COLS];
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

struct SSttFile {
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
  SSmaFile  *pSmaF;
  uint8_t    nSttF;
  SSttFile  *aSttF[TSDB_STT_TRIGGER_ARRAY_SIZE];
};

struct STSDBRowIter {
  TSDBROW *pRow;
  union {
    SRowIter *pIter;
    struct {
      int32_t iColData;
      SColVal cv;
    };
  };
};
struct SRowMerger {
  STSchema *pTSchema;
  int64_t   version;
  SArray   *pArray;  // SArray<SColVal>
};

typedef struct {
  char       *path;
  int32_t     szPage;
  int32_t     flag;
  TdFilePtr   pFD;
  int64_t     pgno;
  uint8_t    *pBuf;
  int64_t     szFile;
  STsdb      *pTsdb;
  const char *objName;
  uint8_t     s3File;
  int32_t     lcn;
  int32_t     fid;
  int64_t     cid;
  int64_t     blkno;
} STsdbFD;

struct SDelFWriter {
  STsdb   *pTsdb;
  SDelFile fDel;
  STsdbFD *pWriteH;
  uint8_t *aBuf[1];
};

#include "tarray2.h"
typedef struct STFileSet STFileSet;
typedef TARRAY2(STFileSet *) TFileSetArray;

// fset range
typedef struct STFileSetRange STFileSetRange;
typedef TARRAY2(STFileSetRange *) TFileSetRangeArray;  // disjoint ranges

void tsdbTFileSetRangeClear(STFileSetRange **fsr);
void tsdbTFileSetRangeArrayDestroy(TFileSetRangeArray **ppArr);

// fset partition
enum {
  TSDB_FSET_RANGE_TYP_HEAD = 0,
  TSDB_FSET_RANGE_TYP_DATA,
  TSDB_FSET_RANGE_TYP_SMA,
  TSDB_FSET_RANGE_TYP_TOMB,
  TSDB_FSET_RANGE_TYP_STT,
  TSDB_FSET_RANGE_TYP_MAX,
};

typedef TARRAY2(SVersionRange) SVerRangeList;

struct STsdbFSetPartition {
  int64_t       fid;
  int8_t        stat;
  SVerRangeList verRanges[TSDB_FSET_RANGE_TYP_MAX];
};

typedef struct STsdbFSetPartition STsdbFSetPartition;
typedef TARRAY2(STsdbFSetPartition *) STsdbFSetPartList;

STsdbFSetPartList *tsdbFSetPartListCreate();
void               tsdbFSetPartListDestroy(STsdbFSetPartList **ppList);
int32_t            tDeserializeTsdbFSetPartList(void *buf, int32_t bufLen, STsdbFSetPartList *pList);
int32_t            tsdbFSetPartListToRangeDiff(STsdbFSetPartList *pList, TFileSetRangeArray **ppRanges);

// snap rep format
typedef enum ETsdbRepFmt {
  TSDB_SNAP_REP_FMT_DEFAULT = 0,
  TSDB_SNAP_REP_FMT_RAW,
  TSDB_SNAP_REP_FMT_HYBRID,
} ETsdbRepFmt;

typedef struct STsdbRepOpts {
  ETsdbRepFmt format;
} STsdbRepOpts;

int32_t tSerializeTsdbRepOpts(void *buf, int32_t bufLen, STsdbRepOpts *pInfo);
int32_t tDeserializeTsdbRepOpts(void *buf, int32_t bufLen, STsdbRepOpts *pInfo);

// snap read
struct STsdbReadSnap {
  SMemTable     *pMem;
  SQueryNode    *pNode;
  SMemTable     *pIMem;
  SQueryNode    *pINode;
  TFileSetArray *pfSetArray;
};

struct SDataFWriter {
  STsdb    *pTsdb;
  SDFileSet wSet;

  STsdbFD *pHeadFD;
  STsdbFD *pDataFD;
  STsdbFD *pSmaFD;
  STsdbFD *pSttFD;

  SHeadFile fHead;
  SDataFile fData;
  SSmaFile  fSma;
  SSttFile  fStt[TSDB_STT_TRIGGER_ARRAY_SIZE];

  uint8_t *aBuf[4];
};

struct SDataFReader {
  STsdb     *pTsdb;
  SDFileSet *pSet;
  STsdbFD   *pHeadFD;
  STsdbFD   *pDataFD;
  STsdbFD   *pSmaFD;
  STsdbFD   *aSttFD[TSDB_STT_TRIGGER_ARRAY_SIZE];
  uint8_t   *aBuf[3];
};

// NOTE: do NOT change the order of the fields
typedef struct {
  int64_t suid;
  int64_t uid;
  TSDBROW row;
} SRowInfo;

typedef struct SSttBlockLoadCostInfo {
  int64_t loadBlocks;
  int64_t loadStatisBlocks;
  double  blockElapsedTime;
  double  statisElapsedTime;
} SSttBlockLoadCostInfo;

typedef struct SBlockDataInfo {
  SBlockData data;
  bool       pin;
  int32_t    sttBlockIndex;
} SBlockDataInfo;

// todo: move away
typedef struct {
  SArray *pUid;
  SArray *pFirstTs;
  SArray *pLastTs;
  SArray *pCount;
  SArray *pFirstKey;
  SArray *pLastKey;
} SSttTableRowsInfo;

typedef struct SSttBlockLoadInfo {
  SBlockDataInfo        blockData[2];  // buffered block data
  SArray               *aSttBlk;
  int32_t               currentLoadBlockIndex;
  STSchema             *pSchema;
  int16_t              *colIds;
  int32_t               numOfCols;
  bool                  checkRemainingRow;  // todo: no assign value?
  bool                  isLast;
  bool                  sttBlockLoaded;
  SSttTableRowsInfo     info;
  SSttBlockLoadCostInfo cost;
} SSttBlockLoadInfo;

typedef struct SMergeTree {
  int8_t      backward;
  SRBTree     rbt;
  SLDataIter *pIter;
  SLDataIter *pPinnedBlockIter;
  const char *idStr;
  bool        ignoreEarlierTs;
} SMergeTree;

typedef struct {
  int64_t   suid;
  int64_t   uid;
  STSchema *pTSchema;
} SSkmInfo;

struct SDiskCol {
  SBlockCol      bCol;
  const uint8_t *pBit;
  const uint8_t *pOff;
  const uint8_t *pVal;
  SColumnDataAgg agg;
};

struct SDiskData {
  SDiskDataHdr   hdr;
  const uint8_t *pUid;
  const uint8_t *pVer;
  const uint8_t *pKey;
  SArray        *aDiskCol;  // SArray<SDiskCol>
};

struct SDiskDataBuilder {
  int64_t      suid;
  int64_t      uid;
  int32_t      nRow;
  uint8_t      cmprAlg;
  uint8_t      calcSma;
  SCompressor *pUidC;
  SCompressor *pVerC;
  SCompressor *pKeyC;
  int32_t      nBuilder;
  SArray      *aBuilder;  // SArray<SDiskColBuilder>
  uint8_t     *aBuf[2];
  SDiskData    dd;
  SBlkInfo     bi;
};

struct SLDataIter {
  SRBTreeNode            node;
  SSttBlk               *pSttBlk;
  int64_t                cid;  // for debug purpose
  int8_t                 backward;
  int32_t                iSttBlk;
  int32_t                iRow;
  SRowInfo               rInfo;
  uint64_t               uid;
  STimeWindow            timeWindow;
  SVersionRange          verRange;
  SSttBlockLoadInfo     *pBlockLoadInfo;
  SRowKey               *pStartRowKey;  // current row key
  bool                   ignoreEarlierTs;
  struct SSttFileReader *pReader;
};

#define tMergeTreeGetRow(_t) (&((_t)->pIter->rInfo.row))

struct SSttFileReader;
typedef int32_t (*_load_tomb_fn)(STsdbReader *pReader, struct SSttFileReader *pSttFileReader,
                                 SSttBlockLoadInfo *pLoadInfo);

typedef struct SMergeTreeConf {
  int8_t        backward;
  STsdb        *pTsdb;
  uint64_t      suid;
  uint64_t      uid;
  STimeWindow   timewindow;
  SVersionRange verRange;
  bool          strictTimeRange;
  SArray       *pSttFileBlockIterArray;
  void         *pCurrentFileset;
  STSchema     *pSchema;
  int16_t      *pCols;
  int32_t       numOfCols;
  SRowKey      *pCurRowKey;
  _load_tomb_fn loadTombFn;
  void         *pReader;
  void         *idstr;
  bool          rspRows;  // response the rows in stt-file, if possible
} SMergeTreeConf;

typedef struct SSttDataInfoForTable {
  SArray *pKeyRangeList;
  int64_t numOfRows;
} SSttDataInfoForTable;

int32_t tMergeTreeOpen2(SMergeTree *pMTree, SMergeTreeConf *pConf, SSttDataInfoForTable *pTableInfo);
void    tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter);
int32_t tMergeTreeNext(SMergeTree *pMTree, bool *pHasNext);
void    tMergeTreePinSttBlock(SMergeTree *pMTree);
void    tMergeTreeUnpinSttBlock(SMergeTree *pMTree);
bool    tMergeTreeIgnoreEarlierTs(SMergeTree *pMTree);
void    tMergeTreeClose(SMergeTree *pMTree);

int32_t tCreateSttBlockLoadInfo(STSchema *pSchema, int16_t *colList, int32_t numOfCols, SSttBlockLoadInfo **pInfo);
void    destroySttBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo);
void    destroySttBlockReader(SArray *pLDataIterArray, SSttBlockLoadCostInfo *pLoadCost);

// tsdbCache ==============================================================================================
typedef enum {
  READER_EXEC_DATA = 0x1,
  READER_EXEC_ROWS = 0x2,
} EExecMode;

#define LAST_COL_VERSION_1 (0x1)  // add primary key, version
#define LAST_COL_VERSION_2 (0x2)  // add cache status
#define LAST_COL_VERSION   LAST_COL_VERSION_2

typedef enum {
  TSDB_LAST_CACHE_VALID = 0,  // last_cache has valid data
  TSDB_LAST_CACHE_NO_CACHE,   // last_cache has no data, but tsdb may have data
} ELastCacheStatus;

typedef struct {
  SRowKey          rowKey;
  int8_t           dirty;
  SColVal          colVal;
  ELastCacheStatus cacheStatus;
} SLastCol;

typedef struct {
  int8_t      lflag;
  STsdbRowKey tsdbRowKey;
  SColVal     colVal;
} SLastUpdateCtx;

int32_t tsdbOpenCache(STsdb *pTsdb);
void    tsdbCloseCache(STsdb *pTsdb);
int32_t tsdbCacheRowFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, int64_t version, int32_t nRow, SRow **aRow);
int32_t tsdbCacheColFormatUpdate(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, SBlockData *pBlockData);
int32_t tsdbCacheDel(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey);

int32_t tsdbCacheInsertLast(SLRUCache *pCache, tb_uid_t uid, TSDBROW *row, STsdb *pTsdb);
int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, STsdb *pTsdb, tb_uid_t uid, TSDBROW *row, bool dup);
void    tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h);

int32_t tsdbCacheGetBlockIdx(SLRUCache *pCache, SDataFReader *pFileReader, LRUHandle **handle);
int32_t tsdbBICacheRelease(SLRUCache *pCache, LRUHandle *h);

int32_t tsdbCacheGetBlockS3(SLRUCache *pCache, STsdbFD *pFD, LRUHandle **handle);
int32_t tsdbCacheGetPageS3(SLRUCache *pCache, STsdbFD *pFD, int64_t pgno, LRUHandle **handle);
void    tsdbCacheSetPageS3(SLRUCache *pCache, STsdbFD *pFD, int64_t pgno, uint8_t *pPage);

int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);
int32_t tsdbCacheDeleteLast(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);
int32_t tsdbCacheDelete(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);

int32_t tsdbGetS3Size(STsdb *tsdb, int64_t *size);

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

// #define SL_NODE_FORWARD(n, l)  ((n)->forwards[l])
// #define SL_NODE_BACKWARD(n, l) ((n)->forwards[(n)->level + (l)])

static FORCE_INLINE TSDBROW *tsdbTbDataIterGet(STbDataIter *pIter) {
  if (pIter == NULL) return NULL;

  if (pIter->pRow) {
    return pIter->pRow;
  }

  if (pIter->backward) {
    if (pIter->pNode == pIter->pTbData->sl.pHead) {
      return NULL;
    }
  } else {
    if (pIter->pNode == pIter->pTbData->sl.pTail) {
      return NULL;
    }
  }

  pIter->pRow = &pIter->row;
  pIter->row = pIter->pNode->row;

  return pIter->pRow;
}

typedef struct {
  int64_t  suid;
  int64_t  uid;
  SDelData delData;
} SDelInfo;

struct STsdbDataIter2 {
  STsdbDataIter2 *next;
  SRBTreeNode     rbtn;

  int32_t  type;
  SRowInfo rowInfo;
  SDelInfo delInfo;
  union {
    // TSDB_MEM_TABLE_DATA_ITER
    struct {
      SMemTable *pMemTable;
    } mIter;

    // TSDB_DATA_FILE_DATA_ITER
    struct {
      SDataFReader *pReader;
      SArray       *aBlockIdx;  // SArray<SBlockIdx>
      SMapData      mDataBlk;
      SBlockData    bData;
      int32_t       iBlockIdx;
      int32_t       iDataBlk;
      int32_t       iRow;
    } dIter;

    // TSDB_STT_FILE_DATA_ITER
    struct {
      SDataFReader *pReader;
      int32_t       iStt;
      SArray       *aSttBlk;
      SBlockData    bData;
      int32_t       iSttBlk;
      int32_t       iRow;
    } sIter;
    // TSDB_TOMB_FILE_DATA_ITER
    struct {
      SDelFReader *pReader;
      SArray      *aDelIdx;
      SArray      *aDelData;
      int32_t      iDelIdx;
      int32_t      iDelData;
    } tIter;
  };
};

struct STsdbFilterInfo {
  int32_t flag;
  int64_t sver;
  int64_t ever;
  TABLEID tbid;
};

typedef enum {
  TSDB_FS_STATE_NORMAL = 0,
  TSDB_FS_STATE_INCOMPLETE,
} ETsdbFsState;

// utils
ETsdbFsState tsdbSnapGetFsState(SVnode *pVnode);
int32_t      tsdbSnapPrepDescription(SVnode *pVnode, SSnapshot *pSnap);

void tsdbRemoveFile(const char *path);

#define taosCloseFileWithLog(fd)         \
  do {                                   \
    if (taosCloseFile(fd) < 0) {         \
      tsdbTrace("failed to close file"); \
    }                                    \
  } while (0)

int32_t tsdbAllocateDisk(STsdb *tsdb, const char *label, int32_t expLevel, SDiskID *diskId);
int32_t tsdbAllocateDiskAtLevel(STsdb *tsdb, int32_t level, const char *label, SDiskID *diskId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_H_*/
