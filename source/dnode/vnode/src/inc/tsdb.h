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

typedef struct TSDBROW       TSDBROW;
typedef struct TSDBKEY       TSDBKEY;
typedef struct TABLEID       TABLEID;
typedef struct KEYINFO       KEYINFO;
typedef struct SDelData      SDelData;
typedef struct SDelIdx       SDelIdx;
typedef struct STbData       STbData;
typedef struct SMemTable     SMemTable;
typedef struct STbDataIter   STbDataIter;
typedef struct SMergeInfo    SMergeInfo;
typedef struct STable        STable;
typedef struct SMapData      SMapData;
typedef struct SBlockSMA     SBlockSMA;
typedef struct SBlockIdx     SBlockIdx;
typedef struct SBlock        SBlock;
typedef struct SBlockStatis  SBlockStatis;
typedef struct SAggrBlkCol   SAggrBlkCol;
typedef struct SColData      SColData;
typedef struct SBlockDataHdr SBlockDataHdr;
typedef struct SBlockData    SBlockData;
typedef struct SReadH        SReadH;

#define TSDB_MAX_SUBBLOCKS 8

#define HAS_NONE  ((int8_t)0x1)
#define HAS_NULL  ((int8_t)0x2)
#define HAS_VALUE ((int8_t)0x4)

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

// tsdbFile.c ==============================================================================================
typedef struct SDelFile       SDelFile;
typedef struct STsdbCacheFile STsdbCacheFile;
typedef struct STsdbIndexFile STsdbIndexFile;
typedef struct STsdbDataFile  STsdbDataFile;
typedef struct STsdbLastFile  STsdbLastFile;
typedef struct STsdbSmaFile   STsdbSmaFile;
typedef struct STsdbSmalFile  STsdbSmalFile;
typedef struct SDFileSet      SDFileSet;

// SDelFile
#define tsdbDelFileCreate() ((SDelFile){.info = KEYINFO_INIT_VAL, .size = 0, .offset = 0})
char *tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile);

// tsdbFS.c ==============================================================================================
typedef struct STsdbFS STsdbFS;

int32_t tsdbFSOpen(STsdb *pTsdb, STsdbFS **ppFS);
int32_t tsdbFSClose(STsdbFS *pFS);
int32_t tsdbFSStart(STsdbFS *pFS);
int32_t tsdbFSEnd(STsdbFS *pFS, int8_t rollback);

// tsdbReaderWriter.c ==============================================================================================

// SDataFWriter
typedef struct SDataFWriter SDataFWriter;

int32_t tsdbDataFWriterOpen(SDataFWriter **ppWriter, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFWriterClose(SDataFWriter *pWriter, int8_t sync);
int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter, uint8_t **ppBuf);
int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SMapData *pMapData, uint8_t **ppBuf);
int32_t tsdbWriteBlock(SDataFWriter *pWriter, SMapData *pMapData, uint8_t **ppBuf, SBlockIdx *pBlockIdx);
int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, uint8_t **ppBuf1, uint8_t **ppBuf2,
                           SBlockIdx *pBlockIdx, SBlock *pBlock);
int32_t tsdbWriteBlockSMA(SDataFWriter *pWriter, SBlockSMA *pBlockSMA, int64_t *rOffset, int64_t *rSize);

// SDataFReader
typedef struct SDataFReader SDataFReader;

int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFReaderClose(SDataFReader *pReader);
int32_t tsdbReadBlockIdx(SDataFReader *pReader, SMapData *pMapData, uint8_t **ppBuf);
int32_t tsdbReadBlock(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *pMapData, uint8_t **ppBuf);
int32_t tsdbReadBlockData(SDataFReader *pReader, SBlockIdx *pBlockIdx, SBlock *pBlock, SBlockData *pBlockData,
                          int16_t *aColId, int32_t nCol, uint8_t **ppBuf1, uint8_t **ppBuf2);
int32_t tsdbReadBlockSMA(SDataFReader *pReader, SBlockSMA *pBlkSMA);

// SDelFWriter
typedef struct SDelFWriter SDelFWriter;

int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb);
int32_t tsdbDelFWriterClose(SDelFWriter *pWriter, int8_t sync);
int32_t tsdbWriteDelData(SDelFWriter *pWriter, SMapData *pDelDataMap, uint8_t **ppBuf, SDelIdx *pDelIdx);
int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SMapData *pDelIdxMap, uint8_t **ppBuf);
int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter, uint8_t **ppBuf);

// SDelFReader
typedef struct SDelFReader SDelFReader;

int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb, uint8_t **ppBuf);
int32_t tsdbDelFReaderClose(SDelFReader *pReader);
int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SMapData *pDelDataMap, uint8_t **ppBuf);
int32_t tsdbReadDelIdx(SDelFReader *pReader, SMapData *pDelIdxMap, uint8_t **ppBuf);

// tsdbUtil.c ==============================================================================================
// TSDBROW
#define tsdbRowFromTSRow(VERSION, TSROW)      ((TSDBROW){.type = 0, .version = (VERSION), .pTSRow = (TSROW)});
#define tsdbRowFromBlockData(BLOCKDATA, IROW) ((TSDBROW){.type = 1, .pBlockData = (BLOCKDATA), .pTSRow = (IROW)});
TSDBKEY tsdbRowKey(TSDBROW *pRow);

int32_t tsdbKeyFid(TSKEY key, int32_t minutes, int8_t precision);
void    tsdbFidKeyRange(int32_t fid, int32_t minutes, int8_t precision, TSKEY *minKey, TSKEY *maxKey);

// memory
int32_t tsdbRealloc(uint8_t **ppBuf, int64_t size);
void    tsdbFree(uint8_t *pBuf);

// TABLEID
int32_t tTABLEIDCmprFn(const void *p1, const void *p2);

// TSDBKEY
int32_t tsdbKeyCmprFn(const void *p1, const void *p2);

int32_t tsdbBuildDeleteSkyline(SArray *aDelData, int32_t sidx, int32_t eidx, SArray *aSkyline);

// SColdata
int32_t tColDataCmprFn(const void *p1, const void *p2);

// KEYINFO
#define KEYINFO_INIT_VAL                  \
  ((KEYINFO){.maxKey.ts = TSKEY_MIN,      \
             .maxKey.version = 0,         \
             .minKey.ts = TSKEY_MAX,      \
             .minKey.version = INT64_MAX, \
             .minVerion = INT64_MAX,      \
             .maxVersion = 0})

int32_t tPutKEYINFO(uint8_t *p, KEYINFO *pKeyInfo);
int32_t tGetKEYINFO(uint8_t *p, KEYINFO *pKeyInfo);

// SMapData
void    tMapDataReset(SMapData *pMapData);
void    tMapDataClear(SMapData *pMapData);
int32_t tMapDataPutItem(SMapData *pMapData, void *pItem, int32_t (*tPutItemFn)(uint8_t *, void *));
int32_t tMapDataGetItemByIdx(SMapData *pMapData, int32_t idx, void *pItem, int32_t (*tGetItemFn)(uint8_t *, void *));
int32_t tPutMapData(uint8_t *p, SMapData *pMapData);
int32_t tGetMapData(uint8_t *p, SMapData *pMapData);

// SBlockIdx
int32_t tPutBlockIdx(uint8_t *p, void *ph);
int32_t tGetBlockIdx(uint8_t *p, void *ph);

// SBlock
#define BLOCK_INIT_VAL ((SBlock){})

int32_t tPutBlock(uint8_t *p, void *ph);
int32_t tGetBlock(uint8_t *p, void *ph);
int32_t tBlockCmprFn(const void *p1, const void *p2);

// SBlockCol
int32_t tPutBlockCol(uint8_t *p, void *ph);
int32_t tGetBlockCol(uint8_t *p, void *ph);

// SBlockData
#define tsdbBlockDataCreate() ((SBlockData){0})
void    tsdbBlockDataClear(SBlockData *pBlockData);
int32_t tsdbBlockDataAppendRow(SBlockData *pBlockData, TSDBROW *pRow, STSchema *pTSchema);
void    tsdbBlockDataDestroy(SBlockData *pBlockData);

// SDelIdx
int32_t tPutDelIdx(uint8_t *p, void *ph);
int32_t tGetDelIdx(uint8_t *p, void *ph);

// SDelData
int32_t tPutDelData(uint8_t *p, void *ph);
int32_t tGetDelData(uint8_t *p, void *ph);

int32_t tPutDelFileHdr(uint8_t *p, SDelFile *pDelFile);
int32_t tGetDelFileHdr(uint8_t *p, SDelFile *pDelFile);

// structs
typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

#define TSDB_DATA_DIR_LEN 6  // adapt accordingly
struct STsdb {
  char         *path;
  SVnode       *pVnode;
  TdThreadMutex mutex;
  char          dir[TSDB_DATA_DIR_LEN];
  bool          repoLocked;
  STsdbKeepCfg  keepCfg;
  SMemTable    *mem;
  SMemTable    *imem;
  SRtn          rtn;
  STsdbFS      *fs;
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

struct KEYINFO {
  TSDBKEY minKey;
  TSDBKEY maxKey;
  int64_t minVerion;
  int64_t maxVersion;
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
  KEYINFO      info;
  SDelData    *pHead;
  SDelData    *pTail;
  SMemSkipList sl;
};

struct SMemTable {
  SRWLatch latch;
  STsdb   *pTsdb;
  int32_t  nRef;
  KEYINFO  info;
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
  KEYINFO info;
  int64_t offset;
  int64_t size;
};

struct SMapData {
  int32_t  nItem;
  uint8_t  flag;
  uint8_t *pOfst;
  uint32_t nData;
  uint8_t *pData;
  uint8_t *pBuf;
};

typedef struct {
  int16_t cid;
  int8_t  type;
  int8_t  flag;
  int64_t offset;
  int64_t size;
} SBlockCol;

typedef struct {
  int64_t  offset;
  int64_t  ksize;
  int64_t  bsize;
  SMapData mBlockCol;  // SMapData<SBlockCol>
} SSubBlock;

struct SBlock {
  KEYINFO   info;
  int32_t   nRow;
  int8_t    last;
  int8_t    hasDup;
  int8_t    cmprAlg;
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
  int32_t  bytes;
  uint8_t  flags;
  uint8_t *pBitMap;
  uint32_t nData;
  uint8_t *pData;
};

struct SBlockData {
  int32_t   maxRow;
  int32_t   nRow;
  int64_t  *aVersion;
  TSKEY    *aTSKEY;
  int32_t   maxCol;
  int32_t   nCol;
  SColData *aColData;
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
  TSKEY    minKey;
  TSKEY    maxKey;
  int64_t  minVersion;
  int64_t  maxVersion;
  int64_t  offset;
  int64_t  size;
};

struct SDelFile {
  KEYINFO info;
  int64_t size;
  int64_t offset;
};

typedef struct {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SColSMA;

struct SBlockSMA {
  int32_t  nCol;
  SColSMA *aColSMA;
};

struct SBlockDataHdr {
  uint32_t delimiter;
  int64_t  suid;
  int64_t  uid;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_H_*/