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

#ifndef TDENGINE_TSDBREADUTIL_H
#define TDENGINE_TSDBREADUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tsdbDataFileRW.h"
#include "tsdbUtil2.h"
#include "storageapi.h"

#define ASCENDING_TRAVERSE(o) (o == TSDB_ORDER_ASC)

#define INIT_TIMEWINDOW(_w) \
  do {                      \
    (_w)->skey = INT64_MAX; \
    (_w)->ekey = INT64_MIN; \
  } while (0);

typedef enum {
  READER_STATUS_SUSPEND = 0x1,
  READER_STATUS_NORMAL = 0x2,
} EReaderStatus;

typedef enum {
  EXTERNAL_ROWS_PREV = 0x1,
  EXTERNAL_ROWS_MAIN = 0x2,
  EXTERNAL_ROWS_NEXT = 0x3,
} EContentData;

typedef struct STsdbReaderInfo {
  uint64_t      suid;
  STSchema*     pSchema;
  EExecMode     execMode;
  STimeWindow   window;
  SVersionRange verRange;
  int16_t       order;
} STsdbReaderInfo;

typedef struct SBlockInfoBuf {
  int32_t currentIndex;
  SArray* pData;
  int32_t numPerBucket;
  int32_t numOfTables;
} SBlockInfoBuf;

typedef struct {
  STbDataIter* iter;
  int32_t      index;
  bool         hasVal;
} SIterInfo;

typedef struct STableDataBlockIdx {
  int32_t globalIndex;
} STableDataBlockIdx;

typedef enum ESttKeyStatus {
  STT_FILE_READER_UNINIT = 0x0,
  STT_FILE_NO_DATA = 0x1,
  STT_FILE_HAS_DATA = 0x2,
} ESttKeyStatus;

typedef struct SSttKeyInfo {
  ESttKeyStatus status;           // this value should be updated when switch to the next fileset
  int64_t       nextProcKey;
} SSttKeyInfo;

// clean stt file blocks:
// 1. not overlap with stt blocks in other stt files of the same fileset
// 2. not overlap with delete skyline
// 3. not overlap with in-memory data (mem/imem)
// 4. not overlap with data file blocks
typedef struct STableBlockScanInfo {
  uint64_t    uid;
  TSKEY       lastProcKey;
  SSttKeyInfo sttKeyInfo;
  SArray*     pBlockList;        // block data index list, SArray<SBrinRecord>
  SArray*     pBlockIdxList;     // SArray<STableDataBlockIndx>
  SArray*     pMemDelData;       // SArray<SDelData>
  SArray*     pFileDelData;      // SArray<SDelData> from each file set
  SIterInfo   iter;              // mem buffer skip list iterator
  SIterInfo   iiter;             // imem buffer skip list iterator
  SArray*     delSkyline;        // delete info for this table
  int32_t     fileDelIndex;      // file block delete index
  int32_t     sttBlockDelIndex;  // delete index for last block
  bool        iterInit;          // whether to initialize the in-memory skip list iterator or not
  bool        cleanSttBlocks;    // stt block is clean in current fileset
  bool        sttBlockReturned;  // result block returned alreay
  int64_t     numOfRowsInStt;
  STimeWindow sttWindow;         // timestamp window for current stt files
  STimeWindow filesetWindow;     // timestamp window for current file set
} STableBlockScanInfo;

typedef struct SResultBlockInfo {
  SSDataBlock* pResBlock;
  bool         freeBlock;
  int64_t      capacity;
} SResultBlockInfo;

typedef struct SReadCostSummary {
  int64_t numOfBlocks;
  double  blockLoadTime;
  double  buildmemBlock;
  int64_t headFileLoad;
  double  headFileLoadTime;
  int64_t smaDataLoad;
  double  smaLoadTime;
  SSttBlockLoadCostInfo sttCost;
  int64_t composedBlocks;
  double  buildComposedBlockTime;
  double  createScanInfoList;
  double  createSkylineIterTime;
  double  initSttBlockReader;
} SReadCostSummary;

typedef struct STableUidList {
  uint64_t* tableUidList;  // access table uid list in uid ascending order list
  int32_t   currentIndex;  // index in table uid list
} STableUidList;

typedef struct {
  int32_t numOfBlocks;
  int32_t numOfSttFiles;
} SBlockNumber;

typedef struct SBlockOrderWrapper {
  int64_t              uid;
  int64_t              offset;
  STableBlockScanInfo* pInfo;
} SBlockOrderWrapper;

typedef struct SBlockOrderSupporter {
  SBlockOrderWrapper** pDataBlockInfo;
  int32_t*             indexPerTable;
  int32_t*             numOfBlocksPerTable;
  int32_t              numOfTables;
} SBlockOrderSupporter;

typedef struct SBlockLoadSuppInfo {
  TColumnDataAggArray colAggArray;
  SColumnDataAgg      tsColAgg;
  int16_t*            colId;
  int16_t*            slotId;
  int32_t             numOfCols;
  char**              buildBuf;  // build string tmp buffer, todo remove it later after all string format being updated.
  bool                smaValid;  // the sma on all queried columns are activated
} SBlockLoadSuppInfo;

// each blocks in stt file not overlaps with in-memory/data-file/tomb-files, and not overlap with any other blocks in stt-file
typedef struct SSttBlockReader {
  STimeWindow        window;
  SVersionRange      verRange;
  int32_t            order;
  uint64_t           uid;
  SMergeTree         mergeTree;
  int64_t            currentKey;
} SSttBlockReader;

typedef struct SFilesetIter {
  int32_t           numOfFiles;    // number of total files
  int32_t           index;         // current accessed index in the list
  TFileSetArray*    pFilesetList;  // data file set list
  int32_t           order;
  SSttBlockReader*  pSttBlockReader;  // last file block reader
} SFilesetIter;

typedef struct SFileDataBlockInfo {
  // index position in STableBlockScanInfo in order to check whether neighbor block overlaps with it
  //  int64_t suid;
  int64_t uid;
  int64_t firstKey;
//  int64_t firstKeyVer;
  int64_t lastKey;
//  int64_t lastKeyVer;
  int64_t minVer;
  int64_t maxVer;
  int64_t blockOffset;
  int64_t smaOffset;
  int32_t blockSize;
  int32_t blockKeySize;
  int32_t smaSize;
  int32_t numRow;
  int32_t count;
  int32_t tbBlockIdx;
} SFileDataBlockInfo;

typedef struct SDataBlockIter {
  int32_t    numOfBlocks;
  int32_t    index;
  SArray*    blockList;  // SArray<SFileDataBlockInfo>
  int32_t    order;
  SDataBlk   block;  // current SDataBlk data
  SSHashObj* pTableMap;
} SDataBlockIter;

typedef struct SFileBlockDumpInfo {
  int32_t totalRows;
  int32_t rowIndex;
  int64_t lastKey;
  bool    allDumped;
} SFileBlockDumpInfo;

typedef struct SReaderStatus {
  bool                  suspendInvoked;
  bool                  loadFromFile;       // check file stage
  bool                  composedDataBlock;  // the returned data block is a composed block or not
  SSHashObj*            pTableMap;          // SHash<STableBlockScanInfo>
  STableBlockScanInfo** pTableIter;         // table iterator used in building in-memory buffer data blocks.
  STableUidList         uidList;            // check tables in uid order, to avoid the repeatly load of blocks in STT.
  SFileBlockDumpInfo    fBlockDumpInfo;
  STFileSet*            pCurrentFileset;  // current opened file set
  SBlockData            fileBlockData;
  SFilesetIter          fileIter;
  SDataBlockIter        blockIter;
  SArray*               pLDataIterArray;
  SRowMerger            merger;
  SColumnInfoData*      pPrimaryTsCol;  // primary time stamp output col info data
  // the following for preceeds fileset memory processing
  // TODO: refactor into seperate struct
  bool                  bProcMemPreFileset;
  int64_t               memTableMaxKey;
  int64_t               memTableMinKey;
  int64_t               prevFilesetStartKey;
  int64_t               prevFilesetEndKey;
  bool                  bProcMemFirstFileset;
  bool                  processingMemPreFileSet;
  STableUidList         procMemUidList;
  STableBlockScanInfo** pProcMemTableIter;
} SReaderStatus;

struct STsdbReader {
  STsdb*             pTsdb;
  STsdbReaderInfo    info;
  TdThreadMutex      readerMutex;
  EReaderStatus      flag;
  int32_t            code;
  uint64_t           rowsNum;
  SResultBlockInfo   resBlockInfo;
  SReaderStatus      status;
  char*              idStr;  // query info handle, for debug purpose
  int32_t            type;   // query type: 1. retrieve all data blocks, 2. retrieve direct prev|next rows
  SBlockLoadSuppInfo suppInfo;
  STsdbReadSnap*     pReadSnap;
  tsem_t             resumeAfterSuspend;
  SReadCostSummary   cost;
  SHashObj**         pIgnoreTables;
  SSHashObj*         pSchemaMap;   // keep the retrieved schema info, to avoid the overhead by repeatly load schema
  SDataFileReader*   pFileReader;  // the file reader
  SBlockInfoBuf      blockInfoBuf;
  EContentData       step;
  STsdbReader*       innerReader[2];
  bool                 bFilesetDelimited;   // duration by duration output
  TsdReaderNotifyCbFn  notifyFn;
  void*              notifyParam;
};

typedef struct SBrinRecordIter {
  SArray*          pBrinBlockList;
  SBrinBlk*        pCurrentBlk;
  int32_t          blockIndex;
  int32_t          recordIndex;
  SDataFileReader* pReader;
  SBrinBlock       block;
  SBrinRecord      record;
} SBrinRecordIter;

int32_t uidComparFunc(const void* p1, const void* p2);

STableBlockScanInfo* getTableBlockScanInfo(SSHashObj* pTableMap, uint64_t uid, const char* id);

SSHashObj* createDataBlockScanInfo(STsdbReader* pTsdbReader, SBlockInfoBuf* pBuf, const STableKeyInfo* idList,
                                   STableUidList* pUidList, int32_t numOfTables);
void       clearBlockScanInfo(STableBlockScanInfo* p);
void       destroyAllBlockScanInfo(SSHashObj* pTableMap);
void       resetAllDataBlockScanInfo(SSHashObj* pTableMap, int64_t ts, int32_t step);
void       cleanupInfoForNextFileset(SSHashObj* pTableMap);
int32_t    ensureBlockScanInfoBuf(SBlockInfoBuf* pBuf, int32_t numOfTables);
void       clearBlockScanInfoBuf(SBlockInfoBuf* pBuf);
void*      getPosInBlockInfoBuf(SBlockInfoBuf* pBuf, int32_t index);

// brin records iterator
void         initBrinRecordIter(SBrinRecordIter* pIter, SDataFileReader* pReader, SArray* pList);
SBrinRecord* getNextBrinRecord(SBrinRecordIter* pIter);
void         clearBrinBlockIter(SBrinRecordIter* pIter);

// initialize block iterator API
int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks, SArray* pTableList);
bool    blockIteratorNext(SDataBlockIter* pBlockIter, const char* idStr);

// load tomb data API (stt/mem only for one table each, tomb data from data files are load for all tables at one time)
void    loadMemTombData(SArray** ppMemDelData, STbData* pMemTbData, STbData* piMemTbData, int64_t ver);
int32_t loadDataFileTombDataForAll(STsdbReader* pReader);
int32_t loadSttTombDataForAll(STsdbReader* pReader, SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pLoadInfo);
int32_t getNumOfRowsInSttBlock(SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pBlockLoadInfo,
                               TStatisBlkArray* pStatisBlkArray, uint64_t suid, const uint64_t* pUidList,
                               int32_t numOfTables);

void    destroyLDataIter(SLDataIter* pIter);
int32_t adjustSttDataIters(SArray* pSttFileBlockIterArray, STFileSet* pFileSet);
int32_t tsdbGetRowsInSttFiles(STFileSet* pFileSet, SArray* pSttFileBlockIterArray, STsdb* pTsdb, SMergeTreeConf* pConf,
                              const char* pstr);
bool    isCleanSttBlock(SArray* pTimewindowList, STimeWindow* pQueryWindow, STableBlockScanInfo* pScanInfo, int32_t order);
bool    overlapWithDelSkyline(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord, int32_t order);

typedef struct {
  SArray* pTombData;
} STableLoadInfo;

struct SDataFileReader;

typedef struct SCacheRowsReader {
  STsdb*                  pTsdb;
  STsdbReaderInfo         info;
  TdThreadMutex           readerMutex;
  SVnode*                 pVnode;
  STSchema*               pSchema;
  STSchema*               pCurrSchema;
  uint64_t                uid;
  char**                  transferBuf;  // todo remove it soon
  int32_t                 numOfCols;
  SArray*                 pCidList;
  int32_t*                pSlotIds;
  int32_t                 type;
  int32_t                 tableIndex;  // currently returned result tables
  STableKeyInfo*          pTableList;  // table id list
  int32_t                 numOfTables;
  uint64_t*               uidList;
  SSHashObj*              pTableMap;
  SArray*                 pLDataIterArray;
  struct SDataFileReader* pFileReader;
  STFileSet*              pCurFileSet;
  const TBrinBlkArray*    pBlkArray;
  STsdbReadSnap*          pReadSnap;
  char*                   idstr;
  int64_t                 lastTs;
  SArray*                 pFuncTypeList;
} SCacheRowsReader;

int32_t tsdbCacheGetBatch(STsdb* pTsdb, tb_uid_t uid, SArray* pLastArray, SCacheRowsReader* pr, int8_t ltype);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSDBREADUTIL_H
