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
#ifndef TDENGINE_EXECUTORINT_H
#define TDENGINE_EXECUTORINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tcommon.h"
#include "tlosertree.h"
#include "tsort.h"
#include "ttszip.h"
#include "tvariant.h"

#include "dataSinkMgt.h"
#include "executil.h"
#include "executor.h"
#include "planner.h"
#include "scalar.h"
#include "taosdef.h"
#include "tarray.h"
#include "tfill.h"
#include "thash.h"
#include "tlockfree.h"
#include "tmsg.h"
#include "tpagedbuf.h"
//#include "tstream.h"
//#include "tstreamUpdate.h"
#include "tlrucache.h"

typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

typedef struct STsdbReader STsdbReader;
typedef struct STqReader  STqReader;

#define IS_VALID_SESSION_WIN(winInfo)        ((winInfo).sessionWin.win.skey > 0)
#define SET_SESSION_WIN_INVALID(winInfo)     ((winInfo).sessionWin.win.skey = INT64_MIN)
#define IS_INVALID_SESSION_WIN_KEY(winKey)   ((winKey).win.skey <= 0)
#define SET_SESSION_WIN_KEY_INVALID(pWinKey) ((pWinKey)->win.skey = INT64_MIN)

/**
 * If the number of generated results is greater than this value,
 * query query will be halt and return results to client immediate.
 */
typedef struct SResultInfo {  // TODO refactor
  int64_t totalRows;          // total generated result size in rows
  int64_t totalBytes;         // total results in bytes.
  int32_t capacity;           // capacity of current result output buffer
  int32_t threshold;          // result size threshold in rows.
} SResultInfo;

typedef struct STableQueryInfo {
  TSKEY              lastKey;  // last check ts, todo remove it later
  SResultRowPosition pos;      // current active time window
} STableQueryInfo;

typedef struct SLimit {
  int64_t limit;
  int64_t offset;
} SLimit;

typedef struct STableScanAnalyzeInfo SFileBlockLoadRecorder;

enum {
  STREAM_RECOVER_STEP__NONE = 0,
  STREAM_RECOVER_STEP__PREPARE1,
  STREAM_RECOVER_STEP__PREPARE2,
  STREAM_RECOVER_STEP__SCAN1,
  STREAM_RECOVER_STEP__SCAN2,
};

extern int32_t exchangeObjRefPool;

typedef struct {
  char*   pData;
  bool    isNull;
  int16_t type;
  int32_t bytes;
} SGroupKeys, SStateKeys;

typedef struct {
  char*           tablename;
  char*           dbname;
  int32_t         tversion;
  SSchemaWrapper* sw;
  SSchemaWrapper* qsw;
} SSchemaInfo;

typedef struct SExchangeOpStopInfo {
  int32_t operatorType;
  int64_t refId;
} SExchangeOpStopInfo;

typedef struct SExprSupp {
  SExprInfo*      pExprInfo;
  int32_t         numOfExprs;  // the number of scalar expression in group operator
  SqlFunctionCtx* pCtx;
  int32_t*        rowEntryInfoOffset;  // offset value for each row result cell info
  SFilterInfo*    pFilterInfo;
} SExprSupp;

typedef enum {
  EX_SOURCE_DATA_NOT_READY = 0x1,
  EX_SOURCE_DATA_READY = 0x2,
  EX_SOURCE_DATA_EXHAUSTED = 0x3,
} EX_SOURCE_STATUS;

#define COL_MATCH_FROM_COL_ID  0x1
#define COL_MATCH_FROM_SLOT_ID 0x2

typedef struct SLoadRemoteDataInfo {
  uint64_t totalSize;     // total load bytes from remote
  uint64_t totalRows;     // total number of rows
  uint64_t totalElapsed;  // total elapsed time
} SLoadRemoteDataInfo;

typedef struct SLimitInfo {
  SLimit   limit;
  SLimit   slimit;
  uint64_t currentGroupId;
  int64_t  remainGroupOffset;
  int64_t  numOfOutputGroups;
  int64_t  remainOffset;
  int64_t  numOfOutputRows;
} SLimitInfo;

typedef struct SExchangeInfo {
  SArray* pSources;
  SArray* pSourceDataInfo;
  tsem_t  ready;
  void*   pTransporter;

  // SArray<SSDataBlock*>, result block list, used to keep the multi-block that
  // passed by downstream operator
  SArray*      pResultBlockList;
  SArray*      pRecycledBlocks;  // build a pool for small data block to avoid to repeatly create and then destroy.
  SSDataBlock* pDummyBlock;      // dummy block, not keep data
  bool         seqLoadData;      // sequential load data or not, false by default
  int32_t      current;
  SLoadRemoteDataInfo loadInfo;
  uint64_t            self;
  SLimitInfo          limitInfo;
  int64_t             openedTs;  // start exec time stamp, todo: move to SLoadRemoteDataInfo
} SExchangeInfo;

typedef struct SScanInfo {
  int32_t numOfAsc;
  int32_t numOfDesc;
} SScanInfo;

typedef struct SSampleExecInfo {
  double   sampleRatio;  // data block sample ratio, 1 by default
  uint32_t seed;         // random seed value
} SSampleExecInfo;

enum {
  TABLE_SCAN__TABLE_ORDER = 1,
  TABLE_SCAN__BLOCK_ORDER = 2,
};

typedef struct SAggSupporter {
  SSHashObj*     pResultRowHashTable;  // quick locate the window object for each result
  char*          keyBuf;               // window key buffer
  SDiskbasedBuf* pResultBuf;           // query result buffer based on blocked-wised disk file
  int32_t        resultRowSize;  // the result buffer size for each result row, with the meta data size for each row
  int32_t        currentPageId;  // current write page id
} SAggSupporter;

typedef struct {
  // if the upstream is an interval operator, the interval info is also kept here to get the time window to check if
  // current data block needs to be loaded.
  SInterval      interval;
  SAggSupporter* pAggSup;
  SExprSupp*     pExprSup;  // expr supporter of aggregate operator
} SAggOptrPushDownInfo;

typedef struct STableMetaCacheInfo {
  SLRUCache* pTableMetaEntryCache;  // 100 by default
  uint64_t   metaFetch;
  uint64_t   cacheHit;
} STableMetaCacheInfo;

typedef struct STableScanBase {
  STsdbReader*           dataReader;
  SFileBlockLoadRecorder readRecorder;
  SQueryTableDataCond    cond;
  SAggOptrPushDownInfo   pdInfo;
  SColMatchInfo          matchInfo;
  SReadHandle            readHandle;
  SExprSupp              pseudoSup;
  STableMetaCacheInfo    metaCache;
  int32_t                scanFlag;  // table scan flag to denote if it is a repeat/reverse/main scan
  int32_t                dataBlockLoadFlag;
  SLimitInfo             limitInfo;
  // there are more than one table list exists in one task, if only one vnode exists.
  STableListInfo* pTableListInfo;
  TsdReader     readerAPI;
} STableScanBase;

typedef struct STableScanInfo {
  STableScanBase  base;
  SScanInfo       scanInfo;
  int32_t         scanTimes;
  SSDataBlock*    pResBlock;
  SHashObj*       pIgnoreTables;
  SSampleExecInfo sample;  // sample execution info
  int32_t         currentGroupId;
  int32_t         currentTable;
  int8_t          scanMode;
  int8_t          assignBlockUid;
  bool            hasGroupByTag;
  bool            countOnly;
//  TsdReader    readerAPI;
} STableScanInfo;

typedef struct STableMergeScanInfo {
  int32_t         tableStartIndex;
  int32_t         tableEndIndex;
  bool            hasGroupId;
  uint64_t        groupId;
  SArray*         queryConds;  // array of queryTableDataCond
  STableScanBase  base;
  int32_t         bufPageSize;
  uint32_t        sortBufSize;  // max buffer size for in-memory sort
  SArray*         pSortInfo;
  SSortHandle*    pSortHandle;
  SSDataBlock*    pSortInputBlock;
  int64_t         startTs;  // sort start time
  SArray*         sortSourceParams;
  SLimitInfo      limitInfo;
  int64_t         numOfRows;
  SScanInfo       scanInfo;
  int32_t         scanTimes;
  SSDataBlock*    pResBlock;
  SSampleExecInfo sample;  // sample execution info
  SSortExecInfo   sortExecInfo;
} STableMergeScanInfo;

typedef struct STagScanInfo {
  SColumnInfo*    pCols;
  SSDataBlock*    pRes;
  SColMatchInfo   matchInfo;
  int32_t         curPos;
  SLimitNode*     pSlimit;
  SReadHandle     readHandle;
  STableListInfo* pTableListInfo;
} STagScanInfo;

typedef enum EStreamScanMode {
  STREAM_SCAN_FROM_READERHANDLE = 1,
  STREAM_SCAN_FROM_RES,
  STREAM_SCAN_FROM_UPDATERES,
  STREAM_SCAN_FROM_DELETE_DATA,
  STREAM_SCAN_FROM_DATAREADER_RETRIEVE,
  STREAM_SCAN_FROM_DATAREADER_RANGE,
} EStreamScanMode;

enum {
  PROJECT_RETRIEVE_CONTINUE = 0x1,
  PROJECT_RETRIEVE_DONE = 0x2,
};

typedef struct SStreamAggSupporter {
  int32_t         resultRowSize;  // the result buffer size for each result row, with the meta data size for each row
  SSDataBlock*    pScanBlock;
  SStreamState*   pState;
  int64_t         gap;        // stream session window gap
  SqlFunctionCtx* pDummyCtx;  // for combine
  SSHashObj*      pResultRows;
  int32_t         stateKeySize;
  int16_t         stateKeyType;
  SDiskbasedBuf*  pResultBuf;
  SStateStore     stateStore;
} SStreamAggSupporter;

typedef struct SWindowSupporter {
  SStreamAggSupporter* pStreamAggSup;
  int64_t              gap;
  uint16_t             parentType;
  SAggSupporter*       pIntervalAggSup;
} SWindowSupporter;

typedef struct SPartitionBySupporter {
  SArray* pGroupCols;     // group by columns, SArray<SColumn>
  SArray* pGroupColVals;  // current group column values, SArray<SGroupKeys>
  char*   keyBuf;         // group by keys for hash
  bool    needCalc;       // partition by column
} SPartitionBySupporter;

typedef struct SPartitionDataInfo {
  uint64_t groupId;
  char*    tbname;
  SArray*  rowIds;
} SPartitionDataInfo;

typedef struct STimeWindowAggSupp {
  int8_t          calTrigger;
  int8_t          calTriggerSaved;
  int64_t         deleteMark;
  int64_t         deleteMarkSaved;
  int64_t         waterMark;
  TSKEY           maxTs;
  TSKEY           minTs;
  TSKEY           checkPointTs;
  TSKEY           checkPointInterval;
  SColumnInfoData timeWindowData;  // query time window info for scalar function execution.
} STimeWindowAggSupp;

typedef struct SStreamScanInfo {
  SExprInfo*    pPseudoExpr;
  int32_t       numOfPseudoExpr;
  SExprSupp     tbnameCalSup;
  SExprSupp     tagCalSup;
  int32_t       primaryTsIndex;  // primary time stamp slot id
  SReadHandle   readHandle;
  SInterval     interval;  // if the upstream is an interval operator, the interval info is also kept here.
  SColMatchInfo matchInfo;

  SArray*      pBlockLists;  // multiple SSDatablock.
  SSDataBlock* pRes;         // result SSDataBlock
  SSDataBlock* pUpdateRes;   // update SSDataBlock
  int32_t      updateResIndex;
  int32_t      blockType;        // current block type
  int32_t      validBlockIndex;  // Is current data has returned?
  uint64_t     numOfExec;        // execution times
  STqReader*   tqReader;

  uint64_t     groupId;
  struct SUpdateInfo* pUpdateInfo;

  EStreamScanMode       scanMode;
  struct SOperatorInfo*        pStreamScanOp;
  struct SOperatorInfo*        pTableScanOp;
  SArray*               childIds;
  SWindowSupporter      windowSup;
  SPartitionBySupporter partitionSup;
  SExprSupp*            pPartScalarSup;
  bool                  assignBlockUid;  // assign block uid to groupId, temporarily used for generating rollup SMA.
  int32_t               scanWinIndex;    // for state operator
  int32_t               pullDataResIndex;
  SSDataBlock*          pPullDataRes;    // pull data SSDataBlock
  SSDataBlock*          pDeleteDataRes;  // delete data SSDataBlock
  int32_t               deleteDataIndex;
  STimeWindow           updateWin;
  STimeWindowAggSupp    twAggSup;
  SSDataBlock*          pUpdateDataRes;
  // status for tmq
  SNodeList* pGroupTags;
  SNode*     pTagCond;
  SNode*     pTagIndexCond;

  // recover
  int32_t      blockRecoverContiCnt;
  int32_t      blockRecoverTotCnt;
  SSDataBlock* pRecoverRes;

  SSDataBlock*  pCreateTbRes;
  int8_t        igCheckUpdate;
  int8_t        igExpired;
  void*         pState;         //void
  SStoreTqReader readerFn;
  SStateStore stateStore;
} SStreamScanInfo;

typedef struct {
  struct SVnode*       vnode;  // todo remove this
  SSDataBlock          pRes;   // result SSDataBlock
  STsdbReader*         dataReader;
  struct SSnapContext* sContext;
  SStorageAPI*         pAPI;
  STableListInfo*      pTableListInfo;
} SStreamRawScanInfo;

typedef struct STableCountScanSupp {
  int16_t dbNameSlotId;
  int16_t stbNameSlotId;
  int16_t tbCountSlotId;
  bool    groupByDbName;
  bool    groupByStbName;
  char    dbNameFilter[TSDB_DB_NAME_LEN];
  char    stbNameFilter[TSDB_TABLE_NAME_LEN];
} STableCountScanSupp;

typedef struct SOptrBasicInfo {
  SResultRowInfo resultRowInfo;
  SSDataBlock*   pRes;
  bool           mergeResultBlock;
  int32_t        inputTsOrder;
  int32_t        outputTsOrder;
} SOptrBasicInfo;

typedef struct SIntervalAggOperatorInfo {
  SOptrBasicInfo     binfo;              // basic info
  SAggSupporter      aggSup;             // aggregate supporter
  SExprSupp          scalarSupp;         // supporter for perform scalar function
  SGroupResInfo      groupResInfo;       // multiple results build supporter
  SInterval          interval;           // interval info
  int32_t            primaryTsIndex;     // primary time stamp slot id from result of downstream operator.
  STimeWindow        win;                // query time range
  bool               timeWindowInterpo;  // interpolation needed or not
  SArray*            pInterpCols;        // interpolation columns
  EOPTR_EXEC_MODEL   execModel;          // operator execution model [batch model|stream model]
  STimeWindowAggSupp twAggSup;
  SArray*            pPrevValues;  //  SArray<SGroupKeys> used to keep the previous not null value for interpolation.
} SIntervalAggOperatorInfo;

typedef struct SMergeAlignedIntervalAggOperatorInfo {
  SIntervalAggOperatorInfo* intervalAggOperatorInfo;

  uint64_t     groupId;  // current groupId
  int64_t      curTs;    // current ts
  SSDataBlock* prefetchedBlock;
  SResultRow*  pResultRow;
} SMergeAlignedIntervalAggOperatorInfo;

typedef struct SStreamIntervalOperatorInfo {
  SOptrBasicInfo     binfo;           // basic info
  SAggSupporter      aggSup;          // aggregate supporter
  SExprSupp          scalarSupp;      // supporter for perform scalar function
  SGroupResInfo      groupResInfo;    // multiple results build supporter
  SInterval          interval;        // interval info
  int32_t            primaryTsIndex;  // primary time stamp slot id from result of downstream operator.
  STimeWindowAggSupp twAggSup;
  bool               invertible;
  bool               ignoreExpiredData;
  bool               ignoreExpiredDataSaved;
  SArray*            pDelWins;  // SWinRes
  int32_t            delIndex;
  SSDataBlock*       pDelRes;
  SPhysiNode*        pPhyNode;  // create new child
  SHashObj*          pPullDataMap;
  SArray*            pPullWins;  // SPullWindowInfo
  int32_t            pullIndex;
  SSDataBlock*       pPullDataRes;
  bool               isFinal;
  SArray*            pChildren;
  int32_t            numOfChild;
  SStreamState*      pState;        // void
  SWinKey            delKey;
  uint64_t           numOfDatapack;
  SArray*            pUpdated;
  SSHashObj*         pUpdatedMap;
  int64_t            dataVersion;
  SStateStore        statestore;
  bool               recvGetAll;
  SHashObj*          pFinalPullDataMap;
} SStreamIntervalOperatorInfo;

typedef struct SDataGroupInfo {
  uint64_t groupId;
  int64_t  numOfRows;
  SArray*  pPageList;
} SDataGroupInfo;

typedef struct SWindowRowsSup {
  STimeWindow win;
  TSKEY       prevTs;
  int32_t     startRowIndex;
  int32_t     numOfRows;
  uint64_t    groupId;
} SWindowRowsSup;

typedef struct SResultWindowInfo {
  void*       pOutputBuf;
  SSessionKey sessionWin;
  bool        isOutput;
} SResultWindowInfo;

typedef struct SStreamSessionAggOperatorInfo {
  SOptrBasicInfo      binfo;
  SStreamAggSupporter streamAggSup;
  SExprSupp           scalarSupp;  // supporter for perform scalar function
  SGroupResInfo       groupResInfo;
  int32_t             primaryTsIndex;  // primary timestamp slot id
  int32_t             endTsIndex;      // window end timestamp slot id
  int32_t             order;           // current SSDataBlock scan order
  STimeWindowAggSupp  twAggSup;
  SSDataBlock*        pWinBlock;   // window result
  SSDataBlock*        pDelRes;     // delete result
  SSDataBlock*        pUpdateRes;  // update window
  bool                returnUpdate;
  SSHashObj*          pStDeleted;
  void*               pDelIterator;
  SArray*             pChildren;  // cache for children's result; final stream operator
  SPhysiNode*         pPhyNode;   // create new child
  bool                isFinal;
  bool                ignoreExpiredData;
  bool                ignoreExpiredDataSaved;
  SArray*             pUpdated;
  SSHashObj*          pStUpdated;
  int64_t             dataVersion;
} SStreamSessionAggOperatorInfo;

typedef struct SStreamStateAggOperatorInfo {
  SOptrBasicInfo      binfo;
  SStreamAggSupporter streamAggSup;
  SExprSupp           scalarSupp;  // supporter for perform scalar function
  SGroupResInfo       groupResInfo;
  int32_t             primaryTsIndex;  // primary timestamp slot id
  STimeWindowAggSupp  twAggSup;
  SColumn             stateCol;
  SSDataBlock*        pDelRes;
  SSHashObj*          pSeDeleted;
  void*               pDelIterator;
  SArray*             pChildren;  // cache for children's result;
  bool                ignoreExpiredData;
  bool                ignoreExpiredDataSaved;
  SArray*             pUpdated;
  SSHashObj*          pSeUpdated;
  int64_t             dataVersion;
} SStreamStateAggOperatorInfo;

typedef struct SStreamPartitionOperatorInfo {
  SOptrBasicInfo        binfo;
  SPartitionBySupporter partitionSup;
  SExprSupp             scalarSup;
  SExprSupp             tbnameCalSup;
  SExprSupp             tagCalSup;
  SHashObj*             pPartitions;
  void*                 parIte;
  void*                 pTbNameIte;
  SSDataBlock*          pInputDataBlock;
  int32_t               tsColIndex;
  SSDataBlock*          pDelRes;
  SSDataBlock*          pCreateTbRes;
} SStreamPartitionOperatorInfo;

typedef struct SStreamFillSupporter {
  int32_t        type;  // fill type
  SInterval      interval;
  SResultRowData prev;
  SResultRowData cur;
  SResultRowData next;
  SResultRowData nextNext;
  SFillColInfo*  pAllColInfo;  // fill exprs and not fill exprs
  SExprSupp      notFillExprSup;
  int32_t        numOfAllCols;  // number of all exprs, including the tags columns
  int32_t        numOfFillCols;
  int32_t        numOfNotFillCols;
  int32_t        rowSize;
  SSHashObj*     pResMap;
  bool           hasDelete;
  SStorageAPI*   pAPI;
} SStreamFillSupporter;

typedef struct SStreamFillOperatorInfo {
  SStreamFillSupporter* pFillSup;
  SSDataBlock*          pRes;
  SSDataBlock*          pSrcBlock;
  int32_t               srcRowIndex;
  SSDataBlock*          pSrcDelBlock;
  int32_t               srcDelRowIndex;
  SSDataBlock*          pDelRes;
  SColMatchInfo         matchInfo;
  int32_t               primaryTsCol;
  int32_t               primarySrcSlotId;
  SStreamFillInfo*      pFillInfo;
} SStreamFillOperatorInfo;

#define OPTR_IS_OPENED(_optr)  (((_optr)->status & OP_OPENED) == OP_OPENED)
#define OPTR_SET_OPENED(_optr) ((_optr)->status |= OP_OPENED)

SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode);
int32_t initQueriedTableSchemaInfo(SReadHandle* pHandle, SScanPhysiNode* pScanNode, const char* dbName, SExecTaskInfo* pTaskInfo);
void    cleanupQueriedTableScanInfo(SSchemaInfo* pSchemaInfo);

void initBasicInfo(SOptrBasicInfo* pInfo, SSDataBlock* pBlock);
void cleanupBasicInfo(SOptrBasicInfo* pInfo);

int32_t initExprSupp(SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfExpr, SFunctionStateStore* pStore);
void    cleanupExprSupp(SExprSupp* pSup);

int32_t initAggSup(SExprSupp* pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols, size_t keyBufSize,
                   const char* pkey, void* pState, SFunctionStateStore* pStore);
void    cleanupAggSup(SAggSupporter* pAggSup);

void initResultSizeInfo(SResultInfo* pResultInfo, int32_t numOfRows);

void doBuildStreamResBlock(struct SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                           SDiskbasedBuf* pBuf);
void doBuildResultDatablock(struct SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                            SDiskbasedBuf* pBuf);

bool hasLimitOffsetInfo(SLimitInfo* pLimitInfo);
bool hasSlimitOffsetInfo(SLimitInfo* pLimitInfo);
void initLimitInfo(const SNode* pLimit, const SNode* pSLimit, SLimitInfo* pLimitInfo);
void resetLimitInfoForNextGroup(SLimitInfo* pLimitInfo);
bool applyLimitOffset(SLimitInfo* pLimitInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo);

void applyAggFunctionOnPartialTuples(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, SColumnInfoData* pTimeWindowData,
                                     int32_t offset, int32_t forwardStep, int32_t numOfTotal, int32_t numOfOutput);

int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, char* pData, SArray* pColList, char** pNextStart);
void    updateLoadRemoteInfo(SLoadRemoteDataInfo* pInfo, int64_t numOfRows, int32_t dataLen, int64_t startTs,
                             struct SOperatorInfo* pOperator);

STimeWindow getFirstQualifiedTimeWindow(int64_t ts, STimeWindow* pWindow, SInterval* pInterval, int32_t order);
int32_t     getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, uint32_t* defaultBufsz);

extern void doDestroyExchangeOperatorInfo(void* param);

void    doFilter(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SColMatchInfo* pColMatchInfo);
int32_t addTagPseudoColumnData(SReadHandle* pHandle, const SExprInfo* pExpr, int32_t numOfExpr, SSDataBlock* pBlock,
                               int32_t rows, const char* idStr, STableMetaCacheInfo* pCache);

void appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle);
void setTbNameColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, const char* name);

void setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset);
void clearResultRowInitFlag(SqlFunctionCtx* pCtx, int32_t numOfOutput);

SResultRow* doSetResultOutBufByKey(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo, char* pData,
                                   int16_t bytes, bool masterscan, uint64_t groupId, SExecTaskInfo* pTaskInfo,
                                   bool isIntervalQuery, SAggSupporter* pSup, bool keepGroup);

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList);

void setInputDataBlock(SExprSupp* pExprSupp, SSDataBlock* pBlock, int32_t order, int32_t scanFlag, bool createDummyCol);

int32_t checkForQueryBuf(size_t numOfTables);

int32_t createDataSinkParam(SDataSinkNode* pNode, void** pParam, SExecTaskInfo* pTask, SReadHandle* readHandle);

STimeWindow getActiveTimeWindow(SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int64_t ts, SInterval* pInterval,
                                int32_t order);
int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos, TSKEY ekey,
                                 __block_search_fn_t searchFn, STableQueryInfo* item, int32_t order);
int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
SResultRow* getNewResultRow(SDiskbasedBuf* pResultBuf, int32_t* currentPageId, int32_t interBufSize);
void getCurSessionWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId, SSessionKey* pKey);
bool isInTimeWindow(STimeWindow* pWin, TSKEY ts, int64_t gap);
bool functionNeedToExecute(SqlFunctionCtx* pCtx);
bool isOverdue(TSKEY ts, STimeWindowAggSupp* pSup);
bool isCloseWindow(STimeWindow* pWin, STimeWindowAggSupp* pSup);
bool isDeletedStreamWindow(STimeWindow* pWin, uint64_t groupId, void* pState, STimeWindowAggSupp* pTwSup, SStateStore* pStore);
void appendOneRowToStreamSpecialBlock(SSDataBlock* pBlock, TSKEY* pStartTs, TSKEY* pEndTs, uint64_t* pUid,
                                      uint64_t* pGp, void* pTbName);
uint64_t calGroupIdByData(SPartitionBySupporter* pParSup, SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t rowId);

int32_t finalizeResultRows(SDiskbasedBuf* pBuf, SResultRowPosition* resultRowPosition, SExprSupp* pSup,
                           SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo);

bool    groupbyTbname(SNodeList* pGroupList);
int32_t buildDataBlockFromGroupRes(struct SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock, SExprSupp* pSup,
                                   SGroupResInfo* pGroupResInfo);
int32_t saveSessionDiscBuf(void* pState, SSessionKey* key, void* buf, int32_t size, SStateStore* pAPI);
int32_t buildSessionResultDataBlock(struct SOperatorInfo* pOperator, void* pState, SSDataBlock* pBlock,
                                    SExprSupp* pSup, SGroupResInfo* pGroupResInfo);
int32_t releaseOutputBuf(void* pState, SWinKey* pKey, SResultRow* pResult, SStateStore* pAPI);
void    getNextIntervalWindow(SInterval* pInterval, STimeWindow* tw, int32_t order);
int32_t getForwardStepsInBlock(int32_t numOfRows, __block_search_fn_t searchFn, TSKEY ekey, int32_t pos, int32_t order,
                               int64_t* pData);
void    appendCreateTableRow(void* pState, SExprSupp* pTableSup, SExprSupp* pTagSup, uint64_t groupId,
                             SSDataBlock* pSrcBlock, int32_t rowId, SSDataBlock* pDestBlock, SStateStore* pAPI);

SSDataBlock* buildCreateTableBlock(SExprSupp* tbName, SExprSupp* tag);
SExprInfo*   createExpr(SNodeList* pNodeList, int32_t* numOfExprs);
void         destroyExprInfo(SExprInfo* pExpr, int32_t numOfExprs);

void copyResultrowToDataBlock(SExprInfo* pExprInfo, int32_t numOfExprs, SResultRow* pRow, SqlFunctionCtx* pCtx,
                              SSDataBlock* pBlock, const int32_t* rowEntryOffset, SExecTaskInfo* pTaskInfo);
void doUpdateNumOfRows(SqlFunctionCtx* pCtx, SResultRow* pRow, int32_t numOfExprs, const int32_t* rowEntryOffset);
void doClearBufferedBlocks(SStreamScanInfo* pInfo);

uint64_t calcGroupId(char* pData, int32_t len);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EXECUTORINT_H
