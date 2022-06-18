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
// clang-format off
#ifndef TDENGINE_EXECUTORIMPL_H
#define TDENGINE_EXECUTORIMPL_H

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
#include "thash.h"
#include "tlockfree.h"
#include "tmsg.h"
#include "tpagedbuf.h"
#include "tstreamUpdate.h"

#include "vnode.h"
#include "executorInt.h"

typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

#define IS_QUERY_KILLED(_q)   ((_q)->code == TSDB_CODE_TSC_QUERY_CANCELLED)
#define Q_STATUS_EQUAL(p, s)  (((p) & (s)) != 0u)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define NEEDTO_COMPRESS_QUERY(size) ((size) > tsCompressColData ? 1 : 0)

enum {
  // when this task starts to execute, this status will set
  TASK_NOT_COMPLETED = 0x1u,

  /* Task is over
   * 1. this status is used in one row result query process, e.g., count/sum/first/last/ avg...etc.
   * 2. when all data within queried time window, it is also denoted as query_completed
   */
  TASK_COMPLETED = 0x2u,
};

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
  TSKEY              lastKey;     // last check ts, todo remove it later
  SResultRowPosition pos;       // current active time window
} STableQueryInfo;

typedef struct SLimit {
  int64_t limit;
  int64_t offset;
} SLimit;

typedef struct STableScanAnalyzeInfo SFileBlockLoadRecorder;

typedef struct STaskCostInfo {
  int64_t  created;
  int64_t  start;
  uint64_t loadStatisTime;
  uint64_t loadFileBlockTime;
  uint64_t loadDataInCacheTime;
  uint64_t loadStatisSize;
  uint64_t loadFileBlockSize;
  uint64_t loadDataInCacheSize;

  uint64_t loadDataTime;

  SFileBlockLoadRecorder* pRecoder;
  uint64_t elapsedTime;

  uint64_t firstStageMergeTime;
  uint64_t winInfoSize;
  uint64_t tableInfoSize;
  uint64_t hashSize;
  uint64_t numOfTimeWindows;

  SArray*   queryProfEvents;      // SArray<SQueryProfEvent>
  SHashObj* operatorProfResults;  // map<operator_type, SQueryProfEvent>
} STaskCostInfo;

typedef struct SOperatorCostInfo {
  double   openCost;
  double   totalCost;
} SOperatorCostInfo;

struct SOperatorInfo;

typedef int32_t (*__optr_encode_fn_t)(struct SOperatorInfo* pOperator, char** result, int32_t* length);
typedef int32_t (*__optr_decode_fn_t)(struct SOperatorInfo* pOperator, char* result);

typedef int32_t (*__optr_open_fn_t)(struct SOperatorInfo* pOptr);
typedef SSDataBlock* (*__optr_fn_t)(struct SOperatorInfo* pOptr);
typedef void (*__optr_close_fn_t)(void* param, int32_t num);
typedef int32_t (*__optr_explain_fn_t)(struct SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);

typedef struct STaskIdInfo {
  uint64_t queryId;  // this is also a request id
  uint64_t subplanId;
  uint64_t templateId;
  char*    str;
} STaskIdInfo;

typedef struct SExecTaskInfo {
  STaskIdInfo      id;
  uint32_t         status;
  STimeWindow      window;
  STaskCostInfo    cost;
  int64_t          owner;  // if it is in execution
  int32_t          code;
  struct {
    char          *tablename;
    char          *dbname;
    int32_t        sversion;
    int32_t        tversion;
  } schemaVer;

  STableListInfo   tableqinfoList;       // this is a table list
  const char*      sql;                  // query sql string
  jmp_buf          env;                  // jump to this position when error happens.
  EOPTR_EXEC_MODEL execModel;            // operator execution model [batch model|stream model]
  struct SOperatorInfo* pRoot;
} SExecTaskInfo;

enum {
  OP_NOT_OPENED    = 0x0,
  OP_OPENED        = 0x1,
  OP_RES_TO_RETURN = 0x5,
  OP_EXEC_DONE     = 0x9,
};

typedef struct SOperatorFpSet {
  __optr_open_fn_t     _openFn;          // DO NOT invoke this function directly
  __optr_fn_t          getNextFn;
  __optr_fn_t          getStreamResFn;  // execute the aggregate in the stream model, todo remove it
  __optr_fn_t          cleanupFn;       // call this function to release the allocated resources ASAP
  __optr_close_fn_t    closeFn;
  __optr_encode_fn_t   encodeResultRow;
  __optr_decode_fn_t   decodeResultRow;
  __optr_explain_fn_t  getExplainFn;
} SOperatorFpSet;

typedef struct SExprSupp {
  SExprInfo*      pExprInfo;
  int32_t         numOfExprs;          // the number of scalar expression in group operator
  SqlFunctionCtx* pCtx;
  int32_t*        rowEntryInfoOffset;  // offset value for each row result cell info
} SExprSupp;

typedef struct SOperatorInfo {
  uint8_t                 operatorType;
  bool                    blocking;      // block operator or not
  uint8_t                 status;        // denote if current operator is completed
  char*                   name;          // name, for debug purpose
  void*                   info;          // extension attribution
  SExprSupp               exprSupp;
  SExecTaskInfo*          pTaskInfo;
  SOperatorCostInfo       cost;
  SResultInfo             resultInfo;
  struct SOperatorInfo**  pDownstream;      // downstram pointer list
  int32_t                 numOfDownstream;  // number of downstream. The value is always ONE expect for join operator
  SOperatorFpSet          fpSet;
} SOperatorInfo;

typedef enum {
  EX_SOURCE_DATA_NOT_READY = 0x1,
  EX_SOURCE_DATA_READY     = 0x2,
  EX_SOURCE_DATA_EXHAUSTED = 0x3,
} EX_SOURCE_STATUS;

#define COL_MATCH_FROM_COL_ID  0x1
#define COL_MATCH_FROM_SLOT_ID 0x2

typedef struct SSourceDataInfo {
  int32_t               index;
  SRetrieveTableRsp*    pRsp;
  uint64_t              totalRows;
  int32_t               code;
  EX_SOURCE_STATUS      status;
  const char*           taskId;
} SSourceDataInfo;

typedef struct SLoadRemoteDataInfo {
  uint64_t totalSize;     // total load bytes from remote
  uint64_t totalRows;     // total number of rows
  uint64_t totalElapsed;  // total elapsed time
} SLoadRemoteDataInfo;

typedef struct SExchangeInfo {
  SArray*             pSources;
  SArray*             pSourceDataInfo;
  tsem_t              ready;
  void*               pTransporter;
  SSDataBlock*        pResult;
  bool                seqLoadData;  // sequential load data or not, false by default
  int32_t             current;
  SLoadRemoteDataInfo loadInfo;
  uint64_t            self;
} SExchangeInfo;

typedef struct SColMatchInfo {
  int32_t srcSlotId;     // source slot id
  int32_t colId;
  int32_t targetSlotId;
  bool    output;
  int32_t matchType;     // determinate the source according to col id or slot id
} SColMatchInfo;

typedef struct SScanInfo {
  int32_t         numOfAsc;
  int32_t         numOfDesc;
} SScanInfo;

typedef struct SSampleExecInfo {
  double          sampleRatio;  // data block sample ratio, 1 by default
  uint32_t        seed;         // random seed value
} SSampleExecInfo;

typedef struct STableScanInfo {
  void*           dataReader;
  SReadHandle     readHandle;

  SFileBlockLoadRecorder readRecorder;
  int64_t         numOfRows;
//  int32_t         prevGroupId;  // previous table group id
  SScanInfo       scanInfo;
  int32_t         scanTimes;
  SNode*          pFilterNode;  // filter info, which is push down by optimizer
  SqlFunctionCtx* pCtx;         // which belongs to the direct upstream operator operator query context
  SResultRowInfo* pResultRowInfo;
  int32_t*        rowEntryInfoOffset;
  SExprInfo*      pExpr;
  SSDataBlock*    pResBlock;
  SArray*         pColMatchInfo;
  int32_t         numOfOutput;

  SExprSupp       pseudoSup;
  SQueryTableDataCond cond;
  int32_t         scanFlag;     // table scan flag to denote if it is a repeat/reverse/main scan
  int32_t         dataBlockLoadFlag;
  SInterval       interval;     // if the upstream is an interval operator, the interval info is also kept here to get the time window to check if current data block needs to be loaded.

  SSampleExecInfo sample;       // sample execution info
  int32_t         curTWinIdx;
} STableScanInfo;

typedef struct STagScanInfo {
  SColumnInfo     *pCols;
  SSDataBlock     *pRes;
  SArray          *pColMatchInfo;
  int32_t          curPos;
  SReadHandle      readHandle;
  STableListInfo  *pTableList;
  SNode*           pFilterNode;  // filter info,
} STagScanInfo;

typedef enum EStreamScanMode {
  STREAM_SCAN_FROM_READERHANDLE = 1,
  STREAM_SCAN_FROM_RES,
  STREAM_SCAN_FROM_UPDATERES,
  STREAM_SCAN_FROM_DATAREADER,
} EStreamScanMode;

typedef struct SCatchSupporter {
  SHashObj* pWindowHashTable;  // quick locate the window object for each window
  SDiskbasedBuf* pDataBuf;           // buffer based on blocked-wised disk file
  int32_t keySize;
  int64_t* pKeyBuf;
} SCatchSupporter;

typedef struct SStreamAggSupporter {
  SHashObj*      pResultRows;
  SArray*        pCurWins;
  int32_t        valueSize;
  int32_t        keySize;
  char*          pKeyBuf;              // window key buffer
  SDiskbasedBuf* pResultBuf;           // query result buffer based on blocked-wised disk file
  int32_t        resultRowSize;        // the result buffer size for each result row, with the meta data size for each row
  SArray*        pScanWindow;
} SStreamAggSupporter;

typedef struct SessionWindowSupporter {
  SStreamAggSupporter* pStreamAggSup;
  int64_t gap;
  uint8_t parentType;
} SessionWindowSupporter;

typedef struct SStreamBlockScanInfo {
  SArray*         pBlockLists;      // multiple SSDatablock.
  SSDataBlock*    pRes;             // result SSDataBlock
  SSDataBlock*    pUpdateRes;       // update SSDataBlock
  int32_t         updateResIndex;
  int32_t         blockType;        // current block type
  int32_t         validBlockIndex;  // Is current data has returned?
  SColumnInfo*    pCols;            // the output column info
  uint64_t        numOfExec;        // execution times
  void*           streamBlockReader;// stream block reader handle
  SArray*         pColMatchInfo;    //
  SNode*          pCondition;
  int32_t         tsArrayIndex;
  SArray*         tsArray;
  uint64_t        groupId;
  SUpdateInfo*    pUpdateInfo;

  SExprInfo*      pPseudoExpr;
  int32_t         numOfPseudoExpr;

  int32_t         primaryTsIndex;    // primary time stamp slot id
  void*           pDataReader;
  SReadHandle     readHandle;
  uint64_t        tableUid;         // queried super table uid
  EStreamScanMode scanMode;
  SOperatorInfo* pOperatorDumy;
  SInterval      interval;     // if the upstream is an interval operator, the interval info is also kept here.
  SArray*        childIds;
  SessionWindowSupporter sessionSup;
  bool            assignBlockUid; // assign block uid to groupId, temporarily used for generating rollup SMA.
  int32_t         scanWinIndex;
} SStreamBlockScanInfo;

typedef struct SSysTableScanInfo {
  SRetrieveMetaTableRsp* pRsp;
  SRetrieveTableReq      req;
  SEpSet                 epSet;
  tsem_t                 ready;
  SReadHandle            readHandle;
  int32_t                accountId;
  bool                   showRewrite;
  SNode*                 pCondition;  // db_name filter condition, to discard data that are not in current database
  SMTbCursor*            pCur;        // cursor for iterate the local table meta store.
  SArray*                scanCols;    // SArray<int16_t> scan column id list
  SName                  name;
  SSDataBlock*           pRes;
  int64_t                numOfBlocks;  // extract basic running information.
  SLoadRemoteDataInfo    loadInfo;
} SSysTableScanInfo;

typedef struct SBlockDistInfo {
  SSDataBlock* pResBlock;
  void*        pHandle;
} SBlockDistInfo;

// todo remove this
typedef struct SOptrBasicInfo {
  SResultRowInfo  resultRowInfo;
  SSDataBlock*    pRes;
} SOptrBasicInfo;

typedef struct SAggSupporter {
  SHashObj*      pResultRowHashTable;  // quick locate the window object for each result
  char*          keyBuf;               // window key buffer
  SDiskbasedBuf* pResultBuf;           // query result buffer based on blocked-wised disk file
  int32_t        resultRowSize;        // the result buffer size for each result row, with the meta data size for each row
} SAggSupporter;

typedef struct STimeWindowSupp {
  int8_t           calTrigger;
  int64_t          waterMark;
  TSKEY            maxTs;
  SColumnInfoData  timeWindowData;     // query time window info for scalar function execution.
} STimeWindowAggSupp;

typedef struct SIntervalAggOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo     binfo;              // basic info
  SAggSupporter      aggSup;             // aggregate supporter

  SGroupResInfo      groupResInfo;       // multiple results build supporter
  SInterval          interval;           // interval info
  int32_t            primaryTsIndex;     // primary time stamp slot id from result of downstream operator.
  STimeWindow        win;                // query time range
  bool               timeWindowInterpo;  // interpolation needed or not
  char**             pRow;               // previous row/tuple of already processed datablock
  SArray*            pInterpCols;        // interpolation columns
  int32_t            order;              // current SSDataBlock scan order
  EOPTR_EXEC_MODEL   execModel;          // operator execution model [batch model|stream model]
  STimeWindowAggSupp twAggSup;
  bool               invertible;
  SArray*            pPrevValues;        //  SArray<SGroupKeys> used to keep the previous not null value for interpolation.
} SIntervalAggOperatorInfo;

typedef struct SStreamFinalIntervalOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo     binfo;              // basic info
  SAggSupporter      aggSup;             // aggregate supporter

  SGroupResInfo      groupResInfo;       // multiple results build supporter
  SInterval          interval;           // interval info
  int32_t            primaryTsIndex;     // primary time stamp slot id from result of downstream operator.
  int32_t            order;              // current SSDataBlock scan order
  STimeWindowAggSupp twAggSup;
  SArray*            pChildren;
  SSDataBlock*       pUpdateRes;
  SPhysiNode*        pPhyNode;           // create new child
} SStreamFinalIntervalOperatorInfo;

typedef struct SAggOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;

  STableQueryInfo   *current;
  uint64_t           groupId;
  SGroupResInfo      groupResInfo;
  SExprSupp          scalarExprSup;
} SAggOperatorInfo;

typedef struct SProjectOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SNode*             pFilterNode;  // filter info, which is push down by optimizer
  SSDataBlock*       existDataBlock;
  SArray*            pPseudoColInfo;
  SLimit             limit;
  SLimit             slimit;

  uint64_t           groupId;
  int64_t            curSOffset;
  int64_t            curGroupOutput;

  int64_t            curOffset;
  int64_t            curOutput;
} SProjectOperatorInfo;

typedef struct SIndefOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SArray*            pPseudoColInfo;
  SExprSupp          scalarSup;
} SIndefOperatorInfo;

typedef struct SFillOperatorInfo {
  struct SFillInfo* pFillInfo;
  SSDataBlock*      pRes;
  int64_t           totalInputRows;
  void**            p;
  SSDataBlock*      existNewGroupBlock;
  bool              multigroupResult;
} SFillOperatorInfo;

typedef struct SGroupbyOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo  binfo;
  SAggSupporter   aggSup;

  SArray*         pGroupCols;     // group by columns, SArray<SColumn>
  SArray*         pGroupColVals;  // current group column values, SArray<SGroupKeys>
  SNode*          pCondition;
  bool            isInit;       // denote if current val is initialized or not
  char*           keyBuf;       // group by keys for hash
  int32_t         groupKeyLen;  // total group by column width
  SGroupResInfo   groupResInfo;
  SExprSupp       scalarSup;
} SGroupbyOperatorInfo;

typedef struct SDataGroupInfo {
  uint64_t        groupId;
  int64_t         numOfRows;
  SArray*         pPageList;
} SDataGroupInfo;

// The sort in partition may be needed later.
typedef struct SPartitionOperatorInfo {
  SOptrBasicInfo binfo;
  SArray*        pGroupCols;
  SArray*        pGroupColVals;  // current group column values, SArray<SGroupKeys>
  char*          keyBuf;         // group by keys for hash
  int32_t        groupKeyLen;    // total group by column width
  SHashObj*      pGroupSet;      // quick locate the window object for each result

  SDiskbasedBuf* pBuf;          // query result buffer based on blocked-wised disk file
  int32_t        rowCapacity;   // maximum number of rows for each buffer page
  int32_t*       columnOffset;  // start position for each column data
  void*          pGroupIter;  // group iterator
  int32_t        pageIndex;   // page index of current group
  SSDataBlock*   pUpdateRes;
  SExprSupp      scalarSup;
} SPartitionOperatorInfo;

typedef struct SWindowRowsSup {
  STimeWindow win;
  TSKEY       prevTs;
  int32_t     startRowIndex;
  int32_t     numOfRows;
} SWindowRowsSup;

typedef struct SSessionAggOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;

  SGroupResInfo      groupResInfo;
  SWindowRowsSup     winSup;
  bool               reptScan;        // next round scan
  int64_t            gap;             // session window gap
  int32_t            tsSlotId;        // primary timestamp slot id
  STimeWindowAggSupp twAggSup;
} SSessionAggOperatorInfo;

typedef struct SResultWindowInfo {
  SResultRowPosition pos;
  STimeWindow win;
  bool isOutput;
  bool isClosed;
} SResultWindowInfo;

typedef struct SStateWindowInfo {
  SResultWindowInfo winInfo;
  SStateKeys stateKey;
} SStateWindowInfo;

typedef struct SStreamSessionAggOperatorInfo {
  SOptrBasicInfo       binfo;
  SStreamAggSupporter  streamAggSup;
  SGroupResInfo        groupResInfo;
  int64_t              gap;             // session window gap
  int32_t              primaryTsIndex;  // primary timestamp slot id
  int32_t              order;           // current SSDataBlock scan order
  STimeWindowAggSupp   twAggSup;
  SSDataBlock*         pWinBlock;       // window result
  SqlFunctionCtx*      pDummyCtx;       // for combine
  SSDataBlock*         pDelRes;
  SHashObj*            pStDeleted;
  void*                pDelIterator;
  SArray*              pChildren;       // cache for children's result; final stream operator
} SStreamSessionAggOperatorInfo;

typedef struct STimeSliceOperatorInfo {
  SOptrBasicInfo binfo;
  STimeWindow    win;
  SInterval      interval;
  int64_t        current;
  SArray*        pPrevRow;      // SArray<SGroupValue>
  SArray*        pCols;         // SArray<SColumn>
  int32_t        fillType;      // fill type
  struct SFillColInfo*  pFillColInfo;  // fill column info
} STimeSliceOperatorInfo;

typedef struct SStateWindowOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;

  SGroupResInfo      groupResInfo;
  SWindowRowsSup     winSup;
  SColumn            stateCol;  // start row index
  bool               hasKey;
  SStateKeys         stateKey;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
  //  bool             reptScan;
} SStateWindowOperatorInfo;

typedef struct SStreamStateAggOperatorInfo {
  SOptrBasicInfo       binfo;
  SStreamAggSupporter  streamAggSup;
  SGroupResInfo        groupResInfo;
  int32_t              primaryTsIndex;  // primary timestamp slot id
  int32_t              order;           // current SSDataBlock scan order
  STimeWindowAggSupp   twAggSup;
  SColumn              stateCol;  // start row index
  SqlFunctionCtx*      pDummyCtx;       // for combine
  SSDataBlock*         pDelRes;
  SHashObj*            pSeDeleted;
  void*                pDelIterator;
  SArray*              pScanWindow;
  SArray*              pChildren;       // cache for children's result;
} SStreamStateAggOperatorInfo;

typedef struct SSortedMergeOperatorInfo {
  // SOptrBasicInfo should be first, SAggSupporter should be second for stream encode
  SOptrBasicInfo   binfo;
  SAggSupporter    aggSup;

  SArray*          pSortInfo;
  int32_t          numOfSources;
  SSortHandle     *pSortHandle;
  int32_t          bufPageSize;
  uint32_t         sortBufSize;  // max buffer size for in-memory sort
  int32_t          resultRowFactor;
  bool             hasGroupVal;
  SDiskbasedBuf   *pTupleStore;  // keep the final results
  int32_t          numOfResPerPage;
  char**           groupVal;
  SArray          *groupInfo;
} SSortedMergeOperatorInfo;

typedef struct SSortOperatorInfo {
  SOptrBasicInfo binfo;
  uint32_t     sortBufSize;    // max buffer size for in-memory sort
  SArray*      pSortInfo;
  SSortHandle* pSortHandle;
  SArray*      pColMatchInfo;  // for index map from table scan output
  int32_t      bufPageSize;

  int64_t      startTs;       // sort start time
  uint64_t     sortElapsed;   // sort elapsed time, time to flush to disk not included.
} SSortOperatorInfo;

typedef struct STagFilterOperatorInfo {
  SOptrBasicInfo binfo;
} STagFilterOperatorInfo;

typedef struct SJoinOperatorInfo {
  SSDataBlock       *pRes;
  int32_t            joinType;

  SSDataBlock       *pLeft;
  int32_t            leftPos;
  SColumnInfo        leftCol;

  SSDataBlock       *pRight;
  int32_t            rightPos;
  SColumnInfo        rightCol;
  SNode             *pOnCondition;
} SJoinOperatorInfo;

#define OPTR_IS_OPENED(_optr)  (((_optr)->status & OP_OPENED) == OP_OPENED)
#define OPTR_SET_OPENED(_optr) ((_optr)->status |= OP_OPENED)

void doDestroyExchangeOperatorInfo(void* param);

SOperatorFpSet createOperatorFpSet(__optr_open_fn_t openFn, __optr_fn_t nextFn, __optr_fn_t streamFn,
    __optr_fn_t cleanup, __optr_close_fn_t closeFn, __optr_encode_fn_t encode,
    __optr_decode_fn_t decode, __optr_explain_fn_t explain);

int32_t operatorDummyOpenFn(SOperatorInfo* pOperator);
void    operatorDummyCloseFn(void* param, int32_t numOfCols);
int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num);

void    initBasicInfo(SOptrBasicInfo* pInfo, SSDataBlock* pBlock);
void    cleanupBasicInfo(SOptrBasicInfo* pInfo);
void    initExprSupp(SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfExpr);
void    cleanupExprSup(SExprSupp* pSup);
int32_t initAggInfo(SExprSupp *pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols, size_t keyBufSize,
                    const char* pkey);
void    initResultSizeInfo(SOperatorInfo* pOperator, int32_t numOfRows);
void    doBuildResultDatablock(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo, SDiskbasedBuf* pBuf);

void    doApplyFunctions(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, STimeWindow* pWin, SColumnInfoData* pTimeWindowData, int32_t offset,
                         int32_t forwardStep, TSKEY* tsCol, int32_t numOfTotal, int32_t numOfOutput, int32_t order);

int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, SLoadRemoteDataInfo* pLoadInfo, int32_t numOfRows, char* pData,
                                  int32_t compLen, int32_t numOfOutput, int64_t startTs, uint64_t* total,
                                  SArray* pColList);
void    getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, STimeWindow* win);
int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t *order, int32_t* scanFlag);
int32_t getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, uint32_t* defaultBufsz);

void    doSetOperatorCompleted(SOperatorInfo* pOperator);
void    doFilter(const SNode* pFilterNode, SSDataBlock* pBlock);

void    cleanupAggSup(SAggSupporter* pAggSup);
void    destroyBasicOperatorInfo(void* param, int32_t numOfOutput);
void    appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle);
void    setTbNameColData(void* pMeta, const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId);

void    cleanupExecSupp(SExprSupp* pSupp);

SSDataBlock* loadNextDataBlock(void* param);

void setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowEntryInfoOffset);

SResultRow* doSetResultOutBufByKey(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo,
                                   char* pData, int16_t bytes, bool masterscan, uint64_t groupId,
                                   SExecTaskInfo* pTaskInfo, bool isIntervalQuery, SAggSupporter* pSup);

SOperatorInfo* createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, tsdbReaderT pDataReader, SReadHandle* pHandle, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pPhyNode,
                                         STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode *pScanPhyNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SExprInfo* pScalarExprInfo,
                                           int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode *pNode, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortPhyNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMultiwaySortMergeOperatorInfo(SOperatorInfo** downStreams, int32_t numStreams, SSDataBlock* pInputBlock,
                                                   SSDataBlock* pResBlock, SArray* pSortInfo, SArray* pColMatchColInfo,
                                                   SExecTaskInfo* pTaskInfo);
SOperatorInfo* createSortedMergeOperatorInfo(SOperatorInfo** downstream, int32_t numOfDownstream, SExprInfo* pExprInfo, int32_t num, SArray* pSortInfo, SArray* pGroupInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          STimeWindowAggSupp *pTwAggSupp, SExecTaskInfo* pTaskInfo, bool isStream);

SOperatorInfo* createMergeIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream,
    SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, int32_t numOfChild);
SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                                SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                                STimeWindowAggSupp *pTwAggSupp, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                            SSDataBlock* pResBlock, int64_t gap, int32_t tsSlotId, STimeWindowAggSupp* pTwAggSupp,
                                            SExecTaskInfo* pTaskInfo);
SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                       SSDataBlock* pResultBlock, SArray* pGroupColList, SNode* pCondition,
                                       SExprInfo* pScalarExprInfo, int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createDataBlockInfoScanOperator(void* dataReader, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamScanOperatorInfo(void* pDataReader, SReadHandle* pHandle,
    STableScanPhysiNode* pTableScanNode, SExecTaskInfo* pTaskInfo, STimeWindowAggSupp* pTwSup);

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SFillPhysiNode* pPhyFillNode, bool multigroupResult,
                                      SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                             SSDataBlock* pResBlock, STimeWindowAggSupp *pTwAggSupp, int32_t tsSlotId, SColumn* pStateKeyCol, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, const SNodeListNode* pValNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SJoinPhysiNode* pJoinNode,
                                           SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream,
    SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream,
    SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, int32_t numOfChild);

SOperatorInfo* createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo);

#if 0
SOperatorInfo* createTableSeqScanOperatorInfo(void* pTsdbReadHandle, STaskRuntimeEnv* pRuntimeEnv);
#endif

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                           int32_t numOfOutput, SArray* pPseudoList);

void setInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order, int32_t scanFlag, bool createDummyCol);

bool    isTaskKilled(SExecTaskInfo* pTaskInfo);
int32_t checkForQueryBuf(size_t numOfTables);

void    setTaskKilled(SExecTaskInfo* pTaskInfo);
void    queryCostStatis(SExecTaskInfo* pTaskInfo);

void    doDestroyTask(SExecTaskInfo* pTaskInfo);
int32_t getMaximumIdleDurationSec();

/*
 * ops:     root operator
 * data:    *data save the result of encode, need to be freed by caller
 * length:  *length save the length of *data
 * return:  result code, 0 means success
 */
int32_t encodeOperator(SOperatorInfo* ops, char** data, int32_t *length);

/*
 * ops:    root operator, created by caller
 * data:   save the result of decode
 * length: the length of data
 * return: result code, 0 means success
 */
int32_t decodeOperator(SOperatorInfo* ops, const char* data, int32_t length);

void    setTaskStatus(SExecTaskInfo* pTaskInfo, int8_t status);
int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                               const char* sql, EOPTR_EXEC_MODEL model);
int32_t createDataSinkParam(SDataSinkNode *pNode, void **pParam, qTaskInfo_t* pTaskInfo);                               
int32_t getOperatorExplainExecInfo(SOperatorInfo* operatorInfo, SExplainExecInfo** pRes, int32_t* capacity,
                                   int32_t* resNum);

int32_t aggDecodeResultRow(SOperatorInfo* pOperator, char* result);
int32_t aggEncodeResultRow(SOperatorInfo* pOperator, char** result, int32_t* length);

STimeWindow getActiveTimeWindow(SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int64_t ts, SInterval* pInterval,
                                int32_t precision, STimeWindow* win);
int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos, TSKEY ekey,
    __block_search_fn_t searchFn, STableQueryInfo* item, int32_t order);
int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, const char* pKey, SqlFunctionCtx* pCtx, int32_t numOfOutput,
    int32_t size);
SResultRow* getNewResultRow(SDiskbasedBuf* pResultBuf, int64_t tableGroupId, int32_t interBufSize);
SResultWindowInfo* getSessionTimeWindow(SStreamAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, int64_t gap, int32_t* pIndex);
int32_t updateSessionWindowInfo(SResultWindowInfo* pWinInfo, TSKEY* pTs, int32_t rows, int32_t start, int64_t gap,
    SHashObj* pStDeleted);

bool functionNeedToExecute(SqlFunctionCtx* pCtx);

int32_t compareTimeWindow(const void* p1, const void* p2, const void* param);
int32_t finalizeResultRowIntoResultDataBlock(SDiskbasedBuf* pBuf, SResultRowPosition* resultRowPosition,
                                       SqlFunctionCtx* pCtx, SExprInfo* pExprInfo, int32_t numOfExprs, const int32_t* rowCellOffset,
                                       SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo);

int32_t createMultipleDataReaders(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle,
                                  STableListInfo* pTableListInfo, SArray* arrayReader, uint64_t queryId,
                                  uint64_t taskId, SNode* pTagCond);
SOperatorInfo* createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SArray* dataReaders,
                                                SReadHandle* readHandle, SExecTaskInfo* pTaskInfo);

void copyUpdateDataBlock(SSDataBlock* pDest, SSDataBlock* pSource, int32_t tsColIndex);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EXECUTORIMPL_H
