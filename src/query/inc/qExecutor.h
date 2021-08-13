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
#ifndef TDENGINE_QEXECUTOR_H
#define TDENGINE_QEXECUTOR_H

#include "os.h"

#include "hash.h"
#include "qAggMain.h"
#include "qFill.h"
#include "qResultbuf.h"
#include "qSqlparser.h"
#include "qTableMeta.h"
#include "qTsbuf.h"
#include "query.h"
#include "taosdef.h"
#include "tarray.h"
#include "tlockfree.h"
#include "tsdb.h"
#include "qUdf.h"

struct SColumnFilterElem;
typedef bool (*__filter_func_t)(struct SColumnFilterElem* pFilter, const char* val1, const char* val2, int16_t type);
typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

#define IS_QUERY_KILLED(_q) ((_q)->code == TSDB_CODE_TSC_QUERY_CANCELLED)
#define Q_STATUS_EQUAL(p, s)  (((p) & (s)) != 0u)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define GET_TABLEGROUP(q, _index)   ((SArray*) taosArrayGetP((q)->tableqinfoGroupInfo.pGroupList, (_index)))

#define GET_NUM_OF_RESULTS(_r) (((_r)->outputBuf) == NULL? 0:((_r)->outputBuf)->info.rows)

enum {
  // when query starts to execute, this status will set
      QUERY_NOT_COMPLETED = 0x1u,

  /* query is over
   * 1. this status is used in one row result query process, e.g., count/sum/first/last/ avg...etc.
   * 2. when all data within queried time window, it is also denoted as query_completed
   */
      QUERY_COMPLETED = 0x2u,

  /* when the result is not completed return to client, this status will be
   * usually used in case of interval query with interpolation option
   */
      QUERY_OVER = 0x4u,
};

typedef struct SResultRowPool {
  int32_t elemSize;
  int32_t blockSize;
  int32_t numOfElemPerBlock;

  struct {
    int32_t blockIndex;
    int32_t pos;
  } position;

  SArray* pData;    // SArray<void*>
} SResultRowPool;

typedef struct SResultRow {
  int32_t       pageId;      // pageId & rowId is the position of current result in disk-based output buffer
  int32_t       offset:29;   // row index in buffer page
  bool          startInterp; // the time window start timestamp has done the interpolation already.
  bool          endInterp;   // the time window end timestamp has done the interpolation already.
  bool          closed;      // this result status: closed or opened
  uint32_t      numOfRows;   // number of rows of current time window
  SResultRowCellInfo*  pCellInfo;  // For each result column, there is a resultInfo
  STimeWindow   win;
  char         *key;               // start key of current result row
} SResultRow;

typedef struct SGroupResInfo {
  int32_t totalGroup;
  int32_t currentGroup;
  int32_t index;
  SArray* pRows;      // SArray<SResultRow*>
} SGroupResInfo;

/**
 * If the number of generated results is greater than this value,
 * query query will be halt and return results to client immediate.
 */
typedef struct SRspResultInfo {
  int64_t total;      // total generated result size in rows
  int32_t capacity;   // capacity of current result output buffer
  int32_t threshold;  // result size threshold in rows.
} SRspResultInfo;

typedef struct SResultRowInfo {
  SResultRow** pResult;    // result list
  int16_t      type:8;     // data type for hash key
  int32_t      size:24;    // number of result set
  int32_t      capacity;   // max capacity
  int32_t      curPos;     // current active result row index of pResult list
} SResultRowInfo;

typedef struct SColumnFilterElem {
  int16_t           bytes;  // column length
  __filter_func_t   fp;
  SColumnFilterInfo filterInfo;
  void              *q;
} SColumnFilterElem;

typedef struct SSingleColumnFilterInfo {
  void*              pData;
  void*              pData2;  //used for nchar column
  int32_t            numOfFilters;
  SColumnInfo        info;
  SColumnFilterElem* pFilters;
} SSingleColumnFilterInfo;

typedef struct STableQueryInfo {
  TSKEY       lastKey;
  int32_t     groupIndex;     // group id in table list
  tVariant    tag;
  STimeWindow win;
  STSCursor   cur;
  void*       pTable;         // for retrieve the page id list
  SResultRowInfo resInfo;
} STableQueryInfo;

typedef enum {
  QUERY_PROF_BEFORE_OPERATOR_EXEC = 0,
  QUERY_PROF_AFTER_OPERATOR_EXEC,
  QUERY_PROF_QUERY_ABORT
} EQueryProfEventType;

typedef struct {
  EQueryProfEventType eventType;
  int64_t eventTime;

  union {
    uint8_t operatorType; //for operator event
    int32_t abortCode; //for query abort event
  };
} SQueryProfEvent;

typedef struct {
  uint8_t operatorType;
  int64_t sumSelfTime;
  int64_t sumRunTimes;
} SOperatorProfResult;

typedef struct SQueryCostInfo {
  uint64_t loadStatisTime;
  uint64_t loadFileBlockTime;
  uint64_t loadDataInCacheTime;
  uint64_t loadStatisSize;
  uint64_t loadFileBlockSize;
  uint64_t loadDataInCacheSize;
  
  uint64_t loadDataTime;
  uint64_t totalRows;
  uint64_t totalCheckedRows;
  uint32_t totalBlocks;
  uint32_t loadBlocks;
  uint32_t loadBlockStatis;
  uint32_t discardBlocks;
  uint64_t elapsedTime;
  uint64_t firstStageMergeTime;
  uint64_t winInfoSize;
  uint64_t tableInfoSize;
  uint64_t hashSize;
  uint64_t numOfTimeWindows;

  SArray*   queryProfEvents;  //SArray<SQueryProfEvent>
  SHashObj* operatorProfResults; //map<operator_type, SQueryProfEvent>
} SQueryCostInfo;

typedef struct {
  int64_t vgroupLimit;
  int64_t ts;
} SOrderedPrjQueryInfo;

typedef struct {
  char*   tags;
  SArray* pResult;  // SArray<SStddevInterResult>
} SInterResult;

typedef struct SSDataBlock {
  SDataStatis *pBlockStatis;
  SArray      *pDataBlock;
  SDataBlockInfo info;
} SSDataBlock;

// The basic query information extracted from the SQueryInfo tree to support the
// execution of query in a data node.
typedef struct SQueryAttr {
  SLimitVal        limit;
  SLimitVal        slimit;

  bool             stableQuery;      // super table query or not
  bool             topBotQuery;      // TODO used bitwise flag
  bool             groupbyColumn;    // denote if this is a groupby normal column query
  bool             hasTagResults;    // if there are tag values in final result or not
  bool             timeWindowInterpo;// if the time window start/end required interpolation
  bool             queryBlockDist;    // if query data block distribution
  bool             stabledev;        // super table stddev query
  bool             tsCompQuery;      // is tscomp query
  bool             diffQuery;        // is diff query
  bool             simpleAgg;
  bool             pointInterpQuery; // point interpolation query
  bool             needReverseScan;  // need reverse scan
  bool             distinct;         // distinct  query or not
  bool             stateWindow;       // window State on sub/normal table
  bool             createFilterOperator; // if filter operator is needed
  int32_t          interBufSize;     // intermediate buffer sizse

  int32_t          havingNum;        // having expr number

  SOrderVal        order;
  int16_t          numOfCols;
  int16_t          numOfTags;

  STimeWindow      window;
  SInterval        interval;
  SSessionWindow   sw;
  int16_t          precision;
  int16_t          numOfOutput;
  int16_t          fillType;

  int32_t          srcRowSize;       // todo extract struct
  int32_t          resultRowSize;
  int32_t          intermediateResultRowSize; // intermediate result row size, in case of top-k query.
  int32_t          maxTableColumnWidth;
  int32_t          tagLen;           // tag value length of current query
  SGroupbyExpr    *pGroupbyExpr;

  SExprInfo*       pExpr1;
  SExprInfo*       pExpr2;
  int32_t          numOfExpr2;
  SExprInfo*       pExpr3;
  int32_t          numOfExpr3;

  SColumnInfo*     tableCols;
  SColumnInfo*     tagColList;
  int32_t          numOfFilterCols;
  int64_t*         fillVal;
  SOrderedPrjQueryInfo prjInfo;        // limit value for each vgroup, only available in global order projection query.

  SSingleColumnFilterInfo* pFilterInfo;
  SFilterInfo     *pFilters;
  
  void*            tsdb;
  SMemRef          memRef;
  STableGroupInfo  tableGroupInfo;       // table <tid, last_key> list  SArray<STableKeyInfo>
  int32_t          vgId;
  SArray          *pUdfInfo;             // no need to free
} SQueryAttr;

typedef SSDataBlock* (*__operator_fn_t)(void* param, bool* newgroup);
typedef void (*__optr_cleanup_fn_t)(void* param, int32_t num);

struct SOperatorInfo;

typedef struct SQueryRuntimeEnv {
  jmp_buf               env;
  SQueryAttr*           pQueryAttr;
  uint32_t              status;           // query status
  void*                 qinfo;
  uint8_t               scanFlag;         // denotes reversed scan of data or not
  void*                 pQueryHandle;

  int32_t               prevGroupId;      // previous executed group id
  bool                  enableGroupData;
  SDiskbasedResultBuf*  pResultBuf;       // query result buffer based on blocked-wised disk file
  SHashObj*             pResultRowHashTable; // quick locate the window object for each result
  SHashObj*             pResultRowListSet;   // used to check if current ResultRowInfo has ResultRow object or not
  char*                 keyBuf;           // window key buffer
  SResultRowPool*       pool;             // window result object pool
  char**                prevRow;

  SArray*               prevResult;       // intermediate result, SArray<SInterResult>
  STSBuf*               pTsBuf;           // timestamp filter list
  STSCursor             cur;

  char*                 tagVal;           // tag value of current data block
  SArithmeticSupport   *sasArray;

  SSDataBlock          *outputBuf;
  STableGroupInfo       tableqinfoGroupInfo;  // this is a group array list, including SArray<STableQueryInfo*> structure
  struct SOperatorInfo *proot;
  SGroupResInfo         groupResInfo;
  int64_t               currentOffset;   // dynamic offset value

  STableQueryInfo      *current;
  SRspResultInfo        resultInfo;
  SHashObj             *pTableRetrieveTsMap;
  SUdfInfo             *pUdfInfo;
} SQueryRuntimeEnv;

enum {
  OP_IN_EXECUTING   = 1,
  OP_RES_TO_RETURN  = 2,
  OP_EXEC_DONE      = 3,
};

enum OPERATOR_TYPE_E {
  OP_TableScan         = 1,
  OP_DataBlocksOptScan = 2,
  OP_TableSeqScan      = 3,
  OP_TagScan           = 4,
  OP_TableBlockInfoScan= 5,
  OP_Aggregate         = 6,
  OP_Project           = 7,
  OP_Groupby           = 8,
  OP_Limit             = 9,
  OP_SLimit            = 10,
  OP_TimeWindow        = 11,
  OP_SessionWindow     = 12,
  OP_Fill              = 13,
  OP_MultiTableAggregate     = 14,
  OP_MultiTableTimeInterval  = 15,
  OP_DummyInput        = 16,   //TODO remove it after fully refactor.
  OP_MultiwayMergeSort = 17,   // multi-way data merge into one input stream.
  OP_GlobalAggregate   = 18,   // global merge for the multi-way data sources.
  OP_Filter            = 19,
  OP_Distinct          = 20,
  OP_Join              = 21,
  OP_StateWindow       = 22,
  OP_AllTimeWindow     = 23,
  OP_AllMultiTableTimeInterval = 24,
  OP_Order             = 25,
};

typedef struct SOperatorInfo {
  uint8_t               operatorType;
  bool                  blockingOptr;  // block operator or not
  uint8_t               status;        // denote if current operator is completed
  int32_t               numOfOutput;   // number of columns of the current operator results
  char                 *name;          // name, used to show the query execution plan
  void                 *info;          // extension attribution
  SExprInfo            *pExpr;
  SQueryRuntimeEnv     *pRuntimeEnv;

  struct SOperatorInfo **upstream;     // upstream pointer list
  int32_t               numOfUpstream; // number of upstream. The value is always ONE expect for join operator
  __operator_fn_t       exec;
  __optr_cleanup_fn_t   cleanup;
} SOperatorInfo;

enum {
  QUERY_RESULT_NOT_READY = 1,
  QUERY_RESULT_READY     = 2,
};

typedef struct {
  int32_t      numOfTags;
  int32_t      numOfCols;
  SColumnInfo *colList;
} SQueriedTableInfo;

typedef struct SQInfo {
  void*            signature;
  uint64_t         qId;
  int32_t          code;        // error code to returned to client
  int64_t          owner;       // if it is in execution

  SQueryRuntimeEnv runtimeEnv;
  SQueryAttr       query;
  void*            pBuf;        // allocated buffer for STableQueryInfo, sizeof(STableQueryInfo)*numOfTables;

  pthread_mutex_t  lock;        // used to synchronize the rsp/query threads
  tsem_t           ready;
  int32_t          dataReady;   // denote if query result is ready or not
  void*            rspContext;  // response context
  int64_t          startExecTs; // start to exec timestamp
  char*            sql;         // query sql string
  SQueryCostInfo   summary;
} SQInfo;

typedef struct SQueryParam {
  char            *sql;
  char            *tagCond;
  char            *colCond;
  char            *tbnameCond;
  char            *prevResult;
  SArray          *pTableIdList;
  SSqlExpr       **pExpr;
  SSqlExpr       **pSecExpr;
  SExprInfo       *pExprs;
  SExprInfo       *pSecExprs;

  SFilterInfo     *pFilters;

  SColIndex       *pGroupColIndex;
  SColumnInfo     *pTagColumnInfo;
  SGroupbyExpr *pGroupbyExpr;
  int32_t          tableScanOperator;
  SArray          *pOperator;
  SUdfInfo        *pUdfInfo;
} SQueryParam;

typedef struct STableScanInfo {
  void           *pQueryHandle;
  int32_t         numOfBlocks;
  int32_t         numOfSkipped;
  int32_t         numOfBlockStatis;
  int64_t         numOfRows;
                 
  int32_t         order;        // scan order
  int32_t         times;        // repeat counts
  int32_t         current;
  int32_t         reverseTimes; // 0 by default

  SQLFunctionCtx *pCtx;         // next operator query context
  SResultRowInfo *pResultRowInfo;
  int32_t        *rowCellInfoOffset;
  SExprInfo      *pExpr;
  SSDataBlock     block;
  int32_t         numOfOutput;
  int64_t         elapsedTime;

  int32_t         tableIndex;
  int32_t         prevGroupId;     // previous table group id
} STableScanInfo;

typedef struct STagScanInfo {
  SColumnInfo* pCols;
  SSDataBlock* pRes;
  int32_t      totalTables;
  int32_t      curPos;
} STagScanInfo;

typedef struct SOptrBasicInfo {
  SResultRowInfo    resultRowInfo;
  int32_t          *rowCellInfoOffset;  // offset value for each row result cell info
  SQLFunctionCtx   *pCtx;
  SSDataBlock      *pRes;
} SOptrBasicInfo;

typedef struct SOptrBasicInfo STableIntervalOperatorInfo;

typedef struct SAggOperatorInfo {
  SOptrBasicInfo binfo;
  uint32_t       seed;
} SAggOperatorInfo;

typedef struct SProjectOperatorInfo {
  SOptrBasicInfo binfo;
  int32_t        bufCapacity;
  uint32_t       seed;

  SSDataBlock   *existDataBlock;
} SProjectOperatorInfo;

typedef struct SLimitOperatorInfo {
  int64_t   limit;
  int64_t   total;
} SLimitOperatorInfo;

typedef struct SSLimitOperatorInfo {
  int64_t   groupTotal;
  int64_t   currentGroupOffset;

  int64_t   rowsTotal;
  int64_t   currentOffset;
  SLimitVal limit;
  SLimitVal slimit;

  char    **prevRow;
  SArray   *orderColumnList;
} SSLimitOperatorInfo;

typedef struct SFilterOperatorInfo {
  SSingleColumnFilterInfo *pFilterInfo;
  int32_t numOfFilterCols;
} SFilterOperatorInfo;

typedef struct SFillOperatorInfo {
  SFillInfo   *pFillInfo;
  SSDataBlock *pRes;
  int64_t      totalInputRows;

  SSDataBlock *existNewGroupBlock;
} SFillOperatorInfo;

typedef struct SGroupbyOperatorInfo {
  SOptrBasicInfo binfo;
  int32_t        colIndex;
  char          *prevData;   // previous group by value
} SGroupbyOperatorInfo;

typedef struct SSWindowOperatorInfo {
  SOptrBasicInfo binfo;
  STimeWindow    curWindow;  // current time window
  TSKEY          prevTs;     // previous timestamp
  int32_t        numOfRows;  // number of rows
  int32_t        start;      // start row index
  bool           reptScan;    // next round scan
} SSWindowOperatorInfo;

typedef struct SStateWindowOperatorInfo {
  SOptrBasicInfo binfo;
  STimeWindow    curWindow;  // current time window
  int32_t        numOfRows;  // number of rows
  int32_t        colIndex;      // start row index
  int32_t        start;
  char*          prevData;    // previous data 
  bool           reptScan;
} SStateWindowOperatorInfo;

typedef struct SDistinctOperatorInfo {
  SHashObj         *pSet;
  SSDataBlock      *pRes;
  bool              recordNullVal;  //has already record the null value, no need to try again
  int64_t           threshold;
  int64_t           outputCapacity;
  int32_t           colIndex; 
} SDistinctOperatorInfo;

struct SGlobalMerger;

typedef struct SMultiwayMergeInfo {
  struct SGlobalMerger *pMerge;
  SOptrBasicInfo       binfo;
  int32_t              bufCapacity;
  int64_t              seed;
  char               **prevRow;
  SArray              *orderColumnList;
  int32_t              resultRowFactor;

  bool                 hasGroupColData;
  char               **currentGroupColData;
  SArray              *groupColumnList;
  bool                 hasDataBlockForNewGroup;
  SSDataBlock         *pExistBlock;

  bool                 hasPrev;
  bool                 groupMix;
  SArray              *udfInfo;
} SMultiwayMergeInfo;

// todo support the disk-based sort
typedef struct SOrderOperatorInfo {
  int32_t      colIndex;
  int32_t      order;
  SSDataBlock *pDataBlock;
} SOrderOperatorInfo;

void appendUpstream(SOperatorInfo* p, SOperatorInfo* pUpstream);

SOperatorInfo* createDataBlocksOptScanInfo(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime, int32_t reverseTime);
SOperatorInfo* createTableScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv, int32_t repeatTime);
SOperatorInfo* createTableSeqScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv);

SOperatorInfo* createAggregateOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createProjectOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createLimitOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream);
SOperatorInfo* createTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createAllTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createSWindowOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createFillOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createGroupbyOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createMultiTableAggOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createMultiTableTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createAllMultiTableTimeIntervalOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createTagScanOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createDistinctOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createTableBlockInfoScanOperator(void* pTsdbQueryHandle, SQueryRuntimeEnv* pRuntimeEnv);
SOperatorInfo* createMultiwaySortOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SExprInfo* pExpr, int32_t numOfOutput,
                                              int32_t numOfRows, void* merger, bool groupMix);
SOperatorInfo* createGlobalAggregateOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput, void* param, SArray* pUdfInfo);
SOperatorInfo* createStatewindowOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createSLimitOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput, void* merger);
SOperatorInfo* createFilterOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr,
                                        int32_t numOfOutput, SColumnInfo* pCols, int32_t numOfFilter);

SOperatorInfo* createJoinOperatorInfo(SOperatorInfo** pUpstream, int32_t numOfUpstream, SSchema* pSchema, int32_t numOfOutput);
SOperatorInfo* createOrderOperatorInfo(SQueryRuntimeEnv* pRuntimeEnv, SOperatorInfo* upstream, SExprInfo* pExpr, int32_t numOfOutput, SOrderVal* pOrderVal);

SSDataBlock* doGlobalAggregate(void* param, bool* newgroup);
SSDataBlock* doMultiwayMergeSort(void* param, bool* newgroup);
SSDataBlock* doSLimit(void* param, bool* newgroup);

int32_t doCreateFilterInfo(SColumnInfo* pCols, int32_t numOfCols, int32_t numOfFilterCols, SSingleColumnFilterInfo** pFilterInfo, uint64_t qId);
void doSetFilterColumnInfo(SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols, SSDataBlock* pBlock);
void doSetFilterColInfo(SFilterInfo     *pFilters, SSDataBlock* pBlock);
bool doFilterDataBlock(SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols, int32_t numOfRows, int8_t* p);
void doCompactSDataBlock(SSDataBlock* pBlock, int32_t numOfRows, int8_t* p);

SSDataBlock* createOutputBuf(SExprInfo* pExpr, int32_t numOfOutput, int32_t numOfRows);
void* destroyOutputBuf(SSDataBlock* pBlock);
void* doDestroyFilterInfo(SSingleColumnFilterInfo* pFilterInfo, int32_t numOfFilterCols);

void setInputDataBlock(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order);
int32_t getNumOfResult(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx* pCtx, int32_t numOfOutput);
void finalizeQueryResult(SOperatorInfo* pOperator, SQLFunctionCtx* pCtx, SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset);
void updateOutputBuf(SOptrBasicInfo* pBInfo, int32_t *bufCapacity, int32_t numOfInputRows);
void clearOutputBuf(SOptrBasicInfo* pBInfo, int32_t *bufCapacity);

void freeParam(SQueryParam *param);
int32_t convertQueryMsg(SQueryTableMsg *pQueryMsg, SQueryParam* param);
int32_t createQueryFunc(SQueriedTableInfo* pTableInfo, int32_t numOfOutput, SExprInfo** pExprInfo,
                        SSqlExpr** pExprMsg, SColumnInfo* pTagCols, int32_t queryType, void* pMsg, SUdfInfo* pUdfInfo);

int32_t createIndirectQueryFuncExprFromMsg(SQueryTableMsg *pQueryMsg, int32_t numOfOutput, SExprInfo **pExprInfo,
                                           SSqlExpr **pExpr, SExprInfo *prevExpr, SUdfInfo *pUdfInfo);

int32_t createQueryFilter(char *data, uint16_t len, SFilterInfo** pFilters);

SGroupbyExpr *createGroupbyExprFromMsg(SQueryTableMsg *pQueryMsg, SColIndex *pColIndex, int32_t *code);
SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SGroupbyExpr *pGroupbyExpr, SExprInfo *pExprs,
                        SExprInfo *pSecExprs, STableGroupInfo *pTableGroupInfo, SColumnInfo* pTagCols, SFilterInfo* pFilters, int32_t vgId, char* sql, uint64_t qId, SUdfInfo* pUdfInfo);

int32_t initQInfo(STsBufInfo* pTsBufInfo, void* tsdb, void* sourceOptr, SQInfo* pQInfo, SQueryParam* param, char* start,
                  int32_t prevResultLen, void* merger);

int32_t createFilterInfo(SQueryAttr* pQueryAttr, uint64_t qId);
void freeColumnFilterInfo(SColumnFilterInfo* pFilter, int32_t numOfFilters);

STableQueryInfo *createTableQueryInfo(SQueryAttr* pQueryAttr, void* pTable, bool groupbyColumn, STimeWindow win, void* buf);
STableQueryInfo* createTmpTableQueryInfo(STimeWindow win);

int32_t buildArithmeticExprFromMsg(SExprInfo *pArithExprInfo, void *pQueryMsg);

bool isQueryKilled(SQInfo *pQInfo);
int32_t checkForQueryBuf(size_t numOfTables);
bool doBuildResCheck(SQInfo* pQInfo);
void setQueryStatus(SQueryRuntimeEnv *pRuntimeEnv, int8_t status);

bool onlyQueryTags(SQueryAttr* pQueryAttr);
void destroyUdfInfo(SUdfInfo* pUdfInfo);

bool isValidQInfo(void *param);

int32_t doDumpQueryResult(SQInfo *pQInfo, char *data);

size_t getResultSize(SQInfo *pQInfo, int64_t *numOfRows);
void setQueryKilled(SQInfo *pQInfo);

void publishOperatorProfEvent(SOperatorInfo* operatorInfo, EQueryProfEventType eventType);
void publishQueryAbortEvent(SQInfo* pQInfo, int32_t code);
void calculateOperatorProfResults(SQInfo* pQInfo);
void queryCostStatis(SQInfo *pQInfo);

void freeQInfo(SQInfo *pQInfo);
void freeQueryAttr(SQueryAttr *pQuery);

int32_t getMaximumIdleDurationSec();

void doInvokeUdf(SUdfInfo* pUdfInfo, SQLFunctionCtx *pCtx, int32_t idx, int32_t type);

#endif  // TDENGINE_QEXECUTOR_H
