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
#ifndef TDENGINE_EXECUTORIMPL_H
#define TDENGINE_EXECUTORIMPL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tsort.h"
#include "tcommon.h"
#include "tlosertree.h"
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
#include "tpagedbuf.h"
#include "tmsg.h"

struct SColumnFilterElem;

typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

#define IS_QUERY_KILLED(_q) ((_q)->code == TSDB_CODE_TSC_QUERY_CANCELLED)
#define Q_STATUS_EQUAL(p, s) (((p) & (s)) != 0u)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define GET_TABLEGROUP(q, _index) ((SArray*)taosArrayGetP((q)->tableqinfoGroupInfo.pGroupList, (_index)))

#define GET_NUM_OF_RESULTS(_r) (((_r)->outputBuf) == NULL ? 0 : ((_r)->outputBuf)->info.rows)

#define NEEDTO_COMPRESS_QUERY(size) ((size) > tsCompressColData ? 1 : 0)

enum {
  // when this task starts to execute, this status will set
  TASK_NOT_COMPLETED = 0x1u,

  /* Task is over
   * 1. this status is used in one row result query process, e.g., count/sum/first/last/ avg...etc.
   * 2. when all data within queried time window, it is also denoted as query_completed
   */
  TASK_COMPLETED = 0x2u,

  /* when the result is not completed return to client, this status will be
   * usually used in case of interval query with interpolation option
   */
  TASK_OVER = 0x4u,
};

typedef struct SResultRowCell {
  uint64_t    groupId;
  SResultRowPosition pos;
} SResultRowCell;

/**
 * If the number of generated results is greater than this value,
 * query query will be halt and return results to client immediate.
 */
typedef struct SResultInfo { // TODO refactor
  int64_t totalRows;      // total generated result size in rows
  int64_t totalBytes;     // total results in bytes.
  int32_t capacity;       // capacity of current result output buffer
  int32_t threshold;      // result size threshold in rows.
} SResultInfo;

typedef struct SColumnFilterElem {
  int16_t           bytes;  // column length
  __filter_func_t   fp;
  SColumnFilterInfo filterInfo;
  void*             q;
} SColumnFilterElem;

typedef struct SSingleColumnFilterInfo {
  void*              pData;
  void*              pData2;  // used for nchar column
  int32_t            numOfFilters;
  SColumnInfo        info;
  SColumnFilterElem* pFilters;
} SSingleColumnFilterInfo;

typedef struct STableQueryInfo {
  TSKEY          lastKey;     // last check ts
  uint64_t       uid;         // table uid
  int32_t        groupIndex;  // group id in table list
//  SVariant       tag;
  SResultRowInfo resInfo;     // result info
} STableQueryInfo;

typedef enum {
  QUERY_PROF_BEFORE_OPERATOR_EXEC = 0,
  QUERY_PROF_AFTER_OPERATOR_EXEC,
  QUERY_PROF_QUERY_ABORT
} EQueryProfEventType;

typedef struct {
  EQueryProfEventType eventType;
  int64_t             eventTime;

  union {
    uint8_t operatorType;  // for operator event
    int32_t abortCode;     // for query abort event
  };
} SQueryProfEvent;

typedef struct {
  uint8_t operatorType;
  int64_t sumSelfTime;
  int64_t sumRunTimes;
} SOperatorProfResult;

typedef struct SLimit {
  int64_t limit;
  int64_t offset;
} SLimit;

typedef struct STaskCostInfo {
  int64_t created;
  int64_t start;
  int64_t end;

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

  SArray*   queryProfEvents;      // SArray<SQueryProfEvent>
  SHashObj* operatorProfResults;  // map<operator_type, SQueryProfEvent>
} STaskCostInfo;

typedef struct SOperatorCostInfo {
  uint64_t openCost;
  uint64_t totalCost;
} SOperatorCostInfo;

typedef struct SOrder {
  uint32_t order;
  SColumn  col;
} SOrder;

// The basic query information extracted from the SQueryInfo tree to support the
// execution of query in a data node.
typedef struct STaskAttr {
  SLimit limit;
  SLimit slimit;

  // todo comment it
  bool    stableQuery;        // super table query or not
  bool    topBotQuery;        // TODO used bitwise flag
  bool    groupbyColumn;      // denote if this is a groupby normal column query
  bool    hasTagResults;      // if there are tag values in final result or not
  bool    timeWindowInterpo;  // if the time window start/end required interpolation
  bool    queryBlockDist;     // if query data block distribution
  bool    stabledev;          // super table stddev query
  bool    tsCompQuery;        // is tscomp query
  bool    diffQuery;          // is diff query
  bool    simpleAgg;
  bool    pointInterpQuery;      // point interpolation query
  bool    needReverseScan;       // need reverse scan
  bool    distinct;              // distinct  query or not
  bool    stateWindow;           // window State on sub/normal table
  bool    createFilterOperator;  // if filter operator is needed
  bool    multigroupResult;      // multigroup result can exist in one SSDataBlock
  int32_t interBufSize;          // intermediate buffer sizse

  int32_t havingNum;  // having expr number

  SOrder  order;
  int16_t numOfCols;
  int16_t numOfTags;

  STimeWindow    window;
  SInterval      interval;
  int16_t        precision;
  int16_t        numOfOutput;
  int16_t        fillType;

  int32_t       srcRowSize;  // todo extract struct
  int32_t       resultRowSize;
  int32_t       intermediateResultRowSize;  // intermediate result row size, in case of top-k query.
  int32_t       maxTableColumnWidth;
  int32_t       tagLen;  // tag value length of current query

  SExprInfo* pExpr1;

  SColumnInfo*         tableCols;
  SColumnInfo*         tagColList;
  int32_t              numOfFilterCols;
  int64_t*             fillVal;

  SSingleColumnFilterInfo* pFilterInfo;
  void*           tsdb;
  STableGroupInfo tableGroupInfo;  // table <tid, last_key> list  SArray<STableKeyInfo>
  int32_t         vgId;
  SArray*         pUdfInfo;  // no need to free
} STaskAttr;

struct SOperatorInfo;
struct SAggSupporter;
struct SOptrBasicInfo;

typedef void (*__optr_encode_fn_t)(struct SOperatorInfo* pOperator, struct SAggSupporter *pSup, struct SOptrBasicInfo *pInfo, char **result, int32_t *length);
typedef bool (*__optr_decode_fn_t)(struct SOperatorInfo* pOperator, struct SAggSupporter *pSup, struct SOptrBasicInfo *pInfo, char *result, int32_t length);

typedef int32_t (*__optr_open_fn_t)(struct SOperatorInfo* pOptr);
typedef SSDataBlock* (*__optr_fn_t)(struct SOperatorInfo* pOptr, bool* newgroup);
typedef void (*__optr_close_fn_t)(void* param, int32_t num);
typedef int32_t (*__optr_get_explain_fn_t)(struct SOperatorInfo* pOptr, void **pOptrExplain);

typedef struct STaskIdInfo {
  uint64_t queryId;  // this is also a request id
  uint64_t subplanId;
  uint64_t templateId;
  char*    str;
} STaskIdInfo;

typedef struct SExecTaskInfo {
  STaskIdInfo     id;
  char*           content;
  uint32_t        status;
  STimeWindow     window;
  STaskCostInfo   cost;
  int64_t         owner;  // if it is in execution
  int32_t         code;
  uint64_t        totalRows;            // total number of rows
  STableGroupInfo tableqinfoGroupInfo;  // this is a group array list, including SArray<STableQueryInfo*> structure
  char*           sql;                  // query sql string
  jmp_buf         env;                  // jump to this position when error happens.
  EOPTR_EXEC_MODEL execModel;            // operator execution model [batch model|stream model]
  struct SOperatorInfo* pRoot;
} SExecTaskInfo;

typedef struct STaskRuntimeEnv {
  jmp_buf         env;
  STaskAttr*      pQueryAttr;
  uint32_t        status;  // query status
  void*           qinfo;
  uint8_t         scanFlag;  // denotes reversed scan of data or not
  void*           pTsdbReadHandle;

  int32_t         prevGroupId;  // previous executed group id
  bool            enableGroupData;
  SDiskbasedBuf*  pResultBuf;           // query result buffer based on blocked-wised disk file
  SHashObj*       pResultRowHashTable;  // quick locate the window object for each result
  SHashObj*       pResultRowListSet;    // used to check if current ResultRowInfo has ResultRow object or not
  SArray*         pResultRowArrayList;  // The array list that contains the Result rows
  char*           keyBuf;               // window key buffer
  // The window result objects pool, all the resultRow Objects are allocated and managed by this object.
  char**          prevRow;
  SArray*         prevResult;  // intermediate result, SArray<SInterResult>
  STSBuf*         pTsBuf;      // timestamp filter list
  STSCursor       cur;

  char*           tagVal;  // tag value of current data block
  struct SScalarFunctionSupport* scalarSup;

  SSDataBlock*    outputBuf;
  STableGroupInfo tableqinfoGroupInfo;  // this is a group array list, including SArray<STableQueryInfo*> structure
  struct SOperatorInfo* proot;
  SGroupResInfo   groupResInfo;
  int64_t         currentOffset;  // dynamic offset value

  STableQueryInfo* current;
  SResultInfo   resultInfo;
  SHashObj*        pTableRetrieveTsMap;
  struct SUdfInfo* pUdfInfo;
} STaskRuntimeEnv;

enum {
  OP_NOT_OPENED    = 0x0,
  OP_OPENED        = 0x1,
  OP_RES_TO_RETURN = 0x5,
  OP_EXEC_DONE     = 0x9,
};

typedef struct SOperatorInfo {
  uint8_t                 operatorType;
  bool                    blockingOptr;  // block operator or not
  uint8_t                 status;        // denote if current operator is completed
  int32_t                 numOfOutput;   // number of columns of the current operator results
  char*                   name;          // name, used to show the query execution plan
  void*                   info;          // extension attribution
  SExprInfo*              pExpr;
  STaskRuntimeEnv*        pRuntimeEnv;   // todo remove it
  SExecTaskInfo*          pTaskInfo;
  SOperatorCostInfo       cost;
  SResultInfo             resultInfo;
  struct SOperatorInfo**  pDownstream;      // downstram pointer list
  int32_t                 numOfDownstream;  // number of downstream. The value is always ONE expect for join operator
  __optr_open_fn_t        _openFn;          // DO NOT invoke this function directly
  __optr_fn_t             getNextFn;
  __optr_fn_t             getStreamResFn;   // execute the aggregate in the stream model.
  __optr_fn_t             cleanupFn;        // call this function to release the allocated resources ASAP
  __optr_close_fn_t       closeFn;
  __optr_encode_fn_t      encodeResultRow;
  __optr_decode_fn_t      decodeResultRow;
  __optr_get_explain_fn_t getExplainFn;
} SOperatorInfo;

typedef struct {
  int32_t      numOfTags;
  int32_t      numOfCols;
  SColumnInfo* colList;
} SQueriedTableInfo;

typedef struct SQInfo {
  void*    signature;
  uint64_t qId;
  int32_t  code;   // error code to returned to client
  int64_t  owner;  // if it is in execution

  STaskRuntimeEnv runtimeEnv;
  STaskAttr       query;
  void*           pBuf;  // allocated buffer for STableQueryInfo, sizeof(STableQueryInfo)*numOfTables;

  TdThreadMutex lock;  // used to synchronize the rsp/query threads
  tsem_t          ready;
  int32_t         dataReady;    // denote if query result is ready or not
  void*           rspContext;   // response context
  int64_t         startExecTs;  // start to exec timestamp
  char*           sql;          // query sql string
  STaskCostInfo   summary;
} SQInfo;

typedef enum {
  EX_SOURCE_DATA_NOT_READY = 0x1,
  EX_SOURCE_DATA_READY     = 0x2,
  EX_SOURCE_DATA_EXHAUSTED = 0x3,
} EX_SOURCE_STATUS;

typedef struct SSourceDataInfo {
  struct SExchangeInfo *pEx;
  int32_t               index;
  SRetrieveTableRsp    *pRsp;
  uint64_t              totalRows;
  int32_t               code;
  EX_SOURCE_STATUS      status;
} SSourceDataInfo;

typedef struct SLoadRemoteDataInfo {
  uint64_t           totalSize;     // total load bytes from remote
  uint64_t           totalRows;     // total number of rows
  uint64_t           totalElapsed;  // total elapsed time
} SLoadRemoteDataInfo;

typedef struct SExchangeInfo {
  SArray*            pSources;
  SArray*            pSourceDataInfo;
  tsem_t             ready;
  void*              pTransporter;
  SSDataBlock*       pResult;
  bool               seqLoadData;   // sequential load data or not, false by default
  int32_t            current;
  SLoadRemoteDataInfo loadInfo;
} SExchangeInfo;

typedef struct SColMatchInfo {
  int32_t colId;
  int32_t targetSlotId;
  bool    output;
} SColMatchInfo;

typedef struct STableScanInfo {
  void*           dataReader;
  int32_t         numOfBlocks;  // extract basic running information.
  int32_t         numOfSkipped;
  int32_t         numOfBlockStatis;
  int64_t         numOfRows;
  int32_t         order;  // scan order
  int32_t         times;  // repeat counts
  int32_t         current;
  int32_t         reverseTimes;  // 0 by default
  SNode*          pFilterNode;   // filter operator info
  SqlFunctionCtx* pCtx;  // next operator query context
  SResultRowInfo* pResultRowInfo;
  int32_t*        rowCellInfoOffset;
  SExprInfo*      pExpr;
  SSDataBlock*    pResBlock;
  SArray*         pColMatchInfo;
  int32_t         numOfOutput;
  int64_t         elapsedTime;
  int32_t         prevGroupId;  // previous table group id
  int32_t         scanFlag;  // table scan flag to denote if it is a repeat/reverse/main scan
} STableScanInfo;

typedef struct STagScanInfo {
  SColumnInfo* pCols;
  SSDataBlock* pRes;
  int32_t      totalTables;
  int32_t      curPos;
} STagScanInfo;

typedef struct SStreamBlockScanInfo {
  SArray*      pBlockLists;   // multiple SSDatablock.
  SSDataBlock* pRes;          // result SSDataBlock
  int32_t      blockType;     // current block type
  int32_t      validBlockIndex;    // Is current data has returned?
  SColumnInfo* pCols;         // the output column info
  uint64_t     numOfRows;     // total scanned rows
  uint64_t     numOfExec;     // execution times
  void*        readerHandle;  // stream block reader handle
  SArray*      pColMatchInfo; //
} SStreamBlockScanInfo;

typedef struct SSysTableScanInfo {
  union {
    void* pTransporter;
    void* readHandle;
  };

  SRetrieveMetaTableRsp *pRsp;
  SRetrieveTableReq   req;
  SEpSet              epSet;
  tsem_t              ready;

  int32_t             accountId;
  bool                showRewrite;
  SNode*              pCondition; // db_name filter condition, to discard data that are not in current database
  void               *pCur;       // cursor for iterate the local table meta store.
  SArray             *scanCols;   // SArray<int16_t> scan column id list

  int32_t             type;       // show type, TODO remove it
  SName               name;
  SSDataBlock*        pRes;
  int32_t             capacity;
  int64_t             numOfBlocks;  // extract basic running information.
  SLoadRemoteDataInfo loadInfo;
} SSysTableScanInfo;

typedef struct SOptrBasicInfo {
  SResultRowInfo     resultRowInfo;
  int32_t*           rowCellInfoOffset;  // offset value for each row result cell info
  SqlFunctionCtx*    pCtx;
  SSDataBlock*       pRes;
  int32_t            capacity;  // TODO remove it
} SOptrBasicInfo;

//TODO move the resultrowsiz together with SOptrBasicInfo:rowCellInfoOffset
typedef struct SAggSupporter {
  SHashObj*          pResultRowHashTable;  // quick locate the window object for each result
  SHashObj*          pResultRowListSet;    // used to check if current ResultRowInfo has ResultRow object or not
  SArray*            pResultRowArrayList;  // The array list that contains the Result rows
  char*              keyBuf;               // window key buffer
  SDiskbasedBuf     *pResultBuf;           // query result buffer based on blocked-wised disk file
  int32_t            resultRowSize;        // the result buffer size for each result row, with the meta data size for each row
} SAggSupporter;

typedef struct STableIntervalOperatorInfo {
  SOptrBasicInfo     binfo;                // basic info
  SGroupResInfo      groupResInfo;         // multiple results build supporter
  SInterval          interval;             // interval info
  int32_t            primaryTsIndex;       // primary time stamp slot id from result of downstream operator.
  STimeWindow        win;                  // query time range
  bool               timeWindowInterpo;    // interpolation needed or not
  char             **pRow;                 // previous row/tuple of already processed datablock
  SAggSupporter      aggSup;               // aggregate supporter
  STableQueryInfo   *pCurrent;             // current tableQueryInfo struct
  int32_t            order;                // current SSDataBlock scan order
  EOPTR_EXEC_MODEL   execModel;            // operator execution model [batch model|stream model]
  SArray            *pUpdatedWindow;       // updated time window due to the input data block from the downstream operator.
  SColumnInfoData    timeWindowData;       // query time window info for scalar function execution.
} STableIntervalOperatorInfo;

typedef struct SAggOperatorInfo {
  SOptrBasicInfo     binfo;
  SDiskbasedBuf     *pResultBuf;           // query result buffer based on blocked-wised disk file
  SAggSupporter      aggSup;
  STableQueryInfo   *current;
  uint32_t           groupId;
  SGroupResInfo      groupResInfo;
  STableQueryInfo   *pTableQueryInfo;
} SAggOperatorInfo;

typedef struct SProjectOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SSDataBlock   *existDataBlock;
  SArray        *pPseudoColInfo;
  SLimit         limit;
  SLimit         slimit;

  uint64_t       groupId;
  int64_t        curSOffset;
  int64_t        curGroupOutput;

  int64_t        curOffset;
  int64_t        curOutput;
} SProjectOperatorInfo;

typedef struct SSLimitOperatorInfo {
  int64_t            groupTotal;
  int64_t            currentGroupOffset;
  int64_t            rowsTotal;
  int64_t            currentOffset;
  SLimit             limit;
  SLimit             slimit;
  char**             prevRow;
  SArray*            orderColumnList;
  bool               hasPrev;
  bool               ignoreCurrentGroup;
  bool               multigroupResult;
  SSDataBlock*       pRes;  // result buffer
  SSDataBlock*       pPrevBlock;
  int64_t            capacity;
  int64_t            threshold;
} SSLimitOperatorInfo;

typedef struct SFillOperatorInfo {
  struct SFillInfo* pFillInfo;
  SSDataBlock*      pRes;
  int64_t           totalInputRows;
  void**            p;
  SSDataBlock*      existNewGroupBlock;
  bool              multigroupResult;
  SInterval         intervalInfo;
  int32_t           capacity;
} SFillOperatorInfo;

typedef struct {
  char   *pData;
  bool    isNull;
  int16_t type;
  int32_t bytes;
} SGroupKeys, SStateKeys;

typedef struct SGroupbyOperatorInfo {
  SOptrBasicInfo binfo;
  SArray*        pGroupCols;
  SArray*        pGroupColVals; // current group column values, SArray<SGroupKeys>
  SNode*         pCondition;
  bool           isInit;        // denote if current val is initialized or not
  char*          keyBuf;        // group by keys for hash
  int32_t        groupKeyLen;   // total group by column width
  SGroupResInfo  groupResInfo;
  SAggSupporter  aggSup;
  SExprInfo*     pScalarExprInfo;
  int32_t        numOfScalarExpr;// the number of scalar expression in group operator
  SqlFunctionCtx*pScalarFuncCtx;
} SGroupbyOperatorInfo;

typedef struct SDataGroupInfo {
  uint64_t groupId;
  int64_t  numOfRows;
  SArray  *pPageList;
} SDataGroupInfo;

// The sort in partition may be needed later.
typedef struct SPartitionOperatorInfo {
  SOptrBasicInfo binfo;
  SArray*        pGroupCols;
  SArray*        pGroupColVals; // current group column values, SArray<SGroupKeys>
  char*          keyBuf;        // group by keys for hash
  int32_t        groupKeyLen;   // total group by column width
  SHashObj*      pGroupSet;     // quick locate the window object for each result

  SDiskbasedBuf* pBuf;          // query result buffer based on blocked-wised disk file
  int32_t        rowCapacity;   // maximum number of rows for each buffer page
  int32_t*       columnOffset;  // start position for each column data

  void*          pGroupIter;    // group iterator
  int32_t        pageIndex;     // page index of current group
} SPartitionOperatorInfo;

typedef struct SWindowRowsSup {
  STimeWindow      win;
  TSKEY            prevTs;
  int32_t          startRowIndex;
  int32_t          numOfRows;
} SWindowRowsSup;

typedef struct SSessionAggOperatorInfo {
  SOptrBasicInfo   binfo;
  SAggSupporter    aggSup;
  SGroupResInfo    groupResInfo;
  SWindowRowsSup   winSup;
  bool             reptScan;         // next round scan
  int64_t          gap;              // session window gap
  SColumnInfoData  timeWindowData;   // query time window info for scalar function execution.
} SSessionAggOperatorInfo;

typedef struct STimeSliceOperatorInfo {
  SOptrBasicInfo   binfo;
  SInterval        interval;
  SGroupResInfo    groupResInfo;         // multiple results build supporter
} STimeSliceOperatorInfo;

typedef struct SStateWindowOperatorInfo {
  SOptrBasicInfo   binfo;
  SAggSupporter    aggSup;
  SGroupResInfo    groupResInfo;
  SWindowRowsSup   winSup;
  int32_t          colIndex;   // start row index
  bool             hasKey;
  SStateKeys       stateKey;
  SColumnInfoData  timeWindowData;   // query time window info for scalar function execution.
//  bool             reptScan;
} SStateWindowOperatorInfo;

typedef struct SSortedMergeOperatorInfo {
  SOptrBasicInfo     binfo;
  bool               hasVarCol;
  
  SArray*            pSortInfo;
  int32_t            numOfSources;

  SSortHandle       *pSortHandle;

  int32_t            bufPageSize;
  uint32_t           sortBufSize;  // max buffer size for in-memory sort

  int32_t            resultRowFactor;
  bool               hasGroupVal;

  SDiskbasedBuf     *pTupleStore;  // keep the final results
  int32_t            numOfResPerPage;

  char**             groupVal;
  SArray            *groupInfo;
  SAggSupporter      aggSup;
} SSortedMergeOperatorInfo;

typedef struct SSortOperatorInfo {
  uint32_t           sortBufSize;  // max buffer size for in-memory sort
  SSDataBlock       *pDataBlock;
  SArray*            pSortInfo;
  SSortHandle       *pSortHandle;
  SArray*            inputSlotMap;  // for index map from table scan output
  int32_t            bufPageSize;
  int32_t            numOfRowsInRes;

  // TODO extact struct
  int64_t            startTs;       // sort start time
  uint64_t           sortElapsed;   // sort elapsed time, time to flush to disk not included.
  uint64_t           totalSize;     // total load bytes from remote
  uint64_t           totalRows;     // total number of rows
  uint64_t           totalElapsed;  // total elapsed time
} SSortOperatorInfo;

int32_t operatorDummyOpenFn(SOperatorInfo* pOperator);
void operatorDummyCloseFn(void* param, int32_t numOfCols);
int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num);
int32_t initAggInfo(SOptrBasicInfo* pBasicInfo, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols,
                    int32_t numOfRows, SSDataBlock* pResultBlock, size_t keyBufSize, const char* pkey);
void toSDatablock(SSDataBlock* pBlock, int32_t rowCapacity, SGroupResInfo* pGroupResInfo, SExprInfo* pExprInfo, SDiskbasedBuf* pBuf, int32_t* rowCellOffset);
void finalizeMultiTupleQueryResult(SqlFunctionCtx* pCtx, int32_t numOfOutput, SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int32_t* rowCellInfoOffset);
void doApplyFunctions(SqlFunctionCtx* pCtx, STimeWindow* pWin, SColumnInfoData* pTimeWindowData, int32_t offset, int32_t forwardStep, TSKEY* tsCol, int32_t numOfTotal, int32_t numOfOutput, int32_t order);
int32_t setGroupResultOutputBuf_rv(SOptrBasicInfo* binfo, int32_t numOfCols, char* pData, int16_t type,
                                   int16_t bytes, int32_t groupId, SDiskbasedBuf* pBuf, SExecTaskInfo* pTaskInfo, SAggSupporter* pAggSup);
void doDestroyBasicInfo(SOptrBasicInfo* pInfo, int32_t numOfOutput);
int32_t setSDataBlockFromFetchRsp(SSDataBlock* pRes, SLoadRemoteDataInfo* pLoadInfo, int32_t numOfRows,
                                         char* pData, int32_t compLen, int32_t numOfOutput, int64_t startTs,
                                         uint64_t* total, SArray* pColList);
void doSetOperatorCompleted(SOperatorInfo* pOperator);
void doFilter(const SNode* pFilterNode, SSDataBlock* pBlock);
SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowCellInfoOffset);

SOperatorInfo* createExchangeOperatorInfo(const SNodeList* pSources, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createTableScanOperatorInfo(void* pTsdbReadHandle, int32_t order, int32_t numOfCols, int32_t repeatTime,
                                           int32_t reverseTime, SArray* pColMatchInfo, SSDataBlock* pResBlock, SNode* pCondition, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock,
                                           SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo);
SOperatorInfo* createMultiTableAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo);

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t num, SSDataBlock* pResBlock, SLimit* pLimit, SLimit* pSlimit, SExecTaskInfo* pTaskInfo);
SOperatorInfo *createSortOperatorInfo(SOperatorInfo* downstream, SSDataBlock* pResBlock, SArray* pSortInfo, SArray* pIndexMap, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createSortedMergeOperatorInfo(SOperatorInfo** downstream, int32_t numOfDownstream, SExprInfo* pExprInfo, int32_t num, SArray* pSortInfo, SArray* pGroupInfo, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createSysTableScanOperatorInfo(void* pSysTableReadHandle, SSDataBlock* pResBlock, const SName* pName,
                                              SNode* pCondition, SEpSet epset, SArray* colList, SExecTaskInfo* pTaskInfo, bool showRewrite, int32_t accountId);
SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlot,
                                          const STableGroupInfo* pTableGroupInfo, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, int64_t gap, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SArray* pGroupColList,
                                       SNode* pCondition, SExprInfo* pScalarExprInfo, int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo);
SOperatorInfo* createDataBlockInfoScanOperator(void* dataReader, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createStreamScanOperatorInfo(void* streamReadHandle, SSDataBlock* pResBlock, SArray* pColList, SArray* pTableIdList, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols, SInterval* pInterval, SSDataBlock* pResBlock,
                                      int32_t fillType, char* fillVal, bool multigroupResult, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SArray* pGroupColList,
                                           SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo);
SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SExecTaskInfo* pTaskInfo);

#if 0
SOperatorInfo* createTableSeqScanOperatorInfo(void* pTsdbReadHandle, STaskRuntimeEnv* pRuntimeEnv);
SOperatorInfo* createMultiTableTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream,
                                                        SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createAllMultiTableTimeIntervalOperatorInfo(STaskRuntimeEnv* pRuntimeEnv, SOperatorInfo* downstream,
                                                           SExprInfo* pExpr, int32_t numOfOutput);
SOperatorInfo* createTagScanOperatorInfo(SReaderHandle* pReaderHandle, SExprInfo* pExpr, int32_t numOfOutput);

SOperatorInfo* createJoinOperatorInfo(SOperatorInfo** pdownstream, int32_t numOfDownstream, SSchema* pSchema,
                                      int32_t numOfOutput);
#endif

void projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList);

void setInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order);

void finalizeQueryResult(SqlFunctionCtx* pCtx, int32_t numOfOutput);
void copyTsColoum(SSDataBlock* pRes, SqlFunctionCtx* pCtx, int32_t numOfOutput);

STableQueryInfo* createTableQueryInfo(void* buf, bool groupbyColumn, STimeWindow win);

bool    isTaskKilled(SExecTaskInfo* pTaskInfo);
int32_t checkForQueryBuf(size_t numOfTables);

void   setTaskKilled(SExecTaskInfo* pTaskInfo);

void publishOperatorProfEvent(SOperatorInfo* operatorInfo, EQueryProfEventType eventType);
void publishQueryAbortEvent(SExecTaskInfo* pTaskInfo, int32_t code);

void calculateOperatorProfResults(SQInfo* pQInfo);
void queryCostStatis(SExecTaskInfo* pTaskInfo);

void doDestroyTask(SExecTaskInfo* pTaskInfo);
int32_t getMaximumIdleDurationSec();

void    doInvokeUdf(struct SUdfInfo* pUdfInfo, SqlFunctionCtx* pCtx, int32_t idx, int32_t type);
void    setTaskStatus(SExecTaskInfo* pTaskInfo, int8_t status);
int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId, EOPTR_EXEC_MODEL model);
int32_t getOperatorExplainExecInfo(SOperatorInfo *operatorInfo, SExplainExecInfo **pRes, int32_t *capacity, int32_t *resNum);

bool aggDecodeResultRow(SOperatorInfo* pOperator, SAggSupporter *pSup, SOptrBasicInfo *pInfo, char* result, int32_t length);
void aggEncodeResultRow(SOperatorInfo* pOperator, SAggSupporter *pSup, SOptrBasicInfo *pInfo, char **result, int32_t *length);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EXECUTORIMPL_H
