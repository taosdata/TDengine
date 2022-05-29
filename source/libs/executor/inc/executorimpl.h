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

//#define GET_TABLEGROUP(q, _index) ((SArray*)taosArrayGetP((q)->tableqinfoGroupInfo.pGroupList, (_index)))

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

typedef struct SResultRowCell {
  uint64_t           groupId;
  SResultRowPosition pos;
} SResultRowCell;

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
//  SVariant       tag;
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

// The basic query information extracted from the SQueryInfo tree to support the
// execution of query in a data node.
typedef struct STaskAttr {
  SLimit      limit;
  SLimit      slimit;
  bool        stableQuery;        // super table query or not
  bool        topBotQuery;        // TODO used bitwise flag
  bool        groupbyColumn;      // denote if this is a groupby normal column query
  bool        timeWindowInterpo;  // if the time window start/end required interpolation
  bool        tsCompQuery;        // is tscomp query
  bool        diffQuery;          // is diff query
  bool        pointInterpQuery;   // point interpolation query
  int32_t     havingNum;          // having expr number
  int16_t     numOfCols;
  int16_t     numOfTags;
  STimeWindow window;
  SInterval   interval;
  int16_t     precision;
  int16_t     numOfOutput;
  int16_t     fillType;
  int32_t     resultRowSize;
  int32_t     tagLen;  // tag value length of current query

  SExprInfo*      pExpr1;
  SColumnInfo*    tagColList;
  int32_t         numOfFilterCols;
  int64_t*        fillVal;
  void*           tsdb;
//  STableListInfo tableGroupInfo;  // table list
  int32_t         vgId;
} STaskAttr;

struct SOperatorInfo;
struct SAggSupporter;
struct SOptrBasicInfo;

typedef void (*__optr_encode_fn_t)(struct SOperatorInfo* pOperator, struct SAggSupporter* pSup,
                                   struct SOptrBasicInfo* pInfo, char** result, int32_t* length);
typedef bool (*__optr_decode_fn_t)(struct SOperatorInfo* pOperator, struct SAggSupporter* pSup,
                                   struct SOptrBasicInfo* pInfo, char* result, int32_t length);

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
//  uint64_t         totalRows;            // total number of rows
  struct {
    char          *tablename;
    char          *dbname;
    int32_t        sversion;
    int32_t        tversion;
  } schemaVer;

  STableListInfo   tableqinfoList;       // this is a table list
  char*            sql;                  // query sql string
  jmp_buf          env;                  // jump to this position when error happens.
  EOPTR_EXEC_MODEL execModel;            // operator execution model [batch model|stream model]
  struct SOperatorInfo* pRoot;
} SExecTaskInfo;

typedef struct STaskRuntimeEnv {
  STaskAttr*      pQueryAttr;
  uint32_t        status;  // query status
  uint8_t         scanFlag;  // denotes reversed scan of data or not
  SDiskbasedBuf*  pResultBuf;           // query result buffer based on blocked-wised disk file
  SHashObj*       pResultRowHashTable;  // quick locate the window object for each result
  SHashObj*       pResultRowListSet;    // used to check if current ResultRowInfo has ResultRow object or not
  SArray*         pResultRowArrayList;  // The array list that contains the Result rows
  char*           keyBuf;               // window key buffer
  // The window result objects pool, all the resultRow Objects are allocated and managed by this object.
  char**    prevRow;
  STSBuf*   pTsBuf;      // timestamp filter list
  STSCursor cur;

  char*                          tagVal;  // tag value of current data block
//  STableGroupInfo tableqinfoGroupInfo;  // this is a table list
  struct SOperatorInfo* proot;
  SGroupResInfo         groupResInfo;
  int64_t               currentOffset;  // dynamic offset value

  STableQueryInfo* current;
  SResultInfo      resultInfo;
  struct SUdfInfo* pUdfInfo;
} STaskRuntimeEnv;

enum {
  OP_NOT_OPENED = 0x0,
  OP_OPENED = 0x1,
  OP_RES_TO_RETURN = 0x5,
  OP_EXEC_DONE = 0x9,
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

typedef struct SOperatorInfo {
  uint8_t                 operatorType;
  bool                    blocking;      // block operator or not
  uint8_t                 status;        // denote if current operator is completed
  int32_t                 numOfExprs;   // number of columns of the current operator results
  char*                   name;          // name, used to show the query execution plan
  void*                   info;          // extension attribution
  SExprInfo*              pExpr;
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

typedef struct SSourceDataInfo {
  struct SExchangeInfo* pEx;
  int32_t               index;
  SRetrieveTableRsp*    pRsp;
  uint64_t              totalRows;
  int32_t               code;
  EX_SOURCE_STATUS      status;
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
} SExchangeInfo;

#define COL_MATCH_FROM_COL_ID  0x1
#define COL_MATCH_FROM_SLOT_ID 0x2

typedef struct SColMatchInfo {
  int32_t srcSlotId;     // source slot id
  int32_t colId;
  int32_t targetSlotId;
  bool    output;
  int32_t matchType;     // determinate the source according to col id or slot id
} SColMatchInfo;

typedef struct SScanInfo {
  int32_t numOfAsc;
  int32_t numOfDesc;
} SScanInfo;

typedef struct STableScanInfo {
  void*           dataReader;
  SReadHandle     readHandle;

  SFileBlockLoadRecorder readRecorder;
  int64_t         numOfRows;
  int64_t         elapsedTime;
//  int32_t         prevGroupId;  // previous table group id
  SScanInfo       scanInfo;
  int32_t         scanTimes;
  SNode*          pFilterNode;  // filter info, which is push down by optimizer
  SqlFunctionCtx* pCtx;         // which belongs to the direct upstream operator operator query context
  SResultRowInfo* pResultRowInfo;
  int32_t*        rowCellInfoOffset;
  SExprInfo*      pExpr;
  SSDataBlock*    pResBlock;
  SArray*         pColMatchInfo;
  int32_t         numOfOutput;

  SExprInfo*      pPseudoExpr;
  int32_t         numOfPseudoExpr;
  SqlFunctionCtx* pPseudoCtx;
//  int32_t*        rowCellInfoOffset;

  SQueryTableDataCond cond;
  int32_t         scanFlag;     // table scan flag to denote if it is a repeat/reverse/main scan
  int32_t         dataBlockLoadFlag;
  double          sampleRatio;  // data block sample ratio, 1 by default
  SInterval       interval;     // if the upstream is an interval operator, the interval info is also kept here to get the time window to check if current data block needs to be loaded.

  int32_t         curTWinIdx;
} STableScanInfo;

typedef struct STagScanInfo {
  SColumnInfo     *pCols;
  SSDataBlock     *pRes;
  SArray          *pColMatchInfo;
  int32_t          curPos;
  SReadHandle      readHandle;
  STableListInfo  *pTableList;
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
  SArray*        pResultRows;          // SResultWindowInfo
  int32_t        keySize;
  char*          pKeyBuf;              // window key buffer
  SDiskbasedBuf* pResultBuf;           // query result buffer based on blocked-wised disk file
  int32_t        resultRowSize;        // the result buffer size for each result row, with the meta data size for each row
} SStreamAggSupporter;

typedef struct SessionWindowSupporter {
  SStreamAggSupporter* pStreamAggSup;
  int64_t gap;
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
  SArray*         tsArray;
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
} SStreamBlockScanInfo;

typedef struct SSysTableScanInfo {
  SRetrieveMetaTableRsp* pRsp;
  SRetrieveTableReq      req;
  SEpSet                 epSet;
  tsem_t                 ready;

  SReadHandle         readHandle;
  int32_t             accountId;
  bool                showRewrite;
  SNode*              pCondition;  // db_name filter condition, to discard data that are not in current database
  SMTbCursor*         pCur;        // cursor for iterate the local table meta store.
  SArray*             scanCols;    // SArray<int16_t> scan column id list
  SName               name;
  SSDataBlock*        pRes;
  int64_t             numOfBlocks;  // extract basic running information.
  SLoadRemoteDataInfo loadInfo;
} SSysTableScanInfo;

typedef struct SOptrBasicInfo {
  SResultRowInfo  resultRowInfo;
  int32_t*        rowCellInfoOffset;  // offset value for each row result cell info
  SqlFunctionCtx* pCtx;
  SSDataBlock*    pRes;
} SOptrBasicInfo;

// TODO move the resultrowsiz together with SOptrBasicInfo:rowCellInfoOffset
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
  SOptrBasicInfo     binfo;              // basic info
  SGroupResInfo      groupResInfo;       // multiple results build supporter
  SInterval          interval;           // interval info
  int32_t            primaryTsIndex;     // primary time stamp slot id from result of downstream operator.
  STimeWindow        win;                // query time range
  bool               timeWindowInterpo;  // interpolation needed or not
  char**             pRow;               // previous row/tuple of already processed datablock
  SAggSupporter      aggSup;             // aggregate supporter
  STableQueryInfo*   pCurrent;           // current tableQueryInfo struct
  int32_t            order;              // current SSDataBlock scan order
  EOPTR_EXEC_MODEL   execModel;          // operator execution model [batch model|stream model]
  SArray*            pUpdatedWindow;     // updated time window due to the input data block from the downstream operator.
  STimeWindowAggSupp twAggSup;
  struct SFillInfo*  pFillInfo;          // fill info
  bool               invertible;
} SIntervalAggOperatorInfo;

typedef struct SStreamFinalIntervalOperatorInfo {
  SOptrBasicInfo     binfo;              // basic info
  SGroupResInfo      groupResInfo;       // multiple results build supporter
  SInterval          interval;           // interval info
  int32_t            primaryTsIndex;     // primary time stamp slot id from result of downstream operator.
  SAggSupporter      aggSup;             // aggregate supporter
  int32_t            order;              // current SSDataBlock scan order
  STimeWindowAggSupp twAggSup;
  SArray*            pChildren;
} SStreamFinalIntervalOperatorInfo;

typedef struct SAggOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  STableQueryInfo   *current;
  uint64_t           groupId;
  SGroupResInfo      groupResInfo;
  STableQueryInfo   *pTableQueryInfo;

  SExprInfo         *pScalarExprInfo;
  int32_t            numOfScalarExpr;      // the number of scalar expression before the aggregate function can be applied
  SqlFunctionCtx    *pScalarCtx;                 // scalar function requried sql function struct.
  int32_t           *rowCellInfoOffset;  // offset value for each row result cell info
} SAggOperatorInfo;

typedef struct SProjectOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
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

typedef struct SFillOperatorInfo {
  struct SFillInfo* pFillInfo;
  SSDataBlock*      pRes;
  int64_t           totalInputRows;
  void**            p;
  SSDataBlock*      existNewGroupBlock;
  bool              multigroupResult;
} SFillOperatorInfo;

typedef struct SGroupbyOperatorInfo {
  SOptrBasicInfo  binfo;
  SArray*         pGroupCols;     // group by columns, SArray<SColumn>
  SArray*         pGroupColVals;  // current group column values, SArray<SGroupKeys>
  SNode*          pCondition;
  bool            isInit;       // denote if current val is initialized or not
  char*           keyBuf;       // group by keys for hash
  int32_t         groupKeyLen;  // total group by column width
  SGroupResInfo   groupResInfo;
  SAggSupporter   aggSup;
  SExprInfo*      pScalarExprInfo;
  int32_t         numOfScalarExpr;  // the number of scalar expression in group operator
  SqlFunctionCtx* pScalarFuncCtx;
  int32_t*        rowCellInfoOffset;  // offset value for each row result cell info
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
} SPartitionOperatorInfo;

typedef struct SWindowRowsSup {
  STimeWindow win;
  TSKEY       prevTs;
  int32_t     startRowIndex;
  int32_t     numOfRows;
} SWindowRowsSup;

typedef struct SSessionAggOperatorInfo {
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
  SArray*              pChildren;       // cache for children's result;
} SStreamSessionAggOperatorInfo;

typedef struct STimeSliceOperatorInfo {
  SOptrBasicInfo binfo;
  SInterval      interval;
  SGroupResInfo  groupResInfo;  // multiple results build supporter
} STimeSliceOperatorInfo;

typedef struct SStateWindowOperatorInfo {
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

typedef struct SSortedMergeOperatorInfo {

  SOptrBasicInfo   binfo;
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
  SAggSupporter    aggSup;
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

SOperatorFpSet createOperatorFpSet(__optr_open_fn_t openFn, __optr_fn_t nextFn, __optr_fn_t streamFn,
    __optr_fn_t cleanup, __optr_close_fn_t closeFn, __optr_encode_fn_t encode,
    __optr_decode_fn_t decode, __optr_explain_fn_t explain);

int32_t operatorDummyOpenFn(SOperatorInfo* pOperator);
void    operatorDummyCloseFn(void* param, int32_t numOfCols);
int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num);
int32_t initAggInfo(SOptrBasicInfo* pBasicInfo, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols,
                    SSDataBlock* pResultBlock, size_t keyBufSize, const char* pkey);
void    initResultSizeInfo(SOperatorInfo* pOperator, int32_t numOfRows);
void    doBuildResultDatablock(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo, SDiskbasedBuf* pBuf);

void    doApplyFunctions(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, STimeWindow* pWin, SColumnInfoData* pTimeWindowData, int32_t offset,
                         int32_t forwardStep, TSKEY* tsCol, int32_t numOfTotal, int32_t numOfOutput, int32_t order);
int32_t setGroupResultOutputBuf(SOptrBasicInfo* binfo, int32_t numOfCols, char* pData, int16_t type, int16_t bytes,
                                int32_t groupId, SDiskbasedBuf* pBuf, SExecTaskInfo* pTaskInfo, SAggSupporter* pAggSup);
void    doDestroyBasicInfo(SOptrBasicInfo* pInfo, int32_t numOfOutput);
int32_t setSDataBlockFromFetchRsp(SSDataBlock* pRes, SLoadRemoteDataInfo* pLoadInfo, int32_t numOfRows, char* pData,
                                  int32_t compLen, int32_t numOfOutput, int64_t startTs, uint64_t* total,
                                  SArray* pColList);
void    getAlignQueryTimeWindow(SInterval* pInterval, int32_t precision, int64_t key, STimeWindow* win);
int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t *order, int32_t* scanFlag);
int32_t getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, uint32_t* defaultBufsz);

void    doSetOperatorCompleted(SOperatorInfo* pOperator);
void    doFilter(const SNode* pFilterNode, SSDataBlock* pBlock, SArray* pColMatchInfo);
SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowCellInfoOffset);
void    relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols);
void    initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow);
void    cleanupAggSup(SAggSupporter* pAggSup);
void    destroyBasicOperatorInfo(void* param, int32_t numOfOutput);
void    appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle);
void    setTbNameColData(void* pMeta, const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId);
SInterval extractIntervalInfo(const STableScanPhysiNode* pTableScanNode);
SColumn extractColumnFromColumnNode(SColumnNode* pColNode);

SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo);
SSDataBlock* loadNextDataBlock(void* param);

void setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t* rowCellInfoOffset);

SArray* extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            SExecTaskInfo* pTaskInfo, int32_t type);

SExprInfo* createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, int32_t* numOfExprs);
SSDataBlock* createResDataBlock(SDataBlockDescNode* pNode);
int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode);

SResultRow* doSetResultOutBufByKey(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo,
                                   char* pData, int16_t bytes, bool masterscan, uint64_t groupId,
                                   SExecTaskInfo* pTaskInfo, bool isIntervalQuery, SAggSupporter* pSup);

SOperatorInfo* createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, tsdbReaderT pDataReader, SReadHandle* pHandle, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SExprInfo* pScalarExprInfo,
                                           int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t num, SSDataBlock* pResBlock, SLimit* pLimit, SLimit* pSlimit, SExecTaskInfo* pTaskInfo);
SOperatorInfo *createSortOperatorInfo(SOperatorInfo* downstream, SSDataBlock* pResBlock, SArray* pSortInfo, SExprInfo* pExprInfo, int32_t numOfCols,
                                      SArray* pIndexMap, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createSortedMergeOperatorInfo(SOperatorInfo** downstream, int32_t numOfDownstream, SExprInfo* pExprInfo, int32_t num, SArray* pSortInfo, SArray* pGroupInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createSysTableScanOperatorInfo(void* pSysTableReadHandle, SSDataBlock* pResBlock, const SName* pName,
                                              SNode* pCondition, SEpSet epset, SArray* colList,
                                              SExecTaskInfo* pTaskInfo, bool showRewrite, int32_t accountId);
SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          STimeWindowAggSupp *pTwAggSupp, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                          SSDataBlock* pResBlock, SInterval* pInterval, int32_t primaryTsSlotId,
                                          STimeWindowAggSupp *pTwAggSupp, SExecTaskInfo* pTaskInfo);
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
    SArray* pTableIdList, STableScanPhysiNode* pTableScanNode, SExecTaskInfo* pTaskInfo,
    STimeWindowAggSupp* pTwSup, int16_t tsColId);


SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                      SInterval* pInterval, STimeWindow* pWindow, SSDataBlock* pResBlock, int32_t fillType, SNodeListNode* fillVal,
                                      bool multigroupResult, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols,
                                             SSDataBlock* pResBlock, STimeWindowAggSupp *pTwAggSupp, int32_t tsSlotId, SColumn* pStateKeyCol, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, SArray* pGroupColList, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                           SSDataBlock* pResultBlock, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, SNode* pOnCondition, SExecTaskInfo* pTaskInfo);
SOperatorInfo* createTagScanOperatorInfo(SReadHandle* pReadHandle, SExprInfo* pExpr, int32_t numOfOutput, SSDataBlock* pResBlock, SArray* pColMatchInfo, STableListInfo* pTableGroupInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream,
    SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResBlock, int64_t gap,
    int32_t tsSlotId, STimeWindowAggSupp* pTwAggSupp, SExecTaskInfo* pTaskInfo);
#if 0
SOperatorInfo* createTableSeqScanOperatorInfo(void* pTsdbReadHandle, STaskRuntimeEnv* pRuntimeEnv);
#endif

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                           int32_t numOfOutput, SArray* pPseudoList);

void setInputDataBlock(SOperatorInfo* pOperator, SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t order, int32_t scanFlag, bool createDummyCol);

void copyTsColoum(SSDataBlock* pRes, SqlFunctionCtx* pCtx, int32_t numOfOutput);

bool    isTaskKilled(SExecTaskInfo* pTaskInfo);
int32_t checkForQueryBuf(size_t numOfTables);

void setTaskKilled(SExecTaskInfo* pTaskInfo);
void queryCostStatis(SExecTaskInfo* pTaskInfo);

void    doDestroyTask(SExecTaskInfo* pTaskInfo);
int32_t getMaximumIdleDurationSec();

void    setTaskStatus(SExecTaskInfo* pTaskInfo, int8_t status);
int32_t createExecTaskInfoImpl(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                               EOPTR_EXEC_MODEL model);
int32_t getOperatorExplainExecInfo(SOperatorInfo* operatorInfo, SExplainExecInfo** pRes, int32_t* capacity,
                                   int32_t* resNum);

bool aggDecodeResultRow(SOperatorInfo* pOperator, SAggSupporter* pSup, SOptrBasicInfo* pInfo, char* result,
                        int32_t length);
void aggEncodeResultRow(SOperatorInfo* pOperator, SAggSupporter* pSup, SOptrBasicInfo* pInfo, char** result,
                        int32_t* length);
STimeWindow getActiveTimeWindow(SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int64_t ts,
                                       SInterval* pInterval, int32_t precision, STimeWindow* win);
int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn,
    int32_t startPos, TSKEY ekey, __block_search_fn_t searchFn, STableQueryInfo* item,
    int32_t order);
int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, const char* pKey);
SResultRow* getNewResultRow_rv(SDiskbasedBuf* pResultBuf, int64_t tableGroupId, int32_t interBufSize);
SResultWindowInfo* getSessionTimeWindow(SArray* pWinInfos, TSKEY ts, int64_t gap,
    int32_t* pIndex);
int32_t updateSessionWindowInfo(SResultWindowInfo* pWinInfo, TSKEY* pTs, int32_t rows,
    int32_t start, int64_t gap, SHashObj* pStDeleted);
bool functionNeedToExecute(SqlFunctionCtx* pCtx);

int32_t compareTimeWindow(const void* p1, const void* p2, const void* param);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EXECUTORIMPL_H
