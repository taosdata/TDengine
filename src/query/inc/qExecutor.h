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
#ifndef TDENGINE_QUERYEXECUTOR_H
#define TDENGINE_QUERYEXECUTOR_H

#include "os.h"

#include "hash.h"
#include "qAggMain.h"
#include "qFill.h"
#include "qResultbuf.h"
#include "qSqlparser.h"
#include "qTsbuf.h"
#include "query.h"
#include "taosdef.h"
#include "tarray.h"
#include "tlockfree.h"
#include "tsdb.h"

struct SColumnFilterElem;
typedef bool (*__filter_func_t)(struct SColumnFilterElem* pFilter, const char* val1, const char* val2, int16_t type);
typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

#define IS_QUERY_KILLED(_q) ((_q)->code == TSDB_CODE_TSC_QUERY_CANCELLED)
#define Q_STATUS_EQUAL(p, s)  (((p) & (s)) != 0u)
#define QUERY_IS_ASC_QUERY(q) (GET_FORWARD_DIRECTION_FACTOR((q)->order.order) == QUERY_ASC_FORWARD_STEP)

#define SET_STABLE_QUERY_OVER(_q) ((_q)->tableIndex = (int32_t)((_q)->tableqinfoGroupInfo.numOfTables))
#define IS_STASBLE_QUERY_OVER(_q) ((_q)->tableIndex >= (int32_t)((_q)->tableqinfoGroupInfo.numOfTables))

#define GET_TABLEGROUP(q, _index)   ((SArray*) taosArrayGetP((q)->tableqinfoGroupInfo.pGroupList, (_index)))

enum {
  // when query starts to execute, this status will set
      QUERY_NOT_COMPLETED = 0x1u,

  /* result output buffer is full, current query is paused.
   * this status is only exist in group-by clause and diff/add/division/multiply/ query.
   */
      QUERY_RESBUF_FULL = 0x2u,

  /* query is over
   * 1. this status is used in one row result query process, e.g., count/sum/first/last/ avg...etc.
   * 2. when all data within queried time window, it is also denoted as query_completed
   */
      QUERY_COMPLETED = 0x4u,

  /* when the result is not completed return to client, this status will be
   * usually used in case of interval query with interpolation option
   */
      QUERY_OVER = 0x8u,
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

typedef struct SSqlGroupbyExpr {
  int16_t tableIndex;
  SArray* columnInfo;  // SArray<SColIndex>, group by columns information
  int16_t numOfGroupCols;
  int16_t orderIndex;  // order by column index
  int16_t orderType;   // order by type: asc/desc
} SSqlGroupbyExpr;

typedef struct SResultRow {
  int32_t       pageId;      // pageId & rowId is the position of current result in disk-based output buffer
  int32_t       rowId:29;    // row index in buffer page
  bool          startInterp; // the time window start timestamp has done the interpolation already.
  bool          endInterp;   // the time window end timestamp has done the interpolation already.
  bool          closed;      // this result status: closed or opened
  uint32_t      numOfRows;   // number of rows of current time window
  SResultRowCellInfo*  pCellInfo;  // For each result column, there is a resultInfo
  union {STimeWindow win; char* key;};  // start key of current time window
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
typedef struct SResultRec {
  int64_t total;      // total generated result size in rows
  int64_t rows;       // current result set size in rows
  int64_t capacity;   // capacity of current result output buffer
  int32_t threshold;  // result size threshold in rows.
} SResultRec;

typedef struct SResultRowInfo {
  SResultRow** pResult;    // result list
  int16_t      type:8;     // data type for hash key
  int32_t      size:24;    // number of result set
  int32_t      capacity;   // max capacity
  int32_t      curIndex;   // current start active index
  int64_t      prevSKey;   // previous (not completed) sliding window start key
} SResultRowInfo;

typedef struct SColumnFilterElem {
  int16_t           bytes;  // column length
  __filter_func_t   fp;
  SColumnFilterInfo filterInfo;
} SColumnFilterElem;

typedef struct SSingleColumnFilterInfo {
  void*              pData;
  int32_t            numOfFilters;
  SColumnInfo        info;
  SColumnFilterElem* pFilters;
} SSingleColumnFilterInfo;

typedef struct STableQueryInfo {
  TSKEY       lastKey;
  int32_t     groupIndex;     // group id in table list
  int16_t     queryRangeSet;  // denote if the query range is set, only available for interval query
  tVariant    tag;
  STimeWindow win;
  STSCursor   cur;
  void*       pTable;         // for retrieve the page id list
  SResultRowInfo resInfo;
} STableQueryInfo;

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

typedef struct SQuery {
  SLimitVal        limit;

  bool             stableQuery;      // super table query or not
  bool             topBotQuery;      // TODO used bitwise flag
  bool             groupbyColumn;    // denote if this is a groupby normal column query
  bool             hasTagResults;    // if there are tag values in final result or not
  bool             timeWindowInterpo;// if the time window start/end required interpolation
  bool             queryWindowIdentical; // all query time windows are identical for all tables in one group
  bool             queryBlockDist;    // if query data block distribution
  bool             stabledev;        // super table stddev query
  int32_t          interBufSize;     // intermediate buffer sizse

  SOrderVal        order;

  int16_t          numOfCols;
  int16_t          numOfTags;

  STimeWindow      window;
  SInterval        interval;
  int16_t          precision;
  int16_t          numOfOutput;
  int16_t          fillType;
  int16_t          checkResultBuf;   // check if the buffer is full during scan each block

  int32_t          srcRowSize;       // todo extract struct
  int32_t          resultRowSize;
  int32_t          maxSrcColumnSize;
  int32_t          tagLen;           // tag value length of current query
  SSqlGroupbyExpr* pGroupbyExpr;
  SExprInfo*       pExpr1;
  SExprInfo*       pExpr2;
  int32_t          numOfExpr2;
  SColumnInfo*     colList;
  SColumnInfo*     tagColList;
  int32_t          numOfFilterCols;
  int64_t*         fillVal;
  SOrderedPrjQueryInfo prjInfo;        // limit value for each vgroup, only available in global order projection query.
  SSingleColumnFilterInfo* pFilterInfo;

  uint32_t         status;             // query status
  SResultRec       rec;
  int32_t          pos;
  tFilePage**      sdata;
  STableQueryInfo* current;
  int32_t          numOfCheckedBlocks; // number of check data blocks

  void*            tsdb;
  SMemRef          memRef;
  STableGroupInfo  tableGroupInfo;       // table <tid, last_key> list  SArray<STableKeyInfo>
  int32_t          vgId;
} SQuery;

typedef SSDataBlock* (*__operator_fn_t)(void* param);

typedef struct SOperatorInfo {
  char      *name;
  bool       blockingOptr;
  bool       completed;
  void      *optInfo;
  SExprInfo *pExpr;
  int32_t    numOfOutput;

  __operator_fn_t       exec;
  struct SOperatorInfo *upstream;
} SOperatorInfo;

typedef struct SQueryRuntimeEnv {
  jmp_buf              env;
  SQuery*              pQuery;
  void*                qinfo;

  SQLFunctionCtx*      pCtx;
  int32_t              numOfRowsPerPage;
  uint16_t*            offset;
  uint16_t             scanFlag;         // denotes reversed scan of data or not
  SFillInfo*           pFillInfo;
  SResultRowInfo       resultRowInfo;
  void*                pQueryHandle;
  void*                pSecQueryHandle;  // another thread for

  int32_t              prevGroupId;      // previous executed group id
  SDiskbasedResultBuf* pResultBuf;       // query result buffer based on blocked-wised disk file
  SHashObj*            pResultRowHashTable; // quick locate the window object for each result
  char*                keyBuf;           // window key buffer
  SResultRowPool*      pool;             // window result object pool

  int32_t*             rowCellInfoOffset;// offset value for each row result cell info
  char**               prevRow;

  SArray*              prevResult;       // intermediate result, SArray<SInterResult>
  STSBuf*              pTsBuf;           // timestamp filter list
  STSCursor            cur;

  char*                tagVal;           // tag value of current data block
  SArithmeticSupport  *sasArray;

  SOperatorInfo*   pi;
  SSDataBlock *outputBuf;

  int32_t          groupIndex;
  int32_t          tableIndex;
  STableGroupInfo  tableqinfoGroupInfo;  // this is a group array list, including SArray<STableQueryInfo*> structure
  SOperatorInfo* proot;
  SGroupResInfo    groupResInfo;
} SQueryRuntimeEnv;

typedef struct {
  char* name;
  void* info;
} SQEStage;

enum {
  QUERY_RESULT_NOT_READY = 1,
  QUERY_RESULT_READY     = 2,
};

typedef struct SQInfo {
  void*            signature;
  int32_t          code;   // error code to returned to client
  int64_t          owner;  // if it is in execution

  SQueryRuntimeEnv runtimeEnv;
  SQuery           query;

  SHashObj*        arrTableIdInfo;

  /*
   * the query is executed position on which meter of the whole list.
   * when the index reaches the last one of the list, it means the query is completed.
   */
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
  char            *tbnameCond;
  char            *prevResult;
  SArray          *pTableIdList;
  SSqlFuncMsg    **pExprMsg;
  SSqlFuncMsg    **pSecExprMsg;
  SExprInfo       *pExprs;
  SExprInfo       *pSecExprs;

  SColIndex       *pGroupColIndex;
  SColumnInfo     *pTagColumnInfo;
  SSqlGroupbyExpr *pGroupbyExpr;
} SQueryParam;

typedef struct STableScanInfo {
  SQueryRuntimeEnv *pRuntimeEnv;
  void        *pQueryHandle;
  int32_t      numOfBlocks;
  int32_t      numOfSkipped;
  int32_t      numOfBlockStatis;

  int64_t      numOfRows;

  int32_t      order;  // scan order
  int32_t      times;  // repeat counts
  int32_t      current;

  int32_t      reverseTimes; // 0 by default

  SSDataBlock  block;
  int64_t      elapsedTime;
} STableScanInfo;

SOperatorInfo optrList[5];

typedef struct SAggOperatorInfo {
  SResultRowInfo   *pResultRowInfo;
  STableQueryInfo  *pTableQueryInfo;
  SQueryRuntimeEnv *pRuntimeEnv;
  SQLFunctionCtx   *pCtx;
} SAggOperatorInfo;

typedef struct SArithOperatorInfo {
  STableQueryInfo  *pTableQueryInfo;
  SQueryRuntimeEnv *pRuntimeEnv;
  SQLFunctionCtx* pCtx;
} SArithOperatorInfo;

typedef struct SLimitOperatorInfo {
  int64_t limit;
  int64_t total;
  SQueryRuntimeEnv* pRuntimeEnv;
} SLimitOperatorInfo;

typedef struct SOffsetOperatorInfo {
  int64_t offset;
  int64_t currentOffset;
  SQueryRuntimeEnv* pRuntimeEnv;
} SOffsetOperatorInfo;

typedef struct SHashIntervalOperatorInfo {
  SResultRowInfo   *pResultRowInfo;
  STableQueryInfo  *pTableQueryInfo;
  SQueryRuntimeEnv *pRuntimeEnv;
  SQLFunctionCtx   *pCtx;
} SHashIntervalOperatorInfo;

void freeParam(SQueryParam *param);
int32_t convertQueryMsg(SQueryTableMsg *pQueryMsg, SQueryParam* param);
int32_t createQueryFuncExprFromMsg(SQueryTableMsg *pQueryMsg, int32_t numOfOutput, SExprInfo **pExprInfo, SSqlFuncMsg **pExprMsg,
                                   SColumnInfo* pTagCols);
SSqlGroupbyExpr *createGroupbyExprFromMsg(SQueryTableMsg *pQueryMsg, SColIndex *pColIndex, int32_t *code);
SQInfo *createQInfoImpl(SQueryTableMsg *pQueryMsg, SSqlGroupbyExpr *pGroupbyExpr, SExprInfo *pExprs,
                        SExprInfo *pSecExprs, STableGroupInfo *pTableGroupInfo, SColumnInfo* pTagCols, bool stableQuery, char* sql);
int32_t initQInfo(SQueryTableMsg *pQueryMsg, void *tsdb, int32_t vgId, SQInfo *pQInfo, SQueryParam* param, bool isSTable);
void freeColumnFilterInfo(SColumnFilterInfo* pFilter, int32_t numOfFilters);

bool isQueryKilled(SQInfo *pQInfo);
int32_t checkForQueryBuf(size_t numOfTables);
bool doBuildResCheck(SQInfo* pQInfo);
void setQueryStatus(SQuery *pQuery, int8_t status);

bool onlyQueryTags(SQuery* pQuery);
void buildTagQueryResult(SQInfo *pQInfo);
void stableQueryImpl(SQInfo *pQInfo);
void buildTableBlockDistResult(SQInfo *pQInfo);
void tableQueryImpl(SQInfo *pQInfo);
bool isValidQInfo(void *param);

int32_t doDumpQueryResult(SQInfo *pQInfo, char *data);

size_t getResultSize(SQInfo *pQInfo, int64_t *numOfRows);
void setQueryKilled(SQInfo *pQInfo);
void queryCostStatis(SQInfo *pQInfo);
void freeQInfo(SQInfo *pQInfo);

int32_t getMaximumIdleDurationSec();

#endif  // TDENGINE_QUERYEXECUTOR_H
