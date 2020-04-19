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
#include "tsdb.h"
#include "qinterpolation.h"
#include "qresultBuf.h"
#include "qsqlparser.h"
#include "qtsbuf.h"
#include "taosdef.h"
#include "tref.h"
#include "tsqlfunction.h"
#include "tarray.h"

typedef struct SData {
  int32_t num;
  char    data[];
} SData;

struct SColumnFilterElem;
typedef bool (*__filter_func_t)(struct SColumnFilterElem* pFilter, char* val1, char* val2);
typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

typedef struct SSqlGroupbyExpr {
  int16_t     tableIndex;
  int16_t     numOfGroupCols;
  SColIndex*  columnInfo;                 // group by columns information
  int16_t     orderIndex;                 // order by column index
  int16_t     orderType;                  // order by type: asc/desc
} SSqlGroupbyExpr;

typedef struct SPosInfo {
  int16_t pageId;
  int16_t rowId;
} SPosInfo;

typedef struct SWindowStatus {
  bool closed;
} SWindowStatus;

typedef struct SWindowResult {
  uint16_t      numOfRows;   // number of rows of current  time window
  SPosInfo      pos;         // Position of current result in disk-based output buffer
  SResultInfo*  resultInfo;  // For each result column, there is a resultInfo
  STimeWindow   window;      // The time window that current result covers.
  SWindowStatus status;      // this result status: closed or opened
} SWindowResult;

typedef struct SResultRec {
  int64_t total;     // total generated result size in rows
  int64_t rows;      // current result set size in rows
  int64_t capacity;  // capacity of current result output buffer
  
  // result size threshold in rows. If the result buffer is larger than this, pause query and return to client
  int32_t threshold;
} SResultRec;

typedef struct SWindowResInfo {
  SWindowResult* pResult;    // result list
  void*          hashList;   // hash list for quick access
  int16_t        type;       // data type for hash key
  int32_t        capacity;   // max capacity
  int32_t        curIndex;   // current start active index
  int32_t        size;       // number of result set
  int64_t        startTime;  // start time of the first time window for sliding query
  int64_t        prevSKey;   // previous (not completed) sliding window start key
  int64_t        threshold;  // threshold to pausing query and return closed results.
} SWindowResInfo;

typedef struct SColumnFilterElem {
  int16_t           bytes;  // column length
  __filter_func_t   fp;
  SColumnFilterInfo filterInfo;
} SColumnFilterElem;

typedef struct SSingleColumnFilterInfo {
  SColumnInfoData    info;
  int32_t            numOfFilters;
  SColumnFilterElem* pFilters;
  void*              pData;
} SSingleColumnFilterInfo;

typedef struct STableQueryInfo {
  int64_t     lastKey;
  STimeWindow win;
  int32_t     numOfRes;
  int16_t     queryRangeSet;  // denote if the query range is set, only available for interval query
  int64_t     tag;
  STSCursor   cur;
  int32_t     tid;  // for retrieve the page id list

  SWindowResInfo windowResInfo;
} STableQueryInfo;

typedef struct STableDataInfo {
  int32_t          tableIndex;
  int32_t          groupIdx;  // group id in table list
  STableQueryInfo* pTableQInfo;
} STableDataInfo;

typedef struct SQuery {
  int16_t           numOfCols;
  SOrderVal         order;
  STimeWindow       window;
  int64_t           intervalTime;
  int64_t           slidingTime;      // sliding time for sliding window query
  char              slidingTimeUnit;  // interval data type, used for daytime revise
  int8_t            precision;
  int16_t           numOfOutputCols;
  int16_t           interpoType;
  int16_t           checkBuffer;  // check if the buffer is full during scan each block
  SLimitVal         limit;
  int32_t           rowSize;
  SSqlGroupbyExpr*  pGroupbyExpr;
  SSqlFunctionExpr* pSelectExpr;
  SColumnInfoData*    colList;
  int32_t           numOfFilterCols;
  int64_t*          defaultVal;
  TSKEY             lastKey;
  uint32_t          status;  // query status
  SResultRec        rec;
  int32_t           pos;
  int64_t           pointsOffset;  // the number of points offset to save read data
  SData**           sdata;
  SSingleColumnFilterInfo* pFilterInfo;
} SQuery;

typedef struct SQueryCostSummary {
} SQueryCostSummary;

typedef struct SQueryRuntimeEnv {
  SResultInfo*       resultInfo;  // todo refactor to merge with SWindowResInfo
  SQuery*            pQuery;
  SData**            pInterpoBuf;
  SQLFunctionCtx*    pCtx;
  int16_t            numOfRowsPerPage;
  int16_t            offset[TSDB_MAX_COLUMNS];
  uint16_t           scanFlag;  // denotes reversed scan of data or not
  SInterpolationInfo interpoInfo;
  SWindowResInfo     windowResInfo;
  STSBuf*            pTSBuf;
  STSCursor          cur;
  SQueryCostSummary  summary;
  bool               stableQuery;  // super table query or not
  void*              pQueryHandle;
  void*              pSecQueryHandle; // another thread for
  SDiskbasedResultBuf* pResultBuf;  // query result buffer based on blocked-wised disk file
} SQueryRuntimeEnv;

typedef struct SQInfo {
  void*            signature;
  TSKEY            startTime;
  TSKEY            elapsedTime;
  int32_t          pointsInterpo;
  int32_t          code;          // error code to returned to client
  sem_t            dataReady;
  void*            tsdb;
  
  STableGroupInfo  groupInfo;     // table id list
  SQueryRuntimeEnv runtimeEnv;
  int32_t          groupIndex;
  int32_t          offset; /* offset in group result set of subgroup */
  
  T_REF_DECLARE()
  /*
   * the query is executed position on which meter of the whole list.
   * when the index reaches the last one of the list, it means the query is completed.
   * We later may refactor to remove this attribution by using another flag to denote
   * whether a multimeter query is completed or not.
   */
  int32_t         tableIndex;
  int32_t         numOfGroupResultPages;
  TSKEY*          tsList;
} SQInfo;

#endif  // TDENGINE_QUERYEXECUTOR_H
