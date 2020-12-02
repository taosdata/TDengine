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
#include "qFill.h"
#include "qResultbuf.h"
#include "qSqlparser.h"
#include "qTsbuf.h"
#include "query.h"
#include "taosdef.h"
#include "tarray.h"
#include "tlockfree.h"
#include "tsdb.h"
#include "tsqlfunction.h"

struct SColumnFilterElem;
typedef bool (*__filter_func_t)(struct SColumnFilterElem* pFilter, char* val1, char* val2);
typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

typedef struct SGroupResInfo {
  int32_t  groupId;
  int32_t  numOfDataPages;
  int32_t  pageId;
  int32_t  rowId;
} SGroupResInfo;

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

typedef struct SWindowResInfo {
  SResultRow** pResult;    // result list
  int16_t      type:8;     // data type for hash key
  int32_t      size:24;    // number of result set
  int32_t      capacity;   // max capacity
  int32_t      curIndex;   // current start active index
  int64_t      startTime;  // start time of the first time window for sliding query
  int64_t      prevSKey;   // previous (not completed) sliding window start key
} SWindowResInfo;

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
  SWindowResInfo windowResInfo;
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

typedef struct SQuery {
  int16_t          numOfCols;
  int16_t          numOfTags;
  SOrderVal        order;
  STimeWindow      window;
  SInterval        interval;
  int16_t          precision;
  int16_t          numOfOutput;
  int16_t          fillType;
  int16_t          checkBuffer;  // check if the buffer is full during scan each block
  SLimitVal        limit;
  int32_t          rowSize;
  SSqlGroupbyExpr* pGroupbyExpr;
  SExprInfo*       pExpr1;
  SExprInfo*       pExpr2;
  int32_t          numOfExpr2;

  SColumnInfo*     colList;
  SColumnInfo*     tagColList;
  int32_t          numOfFilterCols;
  int64_t*         fillVal;
  uint32_t         status;  // query status
  SResultRec       rec;
  int32_t          pos;
  tFilePage**      sdata;
  STableQueryInfo* current;

  SSingleColumnFilterInfo* pFilterInfo;
} SQuery;

typedef struct SQueryRuntimeEnv {
  jmp_buf              env;
  SQuery*              pQuery;
  SQLFunctionCtx*      pCtx;
  int32_t              numOfRowsPerPage;
  uint16_t*            offset;
  uint16_t             scanFlag;         // denotes reversed scan of data or not
  SFillInfo*           pFillInfo;
  SWindowResInfo       windowResInfo;
  STSBuf*              pTSBuf;
  STSCursor            cur;
  SQueryCostInfo       summary;
  void*                pQueryHandle;
  void*                pSecQueryHandle;  // another thread for
  bool                 stableQuery;      // super table query or not
  bool                 topBotQuery;      // false
  bool                 groupbyNormalCol; // denote if this is a groupby normal column query
  bool                 hasTagResults;    // if there are tag values in final result or not
  bool                 timeWindowInterpo;// if the time window start/end required interpolation
  bool                 queryWindowIdentical; // all query time windows are identical for all tables in one group
  int32_t              interBufSize;     // intermediate buffer sizse
  int32_t              prevGroupId;      // previous executed group id
  SDiskbasedResultBuf* pResultBuf;       // query result buffer based on blocked-wised disk file
  SHashObj*            pResultRowHashTable; // quick locate the window object for each result
  char*                keyBuf;           // window key buffer
  SResultRowPool*      pool;             // window result object pool

  int32_t*             rowCellInfoOffset;// offset value for each row result cell info
  char**               prevRow;
  char**               nextRow;
} SQueryRuntimeEnv;

enum {
  QUERY_RESULT_NOT_READY = 1,
  QUERY_RESULT_READY     = 2,
};

typedef struct SQInfo {
  void*            signature;
  int32_t          code;   // error code to returned to client
  int64_t          owner; // if it is in execution
  void*            tsdb;
  SMemRef          memRef; 
  int32_t          vgId;
  STableGroupInfo  tableGroupInfo;       // table <tid, last_key> list  SArray<STableKeyInfo>
  STableGroupInfo  tableqinfoGroupInfo;  // this is a group array list, including SArray<STableQueryInfo*> structure
  SQueryRuntimeEnv runtimeEnv;
//  SArray*          arrTableIdInfo;
  SHashObj*        arrTableIdInfo;
  int32_t          groupIndex;

  /*
   * the query is executed position on which meter of the whole list.
   * when the index reaches the last one of the list, it means the query is completed.
   */
  int32_t          tableIndex;
  SGroupResInfo    groupResInfo;
  void*            pBuf;        // allocated buffer for STableQueryInfo, sizeof(STableQueryInfo)*numOfTables;

  pthread_mutex_t  lock;        // used to synchronize the rsp/query threads
  tsem_t           ready;
  int32_t          dataReady;   // denote if query result is ready or not
  void*            rspContext;  // response context
} SQInfo;

#endif  // TDENGINE_QUERYEXECUTOR_H
