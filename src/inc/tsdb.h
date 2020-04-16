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
#ifndef _TD_TSDB_H_
#define _TD_TSDB_H_

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "dataformat.h"
#include "name.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_VERSION_MAJOR 1
#define TSDB_VERSION_MINOR 0

#define TSDB_INVALID_SUPER_TABLE_ID -1

// --------- TSDB APPLICATION HANDLE DEFINITION
typedef struct {
  // WAL handle
  void *appH;
  int (*walCallBack)(void *);
  int (*eventCallBack)(void *);
  int (*cqueryCallBack)(void *);
} STsdbAppH;

// --------- TSDB REPOSITORY CONFIGURATION DEFINITION
typedef struct {
  int8_t  precision;
  int32_t tsdbId;
  int32_t maxTables;            // maximum number of tables this repository can have
  int32_t daysPerFile;          // day per file sharding policy
  int32_t minRowsPerFileBlock;  // minimum rows per file block
  int32_t maxRowsPerFileBlock;  // maximum rows per file block
  int32_t keep;                 // day of data to keep
  int64_t maxCacheSize;         // maximum cache size this TSDB can use
} STsdbCfg;

void      tsdbSetDefaultCfg(STsdbCfg *pCfg);
STsdbCfg *tsdbCreateDefaultCfg();
void      tsdbFreeCfg(STsdbCfg *pCfg);

// --------- TSDB REPOSITORY DEFINITION
typedef void tsdb_repo_t;  // use void to hide implementation details from outside

int          tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter);
int32_t      tsdbDropRepo(tsdb_repo_t *repo);
tsdb_repo_t *tsdbOpenRepo(char *tsdbDir, STsdbAppH *pAppH);
int32_t      tsdbCloseRepo(tsdb_repo_t *repo);
int32_t      tsdbConfigRepo(tsdb_repo_t *repo, STsdbCfg *pCfg);

// --------- TSDB TABLE DEFINITION
typedef struct {
  int64_t uid;  // the unique table ID
  int32_t tid;  // the table ID in the repository.
} STableId;

// --------- TSDB TABLE configuration
typedef struct {
  ETableType      type;
  STableId        tableId;
  int32_t         sversion;
  int64_t         superUid;
  STSchema *      schema;
  STSchema *      tagSchema;
  SDataRow        tagValues;
} STableCfg;

int  tsdbInitTableCfg(STableCfg *config, ETableType type, int64_t uid, int32_t tid);
int  tsdbTableSetSuperUid(STableCfg *config, int64_t uid);
int  tsdbTableSetSchema(STableCfg *config, STSchema *pSchema, bool dup);
int  tsdbTableSetTagSchema(STableCfg *config, STSchema *pSchema, bool dup);
int  tsdbTableSetTagValue(STableCfg *config, SDataRow row, bool dup);
void tsdbClearTableCfg(STableCfg *config);

int tsdbCreateTable(tsdb_repo_t *repo, STableCfg *pCfg);
int tsdbDropTable(tsdb_repo_t *pRepo, STableId tableId);
int tsdbAlterTable(tsdb_repo_t *repo, STableCfg *pCfg);

// the TSDB repository info
typedef struct STsdbRepoInfo {
  STsdbCfg tsdbCfg;
  int64_t  version;            // version of the repository
  int64_t  tsdbTotalDataSize;  // the original inserted data size
  int64_t  tsdbTotalDiskSize;  // the total disk size taken by this TSDB repository
  // TODO: Other informations to add
} STsdbRepoInfo;
STsdbRepoInfo *tsdbGetStatus(tsdb_repo_t *pRepo);

// the meter information report structure
typedef struct {
  STableCfg tableCfg;
  int64_t   version;
  int64_t   tableTotalDataSize;  // In bytes
  int64_t   tableTotalDiskSize;  // In bytes
} STableInfo;
STableInfo *tsdbGetTableInfo(tsdb_repo_t *pRepo, STableId tid);

// -- FOR INSERT DATA
/**
 * Insert data to a table in a repository
 * @param pRepo the TSDB repository handle
 * @param pData the data to insert (will give a more specific description)
 *
 * @return the number of points inserted, -1 for failure and the error number is set
 */
int32_t tsdbInsertData(tsdb_repo_t *pRepo, SSubmitMsg *pMsg);

// -- FOR QUERY TIME SERIES DATA

typedef void *tsdb_query_handle_t;  // Use void to hide implementation details

typedef struct STableGroupList {  // qualified table object list in group
  SArray *pGroupList;
  int32_t numOfTables;
} STableGroupList;

// query condition to build vnode iterator
typedef struct STsdbQueryCond {
  STimeWindow      twindow;
  int32_t          order;  // desc/asc order to iterate the data block
  SColumnInfoData *colList;
} STsdbQueryCond;

typedef struct SBlockInfo {
  STimeWindow window;

  int32_t numOfRows;
  int32_t numOfCols;

  STableId tableId;
} SBlockInfo;

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rows;
  int32_t     numOfCols;
  int64_t     uid;
  int32_t     sid;
} SDataBlockInfo;

typedef struct {
} SFields;

#define TSDB_TS_GREATER_EQUAL 1
#define TSDB_TS_LESS_EQUAL 2

typedef struct SQueryRowCond {
  int32_t rel;
  TSKEY   ts;
} SQueryRowCond;

typedef void *tsdbpos_t;

/**
 * Get the data block iterator, starting from position according to the query condition
 * @param pCond  query condition, only includes the filter on primary time stamp
 * @param pTableList    table sid list
 * @return
 */
tsdb_query_handle_t *tsdbQueryTables(tsdb_repo_t *tsdb, STsdbQueryCond *pCond, SArray *idList, SArray *pColumnInfo);

/**
 * move to next block
 * @param pQueryHandle
 * @return
 */
bool tsdbNextDataBlock(tsdb_query_handle_t *pQueryHandle);

/**
 * Get current data block information
 *
 * @param pQueryHandle
 * @return
 */
SDataBlockInfo tsdbRetrieveDataBlockInfo(tsdb_query_handle_t *pQueryHandle);

/**
 *
 * Get the pre-calculated information w.r.t. current data block.
 *
 * In case of data block in cache, the pBlockStatis will always be NULL.
 * If a block is not completed loaded from disk, the pBlockStatis will be NULL.

 * @pBlockStatis the pre-calculated value for current data blocks. if the block is a cache block, always return 0
 * @return
 */
int32_t tsdbRetrieveDataBlockStatisInfo(tsdb_query_handle_t *pQueryHandle, SDataStatis **pBlockStatis);

/**
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pQueryHandle
 * @return
 */
SArray *tsdbRetrieveDataBlock(tsdb_query_handle_t *pQueryHandle, SArray *pIdList);

/**
 *  todo remove the parameter of position, and order type
 *
 *  Reset to the start(end) position of current query, from which the iterator starts.
 *
 * @param pQueryHandle
 * @param position  set the iterator traverses position
 * @param order ascending order or descending order
 * @return
 */
int32_t tsdbResetQuery(tsdb_query_handle_t *pQueryHandle, STimeWindow *window, tsdbpos_t position, int16_t order);

/**
 * return the access position of current query handle
 * @param pQueryHandle
 * @return
 */
int32_t tsdbDataBlockSeek(tsdb_query_handle_t *pQueryHandle, tsdbpos_t pos);

/**
 * todo remove this function later
 * @param pQueryHandle
 * @return
 */
tsdbpos_t tsdbDataBlockTell(tsdb_query_handle_t *pQueryHandle);

/**
 * todo remove this function later
 * @param pQueryHandle
 * @param pIdList
 * @return
 */
SArray *tsdbRetrieveDataRow(tsdb_query_handle_t *pQueryHandle, SArray *pIdList, SQueryRowCond *pCond);

/**
 *  Get iterator for super tables, of which tags values satisfy the tag filter info
 *
 *  NOTE: the tagFilterStr is an bin-expression for tag filter, such as ((tag_col = 5) and (tag_col2 > 7))
 *  The filter string is sent from client directly.
 *  The build of the tags filter expression from string is done in the iterator generating function.
 *
 * @param pCond         query condition
 * @param pTagFilterStr tag filter info
 * @return
 */
tsdb_query_handle_t *tsdbQueryFromTagConds(STsdbQueryCond *pCond, int16_t stableId, const char *pTagFilterStr);

/**
 * Get the qualified tables for (super) table query.
 * Used to handle the super table projection queries, the last_row query, the group by on normal columns query,
 * the interpolation query, and timestamp-comp query for join processing.
 *
 * @param pQueryHandle
 * @return table sid list. the invoker is responsible for the release of this the sid list.
 */
SArray *tsdbGetTableList(tsdb_query_handle_t *pQueryHandle);

/**
 * Get the qualified table id for a super table according to the tag query expression.
 * @param stableid. super table sid
 * @param pTagCond. tag query condition
 *
 */
int32_t tsdbQueryTags(tsdb_repo_t *tsdb, int64_t uid, const char *pTagCond, size_t len, SArray **pGroupList,
                      SColIndex *pColIndex, int32_t numOfCols);

int32_t tsdbGetOneTableGroup(tsdb_repo_t *tsdb, int64_t uid, SArray **pGroupList);

/**
 * clean up the query handle
 * @param queryHandle
 */
void tsdbCleanupQueryHandle(tsdb_query_handle_t queryHandle);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_H_
