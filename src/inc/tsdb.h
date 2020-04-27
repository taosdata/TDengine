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

#include "tdataformat.h"
#include "tname.h"
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
  int32_t tsdbId;
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t maxTables;            // maximum number of tables this repository can have
  int32_t daysPerFile;          // day per file sharding policy
  int32_t keep;                 // day of data to keep
  int32_t keep1;
  int32_t keep2;
  int32_t minRowsPerFileBlock;  // minimum rows per file block
  int32_t maxRowsPerFileBlock;  // maximum rows per file block
  int32_t commitTime;
  int8_t  precision;
  int8_t  compression;
} STsdbCfg;

void      tsdbSetDefaultCfg(STsdbCfg *pCfg);
STsdbCfg *tsdbCreateDefaultCfg();
void      tsdbFreeCfg(STsdbCfg *pCfg);

// --------- TSDB REPOSITORY DEFINITION
typedef void TsdbRepoT;  // use void to hide implementation details from outside

int        tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter);
int32_t    tsdbDropRepo(TsdbRepoT *repo);
TsdbRepoT *tsdbOpenRepo(char *tsdbDir, STsdbAppH *pAppH);
int32_t    tsdbCloseRepo(TsdbRepoT *repo);
int32_t    tsdbConfigRepo(TsdbRepoT *repo, STsdbCfg *pCfg);

// --------- TSDB TABLE DEFINITION
typedef struct {
  int64_t uid;  // the unique table ID
  int32_t tid;  // the table ID in the repository.
} STableId;

// --------- TSDB TABLE configuration
typedef struct {
  ETableType type;
  char *     name;
  STableId   tableId;
  int32_t    sversion;
  char *     sname;  // super table name
  int64_t    superUid;
  STSchema * schema;
  STSchema * tagSchema;
  SDataRow   tagValues;
} STableCfg;

int  tsdbInitTableCfg(STableCfg *config, ETableType type, int64_t uid, int32_t tid);
int  tsdbTableSetSuperUid(STableCfg *config, int64_t uid);
int  tsdbTableSetSchema(STableCfg *config, STSchema *pSchema, bool dup);
int  tsdbTableSetTagSchema(STableCfg *config, STSchema *pSchema, bool dup);
int  tsdbTableSetTagValue(STableCfg *config, SDataRow row, bool dup);
int  tsdbTableSetName(STableCfg *config, char *name, bool dup);
int  tsdbTableSetSName(STableCfg *config, char *sname, bool dup);
void tsdbClearTableCfg(STableCfg *config);

int32_t tsdbGetTableTagVal(TsdbRepoT *repo, STableId id, int32_t col, int16_t *type, int16_t *bytes, char **val);
int32_t tsdbTableGetName(TsdbRepoT *repo, STableId id, char** name);

int tsdbCreateTable(TsdbRepoT *repo, STableCfg *pCfg);
int tsdbDropTable(TsdbRepoT *pRepo, STableId tableId);
int tsdbAlterTable(TsdbRepoT *repo, STableCfg *pCfg);

// the TSDB repository info
typedef struct STsdbRepoInfo {
  STsdbCfg tsdbCfg;
  int64_t  version;            // version of the repository
  int64_t  tsdbTotalDataSize;  // the original inserted data size
  int64_t  tsdbTotalDiskSize;  // the total disk size taken by this TSDB repository
  // TODO: Other informations to add
} STsdbRepoInfo;
STsdbRepoInfo *tsdbGetStatus(TsdbRepoT *pRepo);

// the meter information report structure
typedef struct {
  STableCfg tableCfg;
  int64_t   version;
  int64_t   tableTotalDataSize;  // In bytes
  int64_t   tableTotalDiskSize;  // In bytes
} STableInfo;
STableInfo *tsdbGetTableInfo(TsdbRepoT *pRepo, STableId tid);

// -- FOR INSERT DATA
/**
 * Insert data to a table in a repository
 * @param pRepo the TSDB repository handle
 * @param pData the data to insert (will give a more specific description)
 *
 * @return the number of points inserted, -1 for failure and the error number is set
 */
int32_t tsdbInsertData(TsdbRepoT *pRepo, SSubmitMsg *pMsg);

// -- FOR QUERY TIME SERIES DATA

typedef void *TsdbQueryHandleT;  // Use void to hide implementation details

typedef struct STableGroupList {  // qualified table object list in group
  SArray *pGroupList;
  int32_t numOfTables;
} STableGroupList;

// query condition to build vnode iterator
typedef struct STsdbQueryCond {
  STimeWindow      twindow;
  int32_t          order;  // desc/asc order to iterate the data block
  int32_t          numOfCols;
  SColumnInfo     *colList;
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
  size_t  numOfTables;
  SArray *pGroupList;
} STableGroupInfo;

typedef struct {
} SFields;

#define TSDB_TS_GREATER_EQUAL 1
#define TSDB_TS_LESS_EQUAL 2

typedef struct SQueryRowCond {
  int32_t rel;
  TSKEY   ts;
} SQueryRowCond;

typedef void *TsdbPosT;

/**
 * Get the data block iterator, starting from position according to the query condition
 * @param pCond  query condition, only includes the filter on primary time stamp
 * @param pTableList    table sid list
 * @return
 */
TsdbQueryHandleT *tsdbQueryTables(TsdbRepoT *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupInfo);

/**
 * move to next block
 * @param pQueryHandle
 * @return
 */
bool tsdbNextDataBlock(TsdbQueryHandleT *pQueryHandle);

/**
 * Get current data block information
 *
 * @param pQueryHandle
 * @return
 */
SDataBlockInfo tsdbRetrieveDataBlockInfo(TsdbQueryHandleT *pQueryHandle);

/**
 *
 * Get the pre-calculated information w.r.t. current data block.
 *
 * In case of data block in cache, the pBlockStatis will always be NULL.
 * If a block is not completed loaded from disk, the pBlockStatis will be NULL.

 * @pBlockStatis the pre-calculated value for current data blocks. if the block is a cache block, always return 0
 * @return
 */
int32_t tsdbRetrieveDataBlockStatisInfo(TsdbQueryHandleT *pQueryHandle, SDataStatis **pBlockStatis);

/**
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pQueryHandle
 * @return
 */
SArray *tsdbRetrieveDataBlock(TsdbQueryHandleT *pQueryHandle, SArray *pIdList);

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
int32_t tsdbResetQuery(TsdbQueryHandleT *pQueryHandle, STimeWindow *window, TsdbPosT position, int16_t order);

/**
 * todo remove this function later
 * @param pQueryHandle
 * @param pIdList
 * @return
 */
SArray *tsdbRetrieveDataRow(TsdbQueryHandleT *pQueryHandle, SArray *pIdList, SQueryRowCond *pCond);

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
TsdbQueryHandleT *tsdbQueryFromTagConds(STsdbQueryCond *pCond, int16_t stableId, const char *pTagFilterStr);

/**
 * Get the qualified tables for (super) table query.
 * Used to handle the super table projection queries, the last_row query, the group by on normal columns query,
 * the interpolation query, and timestamp-comp query for join processing.
 *
 * @param pQueryHandle
 * @return table sid list. the invoker is responsible for the release of this the sid list.
 */
SArray *tsdbGetTableList(TsdbQueryHandleT *pQueryHandle);

/**
 * Get the qualified table id for a super table according to the tag query expression.
 * @param stableid. super table sid
 * @param pTagCond. tag query condition
 *
 */
int32_t tsdbQueryByTagsCond(
  TsdbRepoT *tsdb,
  int64_t uid,
  const char *pTagCond,
  size_t len,
  int16_t tagNameRelType,
  const char* tbnameCond,
  STableGroupInfo *pGroupList,
  SColIndex *pColIndex,
  int32_t numOfCols
  );

int32_t tsdbGetOneTableGroup(TsdbRepoT *tsdb, int64_t uid, STableGroupInfo *pGroupInfo);

/**
 * clean up the query handle
 * @param queryHandle
 */
void tsdbCleanupQueryHandle(TsdbQueryHandleT queryHandle);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_H_
