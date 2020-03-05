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
#if !defined(_TD_TSDB_H_)
#define _TD_TSDB_H_

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "dataformat.h"
#include "schema.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_VERSION_MAJOR 1
#define TSDB_VERSION_MINOR 0

typedef void tsdb_repo_t;  // use void to hide implementation details from outside

typedef struct {
  int64_t uid;  // the unique table ID
  int32_t tid;  // the table ID in the repository.
} STableId;

// Submit message for this TSDB
typedef struct {
  int32_t numOfTables;
  int32_t compressed;
  char    data[];
} SSubmitMsg;

// Submit message for one table
typedef struct {
  STableId tableId;
  int32_t  sversion;   // data schema version
  int32_t  len;        // message length
  char     data[];
} SSubmitBlock;

enum { TSDB_PRECISION_MILLI, TSDB_PRECISION_MICRO, TSDB_PRECISION_NANO };

// the TSDB repository configuration
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

// the TSDB repository info
typedef struct STsdbRepoInfo {
  STsdbCfg tsdbCfg;
  int64_t  version;            // version of the repository
  int64_t  tsdbTotalDataSize;  // the original inserted data size
  int64_t  tsdbTotalDiskSize;  // the total disk size taken by this TSDB repository
  // TODO: Other informations to add
} STsdbRepoInfo;

// the meter configuration
typedef struct {
  STableId tableId;

  int64_t stableUid;
  int64_t createdTime;

  int32_t  numOfCols;  // number of columns. For table form super table, not includes the tag schema
  SSchema *schema;     // If numOfCols == schema_->numOfCols, it is a normal table, stableName = NULL
                       // If numOfCols < schema->numOfCols, it is a table created from super table
                       // assert(numOfCols <= schema->numOfCols);

  SDataRow tagValues;  // NULL if it is normal table
                       // otherwise, it contains the tag values.
} STableCfg;

// the meter information report structure
typedef struct {
  STableCfg tableCfg;
  int64_t   version;
  int64_t   tableTotalDataSize;  // In bytes
  int64_t   tableTotalDiskSize;  // In bytes
} STableInfo;

/**
 * Create a configuration for TSDB default
 * @return a pointer to a configuration. the configuration must call tsdbFreeCfg to free memory after usage
 */
STsdbCfg *tsdbCreateDefaultCfg();

/**
 * Free
 */
void tsdbFreeCfg(STsdbCfg *pCfg);

/**
 * Create a new TSDB repository
 * @param rootDir the TSDB repository root directory
 * @param pCfg the TSDB repository configuration, upper layer to free the pointer
 *
 * @return a TSDB repository handle on success, NULL for failure and the error number is set
 */
tsdb_repo_t *tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg, void *limiter);

/**
 * Close and free all resources taken by the repository
 * @param repo the TSDB repository handle. The interface will free the handle too, so upper
 *              layer do NOT need to free the repo handle again.
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbDropRepo(tsdb_repo_t *repo);

/**
 * Open an existing TSDB storage repository
 * @param tsdbDir the existing TSDB root directory
 *
 * @return a TSDB repository handle on success, NULL for failure and the error number is set
 */
tsdb_repo_t *tsdbOpenRepo(char *tsdbDir);

/**
 * Close a TSDB repository. Only free memory resources, and keep the files.
 * @param repo the opened TSDB repository handle. The interface will free the handle too, so upper
 *              layer do NOT need to free the repo handle again.
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbCloseRepo(tsdb_repo_t *repo);

/**
 * Change the configuration of a repository
 * @param pCfg the repository configuration, the upper layer should free the pointer
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbConfigRepo(tsdb_repo_t *repo, STsdbCfg *pCfg);

/**
 * Get the TSDB repository information, including some statistics
 * @param pRepo the TSDB repository handle
 * @param error the error number to set when failure occurs
 *
 * @return a info struct handle on success, NULL for failure and the error number is set. The upper
 *         layers should free the info handle themselves or memory leak will occur
 */
STsdbRepoInfo *tsdbGetStatus(tsdb_repo_t *pRepo);

// -- For table manipulation

/**
 * Create/Alter a table in a TSDB repository handle
 * @param repo the TSDB repository handle
 * @param pCfg the table configurations, the upper layer should free the pointer
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbCreateTable(tsdb_repo_t *repo, STableCfg *pCfg);
int32_t tsdbAlterTable(tsdb_repo_t *repo, STableCfg *pCfg);

/**
 * Drop a table in a repository and free all the resources it takes
 * @param pRepo the TSDB repository handle
 * @param tid the ID of the table to drop
 * @param error the error number to set when failure occurs
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbDropTable(tsdb_repo_t *pRepo, STableId tableId);

/**
 * Get the information of a table in the repository
 * @param pRepo the TSDB repository handle
 * @param tid the ID of the table to drop
 * @param error the error number to set when failure occurs
 *
 * @return a table information handle for success, NULL for failure and the error number is set
 */
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

typedef void tsdb_query_handle_t;  // Use void to hide implementation details

// time window
typedef struct STimeWindow {
  int64_t skey;
  int64_t ekey;
} STimeWindow;

typedef struct {
} SColumnFilterInfo;

// query condition to build vnode iterator
typedef struct STSDBQueryCond {
  STimeWindow       twindow;
  int32_t           order;  // desc/asc order to iterate the data block
  SColumnFilterInfo colFilterInfo;
} STSDBQueryCond;

typedef struct SBlockInfo {
  STimeWindow window;

  int32_t numOfRows;
  int32_t numOfCols;

  STableId tableId;
} SBlockInfo;

//  TODO: move this data struct out of the module
typedef struct SData {
  int32_t num;
  char *  data;
} SData;

typedef struct SDataBlock {
  int32_t numOfCols;
  SData **pData;
} SDataBlock;

typedef struct STableIDList {
  STableId *tableIds;
  int32_t   num;
} STableIDList;

typedef struct {
} SFields;

/**
 * Get the data block iterator, starting from position according to the query condition
 * @param pRepo  the TSDB repository to query on
 * @param pCond  query condition, only includes the filter on primary time stamp
 * @param pTableList    table sid list
 * @return
 */
tsdb_query_handle_t *tsdbQueryFromTableID(tsdb_repo_t *pRepo, STSDBQueryCond *pCond, const STableIDList *pTableList);

/**
 *  Get iterator for super tables, of which tags values satisfy the tag filter info
 *
 *  NOTE: the tagFilterStr is an bin-expression for tag filter, such as ((tag_col = 5) and (tag_col2 > 7))
 *  The filter string is sent from client directly.
 *  The build of the tags filter expression from string is done in the iterator generating function.
 *
 * @param pRepo         the repository to query on
 * @param pCond         query condition
 * @param pTagFilterStr tag filter info
 * @return
 */
tsdb_query_handle_t *tsdbQueryFromTagConds(tsdb_repo_t *pRepo, STSDBQueryCond *pCond, int16_t stableId,
                                           const char *pTagFilterStr);

/**
 *  Reset to the start(end) position of current query, from which the iterator starts.
 *
 * @param pQueryHandle
 * @param position  set the iterator traverses position. (TSDB_POS_START|TSDB_POS_END)
 * @return
 */
int32_t tsdbResetQuery(tsdb_query_handle_t *pQueryHandle, int16_t position);

/**
 *  move to next block
 * @param pQueryHandle
 * @param pCond
 * @return
 */
bool tsdbIterNext(tsdb_query_handle_t *pQueryHandle);

/**
 * 当前数据块的信息，调用next函数后，只会获得block的信息，包括：行数、列数、skey/ekey信息。注意该信息并不是现在的SCompBlockInfo信息。
 * 因为SCompBlockInfo是完整的数据块信息，但是迭代器返回并不是。
 * 查询处理引擎会自己决定需要blockInfo, 还是预计算数据，亦或是完整的数据。
 * Get current data block information
 *
 * @param pQueryHandle
 * @return
 */
SBlockInfo tsdbRetrieveDataBlockInfo(tsdb_query_handle_t *pQueryHandle);

/**
 * 获取当前数据块的预计算信息，如果块不完整，无预计算信息，如果是cache块，无预计算信息。
 *
 * Get the pre-calculated information w.r.t. current data block.
 *
 * In case of data block in cache, the pBlockStatis will always be NULL.
 * If a block is not completed loaded from disk, the pBlockStatis will be NULL.

 * @pBlockStatis the pre-calculated value for current data blocks. if the block is a cache block, always return 0
 * @return
 */
int32_t tsdbRetrieveDataBlockStatisInfo(tsdb_query_handle_t *pQueryHandle, SFields *pBlockStatis);

/**
 * 返回加载到缓存中的数据，可能是磁盘数据也可能是内存数据，对客户透明。即使是磁盘数据，返回的结果也是磁盘块中，满足查询时间范围要求的数据行，并不是一个完整的磁盘数
 * 据块。
 *
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pQueryHandle
 * @return
 */
SDataBlock *tsdbRetrieveDataBlock(tsdb_query_handle_t *pQueryHandle);

/**
 * Get the qualified tables for (super) table query.
 * Used to handle the super table projection queries, the last_row query, the group by on normal columns query,
 * the interpolation query, and timestamp-comp query for join processing.
 *
 * @param pQueryHandle
 * @return table sid list. the invoker is responsible for the release of this the sid list.
 */
STableIDList *tsdbGetTableList(tsdb_query_handle_t *pQueryHandle);

/**
 * Get the qualified table sid for a super table according to the tag query expression.
 * @param stableid. super table sid
 * @param pTagCond. tag query condition
 *
 */
STableIDList *tsdbQueryTableList(int16_t stableId, const char *pTagCond);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_H_