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

#include "taosdef.h"
#include "taosmsg.h"
#include "tarray.h"
#include "tdataformat.h"
#include "tname.h"
#include "hash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_VERSION_MAJOR 1
#define TSDB_VERSION_MINOR 0

#define TSDB_INVALID_SUPER_TABLE_ID -1

#define TSDB_STATUS_COMMIT_START 1
#define TSDB_STATUS_COMMIT_OVER 2

// TSDB STATE DEFINITION
#define TSDB_STATE_OK 0x0
#define TSDB_STATE_BAD_FILE 0x1

// --------- TSDB APPLICATION HANDLE DEFINITION
typedef struct {
  void *appH;
  void *cqH;
  int (*notifyStatus)(void *, int status);
  int (*eventCallBack)(void *);
  void *(*cqCreateFunc)(void *handle, uint64_t uid, int sid, char *sqlStr, STSchema *pSchema);
  void (*cqDropFunc)(void *handle);
} STsdbAppH;

// --------- TSDB REPOSITORY CONFIGURATION DEFINITION
typedef struct {
  int32_t tsdbId;
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t daysPerFile;  // day per file sharding policy
  int32_t keep;         // day of data to keep
  int32_t keep1;
  int32_t keep2;
  int32_t minRowsPerFileBlock;  // minimum rows per file block
  int32_t maxRowsPerFileBlock;  // maximum rows per file block
  int8_t  precision;
  int8_t  compression;
  int8_t  update;
} STsdbCfg;

// --------- TSDB REPOSITORY USAGE STATISTICS
typedef struct {
  int64_t totalStorage;  // total bytes occupie
  int64_t compStorage;
  int64_t pointsWritten;  // total data points written
} STsdbStat;

typedef void TSDB_REPO_T;  // use void to hide implementation details from outside

STsdbCfg *tsdbGetCfg(const TSDB_REPO_T *repo);

// --------- TSDB REPOSITORY DEFINITION
int          tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg);
int32_t      tsdbDropRepo(char *rootDir);
TSDB_REPO_T *tsdbOpenRepo(char *rootDir, STsdbAppH *pAppH);
void         tsdbCloseRepo(TSDB_REPO_T *repo, int toCommit);
int32_t      tsdbConfigRepo(TSDB_REPO_T *repo, STsdbCfg *pCfg);
int          tsdbGetState(TSDB_REPO_T *repo);

// --------- TSDB TABLE DEFINITION
typedef struct {
  uint64_t uid;  // the unique table ID
  int32_t  tid;  // the table ID in the repository.
} STableId;

// --------- TSDB TABLE configuration
typedef struct {
  ETableType type;
  char *     name;
  STableId   tableId;
  int32_t    sversion;
  char *     sname;  // super table name
  uint64_t   superUid;
  STSchema * schema;
  STSchema * tagSchema;
  SDataRow   tagValues;
  char *     sql;
} STableCfg;

void tsdbClearTableCfg(STableCfg *config);

void* tsdbGetTableTagVal(const void* pTable, int32_t colId, int16_t type, int16_t bytes);
char* tsdbGetTableName(void *pTable);

#define TSDB_TABLEID(_table) ((STableId*) (_table))

STableCfg *tsdbCreateTableCfgFromMsg(SMDCreateTableMsg *pMsg);

int   tsdbCreateTable(TSDB_REPO_T *repo, STableCfg *pCfg);
int   tsdbDropTable(TSDB_REPO_T *pRepo, STableId tableId);
int   tsdbUpdateTableTagValue(TSDB_REPO_T *repo, SUpdateTableTagValMsg *pMsg);
TSKEY tsdbGetTableLastKey(TSDB_REPO_T *repo, uint64_t uid);

uint32_t tsdbGetFileInfo(TSDB_REPO_T *repo, char *name, uint32_t *index, uint32_t eindex, int64_t *size);

// the TSDB repository info
typedef struct STsdbRepoInfo {
  STsdbCfg tsdbCfg;
  int64_t  version;            // version of the repository
  int64_t  tsdbTotalDataSize;  // the original inserted data size
  int64_t  tsdbTotalDiskSize;  // the total disk size taken by this TSDB repository
  // TODO: Other informations to add
} STsdbRepoInfo;
STsdbRepoInfo *tsdbGetStatus(TSDB_REPO_T *pRepo);

// the meter information report structure
typedef struct {
  STableCfg tableCfg;
  int64_t   version;
  int64_t   tableTotalDataSize;  // In bytes
  int64_t   tableTotalDiskSize;  // In bytes
} STableInfo;
STableInfo *tsdbGetTableInfo(TSDB_REPO_T *pRepo, STableId tid);

// -- FOR INSERT DATA
/**
 * Insert data to a table in a repository
 * @param pRepo the TSDB repository handle
 * @param pData the data to insert (will give a more specific description)
 *
 * @return the number of points inserted, -1 for failure and the error number is set
 */
int32_t tsdbInsertData(TSDB_REPO_T *repo, SSubmitMsg *pMsg, SShellSubmitRspMsg *pRsp);

// -- FOR QUERY TIME SERIES DATA

typedef void *TsdbQueryHandleT;  // Use void to hide implementation details

// query condition to build vnode iterator
typedef struct STsdbQueryCond {
  STimeWindow  twindow;
  int32_t      order;  // desc|asc order to iterate the data block
  int32_t      numOfCols;
  SColumnInfo *colList;
} STsdbQueryCond;

typedef struct SMemRef {
  int32_t  ref;
  void    *mem;
  void    *imem;
} SMemRef;

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rows;
  int32_t     numOfCols;
  int64_t     uid;
  int32_t     tid;
} SDataBlockInfo;

typedef struct {
  void  *pTable;
  TSKEY  lastKey;
} STableKeyInfo;

typedef struct {
  size_t    numOfTables;
  SArray   *pGroupList;
  SHashObj *map;         // speedup acquire the tableQueryInfo by table uid
} STableGroupInfo;

/**
 * Get the data block iterator, starting from position according to the query condition
 *
 * @param tsdb       tsdb handle
 * @param pCond      query condition, including time window, result set order, and basic required columns for each block
 * @param tableInfoGroup  table object list in the form of set, grouped into different sets according to the
 *                        group by condition
 * @param qinfo      query info handle from query processor
 * @return
 */
TsdbQueryHandleT *tsdbQueryTables(TSDB_REPO_T *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfoGroup, void *qinfo, SMemRef* pRef);

/**
 * Get the last row of the given query time window for all the tables in STableGroupInfo object.
 * Note that only one data block with only row will be returned while invoking retrieve data block function for
 * all tables in this group.
 *
 * @param tsdb   tsdb handle
 * @param pCond  query condition, including time window, result set order, and basic required columns for each block
 * @param tableInfo  table list.
 * @return
 */
TsdbQueryHandleT tsdbQueryLastRow(TSDB_REPO_T *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfo, void *qinfo, SMemRef* pRef);

/**
 * get the queried table object list
 * @param pHandle
 * @return
 */
SArray* tsdbGetQueriedTableList(TsdbQueryHandleT *pHandle);

/**
 * get the group list according to table id from client
 * @param tsdb
 * @param pCond
 * @param groupList
 * @param qinfo
 * @return
 */
TsdbQueryHandleT tsdbQueryRowsInExternalWindow(TSDB_REPO_T *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList,
                                               void *qinfo, SMemRef* pRef);

/**
 * move to next block if exists
 *
 * @param pQueryHandle
 * @return
 */
bool tsdbNextDataBlock(TsdbQueryHandleT *pQueryHandle);

/**
 * Get current data block information
 *
 * @param pQueryHandle
 * @param pBlockInfo
 * @return
 */
void tsdbRetrieveDataBlockInfo(TsdbQueryHandleT *pQueryHandle, SDataBlockInfo* pBlockInfo);

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
 *
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pQueryHandle      query handle
 * @param pColumnIdList     required data columns id list
 * @return
 */
SArray *tsdbRetrieveDataBlock(TsdbQueryHandleT *pQueryHandle, SArray *pColumnIdList);

/**
 * Get the qualified table id for a super table according to the tag query expression.
 * @param stableid. super table sid
 * @param pTagCond. tag query condition
 */
int32_t tsdbQuerySTableByTagCond(TSDB_REPO_T *tsdb, uint64_t uid, TSKEY key, const char *pTagCond, size_t len,
                                 int16_t tagNameRelType, const char *tbnameCond, STableGroupInfo *pGroupList,
                                 SColIndex *pColIndex, int32_t numOfCols);

/**
 * destory the created table group list, which is generated by tag query
 * @param pGroupList
 */
void tsdbDestroyTableGroup(STableGroupInfo *pGroupList);

/**
 * create the table group result including only one table, used to handle the normal table query
 *
 * @param tsdb        tsdbHandle
 * @param uid         table uid
 * @param pGroupInfo  the generated result
 * @return
 */
int32_t tsdbGetOneTableGroup(TSDB_REPO_T *tsdb, uint64_t uid, TSKEY startKey, STableGroupInfo *pGroupInfo);

/**
 *
 * @param tsdb
 * @param pTableIdList
 * @param pGroupInfo
 * @return
 */
int32_t tsdbGetTableGroupFromIdList(TSDB_REPO_T* tsdb, SArray* pTableIdList, STableGroupInfo* pGroupInfo);

/**
 * clean up the query handle
 * @param queryHandle
 */
void tsdbCleanupQueryHandle(TsdbQueryHandleT queryHandle);

/**
 * get the statistics of repo usage
 * @param repo. point to the tsdbrepo
 * @param totalPoints. total data point written
 * @param totalStorage. total bytes took by the tsdb
 * @param compStorage. total bytes took by the tsdb after compressed
 */
void tsdbReportStat(void *repo, int64_t *totalPoints, int64_t *totalStorage, int64_t *compStorage);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_H_
