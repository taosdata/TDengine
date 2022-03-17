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

#include "tmallocator.h"
#include "meta.h"
#include "tcommon.h"
#include "tfs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDataStatis {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SDataStatis;

typedef struct STable {
  uint64_t  tid;
  uint64_t  uid;
  STSchema *pSchema;
} STable;

#define BLOCK_LOAD_OFFSET_SEQ_ORDER   1
#define BLOCK_LOAD_TABLE_SEQ_ORDER    2
#define BLOCK_LOAD_TABLE_RR_ORDER     3

#define TABLE_TID(t) (t)->tid
#define TABLE_UID(t) (t)->uid

// TYPES EXPOSED
typedef struct STsdb STsdb;

typedef struct STsdbCfg {
  int8_t   precision;
  int8_t   update;
  int8_t   compression;
  uint64_t lruCacheSize;
  int32_t  daysPerFile;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int32_t  keep;
  int32_t  keep1;
  int32_t  keep2;
} STsdbCfg;

// query condition to build multi-table data block iterator
typedef struct STsdbQueryCond {
  STimeWindow  twindow;
  int32_t      order;             // desc|asc order to iterate the data block
  int32_t      numOfCols;
  SColumnInfo *colList;
  bool         loadExternalRows;  // load external rows or not
  int32_t      type;              // data block load type:
} STsdbQueryCond;

typedef struct {
  TSKEY    lastKey;
  uint64_t uid;
} STableKeyInfo;

// STsdb
STsdb *tsdbOpen(const char *path, int32_t vgId, const STsdbCfg *pTsdbCfg, SMemAllocatorFactory *pMAF, SMeta *pMeta, STfs *pTfs);
void   tsdbClose(STsdb *);
void   tsdbRemove(const char *path);
int    tsdbInsertData(STsdb *pTsdb, SSubmitReq *pMsg, SSubmitRsp *pRsp);
int    tsdbPrepareCommit(STsdb *pTsdb);
int    tsdbCommit(STsdb *pTsdb);

/**
 * @brief Insert tSma(Time-range-wise SMA) data from stream computing engine
 *
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
int32_t tsdbInsertTSmaData(STsdb *pTsdb, char *msg);
int32_t tsdbUpdateSmaWindow(STsdb *pTsdb, int8_t smaType, char *msg);

/**
 * @brief Insert RSma(Time-range-wise Rollup SMA) data.
 *
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
int32_t tsdbInsertRSmaData(STsdb *pTsdb, char *msg);

// TODO: This is the basic params, and should wrap the params to a queryHandle.
int32_t tsdbGetTSmaData(STsdb *pTsdb, STSmaDataWrapper *pData, int64_t indexUid, int64_t interval,
                            int8_t intervalUnit, tb_uid_t tableUid, col_id_t colId, TSKEY querySkey,
                            int32_t nMaxResult);


// STsdbCfg
int  tsdbOptionsInit(STsdbCfg *);
void tsdbOptionsClear(STsdbCfg *);

typedef void* tsdbReaderT;

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
tsdbReaderT *tsdbQueryTables(STsdb *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfoGroup, uint64_t qId, uint64_t taskId);

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
//tsdbReaderT tsdbQueryLastRow(STsdbRepo *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfo, uint64_t qId,
//                                  SMemRef *pRef);


tsdbReaderT tsdbQueryCacheLast(STsdb *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, uint64_t qId, void* pMemRef);

bool isTsdbCacheLastRow(tsdbReaderT* pTsdbReadHandle);

/**
 *
 * @param tsdb
 * @param uid
 * @param skey
 * @param pTagCond
 * @param len
 * @param tagNameRelType
 * @param tbnameCond
 * @param pGroupInfo
 * @param pColIndex
 * @param numOfCols
 * @param reqId
 * @return
 */
int32_t tsdbQuerySTableByTagCond(void* pMeta, uint64_t uid, TSKEY skey, const char* pTagCond, size_t len,
                                 int16_t tagNameRelType, const char* tbnameCond, STableGroupInfo* pGroupInfo,
                                 SColIndex* pColIndex, int32_t numOfCols, uint64_t reqId, uint64_t taskId);
/**
 * get num of rows in mem table
 *
 * @param pHandle
 * @return row size
 */

int64_t tsdbGetNumOfRowsInMemTable(tsdbReaderT* pHandle);

/**
 * move to next block if exists
 *
 * @param pTsdbReadHandle
 * @return
 */
bool tsdbNextDataBlock(tsdbReaderT pTsdbReadHandle);

/**
 * Get current data block information
 *
 * @param pTsdbReadHandle
 * @param pBlockInfo
 * @return
 */
void tsdbRetrieveDataBlockInfo(tsdbReaderT *pTsdbReadHandle, SDataBlockInfo *pBlockInfo);

/**
 *
 * Get the pre-calculated information w.r.t. current data block.
 *
 * In case of data block in cache, the pBlockStatis will always be NULL.
 * If a block is not completed loaded from disk, the pBlockStatis will be NULL.

 * @pBlockStatis the pre-calculated value for current data blocks. if the block is a cache block, always return 0
 * @return
 */
int32_t tsdbRetrieveDataBlockStatisInfo(tsdbReaderT *pTsdbReadHandle, SDataStatis **pBlockStatis);

/**
 *
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pTsdbReadHandle      query handle
 * @param pColumnIdList     required data columns id list
 * @return
 */
SArray *tsdbRetrieveDataBlock(tsdbReaderT *pTsdbReadHandle, SArray *pColumnIdList);

/**
 * destroy the created table group list, which is generated by tag query
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
int32_t tsdbGetOneTableGroup(void *pMeta, uint64_t uid, TSKEY startKey, STableGroupInfo *pGroupInfo);

/**
 *
 * @param tsdb
 * @param pTableIdList
 * @param pGroupInfo
 * @return
 */
int32_t tsdbGetTableGroupFromIdList(STsdb *tsdb, SArray *pTableIdList, STableGroupInfo *pGroupInfo);

/**
 * clean up the query handle
 * @param queryHandle
 */
void tsdbCleanupReadHandle(tsdbReaderT queryHandle);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_H_*/
