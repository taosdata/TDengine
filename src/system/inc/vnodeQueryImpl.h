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

#ifndef TDENGINE_VNODEQUERYUTIL_H
#define TDENGINE_VNODEQUERYUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>

#include "ihash.h"

#define GET_QINFO_ADDR(x)    ((char*)(x)-offsetof(SQInfo, query))
#define Q_STATUS_EQUAL(p, s) (((p) & (s)) != 0)

#define DEFAULT_INTERN_BUF_SIZE            8192L
#define INIT_ALLOCATE_DISK_PAGES           60L
#define DEFAULT_DATA_FILE_MAPPING_PAGES    2L
#define DEFAULT_DATA_FILE_MMAP_WINDOW_SIZE (DEFAULT_DATA_FILE_MAPPING_PAGES * DEFAULT_INTERN_BUF_SIZE)

#define IO_ENGINE_MMAP 0
#define IO_ENGINE_SYNC 1

#define DEFAULT_IO_ENGINE IO_ENGINE_SYNC

/**
 * check if the primary column is load by default, otherwise, the program will
 * forced to load primary column explicitly.
 */
#define PRIMARY_TSCOL_LOADED(query) ((query)->colList[0].data.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)

typedef enum {

  /*
   * the program will call this function again, if this status is set.
   * used to transfer from QUERY_RESBUF_FULL
   */
  QUERY_NOT_COMPLETED = 0x1,

  /*
   * output buffer is full, so, the next query will be employed,
   * in this case, we need to set the appropriated start scan point for
   * the next query.
   *
   * this status is only exist in group-by clause and
   * diff/add/division/mulitply/ query.
   */
  QUERY_RESBUF_FULL = 0x2,

  /*
   * query is over
   * 1. this status is used in one row result query process, e.g.,
   * count/sum/first/last/
   * avg...etc.
   * 2. when the query range on timestamp is satisfied, it is also denoted as
   * query_compeleted
   */
  QUERY_COMPLETED = 0x4,

  /*
   * all data has been scanned, so current search is stopped,
   * At last, the function will transfer this status to QUERY_COMPLETED
   */
  QUERY_NO_DATA_TO_CHECK = 0x8,

} vnodeQueryStatus;

typedef struct SPointInterpoSupporter {
  int32_t numOfCols;
  char**  pPrevPoint;
  char**  pNextPoint;
} SPointInterpoSupporter;

typedef struct SBlockInfo {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int32_t numOfCols;
  int32_t size;
} SBlockInfo;

typedef struct SMeterDataBlockInfoEx {
  SCompBlockFields pBlock;
  SMeterDataInfo*  pMeterDataInfo;
  int32_t          blockIndex;
  int32_t          groupIdx; /* number of group is less than the total number of meters */
} SMeterDataBlockInfoEx;

typedef enum {
  DISK_DATA_LOAD_FAILED = -0x1,
  DISK_DATA_LOADED = 0x0,
  DISK_DATA_DISCARDED = 0x01,
} vnodeDiskLoadStatus;

#define IS_MASTER_SCAN(runtime) ((runtime)->scanFlag == MASTER_SCAN)
#define IS_SUPPLEMENT_SCAN(runtime) (!IS_MASTER_SCAN(runtime))
#define SET_SUPPLEMENT_SCAN_FLAG(runtime) ((runtime)->scanFlag = SUPPLEMENTARY_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime) ((runtime)->scanFlag = MASTER_SCAN)

typedef int (*__block_search_fn_t)(char* data, int num, int64_t key, int order);
typedef int32_t (*__read_data_fn_t)(int fd, SQInfo* pQInfo, SQueryFileInfo* pQueryFile, char* buf, uint64_t offset,
                                    int32_t size);

static FORCE_INLINE SMeterObj* getMeterObj(void* hashHandle, int32_t sid) {
  return *(SMeterObj**)taosGetIntHashData(hashHandle, sid);
}

bool isQueryKilled(SQuery* pQuery);
bool isFixedOutputQuery(SQuery* pQuery);
bool isPointInterpoQuery(SQuery* pQuery);
bool isTopBottomQuery(SQuery* pQuery);
bool isFirstLastRowQuery(SQuery* pQuery);

bool needSupplementaryScan(SQuery* pQuery);
bool onDemandLoadDatablock(SQuery* pQuery, int16_t queryRangeSet);

void setQueryStatus(SQuery* pQuery, int8_t status);

bool doRevisedResultsByLimit(SQInfo* pQInfo);
void truncateResultByLimit(SQInfo* pQInfo, int64_t* final, int32_t* interpo);

void initCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv);
void cleanCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv);
void resetCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv);
void forwardCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv, int64_t output);

bool needPrimaryTimestampCol(SQuery* pQuery, SBlockInfo* pBlockInfo);
void vnodeScanAllData(SQueryRuntimeEnv* pRuntimeEnv);

int32_t vnodeQueryResultInterpolate(SQInfo* pQInfo, tFilePage** pDst, tFilePage** pDataSrc, int32_t numOfRows,
                                    int32_t* numOfInterpo);
void copyResToQueryResultBuf(SMeterQuerySupportObj* pSupporter, SQuery* pQuery);
void moveDescOrderResultsToFront(SQueryRuntimeEnv* pRuntimeEnv);

void doSkipResults(SQueryRuntimeEnv* pRuntimeEnv);
void doFinalizeResult(SQueryRuntimeEnv* pRuntimeEnv);
int64_t getNumOfResult(SQueryRuntimeEnv* pRuntimeEnv);

void forwardIntervalQueryRange(SMeterQuerySupportObj* pSupporter, SQueryRuntimeEnv* pRuntimeEnv);
void forwardQueryStartPosition(SQueryRuntimeEnv* pRuntimeEnv);

bool normalizedFirstQueryRange(bool dataInDisk, bool dataInCache, SMeterQuerySupportObj* pSupporter,
                               SPointInterpoSupporter* pPointInterpSupporter);

void pointInterpSupporterInit(SQuery* pQuery, SPointInterpoSupporter* pInterpoSupport);
void pointInterpSupporterDestroy(SPointInterpoSupporter* pPointInterpSupport);
void pointInterpSupporterSetData(SQInfo* pQInfo, SPointInterpoSupporter* pPointInterpSupport);

int64_t loadRequiredBlockIntoMem(SQueryRuntimeEnv* pRuntimeEnv, SPositionInfo* position);
void doCloseAllOpenedResults(SMeterQuerySupportObj* pSupporter);
void disableFunctForSuppleScanAndSetSortOrder(SQueryRuntimeEnv* pRuntimeEnv, int32_t order);
void enableFunctForMasterScan(SQueryRuntimeEnv* pRuntimeEnv, int32_t order);

int32_t mergeMetersResultToOneGroups(SMeterQuerySupportObj* pSupporter);
void copyFromGroupBuf(SQInfo* pQInfo, SOutputRes* result);

SBlockInfo getBlockBasicInfo(void* pBlock, int32_t blockType);
SCacheBlock* getCacheDataBlock(SMeterObj* pMeterObj, SQuery* pQuery, int32_t slot);

void queryOnBlock(SMeterQuerySupportObj* pSupporter, int64_t* primaryKeys, int32_t blockStatus, char* data,
                  SBlockInfo* pBlockBasicInfo, SMeterDataInfo* pDataHeadInfoEx, SField* pFields,
                  __block_search_fn_t searchFn);

SMeterDataInfo** vnodeFilterQualifiedMeters(SQInfo* pQInfo, int32_t vid, SQueryFileInfo* pQueryFileInfo,
                                            tSidSet* pSidSet, SMeterDataInfo* pMeterDataInfo, int32_t* numOfMeters);
int32_t vnodeGetVnodeHeaderFileIdx(int32_t* fid, SQueryRuntimeEnv* pRuntimeEnv, int32_t order);

int32_t createDataBlocksInfoEx(SMeterDataInfo** pMeterDataInfo, int32_t numOfMeters,
                               SMeterDataBlockInfoEx** pDataBlockInfoEx, int32_t numOfCompBlocks,
                               int32_t* nAllocBlocksInfoSize, int64_t addr);
void freeMeterBlockInfoEx(SMeterDataBlockInfoEx* pDataBlockInfoEx, int32_t len);

void setExecutionContext(SMeterQuerySupportObj* pSupporter, SOutputRes* outputRes, int32_t meterIdx, int32_t groupIdx);
void setIntervalQueryExecutionContext(SMeterQuerySupportObj* pSupporter, int32_t meterIdx, SMeterQueryInfo* sqinfo);

int64_t getQueryStartPositionInCache(SQueryRuntimeEnv* pRuntimeEnv, int32_t* slot, int32_t* pos, bool ignoreQueryRange);
int64_t getNextAccessedKeyInData(SQuery* pQuery, int64_t* pPrimaryCol, SBlockInfo* pBlockInfo, int32_t blockStatus);

void setIntervalQueryRange(SMeterQuerySupportObj* pSupporter, int64_t key, SMeterDataInfo* pInfoEx);
void saveIntervalQueryRange(SQuery* pQuery, SMeterQueryInfo* pInfo);
void restoreIntervalQueryRange(SQuery* pQuery, SMeterQueryInfo* pInfo);

uint32_t getDataBlocksForMeters(SMeterQuerySupportObj* pSupporter, SQuery* pQuery, char* pHeaderData,
                                int32_t numOfMeters, SQueryFileInfo* pQueryFileInfo, SMeterDataInfo** pMeterDataInfo);
int32_t LoadDatablockOnDemand(SCompBlock* pBlock, SField** pFields, int8_t* blkStatus, SQueryRuntimeEnv* pRuntimeEnv,
                              int32_t fileIdx, int32_t slotIdx, __block_search_fn_t searchFn, bool onDemand);

void setMeterQueryInfo(SMeterQuerySupportObj* pSupporter, SMeterDataInfo* pMeterDataInfo);
void setMeterDataInfo(SMeterDataInfo* pMeterDataInfo, SMeterObj* pMeterObj, int32_t meterIdx, int32_t groupId);

void vnodeSetTagValueInParam(tSidSet* pSidSet, SQueryRuntimeEnv* pRuntimeEnv, SMeterSidExtInfo* pMeterInfo);

void vnodeCheckIfDataExists(SQueryRuntimeEnv* pRuntimeEnv, SMeterObj* pMeterObj, bool* dataInDisk, bool* dataInCache);

void displayInterResult(SData** pdata, SQuery* pQuery, int32_t numOfRows);

void vnodePrintQueryStatistics(SMeterQuerySupportObj* pSupporter);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEQUERYUTIL_H
