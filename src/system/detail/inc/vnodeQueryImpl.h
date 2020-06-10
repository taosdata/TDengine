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

#ifndef TDENGINE_VNODEQUERYIMPL_H
#define TDENGINE_VNODEQUERYIMPL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "hash.h"
#include "hashutil.h"

#define GET_QINFO_ADDR(x) ((char*)(x)-offsetof(SQInfo, query))
#define Q_STATUS_EQUAL(p, s) (((p) & (s)) != 0)

/*
 * set the output buffer page size is 16k
 * The page size should be sufficient for at least one output result or intermediate result.
 * Some intermediate results may be extremely large, such as top/bottom(100) query.
 */
#define DEFAULT_INTERN_BUF_SIZE 16384L

#define INIT_ALLOCATE_DISK_PAGES 60L
#define DEFAULT_DATA_FILE_MAPPING_PAGES 2L
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
  QUERY_NOT_COMPLETED = 0x1u,

  /*
   * output buffer is full, so, the next query will be employed,
   * in this case, we need to set the appropriated start scan point for
   * the next query.
   *
   * this status is only exist in group-by clause and
   * diff/add/division/multiply/ query.
   */
  QUERY_RESBUF_FULL = 0x2u,

  /*
   * query is over
   * 1. this status is used in one row result query process, e.g.,
   * count/sum/first/last/
   * avg...etc.
   * 2. when the query range on timestamp is satisfied, it is also denoted as
   * query_compeleted
   */
  QUERY_COMPLETED = 0x4u,

  /*
   * all data has been scanned, so current search is stopped,
   * At last, the function will transfer this status to QUERY_COMPLETED
   */
  QUERY_NO_DATA_TO_CHECK = 0x8u,
} vnodeQueryStatus;

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

#define IS_MASTER_SCAN(runtime) (((runtime)->scanFlag & 1u) == MASTER_SCAN)
#define IS_SUPPLEMENT_SCAN(runtime) ((runtime)->scanFlag == SUPPLEMENTARY_SCAN)
#define SET_SUPPLEMENT_SCAN_FLAG(runtime) ((runtime)->scanFlag = SUPPLEMENTARY_SCAN)
#define SET_MASTER_SCAN_FLAG(runtime) ((runtime)->scanFlag = MASTER_SCAN)

typedef int (*__block_search_fn_t)(char* data, int num, int64_t key, int order);

static FORCE_INLINE SMeterObj* getMeterObj(void* hashHandle, int32_t sid) {
  return *(SMeterObj**)taosGetDataFromHashTable(hashHandle, (const char*)&sid, sizeof(sid));
}

bool isQueryKilled(SQuery* pQuery);
bool isFixedOutputQuery(SQuery* pQuery);
bool isPointInterpoQuery(SQuery* pQuery);
bool isSumAvgRateQuery(SQuery *pQuery);
bool isTopBottomQuery(SQuery* pQuery);
bool isFirstLastRowQuery(SQuery* pQuery);
bool isTSCompQuery(SQuery* pQuery);
bool notHasQueryTimeRange(SQuery* pQuery);

bool needSupplementaryScan(SQuery* pQuery);
bool onDemandLoadDatablock(SQuery* pQuery, int16_t queryRangeSet);

void setQueryStatus(SQuery* pQuery, int8_t status);

bool doRevisedResultsByLimit(SQInfo* pQInfo);
void truncateResultByLimit(SQInfo* pQInfo, int64_t* final, int32_t* interpo);

void initCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv);
void resetCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv);
void forwardCtxOutputBuf(SQueryRuntimeEnv* pRuntimeEnv, int64_t output);

bool needPrimaryTimestampCol(SQuery* pQuery, SBlockInfo* pBlockInfo);
void vnodeScanAllData(SQueryRuntimeEnv* pRuntimeEnv);

int32_t vnodeQueryResultInterpolate(SQInfo* pQInfo, tFilePage** pDst, tFilePage** pDataSrc, int32_t numOfRows,
                                    int32_t* numOfInterpo);
void    copyResToQueryResultBuf(STableQuerySupportObj* pSupporter, SQuery* pQuery);

void    doSkipResults(SQueryRuntimeEnv* pRuntimeEnv);
void    doFinalizeResult(SQueryRuntimeEnv* pRuntimeEnv);
int64_t getNumOfResult(SQueryRuntimeEnv* pRuntimeEnv);

void forwardQueryStartPosition(SQueryRuntimeEnv* pRuntimeEnv);

bool normalizedFirstQueryRange(bool dataInDisk, bool dataInCache, STableQuerySupportObj* pSupporter,
                               SPointInterpoSupporter* pPointInterpSupporter, int64_t* key);

void pointInterpSupporterInit(SQuery* pQuery, SPointInterpoSupporter* pInterpoSupport);
void pointInterpSupporterDestroy(SPointInterpoSupporter* pPointInterpSupport);
void pointInterpSupporterSetData(SQInfo* pQInfo, SPointInterpoSupporter* pPointInterpSupport);

int64_t loadRequiredBlockIntoMem(SQueryRuntimeEnv* pRuntimeEnv, SPositionInfo* position);
void    disableFunctForSuppleScan(STableQuerySupportObj* pSupporter, int32_t order);
void    enableFunctForMasterScan(SQueryRuntimeEnv* pRuntimeEnv, int32_t order);

int32_t mergeMetersResultToOneGroups(STableQuerySupportObj* pSupporter);
void    copyFromWindowResToSData(SQInfo* pQInfo, SWindowResult* result);

SBlockInfo getBlockInfo(SQueryRuntimeEnv *pRuntimeEnv);
SBlockInfo getBlockBasicInfo(SQueryRuntimeEnv *pRuntimeEnv, void* pBlock, int32_t type);

SCacheBlock* getCacheDataBlock(SMeterObj* pMeterObj, SQueryRuntimeEnv* pRuntimeEnv, int32_t slot);

void stableApplyFunctionsOnBlock(STableQuerySupportObj* pSupporter, SMeterDataInfo* pMeterDataInfo,
                               SBlockInfo* pBlockInfo, SField* pFields, __block_search_fn_t searchFn);

int32_t vnodeFilterQualifiedMeters(SQInfo* pQInfo, int32_t vid, tSidSet* pSidSet, SMeterDataInfo* pMeterDataInfo,
                                   int32_t* numOfMeters, SMeterDataInfo*** pReqMeterDataInfo);
int32_t vnodeGetVnodeHeaderFileIndex(int32_t* fid, SQueryRuntimeEnv* pRuntimeEnv, int32_t order);

int32_t createDataBlocksInfoEx(SMeterDataInfo** pMeterDataInfo, int32_t numOfMeters,
                               SMeterDataBlockInfoEx** pDataBlockInfoEx, int32_t numOfCompBlocks,
                               int32_t* nAllocBlocksInfoSize, int64_t addr);
void    freeMeterBlockInfoEx(SMeterDataBlockInfoEx* pDataBlockInfoEx, int32_t len);

void    setExecutionContext(STableQuerySupportObj* pSupporter, SMeterQueryInfo* pMeterQueryInfo, int32_t meterIdx,
                            int32_t groupIdx, TSKEY nextKey);
int32_t setAdditionalInfo(STableQuerySupportObj *pSupporter, int32_t meterIdx, SMeterQueryInfo *pMeterQueryInfo);
void    doGetAlignedIntervalQueryRangeImpl(SQuery* pQuery, int64_t pKey, int64_t keyFirst, int64_t keyLast,
                                           int64_t* actualSkey, int64_t* actualEkey, int64_t* skey, int64_t* ekey);

int64_t getQueryStartPositionInCache(SQueryRuntimeEnv* pRuntimeEnv, int32_t* slot, int32_t* pos, bool ignoreQueryRange);

int32_t getDataBlocksForMeters(STableQuerySupportObj* pSupporter, SQuery* pQuery, int32_t numOfMeters,
                               const char* filePath, SMeterDataInfo** pMeterDataInfo, uint32_t* numOfBlocks);
int32_t LoadDatablockOnDemand(SCompBlock* pBlock, SField** pFields, uint8_t* blkStatus, SQueryRuntimeEnv* pRuntimeEnv,
                              int32_t fileIdx, int32_t slotIdx, __block_search_fn_t searchFn, bool onDemand);
int32_t vnodeGetHeaderFile(SQueryRuntimeEnv* pRuntimeEnv, int32_t fileIndex);

/**
 * Create SMeterQueryInfo.
 * The MeterQueryInfo is created one for each table during super table query
 *
 * @param skey
 * @param ekey
 * @return
 */
SMeterQueryInfo* createMeterQueryInfo(STableQuerySupportObj* pSupporter, int32_t sid, TSKEY skey, TSKEY ekey);

/**
 * Destroy meter query info
 * @param pMeterQInfo
 * @param numOfCols
 */
void destroyMeterQueryInfo(SMeterQueryInfo* pMeterQueryInfo, int32_t numOfCols);

/**
 * change the meter query info for supplement scan
 * @param pMeterQueryInfo
 * @param skey
 * @param ekey
 */
void changeMeterQueryInfoForSuppleQuery(SQuery* pQuery, SMeterQueryInfo* pMeterQueryInfo,
                                        TSKEY skey, TSKEY ekey);

/**
 * add the new allocated disk page to meter query info
 * the new allocated disk page is used to keep the intermediate (interval) results
 * @param pQuery
 * @param pMeterQueryInfo
 * @param pSupporter
 */
tFilePage* addDataPageForMeterQueryInfo(SQuery* pQuery, SMeterQueryInfo* pMeterQueryInfo,
                                        STableQuerySupportObj* pSupporter);

/**
 * restore the query range data from SMeterQueryInfo to runtime environment
 *
 * @param pRuntimeEnv
 * @param pMeterQueryInfo
 */
void restoreIntervalQueryRange(SQueryRuntimeEnv* pRuntimeEnv, SMeterQueryInfo* pMeterQueryInfo);

/**
 * set the interval query range for the interval query, when handling a data(cache) block
 *
 * @param pMeterQueryInfo
 * @param pSupporter
 * @param key
 */
void setIntervalQueryRange(SMeterQueryInfo* pMeterQueryInfo, STableQuerySupportObj* pSupporter, int64_t key);

/**
 * set the meter data information
 * @param pMeterDataInfo
 * @param pMeterObj current query meter object
 * @param meterIdx  meter index in the sid list
 * @param groupId  group index, which the meter is belonged to
 */
void setMeterDataInfo(SMeterDataInfo* pMeterDataInfo, SMeterObj* pMeterObj, int32_t meterIdx, int32_t groupId);

void vnodeSetTagValueInParam(tSidSet* pSidSet, SQueryRuntimeEnv* pRuntimeEnv, SMeterSidExtInfo* pMeterInfo);

void vnodeCheckIfDataExists(SQueryRuntimeEnv* pRuntimeEnv, SMeterObj* pMeterObj, bool* dataInDisk, bool* dataInCache);

void displayInterResult(SData** pdata, SQuery* pQuery, int32_t numOfRows);

void vnodePrintQueryStatistics(STableQuerySupportObj* pSupporter);

void clearTimeWindowResBuf(SQueryRuntimeEnv* pRuntimeEnv, SWindowResult* pOneOutputRes);
void copyTimeWindowResBuf(SQueryRuntimeEnv* pRuntimeEnv, SWindowResult* dst, const SWindowResult* src);

int32_t initWindowResInfo(SWindowResInfo* pWindowResInfo, SQueryRuntimeEnv* pRuntimeEnv, int32_t size,
                          int32_t threshold, int16_t type);

void    cleanupTimeWindowInfo(SWindowResInfo* pWindowResInfo, int32_t numOfCols);
void    resetTimeWindowInfo(SQueryRuntimeEnv* pRuntimeEnv, SWindowResInfo* pWindowResInfo);
void    clearFirstNTimeWindow(SQueryRuntimeEnv *pRuntimeEnv, int32_t num);

void    clearClosedTimeWindow(SQueryRuntimeEnv* pRuntimeEnv);
int32_t numOfClosedTimeWindow(SWindowResInfo* pWindowResInfo);
void    closeTimeWindow(SWindowResInfo* pWindowResInfo, int32_t slot);
void    closeAllTimeWindow(SWindowResInfo* pWindowResInfo);
SWindowResult* getWindowRes(SWindowResInfo* pWindowResInfo, size_t index);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEQUERYIMPL_H
