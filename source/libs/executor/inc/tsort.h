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

#ifndef TDENGINE_TSORT_H
#define TDENGINE_TSORT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tcommon.h"

enum {
  SORT_MULTISOURCE_MERGE = 0x1,
  SORT_SINGLESOURCE_SORT = 0x2,
  SORT_BLOCK_TS_MERGE = 0x3
};

typedef struct SMultiMergeSource {
  int32_t      type;
  int32_t      rowIndex;
  SSDataBlock* pBlock;
} SMultiMergeSource;

typedef struct SSortSource {
  SMultiMergeSource src;
  struct {
    SArray* pageIdList;
    int32_t pageIndex;
  };
  struct {
    void* param;
    bool  onlyRef;
  };
  int64_t fetchUs;
  int64_t fetchNum;
} SSortSource;

typedef struct SMsortComparParam {
  void**  pSources;
  int32_t numOfSources;
  SArray* orderInfo;  // SArray<SBlockOrderInfo>
  bool    cmpGroupId;

  int32_t sortType;
  // the following fields to speed up sorting when sortType == SORT_BLOCK_TS_MERGE
  int32_t tsSlotId;
  int32_t tsOrder;
  __compar_fn_t cmpTsFn;
  void* pPkOrder; // SBlockOrderInfo*
} SMsortComparParam;

typedef struct SSortHandle  SSortHandle;
typedef struct STupleHandle STupleHandle;

typedef int32_t (*_sort_fetch_block_fn_t)(void* param, SSDataBlock** ppBlock);
typedef int32_t (*_sort_merge_compar_fn_t)(const void* p1, const void* p2, void* param);

/**
 *
 * @param type
 * @param maxRows keep maxRows at most, if 0, pq sort will not be used
 * @param maxTupleLength max len of one tuple, for check if pq sort is applicable
 * @param sortBufSize sort memory buf size, for check if heap sort is applicable
 * @return
 */
int32_t tsortCreateSortHandle(SArray* pOrderInfo, int32_t type, int32_t pageSize, int32_t numOfPages,
                              SSDataBlock* pBlock, const char* idstr, uint64_t pqMaxRows, uint32_t pqMaxTupleLength,
                              uint32_t pqSortBufSize, SSortHandle** pHandle);

void tsortSetForceUsePQSort(SSortHandle* pHandle);

/**
 *
 * @param pSortHandle
 */
void tsortDestroySortHandle(SSortHandle* pSortHandle);

/**
 *
 * @param pHandle
 * @return
 */
int32_t tsortOpen(SSortHandle* pHandle);

/**
 *
 * @param pHandle
 * @return
 */
void tsortClose(SSortHandle* pHandle);

/**
 *
 * @return
 */
void tsortSetFetchRawDataFp(SSortHandle* pHandle, _sort_fetch_block_fn_t fetchFp, void (*fp)(SSDataBlock*, void*),
                            void* param);

/**
 *
 * @param pHandle
 * @param fp
 * @return
 */
void tsortSetComparFp(SSortHandle* pHandle, _sort_merge_compar_fn_t fp);

/**
 * 
*/
void tsortSetMergeLimit(SSortHandle* pHandle, int64_t mergeLimit);

/**
 *
 */
void tsortSetCompareGroupId(SSortHandle* pHandle, bool compareGroupId);

/**
 *
 * @param pHandle
 * @param pSource
 * @return success or failed
 */
int32_t tsortAddSource(SSortHandle* pSortHandle, void* pSource);

/**
 *
 * @param pHandle
 * @return
 */
int32_t tsortNextTuple(SSortHandle* pHandle, STupleHandle** pTupleHandle);

/**
 *
 * @param pHandle
 * @param colId
 * @return
 */
bool tsortIsNullVal(STupleHandle* pVHandle, int32_t colId);

/**
 *
 * @param pHandle
 * @param colId
 * @return
 */
void tsortGetValue(STupleHandle* pVHandle, int32_t colId, void** pVal);

/**
 *
 * @param pVHandle
 * @return
 */
uint64_t tsortGetGroupId(STupleHandle* pVHandle);
void     tsortGetBlockInfo(STupleHandle* pVHandle, SDataBlockInfo* pInfo);

/**
 * return the SColumnInfoData of columns in the tuple
 * @param pVHandle
 * @param colId
 * @return
 */
void tsortGetColumnInfo(STupleHandle* pVHandle, int32_t colIndex, SColumnInfoData** pColInfo);

/**
 * return the number of columns in the tuple
 * @param pVHandle
 * @return
 */
size_t tsortGetColNum(STupleHandle* pVHandle);

/**
 *
 * @param pSortHandle
 * @return
 */
int32_t tsortGetSortedDataBlock(const SSortHandle* pSortHandle, SSDataBlock** pBlock);

/**
 * return the sort execution information.
 *
 * @param pHandle
 * @return
 */
SSortExecInfo tsortGetSortExecInfo(SSortHandle* pHandle);

/**
 * get proper sort buffer pages according to the row size
 * @param rowSize
 * @param numOfCols columns count that be put into page
 * @return
 */
int32_t getProperSortPageSize(size_t rowSize, uint32_t numOfCols);


bool tsortIsClosed(SSortHandle* pHandle);
void tsortSetClosed(SSortHandle* pHandle);

void tsortSetSingleTableMerge(SSortHandle* pHandle);
void tsortSetAbortCheckFn(SSortHandle* pHandle, bool (*checkFn)(void* param), void* param);

int32_t tsortSetSortByRowId(SSortHandle* pHandle, int32_t extRowsSize);

int32_t tsortAppendTupleToBlock(SSortHandle* pHandle, SSDataBlock* pBlock, STupleHandle* pTupleHandle);

/**
 * @brief comp the tuple with keyBuf, if not equal, new keys will be built in keyBuf, newLen will be stored in keyLen
 * @param [in] pSortCols cols to comp and build
 * @param [in, out] pass in the old keys, if comp not equal, new keys will be built in it.
 * @param [in, out] keyLen the old keysLen, if comp not equal, new keysLen will be stored in it.
 * @param [in] the tuple to comp with
 * @retval 0 if comp equal, 1 if not
 */
int32_t tsortCompAndBuildKeys(const SArray* pSortCols, char* keyBuf, int32_t* keyLen, const STupleHandle* pTuple);

/**
 * @brief set the merge limit reached callback. it calls mergeLimitReached param with tableUid and param
*/
void tsortSetMergeLimitReachedFp(SSortHandle* pHandle, void (*mergeLimitReached)(uint64_t tableUid, void* param),
                                 void*        param);

int32_t tsortComparBlockCell(SSDataBlock* pLeftBlock, SSDataBlock* pRightBlock, int32_t leftRowIndex,
                             int32_t rightRowIndex, void* pOrder);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSORT_H
