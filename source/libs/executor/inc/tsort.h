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

#include "tcommon.h"
#include "os.h"

enum {
  SORT_MULTISOURCE_MERGE = 0x1,
  SORT_SINGLESOURCE_SORT = 0x2,
};

typedef struct SMultiMergeSource {
  int32_t      type;
  int32_t      rowIndex;
  SSDataBlock *pBlock;
} SMultiMergeSource;

typedef struct SSortSource {
  SMultiMergeSource src;
  union{
    struct{
      SArray*           pageIdList;
      int32_t           pageIndex;
    };
    void             *param;
  };

} SSortSource;

typedef struct SMsortComparParam {
  void        **pSources;
  int32_t       numOfSources;
  SArray       *orderInfo;   // SArray<SBlockOrderInfo>
} SMsortComparParam;

typedef struct SSortHandle SSortHandle;
typedef struct STupleHandle STupleHandle;

typedef SSDataBlock* (*_sort_fetch_block_fn_t)(void* param);
typedef int32_t (*_sort_merge_compar_fn_t)(const void* p1, const void* p2, void* param);

/**
 *
 * @param type
 * @return
 */
SSortHandle* tsortCreateSortHandle(SArray* pOrderInfo, SArray* pIndexMap, int32_t type, int32_t pageSize, int32_t numOfPages, SSDataBlock* pBlock, const char* idstr);

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
int32_t tsortClose(SSortHandle* pHandle);

/**
 *
 * @return
 */
int32_t tsortSetFetchRawDataFp(SSortHandle* pHandle, _sort_fetch_block_fn_t fp);

/**
 *
 * @param pHandle
 * @param fp
 * @return
 */
int32_t tsortSetComparFp(SSortHandle* pHandle, _sort_merge_compar_fn_t fp);

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
STupleHandle* tsortNextTuple(SSortHandle* pHandle);

/**
 *
 * @param pHandle
 * @param colIndex
 * @return
 */
bool tsortIsNullVal(STupleHandle* pVHandle, int32_t colIndex);

/**
 *
 * @param pHandle
 * @param colIndex
 * @return
 */
void* tsortGetValue(STupleHandle* pVHandle, int32_t colIndex);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSORT_H
