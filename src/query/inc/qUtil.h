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
#ifndef TDENGINE_QUERYUTIL_H
#define TDENGINE_QUERYUTIL_H

#include "tbuffer.h"

#define SET_RES_WINDOW_KEY(_k, _ori, _len, _uid)     \
  do {                                               \
    assert(sizeof(_uid) == sizeof(uint64_t));        \
    *(uint64_t *)(_k) = (_uid);                      \
    memcpy((_k) + sizeof(uint64_t), (_ori), (_len)); \
  } while (0)

#define GET_RES_WINDOW_KEY_LEN(_l) ((_l) + sizeof(uint64_t))

#define curTimeWindowIndex(_winres)        ((_winres)->curIndex)
#define GET_ROW_PARAM_FOR_MULTIOUTPUT(_q, tbq, sq) (((tbq) && (!sq))? (_q)->pExpr1[1].base.arg->argValue.i64:1)

int32_t getOutputInterResultBufSize(SQuery* pQuery);

size_t  getResultRowSize(SQueryRuntimeEnv* pRuntimeEnv);
int32_t initResultRowInfo(SResultRowInfo* pResultRowInfo, int32_t size, int16_t type);
void    cleanupResultRowInfo(SResultRowInfo* pResultRowInfo);

void    resetResultRowInfo(SQueryRuntimeEnv* pRuntimeEnv, SResultRowInfo* pResultRowInfo);
int32_t numOfClosedResultRows(SResultRowInfo* pResultRowInfo);
void    closeAllResultRows(SResultRowInfo* pResultRowInfo);

int32_t initResultRow(SResultRow *pResultRow);
void    closeResultRow(SResultRowInfo* pResultRowInfo, int32_t slot);
bool    isResultRowClosed(SResultRowInfo *pResultRowInfo, int32_t slot);
void    clearResultRow(SQueryRuntimeEnv* pRuntimeEnv, SResultRow* pResultRow, int16_t type);

SResultRowCellInfo* getResultCell(const SResultRow* pRow, int32_t index, int32_t* offset);

static FORCE_INLINE SResultRow *getResultRow(SResultRowInfo *pResultRowInfo, int32_t slot) {
  assert(pResultRowInfo != NULL && slot >= 0 && slot < pResultRowInfo->size);
  return pResultRowInfo->pResult[slot];
}

static FORCE_INLINE char *getPosInResultPage(SQuery *pQuery, tFilePage* page, int32_t rowOffset, int16_t offset) {
  assert(rowOffset >= 0 && pQuery != NULL);

  int32_t numOfRows = GET_ROW_PARAM_FOR_MULTIOUTPUT(pQuery, pQuery->topBotQuery, pQuery->stableQuery);
  return ((char *)page->data) + rowOffset + offset * numOfRows;
}

bool isNullOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type);
bool notNullOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type);

__filter_func_t getFilterOperator(int32_t lowerOptr, int32_t upperOptr);

SResultRowPool* initResultRowPool(size_t size);
SResultRow* getNewResultRow(SResultRowPool* p);
int64_t getResultRowPoolMemSize(SResultRowPool* p);
void* destroyResultRowPool(SResultRowPool* p);
int32_t getNumOfAllocatedResultRows(SResultRowPool* p);
int32_t getNumOfUsedResultRows(SResultRowPool* p);

typedef struct {
  SArray* pResult;     // SArray<SResPair>
  int32_t colId;
} SStddevInterResult;

void interResToBinary(SBufferWriter* bw, SArray* pRes, int32_t tagLen);
SArray* interResFromBinary(const char* data, int32_t len);
void freeInterResult(void* param);

void    initGroupResInfo(SGroupResInfo* pGroupResInfo, SResultRowInfo* pResultInfo);
void    cleanupGroupResInfo(SGroupResInfo* pGroupResInfo);
bool    hasRemainDataInCurrentGroup(SGroupResInfo* pGroupResInfo);
bool    hasRemainData(SGroupResInfo* pGroupResInfo);

bool    incNextGroup(SGroupResInfo* pGroupResInfo);
int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo);

int32_t mergeIntoGroupResult(SGroupResInfo* pGroupResInfo, SQueryRuntimeEnv *pRuntimeEnv, int32_t* offset);

#endif  // TDENGINE_QUERYUTIL_H
