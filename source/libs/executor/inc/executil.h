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

#include "tcommon.h"
#include "tbuffer.h"
#include "tpagedbuf.h"

#define SET_RES_WINDOW_KEY(_k, _ori, _len, _uid)     \
  do {                                               \
    assert(sizeof(_uid) == sizeof(uint64_t));        \
    *(uint64_t *)(_k) = (_uid);                      \
    memcpy((_k) + sizeof(uint64_t), (_ori), (_len)); \
  } while (0)

#define SET_RES_EXT_WINDOW_KEY(_k, _ori, _len, _uid, _buf)             \
  do {                                                                 \
    assert(sizeof(_uid) == sizeof(uint64_t));                          \
    *(void **)(_k) = (_buf);                                             \
    *(uint64_t *)((_k) + POINTER_BYTES) = (_uid);                      \
    memcpy((_k) + POINTER_BYTES + sizeof(uint64_t), (_ori), (_len));   \
  } while (0)


#define GET_RES_WINDOW_KEY_LEN(_l) ((_l) + sizeof(uint64_t))
#define GET_RES_EXT_WINDOW_KEY_LEN(_l) ((_l) + sizeof(uint64_t) + POINTER_BYTES)

#define GET_TASKID(_t)  (((SExecTaskInfo*)(_t))->id.str)

#define curTimeWindowIndex(_winres)        ((_winres)->curIndex)

struct SColumnFilterElem;

typedef bool (*__filter_func_t)(struct SColumnFilterElem* pFilter, const char* val1, const char* val2, int16_t type);

typedef struct SGroupResInfo {
  int32_t totalGroup;
  int32_t currentGroup;
  int32_t index;
  SArray* pRows;      // SArray<SResultRowPosition*>
  bool    ordered;
  int32_t position;
} SGroupResInfo;

typedef struct SResultRow {
  int32_t       pageId;      // pageId & rowId is the position of current result in disk-based output buffer
  int32_t       offset:29;   // row index in buffer page
  bool          startInterp; // the time window start timestamp has done the interpolation already.
  bool          endInterp;   // the time window end timestamp has done the interpolation already.
  bool          closed;      // this result status: closed or opened
  uint32_t      numOfRows;   // number of rows of current time window
  struct SResultRowEntryInfo* pEntryInfo;  // For each result column, there is a resultInfo
  STimeWindow   win;
  char         *key;               // start key of current result row
} SResultRow;

typedef struct SResultRowPosition {
  int32_t pageId;
  int32_t offset;
} SResultRowPosition;

typedef struct SResultRowInfo {
  SResultRowPosition *pPosition;
  int32_t      size;       // number of result set
  int32_t      capacity;   // max capacity
  int32_t      curPos;     // current active result row index of pResult list
} SResultRowInfo;

struct STaskAttr;
struct STaskRuntimeEnv;
struct SUdfInfo;
struct SqlFunctionCtx;

int32_t getOutputInterResultBufSize(struct STaskAttr* pQueryAttr);

size_t getResultRowSize(struct SqlFunctionCtx* pCtx, int32_t numOfOutput);
int32_t initResultRowInfo(SResultRowInfo* pResultRowInfo, int32_t size);
void    cleanupResultRowInfo(SResultRowInfo* pResultRowInfo);

void    resetResultRowInfo(struct STaskRuntimeEnv* pRuntimeEnv, SResultRowInfo* pResultRowInfo);
int32_t numOfClosedResultRows(SResultRowInfo* pResultRowInfo);
void    closeAllResultRows(SResultRowInfo* pResultRowInfo);

void    initResultRow(SResultRow *pResultRow);
void    closeResultRow(SResultRow* pResultRow);
bool    isResultRowClosed(SResultRow* pResultRow);

struct SResultRowEntryInfo* getResultCell(const SResultRow* pRow, int32_t index, int32_t* offset);

int32_t getRowNumForMultioutput(struct STaskAttr* pQueryAttr, bool topBottomQuery, bool stable);

static FORCE_INLINE SResultRow *getResultRow(SDiskbasedBuf* pBuf, SResultRowInfo *pResultRowInfo, int32_t slot) {
  ASSERT(pResultRowInfo != NULL && slot >= 0 && slot < pResultRowInfo->size);
  SResultRowPosition* pos = &pResultRowInfo->pPosition[slot];

  SFilePage*  bufPage = (SFilePage*) getBufPage(pBuf, pos->pageId);
  SResultRow* pRow = (SResultRow*)((char*)bufPage + pos->offset);
  return pRow;
}

static FORCE_INLINE SResultRow *getResultRowByPos(SDiskbasedBuf* pBuf, SResultRowPosition* pos) {
  SFilePage*  bufPage = (SFilePage*) getBufPage(pBuf, pos->pageId);
  SResultRow* pRow = (SResultRow*)((char*)bufPage + pos->offset);
  return pRow;
}

static FORCE_INLINE char* getPosInResultPage(struct STaskAttr* pQueryAttr, SFilePage* page, int32_t rowOffset,
                                             int32_t offset) {
  assert(rowOffset >= 0 && pQueryAttr != NULL);

  // int32_t numOfRows = (int32_t)getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery);
  return ((char *)page->data);  

}

static FORCE_INLINE char* getPosInResultPage_rv(SFilePage* page, int32_t rowOffset, int32_t offset) {
  assert(rowOffset >= 0);

  int32_t numOfRows = 1;//(int32_t)getRowNumForMultioutput(pQueryAttr, pQueryAttr->topBotQuery, pQueryAttr->stableQuery);
  return (char*) page + rowOffset + offset * numOfRows;
}

typedef struct {
  SArray* pResult;     // SArray<SResPair>
  int32_t colId;
} SStddevInterResult;

void    initGroupResInfo(SGroupResInfo* pGroupResInfo, SResultRowInfo* pResultInfo);
void    initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList);

void    cleanupGroupResInfo(SGroupResInfo* pGroupResInfo);
bool    hasRemainDataInCurrentGroup(SGroupResInfo* pGroupResInfo);
bool    hasRemainData(SGroupResInfo* pGroupResInfo);

bool    incNextGroup(SGroupResInfo* pGroupResInfo);
int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo);

int32_t mergeIntoGroupResult(SGroupResInfo* pGroupResInfo, struct STaskRuntimeEnv *pRuntimeEnv, int32_t* offset);

//int32_t initUdfInfo(struct SUdfInfo* pUdfInfo);

#endif  // TDENGINE_QUERYUTIL_H
