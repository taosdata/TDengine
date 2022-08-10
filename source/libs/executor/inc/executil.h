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

#include "vnode.h"
#include "function.h"
#include "nodes.h"
#include "plannodes.h"
#include "tbuffer.h"
#include "tcommon.h"
#include "tpagedbuf.h"

#define SET_RES_WINDOW_KEY(_k, _ori, _len, _uid)     \
  do {                                               \
    assert(sizeof(_uid) == sizeof(uint64_t));        \
    *(uint64_t*)(_k) = (_uid);                       \
    memcpy((_k) + sizeof(uint64_t), (_ori), (_len)); \
  } while (0)

#define SET_RES_EXT_WINDOW_KEY(_k, _ori, _len, _uid, _buf)           \
  do {                                                               \
    assert(sizeof(_uid) == sizeof(uint64_t));                        \
    *(void**)(_k) = (_buf);                                          \
    *(uint64_t*)((_k) + POINTER_BYTES) = (_uid);                     \
    memcpy((_k) + POINTER_BYTES + sizeof(uint64_t), (_ori), (_len)); \
  } while (0)

#define GET_RES_WINDOW_KEY_LEN(_l)     ((_l) + sizeof(uint64_t))
#define GET_RES_EXT_WINDOW_KEY_LEN(_l) ((_l) + sizeof(uint64_t) + POINTER_BYTES)

#define GET_TASKID(_t) (((SExecTaskInfo*)(_t))->id.str)

typedef struct SGroupResInfo {
  int32_t index;
  SArray* pRows;  // SArray<SResKeyPos>
} SGroupResInfo;

typedef struct SResultRow {
  int32_t                    pageId;  // pageId & rowId is the position of current result in disk-based output buffer
  int32_t                    offset : 29;  // row index in buffer page
  bool                       startInterp;  // the time window start timestamp has done the interpolation already.
  bool                       endInterp;    // the time window end timestamp has done the interpolation already.
  bool                       closed;       // this result status: closed or opened
  uint32_t                   numOfRows;    // number of rows of current time window
  STimeWindow                win;
  struct SResultRowEntryInfo pEntryInfo[];  // For each result column, there is a resultInfo
} SResultRow;

typedef struct SResultRowPosition {
  int32_t pageId;
  int32_t offset;
} SResultRowPosition;

typedef struct SResKeyPos {
  SResultRowPosition pos;
  uint64_t           groupId;
  char               key[];
} SResKeyPos;

typedef struct SResultRowInfo {
  int32_t            size;  // number of result set
  SResultRowPosition cur;
  SList*             openWindow;
} SResultRowInfo;

struct SqlFunctionCtx;

size_t getResultRowSize(struct SqlFunctionCtx* pCtx, int32_t numOfOutput);
void   initResultRowInfo(SResultRowInfo* pResultRowInfo);
void   cleanupResultRowInfo(SResultRowInfo* pResultRowInfo);

void initResultRow(SResultRow* pResultRow);
void closeResultRow(SResultRow* pResultRow);
bool isResultRowClosed(SResultRow* pResultRow);

struct SResultRowEntryInfo* getResultEntryInfo(const SResultRow* pRow, int32_t index, const int32_t* offset);

static FORCE_INLINE SResultRow* getResultRowByPos(SDiskbasedBuf* pBuf, SResultRowPosition* pos, bool forUpdate) {
  SFilePage*  bufPage = (SFilePage*)getBufPage(pBuf, pos->pageId);
  if (forUpdate) {
    setBufPageDirty(bufPage, true);
  }
  SResultRow* pRow = (SResultRow*)((char*)bufPage + pos->offset);
  return pRow;
}

static FORCE_INLINE void setResultBufPageDirty(SDiskbasedBuf* pBuf, SResultRowPosition* pos) {
  void* pPage = getBufPage(pBuf, pos->pageId);
  setBufPageDirty(pPage, true);
}

void initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SHashObj* pHashmap, int32_t order);
void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo);

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList);
bool hasRemainResults(SGroupResInfo* pGroupResInfo);

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo);

SSDataBlock* createResDataBlock(SDataBlockDescNode* pNode);

EDealRes doTranslateTagExpr(SNode** pNode, void* pContext);
int32_t getTableList(void* metaHandle, void* pVnode, SScanPhysiNode* pScanNode, SNode* pTagCond, SNode* pTagIndexCond, STableListInfo* pListInfo);
int32_t getGroupIdFromTagsVal(void* pMeta, uint64_t uid, SNodeList* pGroupNode, char* keyBuf, uint64_t* pGroupId);
size_t  getTableTagsBufLen(const SNodeList* pGroups);

SArray*  createSortInfo(SNodeList* pNodeList);
SArray*  extractPartitionColInfo(SNodeList* pNodeList);
SArray*  extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                             int32_t type);

SExprInfo* createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, int32_t* numOfExprs);

SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowEntryInfoOffset);
void relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols, bool outputEveryColumn);
void initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow);

SInterval extractIntervalInfo(const STableScanPhysiNode* pTableScanNode);
SColumn   extractColumnFromColumnNode(SColumnNode* pColNode);

int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode);
void    cleanupQueryTableDataCond(SQueryTableDataCond* pCond);

int32_t convertFillType(int32_t mode);

int32_t resultrowComparAsc(const void* p1, const void* p2);

int32_t isQualifiedTable(STableKeyInfo* info, SNode* pTagCond, void* metaHandle, bool* pQualified);

#endif  // TDENGINE_QUERYUTIL_H
