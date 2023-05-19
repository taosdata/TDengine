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

#include "executor.h"
#include "function.h"
#include "nodes.h"
#include "plannodes.h"
#include "tcommon.h"
#include "tpagedbuf.h"
#include "tsimplehash.h"
#include "vnode.h"

#define T_LONG_JMP(_obj, _c) \
  do {                       \
    ASSERT((_c) != -1);      \
    longjmp((_obj), (_c));   \
  } while (0)

#define SET_RES_WINDOW_KEY(_k, _ori, _len, _uid)     \
  do {                                               \
    assert(sizeof(_uid) == sizeof(uint64_t));        \
    *(uint64_t*)(_k) = (_uid);                       \
    memcpy((_k) + sizeof(uint64_t), (_ori), (_len)); \
  } while (0)

#define GET_RES_WINDOW_KEY_LEN(_l) ((_l) + sizeof(uint64_t))

typedef struct SGroupResInfo {
  int32_t index;
  SArray* pRows;  // SArray<SResKeyPos>
  char*   pBuf;
  bool    freeItem;
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

typedef struct SColMatchItem {
  int32_t colId;
  int32_t srcSlotId;
  int32_t dstSlotId;
  bool    needOutput;
} SColMatchItem;

typedef struct SColMatchInfo {
  SArray* pList;      // SArray<SColMatchItem>
  int32_t matchType;  // determinate the source according to col id or slot id
} SColMatchInfo;

typedef struct SExecTaskInfo SExecTaskInfo;

typedef struct STableListIdInfo {
  uint64_t suid;
  uint64_t uid;
  int32_t  tableType;
} STableListIdInfo;

// If the numOfOutputGroups is 1, the data blocks that belongs to different groups will be provided randomly
// The numOfOutputGroups is specified by physical plan. and will not be affect by numOfGroups
typedef struct STableListInfo {
  bool             oneTableForEachGroup;
  int32_t          numOfOuputGroups;  // the data block will be generated one by one
  int32_t*         groupOffset;       // keep the offset value for each group in the tableList
  SArray*          pTableList;
  SHashObj*        map;     // speedup acquire the tableQueryInfo by table uid
  STableListIdInfo idInfo;  // this maybe the super table or ordinary table
} STableListInfo;

struct SqlFunctionCtx;

int32_t createScanTableListInfo(SScanPhysiNode* pScanNode, SNodeList* pGroupTags, bool groupSort, SReadHandle* pHandle,
                                STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond,
                                SExecTaskInfo* pTaskInfo);

STableListInfo* tableListCreate();
void*           tableListDestroy(STableListInfo* pTableListInfo);
void            tableListClear(STableListInfo* pTableListInfo);
int32_t         tableListGetOutputGroups(const STableListInfo* pTableList);
bool            oneTableForEachGroup(const STableListInfo* pTableList);
uint64_t        getTableGroupId(const STableListInfo* pTableList, uint64_t tableUid);
int32_t         tableListAddTableInfo(STableListInfo* pTableList, uint64_t uid, uint64_t gid);
int32_t         tableListGetGroupList(const STableListInfo* pTableList, int32_t ordinalIndex, STableKeyInfo** pKeyInfo,
                                      int32_t* num);
uint64_t        tableListGetSize(const STableListInfo* pTableList);
uint64_t        tableListGetSuid(const STableListInfo* pTableList);
STableKeyInfo*  tableListGetInfo(const STableListInfo* pTableList, int32_t index);
int32_t         tableListFind(const STableListInfo* pTableList, uint64_t uid, int32_t startIndex);
void            tableListGetSourceTableInfo(const STableListInfo* pTableList, uint64_t* psuid, uint64_t* uid, int32_t* type);

size_t getResultRowSize(struct SqlFunctionCtx* pCtx, int32_t numOfOutput);
void   initResultRowInfo(SResultRowInfo* pResultRowInfo);
void   closeResultRow(SResultRow* pResultRow);
void   resetResultRow(SResultRow* pResultRow, size_t entrySize);

struct SResultRowEntryInfo* getResultEntryInfo(const SResultRow* pRow, int32_t index, const int32_t* offset);

static FORCE_INLINE SResultRow* getResultRowByPos(SDiskbasedBuf* pBuf, SResultRowPosition* pos, bool forUpdate) {
  SFilePage* bufPage = (SFilePage*)getBufPage(pBuf, pos->pageId);
  if (forUpdate) {
    setBufPageDirty(bufPage, true);
  }

  SResultRow* pRow = (SResultRow*)((char*)bufPage + pos->offset);
  return pRow;
}

void initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap, int32_t order);
void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo);

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList);
bool hasRemainResults(SGroupResInfo* pGroupResInfo);

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo);

SSDataBlock* createDataBlockFromDescNode(SDataBlockDescNode* pNode);

EDealRes doTranslateTagExpr(SNode** pNode, void* pContext);
int32_t  getGroupIdFromTagsVal(void* pMeta, uint64_t uid, SNodeList* pGroupNode, char* keyBuf, uint64_t* pGroupId);
size_t   getTableTagsBufLen(const SNodeList* pGroups);

SArray* createSortInfo(SNodeList* pNodeList);
SArray* extractPartitionColInfo(SNodeList* pNodeList);
int32_t extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            int32_t type, SColMatchInfo* pMatchInfo);

void       createExprFromOneNode(SExprInfo* pExp, SNode* pNode, int16_t slotId);
void       createExprFromTargetNode(SExprInfo* pExp, STargetNode* pTargetNode);
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

void printDataBlock(SSDataBlock* pBlock, const char* flag);

#endif  // TDENGINE_QUERYUTIL_H
