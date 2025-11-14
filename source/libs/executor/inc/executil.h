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
#ifndef TDENGINE_EXECUTIL_H
#define TDENGINE_EXECUTIL_H

#include "executor.h"
#include "function.h"
#include "nodes.h"
#include "plannodes.h"
#include "query.h"
#include "storageapi.h"
#include "tcommon.h"
#include "tpagedbuf.h"
#include "tsimplehash.h"

#define T_LONG_JMP(_obj, _c)                                                              \
  do {                                                                                    \
    qError("error happens at %s, line:%d, code:%s", __func__, __LINE__, tstrerror((_c))); \
    longjmp((_obj), (_c));                                                                \
  } while (0)

#define SET_RES_WINDOW_KEY(_k, _ori, _len, _uid)           \
  do {                                                     \
    *(uint64_t*)(_k) = (_uid);                             \
    (void)memcpy((_k) + sizeof(uint64_t), (_ori), (_len)); \
  } while (0)

#define GET_RES_WINDOW_KEY_LEN(_l) ((_l) + sizeof(uint64_t))

struct SGroupResInfo {
  int32_t index;    // rows consumed in func:doCopyToSDataBlockXX
  int32_t iter;     // relate to index-1, last consumed data's slot id in hash table
  void*   dataPos;  // relate to index-1, last consumed data's position, in the nodelist of cur slot
  int32_t delIndex; // rows consumed in func:doBuildDeleteDataBlock
  SArray* pRows;    // SArray<SResKeyPos>
  char*   pBuf;
  bool    freeItem;
};

struct SResultRow {
  int32_t                    version;
  int32_t                    pageId;  // pageId & rowId is the position of current result in disk-based output buffer
  int32_t                    offset : 29;  // row index in buffer page
  bool                       startInterp;  // the time window start timestamp has done the interpolation already.
  bool                       endInterp;    // the time window end timestamp has done the interpolation already.
  bool                       closed;       // this result status: closed or opened
  uint32_t                   numOfRows;    // number of rows of current time window
  STimeWindow                win;
  int32_t                    winIdx;
  struct SResultRowEntryInfo pEntryInfo[];  // For each result column, there is a resultInfo
};

typedef struct SResultRowPosition {
  int32_t pageId;
  int32_t offset;
} SResultRowPosition;

typedef struct SResKeyPos {
  SResultRowPosition pos;
  uint64_t           groupId;
  char               key[];
} SResKeyPos;

struct SResultRowInfo {
  int32_t            size;  // number of result set
  SResultRowPosition cur;
  SList*             openWindow;
};

typedef struct SColMatchItem {
  int32_t   colId;
  int32_t   srcSlotId;
  int32_t   dstSlotId;
  bool      needOutput;
  SDataType dataType;
  int32_t   funcType;
  bool      isPk;
} SColMatchItem;

typedef struct SColMatchInfo {
  SArray* pList;      // SArray<SColMatchItem>
  int32_t matchType;  // determinate the source according to col id or slot id
} SColMatchInfo;

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
  SHashObj*        map;           // speedup acquire the tableQueryInfo by table uid
  SHashObj*        remainGroups;  // remaining group has not yet processed the empty group
  STableListIdInfo idInfo;        // this maybe the super table or ordinary table
} STableListInfo;
typedef struct SDBVgInfoReq {
  tsem_t     ready;
  SUseDbRsp* pRsp;
} SDBVgInfoReq;

typedef struct SDBVgInfoMgr {
  SHashObj* dbVgInfoMap;
  SRWLatch  lock;
} SDBVgInfoMgr;

struct SqlFunctionCtx;
typedef struct SInsertTableInfo {
  int64_t                  uid;
  int64_t                  vgid;
  int32_t                  version;
  STSchema*                pSchema;
  char*                    tbname;
} SInsertTableInfo;

int32_t createScanTableListInfo(SScanPhysiNode* pScanNode, SNodeList* pGroupTags, bool groupSort, SReadHandle* pHandle,
                                STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond,
                                SExecTaskInfo* pTaskInfo, SHashObj* groupIdMap);

STableListInfo* tableListCreate();
void            tableListDestroy(STableListInfo* pTableListInfo);
void            tableListClear(STableListInfo* pTableListInfo);
int32_t         tableListGetOutputGroups(const STableListInfo* pTableList);
bool            oneTableForEachGroup(const STableListInfo* pTableList);
uint64_t        tableListGetTableGroupId(const STableListInfo* pTableList, uint64_t tableUid);
int32_t         tableListAddTableInfo(STableListInfo* pTableList, uint64_t uid, uint64_t gid);
int32_t         sortTableGroup(STableListInfo* pTableListInfo);
int32_t         tableListGetGroupList(const STableListInfo* pTableList, int32_t ordinalIndex, STableKeyInfo** pKeyInfo,
                                      int32_t* num);
int32_t         tableListGetSize(const STableListInfo* pTableList, int32_t* pRes);
uint64_t        tableListGetSuid(const STableListInfo* pTableList);
STableKeyInfo*  tableListGetInfo(const STableListInfo* pTableList, int32_t index);
int32_t         tableListFind(const STableListInfo* pTableList, uint64_t uid, int32_t startIndex);
void tableListGetSourceTableInfo(const STableListInfo* pTableList, uint64_t* psuid, uint64_t* uid, int32_t* type);
int32_t doFilterByTagCond(STableListInfo* pListInfo, SArray* pUidList, SNode* pTagCond, void* pVnode,
                                 SIdxFltStatus status, SStorageAPI* pAPI, bool addUid, bool* listAdded, void* pStreamInfo);
int32_t buildGroupIdMapForAllTables(STableListInfo* pTableListInfo, SReadHandle* pHandle, SScanPhysiNode* pScanNode,
  SNodeList* group, bool groupSort, uint8_t* digest, SStorageAPI* pAPI, SHashObj* groupIdMap);
size_t getResultRowSize(struct SqlFunctionCtx* pCtx, int32_t numOfOutput);
void   initResultRowInfo(SResultRowInfo* pResultRowInfo);
void   closeResultRow(SResultRow* pResultRow);
void   resetResultRow(SResultRow* pResultRow, size_t entrySize);

struct SResultRowEntryInfo* getResultEntryInfo(const SResultRow* pRow, int32_t index, const int32_t* offset);

static FORCE_INLINE SResultRow* getResultRowByPos(SDiskbasedBuf* pBuf, SResultRowPosition* pos, bool forUpdate) {
  SFilePage* bufPage = (SFilePage*)getBufPage(pBuf, pos->pageId);
  if (!bufPage) {
    uFatal("failed to get the buffer page:%d since %s", pos->pageId, terrstr());
    return NULL;
  }
  if (forUpdate) {
    setBufPageDirty(bufPage, true);
  }

  SResultRow* pRow = (SResultRow*)((char*)bufPage + pos->offset);
  return pRow;
}

int32_t getResultRowFromBuf(struct SExprSupp *pSup, const char* inBuf, size_t inBufSize, char **outBuf, size_t *outBufSize);
int32_t putResultRowToBuf(struct SExprSupp *pSup, const char* inBuf, size_t inBufSize, char **outBuf, size_t *outBufSize);

int32_t initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap, int32_t order);
void    cleanupGroupResInfo(SGroupResInfo* pGroupResInfo);

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList);
bool hasRemainResults(SGroupResInfo* pGroupResInfo);

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo);

int32_t      prepareDataBlockBuf(SSDataBlock* pDataBlock, SColMatchInfo* pMatchInfo);

EDealRes doTranslateTagExpr(SNode** pNode, void* pContext);
int32_t  getGroupIdFromTagsVal(void* pVnode, uint64_t uid, SNodeList* pGroupNode, char* keyBuf, uint64_t* pGroupId,
                               SStorageAPI* pAPI);
size_t   getTableTagsBufLen(const SNodeList* pGroups);

SArray* createSortInfo(SNodeList* pNodeList);
SArray* makeColumnArrayFromList(SNodeList* pNodeList);
int32_t extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            int32_t type, SColMatchInfo* pMatchInfo);

int32_t createExprFromOneNode(SExprInfo* pExp, SNode* pNode, int16_t slotId);
int32_t createExprFromTargetNode(SExprInfo* pExp, STargetNode* pTargetNode);

void destroySqlFunctionCtx(SqlFunctionCtx* pCtx, SExprInfo* pExpr, int32_t numOfOutput);
SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowEntryInfoOffset,
                                     SFunctionStateStore* pStore);
int32_t relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols, bool outputEveryColumn);
int32_t initExecTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pQueryWindow);

SInterval extractIntervalInfo(const STableScanPhysiNode* pTableScanNode);
SColumn   extractColumnFromColumnNode(SColumnNode* pColNode);

int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode,
                               const SReadHandle* readHandle, bool applyExtWin);
int32_t initQueryTableDataCondWithColArray(SQueryTableDataCond* pCond, SQueryTableDataCond* pOrgCond,
                                     const SReadHandle* readHandle, SArray* colArray);

int32_t convertFillType(int32_t mode);
int32_t resultrowComparAsc(const void* p1, const void* p2);
int32_t isQualifiedTable(STableKeyInfo* info, SNode* pTagCond, void* metaHandle, bool* pQualified, SStorageAPI* pAPI);
char*   getStreamOpName(uint16_t opType);

void    printSpecDataBlock(SSDataBlock* pBlock, const char* flag, const char* opStr, const char* taskIdStr);

TSKEY getStartTsKey(STimeWindow* win, const TSKEY* tsCols);
void  updateTimeWindowInfo(SColumnInfoData* pColData, const STimeWindow* pWin, int64_t delta);

SSDataBlock* createTagValBlockForFilter(SArray* pColList, int32_t numOfTables, SArray* pUidTagList, void* pVnode,
                                        SStorageAPI* pStorageAPI);

/**
 * @brief build a tuple into keyBuf
 * @param [out] keyBuf the output buf
 * @param [in] pSortGroupCols the cols to build
 * @param [in] pBlock block the tuple in
 */
int32_t buildKeys(char* keyBuf, const SArray* pSortGroupCols, const SSDataBlock* pBlock, int32_t rowIndex);

int32_t compKeys(const SArray* pSortGroupCols, const char* oldkeyBuf, int32_t oldKeysLen, const SSDataBlock* pDataBlock,
                 int32_t rowIndex);

uint64_t calcGroupId(char* pData, int32_t len);

SNodeList* makeColsNodeArrFromSortKeys(SNodeList* pSortKeys);

int32_t extractKeysLen(const SArray* keys, int32_t* pLen);

int32_t getDbVgInfoForExec(void* clientRpc, const char* dbFName, const char* tbName, SVgroupInfo* pVgInfo);
void    rmDbVgInfoFromCache(const char* dbFName);

int32_t doDropStreamTable(SMsgCb* pMsgCb, void* pOutput, SSTriggerDropRequest* pReq);
int32_t doDropStreamTableByTbName(SMsgCb* pMsgCb, void* pOutput, SSTriggerDropRequest* pReq, char* tbName);

#endif  // TDENGINE_EXECUTIL_H
