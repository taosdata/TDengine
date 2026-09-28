/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef TDENGINE_STREAM_READER_H
#define TDENGINE_STREAM_READER_H

#include <stdint.h>
#include "executor.h"
#include "filter.h"
#include "plannodes.h"
#include "stream.h"
#include "streamMsg.h"
#include "tarray.h"
#include "tdatablock.h"
#include "thash.h"
#include "tlockfree.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

// Resolved column reference terminal item.
// kind=COL: chain ends at a physical table column.
// kind=TAG: chain ends at a child table tag value (carried by STagValue elsewhere).
typedef struct SColResolveItem {
  bool    hasRef;
  int8_t  refType;
  char    refSourceName[TSDB_EXT_SOURCE_NAME_LEN];
  char    refSchemaName[TSDB_EXT_SOURCE_SCHEMA_LEN];
  char    refDbName   [TSDB_DB_NAME_LEN];
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName  [TSDB_COL_NAME_LEN];
  int32_t tagCondLen;
  char   *tagCondJson;  // Owned by the resolve result and freed with the item.
} SColResolveItem;

typedef struct STagValue {
  int8_t   type;
  int32_t  nLen;
  char    *pData;       // owned, freed by destroy helper
} STagValue;

typedef struct SVTableResolveResult {
  SSHashObj *colMap;    // key: virtual col cid (col_id_t), value: SColResolveItem*
  SSHashObj *tagMap;    // key: virtual tag cid (col_id_t), value: STagValue*
} SVTableResolveResult;

// Per-table per-column resolved ref cache. Flat single-level hash keyed by
// "dbName\0tableName\0colName"; value is the resolved SVTableRefResolveRspItem
// (tagData deep-copied). Tags and columns cannot share a name within the same
// physical table, so a single (db,table,col) key is unambiguous.
typedef struct SStreamVTableInfoCache {
  SRWLatch    lock;
  SArray     *reqColCids;     // SArray<col_id_t>
  SArray     *reqTagCids;     // SArray<col_id_t>
  SSHashObj  *uid2Result;     // key: int64_t uid, value: SVTableResolveResult*
  SHashObj   *dbVgInfo;       // key: dbFName, value: SUseDbRsp
  SHashObj   *tblRefCache;    // key: "dbName\0tableName\0colName", value: SVTableRefResolveRspItem
  int64_t     lastCheckMs;
  // Sliced recheck cursor: every throttle tick scans at most
  // STREAM_VTB_RECHECK_SLICE_SIZE uids from uidSlice[sliceCursor..]; when the
  // cursor wraps to 0, uidSlice is rebuilt from the current uid2Result keys
  // so newly-added uids are picked up on the next round.
  SArray     *uidSlice;       // SArray<int64_t>: snapshot of uids to scan
  int32_t     sliceCursor;
  bool        valid;
} SStreamVTableInfoCache;

int32_t streamVTableInfoCacheInit   (SStreamVTableInfoCache *pCache);
void    streamVTableInfoCacheDestroy(SStreamVTableInfoCache *pCache);
void    streamVTableResolveResultDestroy(void *pRes);

typedef struct SStreamTableKeyInfo {
  int64_t uid;
  uint64_t groupId;
  bool     markedDeleted;
  struct SStreamTableKeyInfo* prev;
  struct SStreamTableKeyInfo* next;
} SStreamTableKeyInfo;

typedef struct SStreamTableList {
  SStreamTableKeyInfo* head;
  SStreamTableKeyInfo* tail;
  int32_t size;
} SStreamTableList;

typedef struct SStreamTableMapElement {
  SStreamTableKeyInfo* table;
  int32_t index;
} SStreamTableMapElement;

typedef enum { UIDMAP_SINGLE, UIDMAP_MULTI } EUidMapMode;

typedef struct StreamTableListInfo {
  SArray*          pTableList;   // element type: SStreamTableKeyInfo*
  SHashObj*        gIdMap;       // key: groupId/suid, value: SStreamTableList
  SHashObj*        uIdMap;       // SINGLE: uid -> SStreamTableMapElement; MULTI: uid -> SArray<SStreamTableMapElement>*
  EUidMapMode      uIdMapMode;
  void*            pIter;        // iterator for gIdMap
  int64_t          version;
} StreamTableListInfo;

typedef struct SStreamTriggerReaderInfo {
  void*        pTask;
  int32_t      order;
  STimeWindow  twindows;
  uint64_t     suid;
  uint64_t     uid;
  int8_t       tableType;
  int8_t       isVtableStream;  // whether is virtual table stream
  int8_t       isVtableOnlyTs;
  int8_t       vTableReady;
  int8_t       deleteReCalc;
  int8_t       deleteOutTbl;
  SNode*       pTagCond;
  SNode*       pTagIndexCond;
  SNode*       pConditions;
  SNodeList*   partitionCols;
  SNodeList*   pRollupTagCols;  // SNodeList<SColumnNode>; NULL = not rollup
  SNodeList*   triggerCols;
  SNodeList*   triggerPseudoCols;
  SHashObj*    streamTaskMap;
  SHashObj*    groupIdMap;
  SSubplan*    triggerAst;
  SSubplan*    calcAst;
  SSDataBlock* triggerResBlock;
  SSDataBlock* triggerBlock;
  SSDataBlock* calcResBlock;
  SSDataBlock* calcBlock;
  SSDataBlock* metaBlock;
  SSDataBlock* tsBlock;
  SArray*      tsSchemas;
  SExprInfo*   pExprInfoTriggerTag;
  int32_t      numOfExprTriggerTag;
  SExprInfo*   pExprInfoCalcTag;
  int32_t      numOfExprCalcTag;
  SSHashObj*   uidHashTrigger;  // < uid -> SHashObj < slotId -> colId > >
  SSHashObj*   uidHashCalc;     // < uid -> SHashObj < slotId -> colId > >
  void*        historyTableList;
  SFilterInfo* pFilterInfo;
  SHashObj*    pTableMetaCacheTrigger;
  SHashObj*    pTableMetaCacheCalc;
  SHashObj*    triggerTableSchemaMapVTable; // key: uid, value: STSchema*
  STSchema*    triggerTableSchema;
  bool         groupByTbname;
  bool         isRollupReader;
  char*        extraErrMsg;
  void*        pVnode;
  SStorageAPI  storageApi;
  SRWLatch     lock;

  StreamTableListInfo        tableList;
  StreamTableListInfo        vSetTableList;

  SStreamVTableInfoCache *vtbCache;

} SStreamTriggerReaderInfo;

bool    stReaderTaskGetShallowMemoryEstimate(const SStreamReaderTask *pTask, int64_t *pBytes);

typedef struct SStreamReaderFetchState {
  bool     enabled;
  uint64_t id;
  uint64_t queryId;
  uint64_t taskId;
  int32_t  execId;
  int32_t  nodeId;
  uint64_t streamGen;
  int64_t  sessionId;
  bool     dynTbname;
  bool     readerGroupMode;
} SStreamReaderFetchState;

typedef struct SStreamTriggerReaderCalcInfo {
  SStreamReaderFetchState fetchState;
  void*       pTask;
  void*       pFilterInfo;
  void*       tsConditions;
  SSubplan*    calcAst;
  STargetNode* pTargetNodeTs;
  char*       calcScanPlan;
  bool        hasPlaceHolder;
  bool                   readerGroupRequested;
  bool                   readerGroupAccepted;
  bool                   readerGroupModeSet;
  bool                   requiresContextPolicy;
  qTaskInfo_t pTaskInfo;
  SStreamRuntimeInfo rtInfo;
  SStreamRuntimeFuncInfo tmpRtFuncInfo;
} SStreamTriggerReaderCalcInfo;

void    stReaderCaptureFetchState(SStreamReaderFetchState* pState, const SResFetchReq* pReq, int32_t actualNodeId);
int32_t stReaderCheckFetchContinuation(const SStreamTriggerReaderCalcInfo* pCalc, const SResFetchReq* pReq,
                                       int32_t actualNodeId);
void    stReaderInvalidateFetchState(SStreamTriggerReaderCalcInfo* pCalc);

int32_t stReaderSetGroupOutputMode(SStreamTriggerReaderCalcInfo* pCalc, const SResFetchReq* pReq);

typedef struct SStreamReaderCalcProjection SStreamReaderCalcProjection;

int32_t stProjectReaderCalcContext(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId, int32_t readInfoIndex,
                                   int32_t sourceParamIndex, SStreamRuntimeFuncInfo* pTarget);
int32_t stPrepareReaderCalcProjection(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId,
                                      SStreamReaderCalcProjection** ppProjection);
int32_t stProjectPreparedReaderCalcContext(const SStreamReaderCalcProjection* pProjection, int32_t readInfoIndex,
                                           SStreamRuntimeFuncInfo* pTarget);
void    stDestroyReaderCalcProjection(SStreamReaderCalcProjection** ppProjection);
int32_t stValidateNodeGroupReadBindings(const SStreamRuntimeFuncInfo* pSource, int32_t actualNodeId);

// typedef enum { STREAM_SCAN_GROUP_ONE_BY_ONE, STREAM_SCAN_ALL } EScanMode;

typedef enum { WAL_SUBMIT_DATA = 0, WAL_DELETE_DATA, WAL_DELETE_TABLE } ESWalType;

typedef struct {
  int32_t     order;
  void*       schemas;
  bool        isSchema;
  STimeWindow twindows;
  int64_t     suid;
  int64_t     ver;
  int32_t**   pSlotList;
} SStreamOptions;

typedef struct {
  int64_t                              streamId;
  int64_t                              sessionId;
  SStorageAPI*                         storageApi;
  void*                                pReader;
  SSDataBlock*                         pResBlock;
  SSDataBlock*                         pResBlockDst;
  SStreamOptions*                      options;
  SSHashObj*                           pRollupMetaByUid;
  SSHashObj*                           pRollupMetaCount;
  char*                                idStr;
  SQueryTableDataCond                  cond;
} SStreamReaderTaskInner;

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, void* schemas, bool isSchema,
                                      STimeWindow twindows, uint64_t suid, int64_t ver, int32_t** pSlotList);
int32_t createDataBlockForStream(SArray* schemas, SSDataBlock** pBlockRet);
int32_t qStreamBuildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId);
void    releaseStreamTask(void* p);
void*   qStreamGetReaderInfo(int64_t streamId, int64_t taskId, void** taskAddr);
void    qStreamSetTaskRunning(int64_t streamId, int64_t taskId);
int32_t streamBuildFetchRsp(SArray* pResList, bool hasNext, void** data, size_t* size, int8_t precision);
/** Builds a Reader batch response while borrowing every block in pBlocks. */
int32_t streamBuildGroupFetchRsp(const SArray* pBlocks, bool hasNext, void** ppData, size_t* pSize, int8_t precision);

int32_t qBuildVTableList(SStreamTriggerReaderInfo* sStreamReaderInfo);

int32_t createStreamTask(void* pVnode, SStreamOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, STableKeyInfo* pList, int32_t pNum, SStorageAPI* storageApi);

int32_t createStreamTaskForTs(SStreamOptions* options, SStreamReaderTaskInner** ppTask, SStorageAPI* api);
bool isRollupMultiReader(SStreamTriggerReaderInfo* sStreamReaderInfo);

int32_t initStreamTableListInfo(StreamTableListInfo* pTableListInfo, EUidMapMode uIdMapMode);
int32_t  qStreamGetTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid, STableKeyInfo** pKeyInfo, int32_t* size);
void     qStreamDestroyTableInfo(StreamTableListInfo* pTableListInfo);
void     qStreamClearTableInfo(StreamTableListInfo* pTableListInfo);
int32_t  qStreamCopyTableInfo(SStreamTriggerReaderInfo* sStreamReaderInfo, StreamTableListInfo* dst);
int32_t  qStreamSetTableList(StreamTableListInfo* pTableListInfo, int64_t uid, uint64_t gid);
int32_t  qStreamGetTableListGroupNum(SStreamTriggerReaderInfo* sStreamReaderInfo);
int32_t  qStreamGetTableListNum(SStreamTriggerReaderInfo* sStreamReaderInfo);
int32_t  qStreamGetGroupTableCount(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid);
SArray*  qStreamGetTableArrayList(SStreamTriggerReaderInfo* sStreamReaderInfo);
int32_t  qStreamIterTableList(StreamTableListInfo* sStreamReaderInfo, STableKeyInfo** pKeyInfo, int32_t* size, int64_t* suid);
uint64_t qStreamGetGroupIdFromOrigin(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid);
uint64_t qStreamGetGroupIdFromSet(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid);
int32_t  qStreamRemoveTableList(StreamTableListInfo* pTableListInfo, int64_t uid);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H
