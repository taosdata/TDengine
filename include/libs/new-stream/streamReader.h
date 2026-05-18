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
  char    refDbName   [TSDB_DB_NAME_LEN];
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName  [TSDB_COL_NAME_LEN];
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

// Per-table resolved column ref cache entry. Keyed by "dbName\0tableName" in
// tblRefCache. Each entry stores per-column resolve results so that duplicate
// work items referencing the same table skip RPC entirely.
typedef struct STableRefCacheEntry {
  SHashObj *colResults;       // key: colName (TSDB_COL_NAME_LEN), value: SVTableRefResolveRspItem
} STableRefCacheEntry;

typedef struct SStreamVTableInfoCache {
  SRWLatch    lock;
  SArray     *reqColCids;     // SArray<col_id_t>
  SArray     *reqTagCids;     // SArray<col_id_t>
  SSHashObj  *uid2Result;     // key: int64_t uid, value: SVTableResolveResult*
  SHashObj   *dbVgInfo;       // key: dbFName, value: SUseDbRsp
  SHashObj   *tblRefCache;    // key: "dbName\0tableName", value: STableRefCacheEntry
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
void    streamVTableResolveResultDestroy(SVTableResolveResult *pRes);
void    streamDestroyUid2ResultMap(SSHashObj **ppMap);
void    streamTblRefCacheDestroy(SHashObj **ppCache);
void    streamTblRefCacheInvalidate(SStreamVTableInfoCache *pCache);

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

typedef struct StreamTableListInfo {
  SArray*          pTableList;   // element type: SStreamTableKeyInfo*
  SHashObj*        gIdMap;       // key: groupId/suid, value: SStreamTableList
  SHashObj*        uIdMap;       // key: uid, value: SStreamTableKeyInfo*,index
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
  int8_t       deleteReCalc;
  int8_t       deleteOutTbl;
  SNode*       pTagCond;
  SNode*       pTagIndexCond;
  SNode*       pConditions;
  SNodeList*   partitionCols;
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
  void*        pVnode;
  SStorageAPI  storageApi;
  SRWLatch     lock;

  StreamTableListInfo        tableList;
  StreamTableListInfo        vSetTableList;

  SStreamVTableInfoCache *vtbCache;

} SStreamTriggerReaderInfo;

typedef struct SStreamTriggerReaderCalcInfo {
  void*       pTask;
  void*       pFilterInfo;
  void*       tsConditions;
  SSubplan*    calcAst;
  STargetNode* pTargetNodeTs;
  char*       calcScanPlan;
  bool        hasPlaceHolder;
  qTaskInfo_t pTaskInfo;
  SStreamRuntimeInfo rtInfo;
  SStreamRuntimeFuncInfo tmpRtFuncInfo;
} SStreamTriggerReaderCalcInfo;

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

int32_t qBuildVTableList(SStreamTriggerReaderInfo* sStreamReaderInfo);

int32_t createStreamTask(void* pVnode, SStreamOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, STableKeyInfo* pList, int32_t pNum, SStorageAPI* storageApi);

int32_t createStreamTaskForTs(SStreamOptions* options, SStreamReaderTaskInner** ppTask, SStorageAPI* api);
         
int32_t  initStreamTableListInfo(StreamTableListInfo* pTableListInfo);
int32_t  qStreamGetTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid, STableKeyInfo** pKeyInfo, int32_t* size);
void     qStreamDestroyTableInfo(StreamTableListInfo* pTableListInfo);
void     qStreamClearTableInfo(StreamTableListInfo* pTableListInfo);
int32_t  qStreamCopyTableInfo(SStreamTriggerReaderInfo* sStreamReaderInfo, StreamTableListInfo* dst);
int32_t  qStreamSetTableList(StreamTableListInfo* pTableListInfo, int64_t uid, uint64_t gid);
int32_t  qStreamGetTableListGroupNum(SStreamTriggerReaderInfo* sStreamReaderInfo);
int32_t  qStreamGetTableListNum(SStreamTriggerReaderInfo* sStreamReaderInfo);
SArray*  qStreamGetTableArrayList(SStreamTriggerReaderInfo* sStreamReaderInfo);
int32_t  qStreamIterTableList(StreamTableListInfo* sStreamReaderInfo, STableKeyInfo** pKeyInfo, int32_t* size, int64_t* suid);
uint64_t qStreamGetGroupIdFromOrigin(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid);
uint64_t qStreamGetGroupIdFromSet(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid);
int32_t  qStreamRemoveTableList(StreamTableListInfo* pTableListInfo, int64_t uid);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H
