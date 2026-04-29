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

#ifdef __cplusplus
extern "C" {
#endif

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

typedef struct slotInfo{
  SArray*   schemas;
  int32_t*  slotIdList;
} SlotInfo;

static inline void destroySlotInfo(void* p) {
  if (p) {
    SlotInfo* info = (SlotInfo*)p;
    taosArrayDestroy(info->schemas);
    taosMemoryFree(info->slotIdList);
  }
}

typedef struct SStreamTriggerReaderInfo {
  void*        pTask;
  int32_t      order;
  STimeWindow  twindows;
  uint64_t     suid;
  uint64_t     uid;
  int8_t       isOldPlan;
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
  SNodeList*   calcCols;
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
  SFilterInfo* pFilterInfoTrigger;
  SFilterInfo* pFilterInfoCalc;
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
  SSHashObj*                 uidHashTrigger;  // < uid -> SHashObj < slotId -> colId > >
  SSHashObj*                 uidHashCalc;     // < uid -> SHashObj < slotId -> colId > >

  // ===== v3.4.2 sub-project C DS v6.1 §6.1.3 =====
  // F7/F8 virtual-table two-layer cache; outer key = getSessionKey(sessionId, firstType),
  // value = SHashObj* uidTaskMap (inner key = uid, value = SStreamReaderTaskInner*).
  SHashObj* vtableTaskMap;

  // F9 history-side triple (replaced atomically by STRIGGER_PULL_SET_TABLE_HISTORY; see DS §6.4).
  StreamTableListInfo vSetTableListHistory;
  SSHashObj*          uidHashTriggerHistory;  // <(suid,uid)[2] -> SHashObj<slotId,colId>>
  SSHashObj*          uidHashCalcHistory;     // <(suid,uid)[2] -> SHashObj<slotId,colId>>
  SSHashObj*          uidHashTriggerHistorySlotInfo;     // <uid -> SlotInfo>
  SSHashObj*          uidHashCalcHistorySlotInfo;        // <uid -> SlotInfo>
} SStreamTriggerReaderInfo;

// v3.4.2 DS v6.1 §6.1.3 - normalize NEXT type to the FIRST pull type used as cache key.
// Cache key always derives from the first type, regardless of whether the request is a
// first pull or a continuation pull (see DS §6.2.1 / §6.3.2).
static inline ESTriggerPullType getFirstTypeFromNext(ESTriggerPullType t) {
  switch (t) {
    case STRIGGER_PULL_TSDB_DATA_NEW_NEXT:              return STRIGGER_PULL_TSDB_DATA_NEW;
    case STRIGGER_PULL_TSDB_DATA_NEW_CALC_NEXT:         return STRIGGER_PULL_TSDB_DATA_NEW_CALC;
    case STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_NEXT:       return STRIGGER_PULL_TSDB_DATA_VTABLE_NEW;
    case STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC_NEXT:  return STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC;
    default:                                            return t;
  }
}

static inline bool isFirstPullType(ESTriggerPullType t) {
  return getFirstTypeFromNext(t) == t;
}

// dual-mode helper: only true plan reuses a calc-side filter; old plans share trigger filter
static inline bool isNewCalc(SStreamTriggerReaderInfo* pInfo, bool isCalc) {
  return !pInfo->isOldPlan && isCalc;
}

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

int32_t qBuildVTableList(SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo, StreamTableListInfo* dst,
                             SSHashObj** uidInfoTrigger, SSHashObj** uidInfoCalc);

int32_t qBuildVTableListInto(SStreamTriggerReaderInfo* sStreamReaderInfo,
                             StreamTableListInfo* dst,
                             SSHashObj* uidInfoTrigger);

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
uint64_t qStreamGetGroupId(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid, bool lock);
int32_t  qStreamRemoveTableList(StreamTableListInfo* pTableListInfo, int64_t uid);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H
