#ifndef TDENGINE_STREAM_READER_H
#define TDENGINE_STREAM_READER_H

#include <stdint.h>
#include "executor.h"
#include "filter.h"
#include "plannodes.h"
#include "stream.h"
#include "streamMsg.h"
#include "tdatablock.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamTriggerReaderInfo {
  void*        pTask;
  int32_t      order;
  // SArray*      schemas;
  STimeWindow  twindows;
  uint64_t     suid;
  uint64_t     uid;
  int8_t       tableType;
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
  SSDataBlock* calcResBlock;
  SSDataBlock* tsBlock;
  SSDataBlock* triggerBlock;
  SSDataBlock* calcBlock;
  SSDataBlock* metaBlock;
  SExprInfo*   pExprInfoTriggerTag;
  int32_t      numOfExprTriggerTag;
  SExprInfo*   pExprInfoCalcTag;
  int32_t      numOfExprCalcTag;
  SSHashObj*   uidHashTrigger;  // < uid -> SHashObj < slotId -> colId > >
  SSHashObj*   uidHashCalc;     // < uid -> SHashObj < slotId -> colId > >
  bool         isVtableStream;  // whether is virtual table stream
  void*        tableList;
  void*        historyTableList;
  SFilterInfo* pFilterInfo;
  SHashObj*    pTableMetaCacheTrigger;
  SHashObj*    pTableMetaCacheCalc;
  SSHashObj*   indexHash;  // index hash for wal data
  bool         groupByTbname;
  void*        pVnode;
  TdThreadMutex mutex;
} SStreamTriggerReaderInfo;

typedef struct SStreamTriggerReaderCalcInfo {
  void*       pTask;
  void*       pFilterInfo;
  void*       tsConditions;
  SSubplan*    calcAst;
  STargetNode* pTargetNodeTs;
  char*       calcScanPlan;
  qTaskInfo_t pTaskInfo;
  SStreamRuntimeInfo rtInfo;
  SStreamRuntimeFuncInfo tmpRtFuncInfo;
} SStreamTriggerReaderCalcInfo;

typedef enum { STREAM_SCAN_GROUP_ONE_BY_ONE, STREAM_SCAN_ALL } EScanMode;

typedef enum { WAL_SUBMIT_DATA = 0, WAL_DELETE_DATA, WAL_DELETE_TABLE } ESWalType;

typedef struct SStreamTriggerReaderTaskInnerOptions {
  int32_t     order;
  void*       schemas;
  bool        isSchema;
  STimeWindow twindows;
  uint64_t    suid;
  uint64_t    uid;
  int64_t     ver;
  uint64_t    gid;
  int8_t      tableType;
  EScanMode   scanMode;
  bool        initReader;  // whether to init the reader
  SSHashObj*  mapInfo;    // SArray<SetTableMapInfo>
  SStreamTriggerReaderInfo* sStreamReaderInfo;
} SStreamTriggerReaderTaskInnerOptions;

typedef struct SStreamReaderTaskInner {
  int64_t                              streamId;
  int64_t                              sessionId;
  SStorageAPI                          api;
  void*                                pReader;
  SSDataBlock*                         pResBlock;
  SSDataBlock*                         pResBlockDst;
  SStreamTriggerReaderTaskInnerOptions options;
  void*                                pTableList;
  int32_t                              currentGroupIndex;
  SFilterInfo*                         pFilterInfo;
  char*                                idStr;
  SQueryTableDataCond                  cond;
} SStreamReaderTaskInner;

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, void* schemas, bool isSchema,
                                      STimeWindow twindows, uint64_t suid, int64_t ver, int32_t** pSlotList);
int32_t createDataBlockForStream(SArray* schemas, SSDataBlock** pBlockRet);
int32_t qStreamBuildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId);
void    releaseStreamTask(void* p);
int32_t createStreamTask(void* pVnode, SStreamTriggerReaderTaskInnerOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, SStorageAPI*  api);
void*   qStreamGetReaderInfo(int64_t streamId, int64_t taskId, void** taskAddr);
void    qStreamSetTaskRunning(int64_t streamId, int64_t taskId);
int32_t streamBuildFetchRsp(SArray* pResList, bool hasNext, void** data, size_t* size, int8_t precision);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H
