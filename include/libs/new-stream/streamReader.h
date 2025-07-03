#ifndef TDENGINE_STREAM_READER_H
#define TDENGINE_STREAM_READER_H

#include <stdint.h>
#include "executor.h"
#include "filter.h"
#include "plannodes.h"
#include "stream.h"
#include "streamMsg.h"
#include "tdatablock.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamTriggerReaderInfo {
  int32_t      order;
  // SArray*      schemas;
  STimeWindow  twindows;
  uint64_t     suid;
  uint64_t     uid;
  int8_t       tableType;
  int8_t       deleteReCalc;
  int8_t       deleteOutTbl;
  SNode*       pTagCond;
  SNode*       pCalcTagCond;
  SNode*       pTagIndexCond;
  SNode*       pConditions;
  SNode*       pCalcConditions;
  SNodeList*   partitionCols;
  SNodeList*   triggerCols;
  SHashObj*    streamTaskMap;
  SHashObj*    groupIdMap;
  SSubplan*    triggerAst;
  SSubplan*    calcAst;
  SSDataBlock* triggerResBlock;
  SSDataBlock* calcResBlock;
  SSDataBlock* calcResBlockTmp;
  STSchema*    triggerSchema;
  SExprInfo*   pExprInfo;
  int32_t      numOfExpr;
  SArray*      uidList;       // for virtual table stream, uid list
  SArray*      uidListIndex;
  SHashObj*    uidHash;
} SStreamTriggerReaderInfo;

typedef struct SStreamTriggerReaderCalcInfo {
  void*       pFilterInfo;
  void*       tsConditions;
  SSubplan*    calcAst;
  STargetNode* pTargetNodeTs;
  char*       calcScanPlan;
  qTaskInfo_t pTaskInfo;
  SStreamRuntimeInfo rtInfo;
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
  uint64_t    gid;
  int8_t      tableType;
  bool        groupSort;
  EScanMode   scanMode;
  SNode*      pTagCond;
  SNode*      pTagIndexCond;
  SNode*      pConditions;
  SNodeList*  partitionCols;
  bool        initReader;  // whether to init the reader
  SArray*     uidList;
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
} SStreamReaderTaskInner;

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, void* schemas, bool isSchema,
                                      STimeWindow twindows, uint64_t suid);
int32_t createDataBlockForStream(SArray* schemas, SSDataBlock** pBlockRet);
int32_t qStreamBuildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId);
void    releaseStreamTask(void* p);
int32_t createStreamTask(void* pVnode, SStreamTriggerReaderTaskInnerOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, SHashObj* groupIdMap, SStorageAPI*  api);
void*   qStreamGetReaderInfo(int64_t streamId, int64_t taskId, void** taskAddr);
void    qStreamSetTaskRunning(int64_t streamId, int64_t taskId);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H