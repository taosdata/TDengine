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
  int64_t suid;
} SStreamTableMapElement;

typedef struct StreamTableListInfo {
  SArray*          pTableList;   // element type: SStreamTableKeyInfo*
  SHashObj*        gIdMap;       // key: groupId, value: SStreamTableList
  SHashObj*        uIdMap;       // key: uid, value: SStreamTableKeyInfo*,index
  void*            pIter;        // iterator for gIdMap
} StreamTableListInfo;

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
  bool         isVtableStream;  // whether is virtual table stream
  void*        historyTableList;
  SFilterInfo* pFilterInfo;
  SHashObj*    pTableMetaCacheTrigger;
  SHashObj*    pTableMetaCacheCalc;
  STSchema*    triggerTableSchema;
  bool         groupByTbname;
  void*        pVnode;
  SRWLatch     lock;

  StreamTableListInfo        tableList;
  StreamTableListInfo        vSetTableList;

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
  SStorageAPI                          api;
  void*                                pReader;
  SSDataBlock*                         pResBlock;
  SSDataBlock*                         pResBlockDst;
  SStreamOptions                       options;
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

int32_t qBuildVTableList(SSHashObj* uidHash, SStreamTriggerReaderInfo* sStreamReaderInfo);

int32_t createStreamTask(void* pVnode, SStreamOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, SStorageAPI* api, STableKeyInfo* pList, int32_t pNum);
                        
int32_t  qStreamGetTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid, STableKeyInfo** pKeyInfo, int32_t* size);
void     qStreamDestroyTableInfo(StreamTableListInfo* pTableListInfo);
int32_t  qStreamCopyTableInfo(SStreamTriggerReaderInfo* sStreamReaderInfo, StreamTableListInfo* dst);
int32_t  qTransformStreamTableList(void* pTableListInfo, StreamTableListInfo* tableInfo);
int32_t  qStreamGetTableListGroupNum(SStreamTriggerReaderInfo* sStreamReaderInfo);
SArray*  qStreamGetTableArrayList(SStreamTriggerReaderInfo* sStreamReaderInfo);
int32_t  qStreamIterTableList(StreamTableListInfo* sStreamReaderInfo, STableKeyInfo** pKeyInfo, int32_t* size);
uint64_t qStreamGetGroupIdFromOrigin(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid);
uint64_t qStreamGetGroupIdFromSet(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid);
int32_t  qStreamModifyTableList(StreamTableListInfo* tableInfo, SArray* tableListAdd, SArray* tableListDel, SRWLatch* lock);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H
