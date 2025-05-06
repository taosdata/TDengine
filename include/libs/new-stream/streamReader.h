#ifndef TDENGINE_STREAM_READER_H
#define TDENGINE_STREAM_READER_H

#include <stdint.h>
#include "stream.h"
#include "streamMsg.h"
#include "executor.h"
#include "tdatablock.h"
#include "filter.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_RETURN_ROWS_NUM 100

#define STREAM_CHECK_RET_GOTO(CMD) \
  code = (CMD);                    \
  if (code != TSDB_CODE_SUCCESS) { \
    lino = __LINE__;               \
    goto end;                      \
  }

#define STREAM_CHECK_NULL_GOTO(CMD, ret) \
  if ((CMD) == NULL) {                   \
    code = ret;                          \
    lino = __LINE__;                     \
    goto end;                            \
  }

#define STREAM_CHECK_CONDITION_GOTO(CMD, ret) \
  if (CMD) {                                  \
    code = ret;                               \
    lino = __LINE__;                          \
    goto end;                                 \
  }

#define PRINT_LOG_END(code, lino)                                             \
  if (code != 0) {                                                            \
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code)); \
  } else {                                                                    \
    stDebug("%s done success", __func__);                                      \
  }

typedef struct SStreamTriggerReaderInfo {
  int32_t     order;
  SArray*     schemas;
  STimeWindow twindows;
  uint64_t    suid;
  uint64_t    uid;
  int8_t      tableType;
  SNode*      pTagCond;
  SNode*      pTagIndexCond;
  SNode*      pConditions;
  SNodeList*  pGroupTags;
  SHashObj*   streamTaskMap;
} SStreamTriggerReaderInfo;

typedef struct SStreamCalcReaderInfo {
  void*       calcScanPlan;
  qTaskInfo_t pTaskInfo;
} SStreamCalcReaderInfo;

typedef enum { STREAM_SCAN_GROUP_ONE_BY_ONE, STREAM_SCAN_ALL } EScanMode;

typedef enum { WAL_SUBMIT_DATA = 0, WAL_DELETE_DATA, WAL_DELETE_TABLE } ESWalType;

typedef struct SStreamTriggerReaderTaskInnerOptions {
  int32_t     order;
  SArray*     schemas;
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
  SNodeList*  pGroupTags;
} SStreamTriggerReaderTaskInnerOptions;

typedef struct SStreamReaderTaskInner {
  int64_t                       streamId;
  int64_t                       sessionId;
  SStorageAPI                   api;
  void*                         pReader;
  SHashObj*                     pIgnoreTables;
  SSDataBlock*                  pResBlock;
  SSDataBlock*                  pResBlockDst;
  SStreamTriggerReaderTaskInnerOptions options;
  void*                         pTableList;
  int32_t                       currentGroupIndex;
  SFilterInfo*                  pFilterInfo;
  char*                         idStr;
} SStreamReaderTaskInner;

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, SArray* schemas, STimeWindow twindows, uint64_t suid);
int32_t createDataBlockForStream(SArray* schemas, SSDataBlock** pBlockRet);
void    releaseStreamTask(void* p);
int32_t createStreamTask(void* pVnode, SStreamTriggerReaderTaskInnerOptions* options, SStreamReaderTaskInner** ppTask);
void*   qStreamGetReaderInfo(int64_t streamId, int64_t taskId);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_READER_H