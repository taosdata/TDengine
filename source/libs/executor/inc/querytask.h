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

#ifndef TDENGINE_QUERYTASK_H
#define TDENGINE_QUERYTASK_H

#ifdef __cplusplus
extern "C" {
#endif

#define GET_TASKID(_t) (((SExecTaskInfo*)(_t))->id.str)

enum {
  // when this task starts to execute, this status will set
      TASK_NOT_COMPLETED = 0x1u,

  /* Task is over
   * 1. this status is used in one row result query process, e.g., count/sum/first/last/ avg...etc.
   * 2. when all data within queried time window, it is also denoted as query_completed
   */
      TASK_COMPLETED = 0x2u,
};

typedef struct STaskIdInfo {
  uint64_t queryId;  // this is also a request id
  uint64_t subplanId;
  uint64_t templateId;
  char*    str;
  int32_t  vgId;
  uint64_t taskId;
} STaskIdInfo;

typedef struct STaskCostInfo {
  int64_t                 created;
  int64_t                 start;
  uint64_t                elapsedTime;
  double                  extractListTime;
  double                  groupIdMapTime;
  SFileBlockLoadRecorder* pRecoder;
} STaskCostInfo;

typedef struct STaskStopInfo {
  SRWLatch lock;
  SArray*  pStopInfo;
} STaskStopInfo;

typedef struct {
  STqOffsetVal         currentOffset;  // for tmq
  SMqMetaRsp           metaRsp;        // for tmq fetching meta
  int8_t               sourceExcluded;
  int64_t              snapshotVer;
  SSchemaWrapper*      schema;
  char                 tbName[TSDB_TABLE_NAME_LEN];  // this is the current scan table: todo refactor
  int8_t               recoverStep;
  int8_t               recoverScanFinished;
  SQueryTableDataCond  tableCond;
  SVersionRange        fillHistoryVer;
  STimeWindow          fillHistoryWindow;
  SStreamState*        pState;
} SStreamTaskInfo;

struct SExecTaskInfo {
  STaskIdInfo           id;
  uint32_t              status;
  STimeWindow           window;
  STaskCostInfo         cost;
  int64_t               owner;  // if it is in execution
  int32_t               code;
  int32_t               qbufQuota;  // total available buffer (in KB) during execution query
  int64_t               version;    // used for stream to record wal version, why not move to sschemainfo
  SStreamTaskInfo       streamInfo;
  SArray*               schemaInfos;
  const char*           sql;        // query sql string
  jmp_buf               env;        // jump to this position when error happens.
  EOPTR_EXEC_MODEL      execModel;  // operator execution model [batch model|stream model]
  SSubplan*             pSubplan;
  struct SOperatorInfo* pRoot;
  SLocalFetch           localFetch;
  SArray*               pResultBlockList;  // result block list
  STaskStopInfo         stopInfo;
  SRWLatch              lock;  // secure the access of STableListInfo
  SStorageAPI           storageAPI;
  int8_t                dynamicTask;
  SOperatorParam*       pOpParam;
  bool                  paramSet;
};

void           buildTaskId(uint64_t taskId, uint64_t queryId, char* dst);
SExecTaskInfo* doCreateTask(uint64_t queryId, uint64_t taskId, int32_t vgId, EOPTR_EXEC_MODEL model, SStorageAPI* pAPI);
void           doDestroyTask(SExecTaskInfo* pTaskInfo);
bool           isTaskKilled(void* pTaskInfo);
void           setTaskKilled(SExecTaskInfo* pTaskInfo, int32_t rspCode);
void           setTaskStatus(SExecTaskInfo* pTaskInfo, int8_t status);
int32_t        createExecTaskInfo(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                                  int32_t vgId, char* sql, EOPTR_EXEC_MODEL model);
int32_t        qAppendTaskStopInfo(SExecTaskInfo* pTaskInfo, SExchangeOpStopInfo* pInfo);
SArray*        getTableListInfo(const SExecTaskInfo* pTaskInfo);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QUERYTASK_H
