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

#ifndef _TD_QWORKER_INT_H_
#define _TD_QWORKER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tlockfree.h"

#define QWORKER_DEFAULT_SCHEDULER_NUMBER 10000
#define QWORKER_DEFAULT_TASK_NUMBER 10000
#define QWORKER_DEFAULT_SCH_TASK_NUMBER 10000

enum {
  QW_READY_NOT_RECEIVED = 0,
  QW_READY_RECEIVED,
  QW_READY_RESPONSED,
};

enum {
  QW_TASK_INFO_STATUS = 1,
  QW_TASK_INFO_READY,
};

enum {
  QW_READ = 1,
  QW_WRITE,
};

enum {
  QW_EXIST_ACQUIRE = 1,
  QW_EXIST_RET_ERR,
};

enum {
  QW_NOT_EXIST_RET_ERR = 1,
  QW_NOT_EXIST_ADD,
};

enum {
  QW_ADD_RET_ERR = 1,
  QW_ADD_ACQUIRE,
};

typedef struct SQWTaskStatus {  
  SRWLatch lock;
  int32_t  code;
  int8_t   status;
  int8_t   ready; 
  bool     cancel;
  bool     drop;
} SQWTaskStatus;

typedef struct SQWTaskCtx {
  SRWLatch        lock;
  int8_t          sinkScheduled;
  int8_t          queryScheduled;
  bool            needRsp;
  qTaskInfo_t     taskHandle;
  DataSinkHandle  sinkHandle;
} SQWTaskCtx;

typedef struct SQWSchStatus {
  int32_t   lastAccessTs; // timestamp in second
  SRWLatch  tasksLock;
  SHashObj *tasksHash;   // key:queryId+taskId, value: SQWTaskStatus
} SQWSchStatus;

// Qnode/Vnode level task management
typedef struct SQWorkerMgmt {
  SQWorkerCfg      cfg;
  int8_t           nodeType;
  int32_t          nodeId;
  SRWLatch         schLock;
  SRWLatch         ctxLock;
  SHashObj        *schHash;       //key: schedulerId,    value: SQWSchStatus
  SHashObj        *ctxHash;       //key: queryId+taskId, value: SQWTaskCtx
  void            *nodeObj;
  putReqToQueryQFp putToQueueFp;
} SQWorkerMgmt;

#define QW_GOT_RES_DATA(data) (true)
#define QW_LOW_RES_DATA(data) (false)

#define QW_TASK_NOT_EXIST(code) (TSDB_CODE_QRY_SCH_NOT_EXIST == (code) || TSDB_CODE_QRY_TASK_NOT_EXIST == (code))
#define QW_TASK_ALREADY_EXIST(code) (TSDB_CODE_QRY_TASK_ALREADY_EXIST == (code))
#define QW_TASK_READY_RESP(status) (status == JOB_TASK_STATUS_SUCCEED || status == JOB_TASK_STATUS_FAILED || status == JOB_TASK_STATUS_CANCELLED || status == JOB_TASK_STATUS_PARTIAL_SUCCEED)
#define QW_SET_QTID(id, qId, tId) do { *(uint64_t *)(id) = (qId); *(uint64_t *)((char *)(id) + sizeof(qId)) = (tId); } while (0)
#define QW_GET_QTID(id, qId, tId) do { (qId) = *(uint64_t *)(id); (tId) = *(uint64_t *)((char *)(id) + sizeof(qId)); } while (0)
#define QW_IDS() sId, qId, tId

#define QW_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define QW_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define QW_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define QW_ELOG(param, ...) qError("QW:%p " param, mgmt, __VA_ARGS__)
#define QW_DLOG(param, ...) qDebug("QW:%p " param, mgmt, __VA_ARGS__)

#define QW_SCH_ELOG(param, ...) qError("QW:%p SID:%"PRIx64 param, mgmt, sId, __VA_ARGS__)
#define QW_SCH_DLOG(param, ...) qDebug("QW:%p SID:%"PRIx64 param, mgmt, sId, __VA_ARGS__)

#define QW_TASK_ELOG(param, ...) qError("QW:%p QID:%"PRIx64",TID:%"PRIx64 param, mgmt, qId, tId, __VA_ARGS__)
#define QW_TASK_WLOG(param, ...) qWarn("QW:%p QID:%"PRIx64",TID:%"PRIx64 param, mgmt, qId, tId, __VA_ARGS__)
#define QW_TASK_DLOG(param, ...) qDebug("QW:%p QID:%"PRIx64",TID:%"PRIx64 param, mgmt, qId, tId, __VA_ARGS__)

#define QW_SCH_TASK_ELOG(param, ...) qError("QW:%p SID:%"PRIx64",QID:%"PRIx64",TID:%"PRIx64 param, mgmt, sId, qId, tId, __VA_ARGS__)
#define QW_SCH_TASK_WLOG(param, ...) qWarn("QW:%p SID:%"PRIx64",QID:%"PRIx64",TID:%"PRIx64 param, mgmt, sId, qId, tId, __VA_ARGS__)
#define QW_SCH_TASK_DLOG(param, ...) qDebug("QW:%p SID:%"PRIx64",QID:%"PRIx64",TID:%"PRIx64 param, mgmt, sId, qId, tId, __VA_ARGS__)


#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define QW_LOCK(type, _lock) do {   \
  if (QW_READ == (type)) {          \
    assert(atomic_load_32((_lock)) >= 0);  \
    qDebug("QW RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRLockLatch(_lock);           \
    qDebug("QW RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) > 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) >= 0);  \
    qDebug("QW WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    taosWLockLatch(_lock);                                \
    qDebug("QW WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
  }                                                       \
} while (0)
    
#define QW_UNLOCK(type, _lock) do {                       \
  if (QW_READ == (type)) {                                \
    assert(atomic_load_32((_lock)) > 0);  \
    qDebug("QW RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRUnLockLatch(_lock);                              \
    qDebug("QW RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
    qDebug("QW WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosWUnLockLatch(_lock);                              \
    qDebug("QW WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  }                                                       \
} while (0)



int32_t qwAcquireScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch);
int32_t qwAcquireAddScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch);
int32_t qwAcquireTask(SQWorkerMgmt *mgmt, int32_t rwType, SQWSchStatus *sch, uint64_t qId, uint64_t tId, SQWTaskStatus **task);


#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
