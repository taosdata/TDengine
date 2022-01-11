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
#define QWORKER_DEFAULT_RES_CACHE_NUMBER 10000
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

typedef struct SQWorkerTaskHandleCache {
  SRWLatch        lock;
  qTaskInfo_t     taskHandle;
  DataSinkHandle  sinkHandle;
} SQWorkerTaskHandleCache;

typedef struct SQWSchStatus {
  int32_t   lastAccessTs; // timestamp in second
  SRWLatch  tasksLock;
  SHashObj *tasksHash;   // key:queryId+taskId, value: SQWTaskStatus
} SQWSchStatus;

// Qnode/Vnode level task management
typedef struct SQWorkerMgmt {
  SQWorkerCfg cfg;
  SRWLatch  schLock;
  SRWLatch  resLock;
  SHashObj *schHash;    //key: schedulerId, value: SQWSchStatus
  SHashObj *resHash;       //key: queryId+taskId, value: SQWorkerResCache
} SQWorkerMgmt;

#define QW_GOT_RES_DATA(data) (true)
#define QW_LOW_RES_DATA(data) (false)

#define QW_TASK_NOT_EXIST(code) (TSDB_CODE_QRY_SCH_NOT_EXIST == (code) || TSDB_CODE_QRY_TASK_NOT_EXIST == (code))
#define QW_TASK_ALREADY_EXIST(code) (TSDB_CODE_QRY_TASK_ALREADY_EXIST == (code))
#define QW_TASK_READY_RESP(status) (status == JOB_TASK_STATUS_SUCCEED || status == JOB_TASK_STATUS_FAILED || status == JOB_TASK_STATUS_CANCELLED || status == JOB_TASK_STATUS_PARTIAL_SUCCEED)
#define QW_SET_QTID(id, qid, tid) do { *(uint64_t *)(id) = (qid); *(uint64_t *)((char *)(id) + sizeof(qid)) = (tid); } while (0)
#define QW_GET_QTID(id, qid, tid) do { (qid) = *(uint64_t *)(id); (tid) = *(uint64_t *)((char *)(id) + sizeof(qid)); } while (0)


#define QW_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define QW_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define QW_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); terrno = _code; return _code; } } while (0)
#define QW_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define QW_LOCK(type, _lock) do {   \
  if (QW_READ == (type)) {          \
    if ((*(_lock)) < 0) assert(0);    \
    taosRLockLatch(_lock);          \
    qDebug("QW RLOCK%p, %s:%d", (_lock), __FILE__, __LINE__); \
  } else {                                                \
    if ((*(_lock)) < 0) assert(0);                          \
    taosWLockLatch(_lock);                                \
    qDebug("QW WLOCK%p, %s:%d", (_lock), __FILE__, __LINE__);  \
  }                                                       \
} while (0)

#define QW_UNLOCK(type, _lock) do {                       \
  if (QW_READ == (type)) {                                \
    if ((*(_lock)) <= 0) assert(0);                         \
    taosRUnLockLatch(_lock);                              \
    qDebug("QW RULOCK%p, %s:%d", (_lock), __FILE__, __LINE__); \
  } else {                                                \
    if ((*(_lock)) <= 0) assert(0);                         \
    taosWUnLockLatch(_lock);                              \
    qDebug("QW WULOCK%p, %s:%d", (_lock), __FILE__, __LINE__); \
  }                                                       \
} while (0)

static int32_t qwAcquireScheduler(int32_t rwType, SQWorkerMgmt *mgmt, uint64_t sId, SQWSchStatus **sch, int32_t nOpt);


#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
