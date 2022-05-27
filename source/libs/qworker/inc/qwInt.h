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

#include "osDef.h"
#include "qworker.h"
#include "tlockfree.h"
#include "ttimer.h"
#include "tref.h"
#include "plannodes.h"
#include "executor.h"
#include "trpc.h"

#define QW_DEFAULT_SCHEDULER_NUMBER 10000
#define QW_DEFAULT_TASK_NUMBER      10000
#define QW_DEFAULT_SCH_TASK_NUMBER  10000
#define QW_DEFAULT_SHORT_RUN_TIMES  2
#define QW_DEFAULT_HEARTBEAT_MSEC   3000

enum {
  QW_PHASE_PRE_QUERY = 1,
  QW_PHASE_POST_QUERY,
  QW_PHASE_PRE_FETCH,
  QW_PHASE_POST_FETCH,
  QW_PHASE_PRE_CQUERY,
  QW_PHASE_POST_CQUERY,
};

enum {
  QW_EVENT_CANCEL = 1,
  QW_EVENT_READY,
  QW_EVENT_FETCH,
  QW_EVENT_DROP,
  QW_EVENT_CQUERY,

  QW_EVENT_MAX,
};

enum {
  QW_EVENT_NOT_RECEIVED = 0,
  QW_EVENT_RECEIVED,
  QW_EVENT_PROCESSED,
};

enum {
  QW_READ = 1,
  QW_WRITE,
};

enum {
  QW_NOT_EXIST_RET_ERR = 1,
  QW_NOT_EXIST_ADD,
};

typedef struct SQWDebug {
  bool lockEnable;
  bool statusEnable;
  bool dumpEnable;
} SQWDebug;

extern SQWDebug gQWDebug;

typedef struct SQWMsg {
  void          *node;
  int32_t        code;
  char          *msg;
  int32_t        msgLen;
  SRpcHandleInfo connInfo;
} SQWMsg;

typedef struct SQWHbParam {
  bool    inUse;
  int32_t qwrId;
  int64_t refId;
} SQWHbParam;

typedef struct SQWHbInfo {
  SSchedulerHbRsp rsp;
  SRpcHandleInfo  connInfo;
} SQWHbInfo;

typedef struct SQWPhaseInput {
  int32_t code;
} SQWPhaseInput;

typedef struct SQWPhaseOutput {
#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif
} SQWPhaseOutput;

typedef struct SQWTaskStatus {
  int64_t refId;  // job's refId
  int32_t code;
  int8_t  status;
} SQWTaskStatus;

typedef struct SQWTaskCtx {
  SRWLatch lock;
  int8_t   phase;
  int8_t   taskType;
  int8_t   explain;

  bool    queryFetched;
  bool    queryEnd;
  bool    queryContinue;
  bool    queryInQueue;
  int32_t rspCode;

  SRpcHandleInfo ctrlConnInfo;
  SRpcHandleInfo dataConnInfo;

  int8_t events[QW_EVENT_MAX];

  void     *taskHandle;
  void     *sinkHandle;
  SSubplan *plan;
  STbVerInfo tbInfo;
} SQWTaskCtx;

typedef struct SQWSchStatus {
  int32_t        lastAccessTs;  // timestamp in second
  SRWLatch       hbConnLock;
  SRpcHandleInfo hbConnInfo;
  SQueryNodeEpId hbEpId;
  SRWLatch       tasksLock;
  SHashObj      *tasksHash;  // key:queryId+taskId, value: SQWTaskStatus
} SQWSchStatus;

// Qnode/Vnode level task management
typedef struct SQWorker {
  int64_t     refId;
  SQWorkerCfg cfg;
  int8_t      nodeType;
  int32_t     nodeId;
  void *      timer;
  tmr_h       hbTimer;
  SRWLatch    schLock;
  // SRWLatch ctxLock;
  SHashObj *schHash;  // key: schedulerId,    value: SQWSchStatus
  SHashObj *ctxHash;  // key: queryId+taskId, value: SQWTaskCtx
  SMsgCb    msgCb;
} SQWorker;

typedef struct SQWorkerMgmt {
  SRWLatch   lock;
  int32_t    qwRef;
  int32_t    qwNum;
  SQWHbParam param[1024];
  int32_t    paramIdx;
} SQWorkerMgmt;

#define QW_FPARAMS_DEF SQWorker *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, int64_t rId
#define QW_IDS()       sId, qId, tId, rId
#define QW_FPARAMS()   mgmt, QW_IDS()

#define QW_GET_EVENT_VALUE(ctx, event) atomic_load_8(&(ctx)->events[event])

#define QW_IS_EVENT_RECEIVED(ctx, event)   (atomic_load_8(&(ctx)->events[event]) == QW_EVENT_RECEIVED)
#define QW_IS_EVENT_PROCESSED(ctx, event)  (atomic_load_8(&(ctx)->events[event]) == QW_EVENT_PROCESSED)
#define QW_SET_EVENT_RECEIVED(ctx, event)  atomic_store_8(&(ctx)->events[event], QW_EVENT_RECEIVED)
#define QW_SET_EVENT_PROCESSED(ctx, event) atomic_store_8(&(ctx)->events[event], QW_EVENT_PROCESSED)

#define QW_GET_PHASE(ctx) atomic_load_8(&(ctx)->phase)

#define QW_SET_RSP_CODE(ctx, code)    atomic_store_32(&(ctx)->rspCode, code)
#define QW_UPDATE_RSP_CODE(ctx, code) atomic_val_compare_exchange_32(&(ctx)->rspCode, 0, code)

#define QW_IS_QUERY_RUNNING(ctx) (QW_GET_PHASE(ctx) == QW_PHASE_PRE_QUERY || QW_GET_PHASE(ctx) == QW_PHASE_PRE_CQUERY)

#define QW_TASK_NOT_EXIST(code)     (TSDB_CODE_QRY_SCH_NOT_EXIST == (code) || TSDB_CODE_QRY_TASK_NOT_EXIST == (code))
#define QW_TASK_ALREADY_EXIST(code) (TSDB_CODE_QRY_TASK_ALREADY_EXIST == (code))
#define QW_TASK_READY(status)                                                                                      \
  (status == JOB_TASK_STATUS_SUCCEED || status == JOB_TASK_STATUS_FAILED || status == JOB_TASK_STATUS_CANCELLED || \
   status == JOB_TASK_STATUS_PARTIAL_SUCCEED)
#define QW_SET_QTID(id, qId, tId)                      \
  do {                                                 \
    *(uint64_t *)(id) = (qId);                         \
    *(uint64_t *)((char *)(id) + sizeof(qId)) = (tId); \
  } while (0)
#define QW_GET_QTID(id, qId, tId)                      \
  do {                                                 \
    (qId) = *(uint64_t *)(id);                         \
    (tId) = *(uint64_t *)((char *)(id) + sizeof(qId)); \
  } while (0)

#define QW_ERR_RET(c)                 \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
#define QW_RET(c)                     \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
#define QW_ERR_JRET(c)               \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

#define QW_ELOG(_param, ...) qError("QW:%p " _param, mgmt, __VA_ARGS__)
#define QW_DLOG(_param, ...) qDebug("QW:%p " _param, mgmt, __VA_ARGS__)
#define QW_TLOG(_param, ...) qTrace("QW:%p " _param, mgmt, __VA_ARGS__)

#define QW_DUMP(_param, ...)                      \
  do {                                           \
    if (gQWDebug.dumpEnable) {                   \
      qDebug("QW:%p " _param, mgmt, __VA_ARGS__); \
    }                                            \
  } while (0)

#define QW_SCH_ELOG(param, ...) qError("QW:%p SID:%" PRIx64 " " param, mgmt, sId, __VA_ARGS__)
#define QW_SCH_DLOG(param, ...) qDebug("QW:%p SID:%" PRIx64 " " param, mgmt, sId, __VA_ARGS__)

#define QW_TASK_ELOG(param, ...) qError("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId, __VA_ARGS__)
#define QW_TASK_WLOG(param, ...) qWarn("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId, __VA_ARGS__)
#define QW_TASK_DLOG(param, ...) qDebug("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId, __VA_ARGS__)
#define QW_TASK_DLOGL(param, ...) \
  qDebugL("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId, __VA_ARGS__)

#define QW_TASK_ELOG_E(param) qError("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId)
#define QW_TASK_WLOG_E(param) qWarn("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId)
#define QW_TASK_DLOG_E(param) qDebug("QW:%p QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, qId, tId)

#define QW_SCH_TASK_ELOG(param, ...) \
  qError("QW:%p SID:0x%" PRIx64 ",QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, sId, qId, tId, __VA_ARGS__)
#define QW_SCH_TASK_WLOG(param, ...) \
  qWarn("QW:%p SID:0x%" PRIx64 ",QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, sId, qId, tId, __VA_ARGS__)
#define QW_SCH_TASK_DLOG(param, ...) \
  qDebug("QW:%p SID:0x%" PRIx64 ",QID:0x%" PRIx64 ",TID:0x%" PRIx64 " " param, mgmt, sId, qId, tId, __VA_ARGS__)

#define QW_LOCK_DEBUG(...)     \
  do {                         \
    if (gQWDebug.lockEnable) { \
      qDebug(__VA_ARGS__);     \
    }                          \
  } while (0)

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define QW_LOCK(type, _lock)                                                                       \
  do {                                                                                             \
    if (QW_READ == (type)) {                                                                       \
      assert(atomic_load_32((_lock)) >= 0);                                                        \
      QW_LOCK_DEBUG("QW RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      taosRLockLatch(_lock);                                                                       \
      QW_LOCK_DEBUG("QW RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      assert(atomic_load_32((_lock)) > 0);                                                         \
    } else {                                                                                       \
      assert(atomic_load_32((_lock)) >= 0);                                                        \
      QW_LOCK_DEBUG("QW WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      taosWLockLatch(_lock);                                                                       \
      QW_LOCK_DEBUG("QW WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);                               \
    }                                                                                              \
  } while (0)

#define QW_UNLOCK(type, _lock)                                                                      \
  do {                                                                                              \
    if (QW_READ == (type)) {                                                                        \
      assert(atomic_load_32((_lock)) > 0);                                                          \
      QW_LOCK_DEBUG("QW RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      taosRUnLockLatch(_lock);                                                                      \
      QW_LOCK_DEBUG("QW RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      assert(atomic_load_32((_lock)) >= 0);                                                         \
    } else {                                                                                        \
      assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);                                \
      QW_LOCK_DEBUG("QW WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      taosWUnLockLatch(_lock);                                                                      \
      QW_LOCK_DEBUG("QW WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      assert(atomic_load_32((_lock)) >= 0);                                                         \
    }                                                                                               \
  } while (0)


extern SQWorkerMgmt gQwMgmt;

static FORCE_INLINE SQWorker *qwAcquire(int64_t refId) { return (SQWorker *)taosAcquireRef(atomic_load_32(&gQwMgmt.qwRef), refId); }
static FORCE_INLINE int32_t qwRelease(int64_t refId) { return taosReleaseRef(gQwMgmt.qwRef, refId); }

char *qwPhaseStr(int32_t phase);
char *qwBufStatusStr(int32_t bufStatus);
int32_t qwAcquireAddScheduler(SQWorker *mgmt, uint64_t sId, int32_t rwType, SQWSchStatus **sch);
void qwReleaseScheduler(int32_t rwType, SQWorker *mgmt);
int32_t qwAddTaskStatus(QW_FPARAMS_DEF, int32_t status);
int32_t qwAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx);
int32_t qwGetTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx);
int32_t qwAddAcquireTaskCtx(QW_FPARAMS_DEF, SQWTaskCtx **ctx);
void qwReleaseTaskCtx(SQWorker *mgmt, void *ctx);
int32_t qwKillTaskHandle(QW_FPARAMS_DEF, SQWTaskCtx *ctx);
int32_t qwUpdateTaskStatus(QW_FPARAMS_DEF, int8_t status);
int32_t qwDropTask(QW_FPARAMS_DEF);
void qwSaveTbVersionInfo(qTaskInfo_t       pTaskInfo, SQWTaskCtx *ctx);
int32_t qwOpenRef(void);
void qwSetHbParam(int64_t refId, SQWHbParam **pParam);

void qwDbgDumpMgmtInfo(SQWorker *mgmt);
int32_t qwDbgValidateStatus(QW_FPARAMS_DEF, int8_t oriStatus, int8_t newStatus, bool *ignore);


#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
