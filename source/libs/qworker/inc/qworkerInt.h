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

#define QWORKER_DEFAULT_SCHEDULER_NUMBER 10000
#define QWORKER_DEFAULT_RES_CACHE_NUMBER 10000
#define QWORKER_DEFAULT_SCH_TASK_NUMBER 10000

enum {
  QW_READY_NOT_RECEIVED = 0,
  QW_READY_RECEIVED,
  QW_READY_RESPONSED,
};

typedef struct SQWorkerTaskStatus {
  int8_t status;
  int8_t ready; 
} SQWorkerTaskStatus;

typedef struct SQWorkerResCache {
  void *data;
} SQWorkerResCache;

typedef struct SQWorkerSchTaskStatus {
  int32_t   lastAccessTs; // timestamp in second
  SHashObj *taskStatus;   // key:queryId+taskId, value: SQWorkerTaskStatus
} SQWorkerSchTaskStatus;

// Qnode/Vnode level task management
typedef struct SQWorkerMgmt {
  SQWorkerCfg cfg;
  SHashObj *scheduleHash;    //key: schedulerId, value: SQWorkerSchTaskStatus
  SHashObj *resHash;       //key: queryId+taskId, value: SQWorkerResCache
} SQWorkerMgmt;

#define QW_TASK_DONE(status) (status == JOB_TASK_STATUS_SUCCEED || status == JOB_TASK_STATUS_FAILED || status == status == JOB_TASK_STATUS_CANCELLED)
#define QW_SET_QTID(id, qid, tid) do { *(uint64_t *)(id) = (qid); *(uint64_t *)((char *)(id) + sizeof(qid)) = (tid); } while (0)
#define QW_GET_QTID(id, qid, tid) do { (qid) = *(uint64_t *)(id); (tid) = *(uint64_t *)((char *)(id) + sizeof(qid)); } while (0)

#define QW_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define QW_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define QW_ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); terrno = _code; return _code; } } while (0)
#define QW_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
