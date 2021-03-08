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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tqueue.h"
#include "tnote.h"
#include "taos.h"
#include "tsclient.h"
#include "httpInt.h"
#include "httpContext.h"
#include "httpSql.h"
#include "httpResp.h"
#include "httpAuth.h"
#include "httpSession.h"
#include "httpQueue.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SHttpWorker;

typedef struct {
  int32_t      num;
  SHttpWorker *httpWorker;
} SHttpWorkerPool;

typedef struct {
  void *        param;
  void *        result;
  int32_t       code;
  int32_t       rows;
  FHttpResultFp fp;
} SHttpResult;

static SHttpWorkerPool tsHttpPool;
static taos_qset       tsHttpQset;
static taos_queue      tsHttpQueue;

void httpDispatchToResultQueue(void *param, TAOS_RES *result, int32_t code, int32_t rows, FHttpResultFp fp) {
  if (tsHttpQueue != NULL) {
    SHttpResult *pMsg = taosAllocateQitem(sizeof(SHttpResult));
    pMsg->param = param;
    pMsg->result = result;
    pMsg->code = code;
    pMsg->rows = rows;
    pMsg->fp = fp;
    taosWriteQitem(tsHttpQueue, TAOS_QTYPE_RPC, pMsg);
  } else {
    taos_stop_query(result);
    taos_free_result(result);
    //(*fp)(param, result, code, rows);
  }
}

static void *httpProcessResultQueue(void *param) {
  SHttpResult *pMsg;
  int32_t      type;
  void *       unUsed;

  while (1) {
    if (taosReadQitemFromQset(tsHttpQset, &type, (void **)&pMsg, &unUsed) == 0) {
      httpDebug("qset:%p, http queue got no message from qset, exiting", tsHttpQset);
      break;
    }

    httpTrace("context:%p, res:%p will be processed in result queue, code:%d rows:%d", pMsg->param, pMsg->result,
              pMsg->code, pMsg->rows);
    (*pMsg->fp)(pMsg->param, pMsg->result, pMsg->code, pMsg->rows);
    taosFreeQitem(pMsg);
  }

  return NULL;
}

static bool httpAllocateResultQueue() {
  tsHttpQueue = taosOpenQueue();
  if (tsHttpQueue == NULL) return false;

  taosAddIntoQset(tsHttpQset, tsHttpQueue, NULL);

  for (int32_t i = 0; i < tsHttpPool.num; ++i) {
    SHttpWorker *pWorker = tsHttpPool.httpWorker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, httpProcessResultQueue, pWorker) != 0) {
      httpError("failed to create thread to process http result queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
    httpDebug("http result worker:%d is launched, total:%d", pWorker->workerId, tsHttpPool.num);
  }

  httpInfo("http result queue is opened");
  return true;
}

static void httpFreeResultQueue() {
  taosCloseQueue(tsHttpQueue);
  tsHttpQueue = NULL;
}

bool httpInitResultQueue() {
  tsHttpQset = taosOpenQset();

  tsHttpPool.num = tsHttpMaxThreads;
  tsHttpPool.httpWorker = (SHttpWorker *)calloc(sizeof(SHttpWorker), tsHttpPool.num);

  if (tsHttpPool.httpWorker == NULL) return -1;
  for (int32_t i = 0; i < tsHttpPool.num; ++i) {
    SHttpWorker *pWorker = tsHttpPool.httpWorker + i;
    pWorker->workerId = i;
  }

  return httpAllocateResultQueue();
}

void httpCleanupResultQueue() {
  httpFreeResultQueue();

  for (int32_t i = 0; i < tsHttpPool.num; ++i) {
    SHttpWorker *pWorker = tsHttpPool.httpWorker + i;
    if (taosCheckPthreadValid(pWorker->thread)) {
      taosQsetThreadResume(tsHttpQset);
    }
  }

  for (int32_t i = 0; i < tsHttpPool.num; ++i) {
    SHttpWorker *pWorker = tsHttpPool.httpWorker + i;
    if (taosCheckPthreadValid(pWorker->thread)) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  taosCloseQset(tsHttpQset);
  free(tsHttpPool.httpWorker);

  httpInfo("http result queue is closed");
}
