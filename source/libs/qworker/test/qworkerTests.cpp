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

#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"
#include <addr_any.h>

#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif
#include "os.h"

#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "qworker.h"
#include "stub.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "trpc.h"
#include "tvariant.h"

namespace {

#define qwtTestQueryQueueSize 1000000
#define qwtTestFetchQueueSize 1000000

bool qwtEnableLog = true;

int32_t qwtTestMaxExecTaskUsec = 2;
int32_t qwtTestReqMaxDelayUsec = 2;

int64_t  qwtTestQueryId = 0;
bool     qwtTestEnableSleep = true;
bool     qwtTestStop = false;
bool     qwtTestDeadLoop = false;
int32_t  qwtTestMTRunSec = 2;
int32_t  qwtTestPrintNum = 10000;
uint64_t qwtTestCaseIdx = 0;
uint64_t qwtTestCaseNum = 4;
bool     qwtTestCaseFinished = false;
tsem_t   qwtTestQuerySem;
tsem_t   qwtTestFetchSem;
int32_t  qwtTestQuitThreadNum = 0;

int32_t         qwtTestQueryQueueRIdx = 0;
int32_t         qwtTestQueryQueueWIdx = 0;
int32_t         qwtTestQueryQueueNum = 0;
SRWLatch        qwtTestQueryQueueLock = 0;
struct SRpcMsg *qwtTestQueryQueue[qwtTestQueryQueueSize] = {0};

int32_t         qwtTestFetchQueueRIdx = 0;
int32_t         qwtTestFetchQueueWIdx = 0;
int32_t         qwtTestFetchQueueNum = 0;
SRWLatch        qwtTestFetchQueueLock = 0;
struct SRpcMsg *qwtTestFetchQueue[qwtTestFetchQueueSize] = {0};

int32_t  qwtTestSinkBlockNum = 0;
int32_t  qwtTestSinkMaxBlockNum = 0;
bool     qwtTestSinkQueryEnd = false;
SRWLatch qwtTestSinkLock = 0;
int32_t  qwtTestSinkLastLen = 0;

SSubQueryMsg       qwtqueryMsg = {0};
SRpcMsg            qwtfetchRpc = {0};
SResFetchReq       qwtfetchMsg = {0};
SRpcMsg            qwtreadyRpc = {0};
SResReadyReq       qwtreadyMsg = {0};
SRpcMsg            qwtdropRpc = {0};
STaskDropReq       qwtdropMsg = {0};
SSchTasksStatusReq qwtstatusMsg = {0};

void qwtInitLogFile() {
  if (!qwtEnableLog) {
    return;
  }
  const char   *defaultLogFileNamePrefix = "taosdlog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

void qwtBuildQueryReqMsg(SRpcMsg *queryRpc) {
  qwtqueryMsg.queryId = htobe64(atomic_add_fetch_64(&qwtTestQueryId, 1));
  qwtqueryMsg.sId = htobe64(1);
  qwtqueryMsg.taskId = htobe64(1);
  qwtqueryMsg.msgLen = htonl(100);
  qwtqueryMsg.sqlLen = 0;
  queryRpc->msgType = TDMT_SCH_QUERY;
  queryRpc->pCont = &qwtqueryMsg;
  queryRpc->contLen = sizeof(SSubQueryMsg) + 100;
}

void qwtBuildFetchReqMsg(SResFetchReq *fetchMsg, SRpcMsg *fetchRpc) {
  fetchMsg->sId = htobe64(1);
  fetchMsg->queryId = htobe64(atomic_load_64(&qwtTestQueryId));
  fetchMsg->taskId = htobe64(1);
  fetchRpc->msgType = TDMT_SCH_FETCH;
  fetchRpc->pCont = fetchMsg;
  fetchRpc->contLen = sizeof(SResFetchReq);
}

void qwtBuildDropReqMsg(STaskDropReq *dropMsg, SRpcMsg *dropRpc) {
  dropMsg->sId = 1;
  dropMsg->queryId = atomic_load_64(&qwtTestQueryId);
  dropMsg->taskId = 1;
  
  int32_t msgSize = tSerializeSTaskDropReq(NULL, 0, dropMsg);
  if (msgSize < 0) {
    return;
  }
  
  char *msg = (char*)taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    return;
  }
  
  if (tSerializeSTaskDropReq(msg, msgSize, dropMsg) < 0) {
    taosMemoryFree(msg);
    return;
  }


  dropRpc->msgType = TDMT_SCH_DROP_TASK;
  dropRpc->pCont = msg;
  dropRpc->contLen = msgSize;
}

int32_t qwtStringToPlan(const char *str, SSubplan **subplan) {
  *subplan = (SSubplan *)0x1;
  return 0;
}

int32_t qwtPutReqToFetchQueue(void *node, struct SRpcMsg *pMsg) {
  taosWLockLatch(&qwtTestFetchQueueLock);
  struct SRpcMsg *newMsg = (struct SRpcMsg *)taosMemoryCalloc(1, sizeof(struct SRpcMsg));
  memcpy(newMsg, pMsg, sizeof(struct SRpcMsg));
  qwtTestFetchQueue[qwtTestFetchQueueWIdx++] = newMsg;
  if (qwtTestFetchQueueWIdx >= qwtTestFetchQueueSize) {
    qwtTestFetchQueueWIdx = 0;
  }

  qwtTestFetchQueueNum++;

  if (qwtTestFetchQueueWIdx == qwtTestFetchQueueRIdx) {
    printf("Fetch queue is full");
    assert(0);
  }
  taosWUnLockLatch(&qwtTestFetchQueueLock);

  tsem_post(&qwtTestFetchSem);

  return 0;
}

int32_t qwtPutReqToQueue(void *node, EQueueType qtype, struct SRpcMsg *pMsg) {
  taosWLockLatch(&qwtTestQueryQueueLock);
  struct SRpcMsg *newMsg = (struct SRpcMsg *)taosMemoryCalloc(1, sizeof(struct SRpcMsg));
  memcpy(newMsg, pMsg, sizeof(struct SRpcMsg));
  qwtTestQueryQueue[qwtTestQueryQueueWIdx++] = newMsg;
  if (qwtTestQueryQueueWIdx >= qwtTestQueryQueueSize) {
    qwtTestQueryQueueWIdx = 0;
  }

  qwtTestQueryQueueNum++;

  if (qwtTestQueryQueueWIdx == qwtTestQueryQueueRIdx) {
    printf("query queue is full");
    assert(0);
  }
  taosWUnLockLatch(&qwtTestQueryQueueLock);

  tsem_post(&qwtTestQuerySem);

  return 0;
}

void qwtSendReqToDnode(void *pVnode, struct SEpSet *epSet, struct SRpcMsg *pReq) {}

void qwtRpcSendResponse(const SRpcMsg *pRsp) {
  switch (pRsp->msgType) {
    case TDMT_SCH_QUERY_RSP:
    case TDMT_SCH_MERGE_QUERY_RSP: {
      SQueryTableRsp *rsp = (SQueryTableRsp *)pRsp->pCont;

      if (pRsp->code) {
        qwtBuildDropReqMsg(&qwtdropMsg, &qwtdropRpc);
        qwtPutReqToFetchQueue((void *)0x1, &qwtdropRpc);
      }

      rpcFreeCont(rsp);
      break;
    }
    case TDMT_SCH_FETCH_RSP:
    case TDMT_SCH_MERGE_FETCH_RSP: {
      SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)pRsp->pCont;

      if (0 == pRsp->code && 0 == rsp->completed) {
        qwtBuildFetchReqMsg(&qwtfetchMsg, &qwtfetchRpc);
        qwtPutReqToFetchQueue((void *)0x1, &qwtfetchRpc);
        rpcFreeCont(rsp);
        return;
      }

      qwtBuildDropReqMsg(&qwtdropMsg, &qwtdropRpc);
      qwtPutReqToFetchQueue((void *)0x1, &qwtdropRpc);
      rpcFreeCont(rsp);

      break;
    }
    case TDMT_SCH_DROP_TASK_RSP: {
      STaskDropRsp *rsp = (STaskDropRsp *)pRsp->pCont;
      rpcFreeCont(rsp);

      qwtTestCaseFinished = true;
      break;
    }
  }

  return;
}

int32_t qwtCreateExecTask(void *tsdb, int32_t vgId, uint64_t taskId, struct SSubplan *pPlan, qTaskInfo_t *pTaskInfo,
                          DataSinkHandle *handle) {
  qwtTestSinkBlockNum = 0;
  qwtTestSinkMaxBlockNum = taosRand() % 100 + 1;
  qwtTestSinkQueryEnd = false;

  *pTaskInfo = (qTaskInfo_t)((char *)qwtTestCaseIdx + 1);
  *handle = (DataSinkHandle)((char *)qwtTestCaseIdx + 2);

  ++qwtTestCaseIdx;

  return 0;
}

int32_t qwtExecTask(qTaskInfo_t tinfo, SSDataBlock **pRes, uint64_t *useconds) {
  int32_t endExec = 0;

  if (NULL == tinfo) {
    *pRes = NULL;
    *useconds = 0;
  } else {
    if (qwtTestSinkQueryEnd) {
      *pRes = NULL;
      *useconds = taosRand() % 10;
      return 0;
    }

    endExec = taosRand() % 5;

    int32_t runTime = 0;
    if (qwtTestEnableSleep && qwtTestMaxExecTaskUsec > 0) {
      runTime = taosRand() % qwtTestMaxExecTaskUsec;
    }

    if (qwtTestEnableSleep) {
      if (runTime) {
        taosUsleep(runTime);
      }
    }

    if (endExec) {
      *pRes = (SSDataBlock *)taosMemoryCalloc(1, sizeof(SSDataBlock));
      (*pRes)->info.rows = taosRand() % 1000 + 1;
    } else {
      *pRes = NULL;
      *useconds = taosRand() % 10;
    }
  }

  return 0;
}

int32_t qwtKillTask(qTaskInfo_t qinfo, int32_t rspCode) { return 0; }

void qwtDestroyTask(qTaskInfo_t qHandle) {}

int32_t qwtPutDataBlock(DataSinkHandle handle, const SInputData *pInput, bool *pContinue) {
  if (NULL == handle || NULL == pInput || NULL == pContinue) {
    assert(0);
  }

  taosMemoryFree((void *)pInput->pData);

  taosWLockLatch(&qwtTestSinkLock);

  qwtTestSinkBlockNum++;

  if (qwtTestSinkBlockNum >= qwtTestSinkMaxBlockNum) {
    *pContinue = false;
  } else {
    *pContinue = true;
  }
  taosWUnLockLatch(&qwtTestSinkLock);

  return 0;
}

void qwtEndPut(DataSinkHandle handle, uint64_t useconds) {
  if (NULL == handle) {
    assert(0);
  }

  qwtTestSinkQueryEnd = true;
}

void qwtGetDataLength(DataSinkHandle handle, int64_t *pLen, bool *pQueryEnd) {
  static int32_t in = 0;

  if (in > 0) {
    assert(0);
  }

  atomic_add_fetch_32(&in, 1);

  if (NULL == handle) {
    assert(0);
  }

  taosWLockLatch(&qwtTestSinkLock);
  if (qwtTestSinkBlockNum > 0) {
    *pLen = taosRand() % 100 + 1;
    qwtTestSinkBlockNum--;
  } else {
    *pLen = 0;
  }
  qwtTestSinkLastLen = *pLen;
  taosWUnLockLatch(&qwtTestSinkLock);

  *pQueryEnd = qwtTestSinkQueryEnd;

  atomic_sub_fetch_32(&in, 1);
}

int32_t qwtGetDataBlock(DataSinkHandle handle, SOutputData *pOutput) {
  taosWLockLatch(&qwtTestSinkLock);
  if (qwtTestSinkLastLen > 0) {
    pOutput->numOfRows = taosRand() % 10 + 1;
    pOutput->compressed = 1;
    pOutput->queryEnd = qwtTestSinkQueryEnd;
    if (qwtTestSinkBlockNum == 0) {
      pOutput->bufStatus = DS_BUF_EMPTY;
    } else if (qwtTestSinkBlockNum <= qwtTestSinkMaxBlockNum * 0.5) {
      pOutput->bufStatus = DS_BUF_LOW;
    } else {
      pOutput->bufStatus = DS_BUF_FULL;
    }
    pOutput->useconds = taosRand() % 10 + 1;
    pOutput->precision = 1;
  } else if (qwtTestSinkLastLen == 0) {
    pOutput->numOfRows = 0;
    pOutput->compressed = 1;
    pOutput->pData = NULL;
    pOutput->queryEnd = qwtTestSinkQueryEnd;
    if (qwtTestSinkBlockNum == 0) {
      pOutput->bufStatus = DS_BUF_EMPTY;
    } else if (qwtTestSinkBlockNum <= qwtTestSinkMaxBlockNum * 0.5) {
      pOutput->bufStatus = DS_BUF_LOW;
    } else {
      pOutput->bufStatus = DS_BUF_FULL;
    }
    pOutput->useconds = taosRand() % 10 + 1;
    pOutput->precision = 1;
  } else {
    assert(0);
  }
  taosWUnLockLatch(&qwtTestSinkLock);

  return 0;
}

void qwtDestroyDataSinker(DataSinkHandle handle) {}

void stubSetStringToPlan() {
  static Stub stub;
  stub.set(qStringToSubplan, qwtStringToPlan);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qStringToSubplan", result);
#endif
#ifdef LINUX
    AddrAny                       any("libplanner.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qStringToSubplan$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtStringToPlan);
    }
  }
}

void stubSetExecTask() {
  static Stub stub;
  stub.set(qExecTask, qwtExecTask);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qExecTask", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qExecTask$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtExecTask);
    }
  }
}

void stubSetCreateExecTask() {
  static Stub stub;
  stub.set(qCreateExecTask, qwtCreateExecTask);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qCreateExecTask", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qCreateExecTask$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtCreateExecTask);
    }
  }
}

void stubSetAsyncKillTask() {
  static Stub stub;
  stub.set(qAsyncKillTask, qwtKillTask);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qAsyncKillTask", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qAsyncKillTask$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtKillTask);
    }
  }
}

void stubSetDestroyTask() {
  static Stub stub;
  stub.set(qDestroyTask, qwtDestroyTask);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qDestroyTask", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qDestroyTask$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtDestroyTask);
    }
  }
}

void stubSetDestroyDataSinker() {
  static Stub stub;
  stub.set(dsDestroyDataSinker, qwtDestroyDataSinker);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("dsDestroyDataSinker", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^dsDestroyDataSinker$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtDestroyDataSinker);
    }
  }
}

void stubSetGetDataLength() {
  static Stub stub;
  stub.set(dsGetDataLength, qwtGetDataLength);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("dsGetDataLength", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^dsGetDataLength$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtGetDataLength);
    }
  }
}

void stubSetEndPut() {
  static Stub stub;
  stub.set(dsEndPut, qwtEndPut);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("dsEndPut", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^dsEndPut$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtEndPut);
    }
  }
}

void stubSetPutDataBlock() {
  static Stub stub;
  stub.set(dsPutDataBlock, qwtPutDataBlock);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("dsPutDataBlock", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^dsPutDataBlock$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtPutDataBlock);
    }
  }
}

void stubSetRpcSendResponse() {
  static Stub stub;
  stub.set(rpcSendResponse, qwtRpcSendResponse);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("rpcSendResponse", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendResponse$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtRpcSendResponse);
    }
  }
}

void stubSetGetDataBlock() {
  static Stub stub;
  stub.set(dsGetDataBlock, qwtGetDataBlock);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("dsGetDataBlock", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^dsGetDataBlock$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, qwtGetDataBlock);
    }
  }
}

void *queryThread(void *param) {
  SRpcMsg  queryRpc = {0};
  int32_t  code = 0;
  uint32_t n = 0;
  void    *mockPointer = (void *)0x1;
  void    *mgmt = param;

  while (!qwtTestStop) {
    qwtBuildQueryReqMsg(&queryRpc);
    qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc, 0);
    if (qwtTestEnableSleep) {
      taosUsleep(taosRand() % 5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("query:%d\n", n);
    }
  }

  return NULL;
}

void *fetchThread(void *param) {
  SRpcMsg      fetchRpc = {0};
  int32_t      code = 0;
  uint32_t     n = 0;
  void        *mockPointer = (void *)0x1;
  void        *mgmt = param;
  SResFetchReq fetchMsg = {0};

  while (!qwtTestStop) {
    qwtBuildFetchReqMsg(&fetchMsg, &fetchRpc);
    code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc, 0);
    if (qwtTestEnableSleep) {
      taosUsleep(taosRand() % 5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("fetch:%d\n", n);
    }
  }

  return NULL;
}

void *dropThread(void *param) {
  SRpcMsg      dropRpc = {0};
  int32_t      code = 0;
  uint32_t     n = 0;
  void        *mockPointer = (void *)0x1;
  void        *mgmt = param;
  STaskDropReq dropMsg = {0};

  while (!qwtTestStop) {
    qwtBuildDropReqMsg(&dropMsg, &dropRpc);
    code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc, 0);
    if (qwtTestEnableSleep) {
      taosUsleep(taosRand() % 5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("drop:%d\n", n);
    }
  }

  return NULL;
}

void *qwtclientThread(void *param) {
  int32_t  code = 0;
  uint32_t n = 0;
  void    *mgmt = param;
  void    *mockPointer = (void *)0x1;
  SRpcMsg  queryRpc = {0};

  taosSsleep(1);

  while (!qwtTestStop) {
    qwtTestCaseFinished = false;

    qwtBuildQueryReqMsg(&queryRpc);
    qwtPutReqToQueue((void *)0x1, QUERY_QUEUE, &queryRpc);

    while (!qwtTestCaseFinished) {
      taosUsleep(1);
    }

    if (++n % qwtTestPrintNum == 0) {
      printf("case run:%d\n", n);
    }
  }

  atomic_add_fetch_32(&qwtTestQuitThreadNum, 1);

  return NULL;
}

void *queryQueueThread(void *param) {
  void    *mockPointer = (void *)0x1;
  SRpcMsg *queryRpc = NULL;
  void    *mgmt = param;

  while (true) {
    tsem_wait(&qwtTestQuerySem);

    if (qwtTestStop && qwtTestQueryQueueNum <= 0 && qwtTestCaseFinished) {
      break;
    }

    taosWLockLatch(&qwtTestQueryQueueLock);
    if (qwtTestQueryQueueNum <= 0 || qwtTestQueryQueueRIdx == qwtTestQueryQueueWIdx) {
      printf("query queue is empty\n");
      assert(0);
    }

    queryRpc = qwtTestQueryQueue[qwtTestQueryQueueRIdx++];

    if (qwtTestQueryQueueRIdx >= qwtTestQueryQueueSize) {
      qwtTestQueryQueueRIdx = 0;
    }

    qwtTestQueryQueueNum--;
    taosWUnLockLatch(&qwtTestQueryQueueLock);

    if (qwtTestEnableSleep && qwtTestReqMaxDelayUsec > 0) {
      int32_t delay = taosRand() % qwtTestReqMaxDelayUsec;

      if (delay) {
        taosUsleep(delay);
      }
    }

    if (TDMT_SCH_QUERY == queryRpc->msgType) {
      qWorkerProcessQueryMsg(mockPointer, mgmt, queryRpc, 0);
    } else if (TDMT_SCH_QUERY_CONTINUE == queryRpc->msgType) {
      qWorkerProcessCQueryMsg(mockPointer, mgmt, queryRpc, 0);
    } else {
      printf("unknown msg in query queue, type:%d\n", queryRpc->msgType);
      assert(0);
    }

    taosMemoryFree(queryRpc);

    if (qwtTestStop && qwtTestQueryQueueNum <= 0 && qwtTestCaseFinished) {
      break;
    }
  }

  atomic_add_fetch_32(&qwtTestQuitThreadNum, 1);

  return NULL;
}

void *fetchQueueThread(void *param) {
  void    *mockPointer = (void *)0x1;
  SRpcMsg *fetchRpc = NULL;
  void    *mgmt = param;

  while (true) {
    tsem_wait(&qwtTestFetchSem);

    if (qwtTestStop && qwtTestFetchQueueNum <= 0 && qwtTestCaseFinished) {
      break;
    }

    taosWLockLatch(&qwtTestFetchQueueLock);
    if (qwtTestFetchQueueNum <= 0 || qwtTestFetchQueueRIdx == qwtTestFetchQueueWIdx) {
      printf("Fetch queue is empty\n");
      assert(0);
    }

    fetchRpc = qwtTestFetchQueue[qwtTestFetchQueueRIdx++];

    if (qwtTestFetchQueueRIdx >= qwtTestFetchQueueSize) {
      qwtTestFetchQueueRIdx = 0;
    }

    qwtTestFetchQueueNum--;
    taosWUnLockLatch(&qwtTestFetchQueueLock);

    if (qwtTestEnableSleep && qwtTestReqMaxDelayUsec > 0) {
      int32_t delay = taosRand() % qwtTestReqMaxDelayUsec;

      if (delay) {
        taosUsleep(delay);
      }
    }

    switch (fetchRpc->msgType) {
      case TDMT_SCH_FETCH:
      case TDMT_SCH_MERGE_FETCH:
        qWorkerProcessFetchMsg(mockPointer, mgmt, fetchRpc, 0);
        break;
      case TDMT_SCH_CANCEL_TASK:
        //qWorkerProcessCancelMsg(mockPointer, mgmt, fetchRpc, 0);
        break;
      case TDMT_SCH_DROP_TASK:
        qWorkerProcessDropMsg(mockPointer, mgmt, fetchRpc, 0);
        break;
      case TDMT_SCH_TASK_NOTIFY:
        qWorkerProcessNotifyMsg(mockPointer, mgmt, fetchRpc, 0);
        break;
      default:
        printf("unknown msg type:%d in fetch queue", fetchRpc->msgType);
        assert(0);
        break;
    }

    taosMemoryFree(fetchRpc);

    if (qwtTestStop && qwtTestFetchQueueNum <= 0 && qwtTestCaseFinished) {
      break;
    }
  }

  atomic_add_fetch_32(&qwtTestQuitThreadNum, 1);

  return NULL;
}

}  // namespace

TEST(seqTest, normalCase) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;
  SRpcMsg queryRpc = {0};
  SRpcMsg fetchRpc = {0};
  SRpcMsg dropRpc = {0};

  qwtInitLogFile();

  qwtBuildQueryReqMsg(&queryRpc);
  qwtBuildFetchReqMsg(&qwtfetchMsg, &fetchRpc);
  qwtBuildDropReqMsg(&qwtdropMsg, &dropRpc);

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetExecTask();
  stubSetCreateExecTask();
  stubSetAsyncKillTask();
  stubSetDestroyTask();
  stubSetDestroyDataSinker();
  stubSetGetDataLength();
  stubSetEndPut();
  stubSetPutDataBlock();
  stubSetGetDataBlock();

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc, 0);
  ASSERT_EQ(code, 0);

  // code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
  // ASSERT_EQ(code, 0);

  code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc, 0);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc, 0);
  ASSERT_EQ(code, 0);

  qWorkerDestroy(&mgmt);
}

TEST(seqTest, cancelFirst) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;
  SRpcMsg queryRpc = {0};
  SRpcMsg dropRpc = {0};

  qwtInitLogFile();

  qwtBuildQueryReqMsg(&queryRpc);
  qwtBuildDropReqMsg(&qwtdropMsg, &dropRpc);

  stubSetStringToPlan();
  stubSetRpcSendResponse();

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc, 0);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc, 0);
  ASSERT_TRUE(0 != code);

  qWorkerDestroy(&mgmt);
}

TEST(seqTest, randCase) {
  void              *mgmt = NULL;
  int32_t            code = 0;
  void              *mockPointer = (void *)0x1;
  SRpcMsg            queryRpc = {0};
  SRpcMsg            readyRpc = {0};
  SRpcMsg            fetchRpc = {0};
  SRpcMsg            dropRpc = {0};
  SRpcMsg            statusRpc = {0};
  SResReadyReq       readyMsg = {0};
  SResFetchReq       fetchMsg = {0};
  STaskDropReq       dropMsg = {0};
  SSchTasksStatusReq statusMsg = {0};

  qwtInitLogFile();

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetCreateExecTask();

  taosSeedRand(taosGetTimestampSec());

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  int32_t t = 0;
  int32_t maxr = 10001;
  while (true) {
    int32_t r = taosRand() % maxr;

    if (r >= 0 && r < maxr / 5) {
      printf("Query,%d\n", t++);
      qwtBuildQueryReqMsg(&queryRpc);
      code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc, 0);
    } else if (r >= maxr / 5 && r < maxr * 2 / 5) {
      // printf("Ready,%d\n", t++);
      // qwtBuildReadyReqMsg(&readyMsg, &readyRpc);
      // code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
      // if (qwtTestEnableSleep) {
      //   taosUsleep(1);
      // }
    } else if (r >= maxr * 2 / 5 && r < maxr * 3 / 5) {
      printf("Fetch,%d\n", t++);
      qwtBuildFetchReqMsg(&fetchMsg, &fetchRpc);
      code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc, 0);
      if (qwtTestEnableSleep) {
        taosUsleep(1);
      }
    } else if (r >= maxr * 3 / 5 && r < maxr * 4 / 5) {
      printf("Drop,%d\n", t++);
      qwtBuildDropReqMsg(&dropMsg, &dropRpc);
      code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc, 0);
      if (qwtTestEnableSleep) {
        taosUsleep(1);
      }
    } else if (r >= maxr * 4 / 5 && r < maxr - 1) {
      printf("Status,%d\n", t++);
      if (qwtTestEnableSleep) {
        taosUsleep(1);
      }
    } else {
      printf("QUIT RAND NOW");
      break;
    }
  }

  qWorkerDestroy(&mgmt);
}

TEST(seqTest, multithreadRand) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;

  qwtInitLogFile();

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetExecTask();
  stubSetCreateExecTask();
  stubSetAsyncKillTask();
  stubSetDestroyTask();
  stubSetDestroyDataSinker();
  stubSetGetDataLength();
  stubSetEndPut();
  stubSetPutDataBlock();
  stubSetGetDataBlock();

  taosSeedRand(taosGetTimestampSec());

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread t1, t2, t3, t4, t5, t6;
  taosThreadCreate(&(t1), &thattr, queryThread, mgmt);
  // taosThreadCreate(&(t2), &thattr, readyThread, NULL);
  taosThreadCreate(&(t3), &thattr, fetchThread, NULL);
  taosThreadCreate(&(t4), &thattr, dropThread, NULL);
  taosThreadCreate(&(t6), &thattr, fetchQueueThread, mgmt);

  while (true) {
    if (qwtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(qwtTestMTRunSec);
      break;
    }
  }

  qwtTestStop = true;
  taosSsleep(3);

  qwtTestQueryQueueNum = 0;
  qwtTestQueryQueueRIdx = 0;
  qwtTestQueryQueueWIdx = 0;
  qwtTestQueryQueueLock = 0;
  qwtTestFetchQueueNum = 0;
  qwtTestFetchQueueRIdx = 0;
  qwtTestFetchQueueWIdx = 0;
  qwtTestFetchQueueLock = 0;

  qWorkerDestroy(&mgmt);
}

TEST(rcTest, shortExecshortDelay) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;

  qwtInitLogFile();

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetExecTask();
  stubSetCreateExecTask();
  stubSetAsyncKillTask();
  stubSetDestroyTask();
  stubSetDestroyDataSinker();
  stubSetGetDataLength();
  stubSetEndPut();
  stubSetPutDataBlock();
  stubSetGetDataBlock();

  taosSeedRand(taosGetTimestampSec());
  qwtTestStop = false;
  qwtTestQuitThreadNum = 0;

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  qwtTestMaxExecTaskUsec = 0;
  qwtTestReqMaxDelayUsec = 0;

  tsem_init(&qwtTestQuerySem, 0, 0);
  tsem_init(&qwtTestFetchSem, 0, 0);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread t1, t2, t3, t4, t5;
  taosThreadCreate(&(t1), &thattr, qwtclientThread, mgmt);
  taosThreadCreate(&(t2), &thattr, queryQueueThread, mgmt);
  taosThreadCreate(&(t3), &thattr, fetchQueueThread, mgmt);

  while (true) {
    if (qwtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(qwtTestMTRunSec);
      break;
    }
  }

  qwtTestStop = true;

  while (true) {
    if (qwtTestQuitThreadNum == 3) {
      break;
    }

    taosSsleep(1);

    if (qwtTestCaseFinished) {
      if (qwtTestQuitThreadNum < 3) {
        tsem_post(&qwtTestQuerySem);
        tsem_post(&qwtTestFetchSem);

        taosUsleep(10);
      }
    }
  }

  qwtTestQueryQueueNum = 0;
  qwtTestQueryQueueRIdx = 0;
  qwtTestQueryQueueWIdx = 0;
  qwtTestQueryQueueLock = 0;
  qwtTestFetchQueueNum = 0;
  qwtTestFetchQueueRIdx = 0;
  qwtTestFetchQueueWIdx = 0;
  qwtTestFetchQueueLock = 0;

  qWorkerDestroy(&mgmt);
}

TEST(rcTest, longExecshortDelay) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;

  qwtInitLogFile();

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetExecTask();
  stubSetCreateExecTask();
  stubSetAsyncKillTask();
  stubSetDestroyTask();
  stubSetDestroyDataSinker();
  stubSetGetDataLength();
  stubSetEndPut();
  stubSetPutDataBlock();
  stubSetGetDataBlock();

  taosSeedRand(taosGetTimestampSec());
  qwtTestStop = false;
  qwtTestQuitThreadNum = 0;

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  qwtTestMaxExecTaskUsec = 1000000;
  qwtTestReqMaxDelayUsec = 0;

  tsem_init(&qwtTestQuerySem, 0, 0);
  tsem_init(&qwtTestFetchSem, 0, 0);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread t1, t2, t3, t4, t5;
  taosThreadCreate(&(t1), &thattr, qwtclientThread, mgmt);
  taosThreadCreate(&(t2), &thattr, queryQueueThread, mgmt);
  taosThreadCreate(&(t3), &thattr, fetchQueueThread, mgmt);

  while (true) {
    if (qwtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(qwtTestMTRunSec);
      break;
    }
  }

  qwtTestStop = true;

  while (true) {
    if (qwtTestQuitThreadNum == 3) {
      break;
    }

    taosSsleep(1);

    if (qwtTestCaseFinished) {
      if (qwtTestQuitThreadNum < 3) {
        tsem_post(&qwtTestQuerySem);
        tsem_post(&qwtTestFetchSem);

        taosUsleep(10);
      }
    }
  }

  qwtTestQueryQueueNum = 0;
  qwtTestQueryQueueRIdx = 0;
  qwtTestQueryQueueWIdx = 0;
  qwtTestQueryQueueLock = 0;
  qwtTestFetchQueueNum = 0;
  qwtTestFetchQueueRIdx = 0;
  qwtTestFetchQueueWIdx = 0;
  qwtTestFetchQueueLock = 0;

  qWorkerDestroy(&mgmt);
}

TEST(rcTest, shortExeclongDelay) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;

  qwtInitLogFile();

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetExecTask();
  stubSetCreateExecTask();
  stubSetAsyncKillTask();
  stubSetDestroyTask();
  stubSetDestroyDataSinker();
  stubSetGetDataLength();
  stubSetEndPut();
  stubSetPutDataBlock();
  stubSetGetDataBlock();

  taosSeedRand(taosGetTimestampSec());
  qwtTestStop = false;
  qwtTestQuitThreadNum = 0;

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  qwtTestMaxExecTaskUsec = 0;
  qwtTestReqMaxDelayUsec = 1000000;

  tsem_init(&qwtTestQuerySem, 0, 0);
  tsem_init(&qwtTestFetchSem, 0, 0);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread t1, t2, t3, t4, t5;
  taosThreadCreate(&(t1), &thattr, qwtclientThread, mgmt);
  taosThreadCreate(&(t2), &thattr, queryQueueThread, mgmt);
  taosThreadCreate(&(t3), &thattr, fetchQueueThread, mgmt);

  while (true) {
    if (qwtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(qwtTestMTRunSec);
      break;
    }
  }

  qwtTestStop = true;

  while (true) {
    if (qwtTestQuitThreadNum == 3) {
      break;
    }

    taosSsleep(1);

    if (qwtTestCaseFinished) {
      if (qwtTestQuitThreadNum < 3) {
        tsem_post(&qwtTestQuerySem);
        tsem_post(&qwtTestFetchSem);

        taosUsleep(10);
      }
    }
  }

  qwtTestQueryQueueNum = 0;
  qwtTestQueryQueueRIdx = 0;
  qwtTestQueryQueueWIdx = 0;
  qwtTestQueryQueueLock = 0;
  qwtTestFetchQueueNum = 0;
  qwtTestFetchQueueRIdx = 0;
  qwtTestFetchQueueWIdx = 0;
  qwtTestFetchQueueLock = 0;

  qWorkerDestroy(&mgmt);
}

TEST(rcTest, dropTest) {
  void   *mgmt = NULL;
  int32_t code = 0;
  void   *mockPointer = (void *)0x1;

  qwtInitLogFile();

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetExecTask();
  stubSetCreateExecTask();
  stubSetAsyncKillTask();
  stubSetDestroyTask();
  stubSetDestroyDataSinker();
  stubSetGetDataLength();
  stubSetEndPut();
  stubSetPutDataBlock();
  stubSetGetDataBlock();

  taosSeedRand(taosGetTimestampSec());

  SMsgCb msgCb = {0};
  msgCb.mgmt = (void *)mockPointer;
  msgCb.putToQueueFp = (PutToQueueFp)qwtPutReqToQueue;
  code = qWorkerInit(NODE_TYPE_VNODE, 1, &mgmt, &msgCb);
  ASSERT_EQ(code, 0);

  tsem_init(&qwtTestQuerySem, 0, 0);
  tsem_init(&qwtTestFetchSem, 0, 0);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread t1, t2, t3, t4, t5;
  taosThreadCreate(&(t1), &thattr, qwtclientThread, mgmt);
  taosThreadCreate(&(t2), &thattr, queryQueueThread, mgmt);
  taosThreadCreate(&(t3), &thattr, fetchQueueThread, mgmt);

  while (true) {
    if (qwtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(qwtTestMTRunSec);
      break;
    }
  }

  qwtTestStop = true;
  taosSsleep(3);

  qWorkerDestroy(&mgmt);
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
