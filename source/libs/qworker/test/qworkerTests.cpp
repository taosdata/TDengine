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
#include <tglobal.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "tep.h"
#include "trpc.h"
#include "planner.h"
#include "qworker.h"
#include "stub.h"
#include "addr_any.h"
#include "executor.h"
#include "dataSinkMgt.h"


namespace {

bool qwtTestEnableSleep = true;
bool qwtTestStop = false;
bool qwtTestDeadLoop = true;
int32_t qwtTestMTRunSec = 10;
int32_t qwtTestPrintNum = 100000;
int32_t qwtTestCaseIdx = 0;
int32_t qwtTestCaseNum = 4;

void qwtInitLogFile() {
  const char    *defaultLogFileNamePrefix = "taosdlog";
  const int32_t  maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;

  char temp[128] = {0};
  sprintf(temp, "%s/%s", tsLogDir, defaultLogFileNamePrefix);
  if (taosInitLog(temp, tsNumOfLogLines, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }

}

void qwtBuildQueryReqMsg(SRpcMsg *queryRpc) {
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc->pCont = queryMsg;
  queryRpc->contLen = sizeof(SSubQueryMsg) + 100;
}

void qwtBuildReadyReqMsg(SResReadyReq *readyMsg, SRpcMsg *readyRpc) {
  readyMsg->sId = htobe64(1);
  readyMsg->queryId = htobe64(1);
  readyMsg->taskId = htobe64(1);
  readyRpc->pCont = readyMsg;
  readyRpc->contLen = sizeof(SResReadyReq);
}

void qwtBuildFetchReqMsg(SResFetchReq *fetchMsg, SRpcMsg *fetchRpc) {
  fetchMsg->sId = htobe64(1);
  fetchMsg->queryId = htobe64(1);
  fetchMsg->taskId = htobe64(1);
  fetchRpc->pCont = fetchMsg;
  fetchRpc->contLen = sizeof(SResFetchReq);
}

void qwtBuildDropReqMsg(STaskDropReq *dropMsg, SRpcMsg *dropRpc) {
  dropMsg->sId = htobe64(1);
  dropMsg->queryId = htobe64(1);
  dropMsg->taskId = htobe64(1);
  dropRpc->pCont = dropMsg;
  dropRpc->contLen = sizeof(STaskDropReq);
}

void qwtBuildStatusReqMsg(SSchTasksStatusReq *statusMsg, SRpcMsg *statusRpc) {
  statusMsg->sId = htobe64(1);
  statusRpc->pCont = statusMsg;
  statusRpc->contLen = sizeof(SSchTasksStatusReq);
  statusRpc->msgType = TDMT_VND_TASKS_STATUS;
}

int32_t qwtStringToPlan(const char* str, SSubplan** subplan) {
  return 0;
}

int32_t qwtPutReqToQueue(void *node, struct SRpcMsg *pMsg) {
  return 0;
}


void qwtRpcSendResponse(const SRpcMsg *pRsp) {
/*
  if (TDMT_VND_TASKS_STATUS_RSP == pRsp->msgType) {
    SSchedulerStatusRsp *rsp = (SSchedulerStatusRsp *)pRsp->pCont;
    printf("task num:%d\n", rsp->num);
    for (int32_t i = 0; i < rsp->num; ++i) {
      STaskStatus *task = &rsp->status[i];
      printf("qId:%"PRIx64",tId:%"PRIx64",status:%d\n", task->queryId, task->taskId, task->status);
    }
  }
*/  
  return;
}

int32_t qwtCreateExecTask(void* tsdb, int32_t vgId, struct SSubplan* pPlan, qTaskInfo_t* pTaskInfo, DataSinkHandle* handle) {
  int32_t idx = qwtTestCaseIdx % qwtTestCaseNum;
  
  if (0 == idx) {
    *pTaskInfo = (qTaskInfo_t)qwtTestCaseIdx;
    *handle = (DataSinkHandle)qwtTestCaseIdx+1;
  } else if (1 == idx) {
    *pTaskInfo = NULL;
    *handle = NULL;
  } else if (2 == idx) {
    *pTaskInfo = (qTaskInfo_t)qwtTestCaseIdx;
    *handle = NULL;
  } else if (3 == idx) {
    *pTaskInfo = NULL;
    *handle = (DataSinkHandle)qwtTestCaseIdx;
  }

  ++qwtTestCaseIdx;
  
  return 0;
}

int32_t qwtExecTask(qTaskInfo_t tinfo, SSDataBlock** pRes, uint64_t *useconds) {
  return 0;
}

int32_t qwtKillTask(qTaskInfo_t qinfo) {
  return 0;
}

void qwtDestroyTask(qTaskInfo_t qHandle) {

}


int32_t qwtPutDataBlock(DataSinkHandle handle, const SInputData* pInput, bool* pContinue) {
  return 0;
}

void qwtEndPut(DataSinkHandle handle, uint64_t useconds) {
}

void qwtGetDataLength(DataSinkHandle handle, int32_t* pLen, bool* pQueryEnd) {
}

int32_t qwtGetDataBlock(DataSinkHandle handle, SOutputData* pOutput) {
  return 0;
}

void qwtDestroyDataSinker(DataSinkHandle handle) {

}



void stubSetStringToPlan() {
  static Stub stub;
  stub.set(qStringToSubplan, qwtStringToPlan);
  {
    AddrAny any("libplanner.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qStringToSubplan$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtStringToPlan);
    }
  }
}

void stubSetExecTask() {
  static Stub stub;
  stub.set(qExecTask, qwtExecTask);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qExecTask$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtExecTask);
    }
  }
}



void stubSetCreateExecTask() {
  static Stub stub;
  stub.set(qCreateExecTask, qwtCreateExecTask);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qCreateExecTask$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtCreateExecTask);
    }
  }
}

void stubSetAsyncKillTask() {
  static Stub stub;
  stub.set(qAsyncKillTask, qwtKillTask);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qAsyncKillTask$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtKillTask);
    }
  }
}

void stubSetDestroyTask() {
  static Stub stub;
  stub.set(qDestroyTask, qwtDestroyTask);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qDestroyTask$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtDestroyTask);
    }
  }
}


void stubSetDestroyDataSinker() {
  static Stub stub;
  stub.set(dsDestroyDataSinker, qwtDestroyDataSinker);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^dsDestroyDataSinker$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtDestroyDataSinker);
    }
  }
}

void stubSetGetDataLength() {
  static Stub stub;
  stub.set(dsGetDataLength, qwtGetDataLength);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^dsGetDataLength$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtGetDataLength);
    }
  }
}

void stubSetEndPut() {
  static Stub stub;
  stub.set(dsEndPut, qwtEndPut);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^dsEndPut$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtEndPut);
    }
  }
}

void stubSetPutDataBlock() {
  static Stub stub;
  stub.set(dsPutDataBlock, qwtPutDataBlock);
  {
    AddrAny any("libexecutor.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^dsPutDataBlock$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtPutDataBlock);
    }
  }
}

void stubSetRpcSendResponse() {
  static Stub stub;
  stub.set(rpcSendResponse, qwtRpcSendResponse);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendResponse$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtRpcSendResponse);
    }
  }
}

void stubSetGetDataBlock() {
  static Stub stub;
  stub.set(dsGetDataBlock, qwtGetDataBlock);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^dsGetDataBlock$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtGetDataBlock);
    }
  }
}


void *queryThread(void *param) {
  SRpcMsg queryRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;

  while (!qwtTestStop) {
    qwtBuildQueryReqMsg(&queryRpc);
    qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);    
    free(queryRpc.pCont);
    if (qwtTestEnableSleep) {
      usleep(rand()%5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("query:%d\n", n);
    }
  }

  return NULL;
}

void *readyThread(void *param) {
  SRpcMsg readyRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;  
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;
  SResReadyReq readyMsg = {0};

  while (!qwtTestStop) {
    qwtBuildReadyReqMsg(&readyMsg, &readyRpc);
    code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
    if (qwtTestEnableSleep) {
      usleep(rand()%5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("ready:%d\n", n);
    }    
  }

  return NULL;
}

void *fetchThread(void *param) {
  SRpcMsg fetchRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;  
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;
  SResFetchReq fetchMsg = {0};

  while (!qwtTestStop) {
    qwtBuildFetchReqMsg(&fetchMsg, &fetchRpc);
    code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc);
    if (qwtTestEnableSleep) {
      usleep(rand()%5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("fetch:%d\n", n);
    }    
  }

  return NULL;
}

void *dropThread(void *param) {
  SRpcMsg dropRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;  
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;
  STaskDropReq dropMsg = {0};  

  while (!qwtTestStop) {
    qwtBuildDropReqMsg(&dropMsg, &dropRpc);
    code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
    if (qwtTestEnableSleep) {
      usleep(rand()%5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("drop:%d\n", n);
    }    
  }

  return NULL;
}

void *statusThread(void *param) {
  SRpcMsg statusRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;  
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;
  SSchTasksStatusReq statusMsg = {0};

  while (!qwtTestStop) {
    qwtBuildStatusReqMsg(&statusMsg, &statusRpc);
    code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
    if (qwtTestEnableSleep) {
      usleep(rand()%5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("status:%d\n", n);
    }    
  }

  return NULL;
}


void *controlThread(void *param) {
  SRpcMsg queryRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;

  while (!qwtTestStop) {
    qwtBuildQueryReqMsg(&queryRpc);
    qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);    
    free(queryRpc.pCont);
    if (qwtTestEnableSleep) {
      usleep(rand()%5);
    }
    if (++n % qwtTestPrintNum == 0) {
      printf("query:%d\n", n);
    }
  }

  return NULL;
}

void *queryQueueThread(void *param) {

}

void *fetchQueueThread(void *param) {

}



}


TEST(seqTest, normalCase) {
  void *mgmt = NULL;
  int32_t code = 0;
  void *mockPointer = (void *)0x1;
  SRpcMsg queryRpc = {0};
  SRpcMsg readyRpc = {0};
  SRpcMsg fetchRpc = {0};
  SRpcMsg dropRpc = {0};
  SRpcMsg statusRpc = {0};

  qwtInitLogFile();
  
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc.pCont = queryMsg;
  queryRpc.contLen = sizeof(SSubQueryMsg) + 100;

  SResReadyReq readyMsg = {0};
  readyMsg.sId = htobe64(1);
  readyMsg.queryId = htobe64(1);
  readyMsg.taskId = htobe64(1);
  readyRpc.pCont = &readyMsg;
  readyRpc.contLen = sizeof(SResReadyReq);

  SResFetchReq fetchMsg = {0};
  fetchMsg.sId = htobe64(1);
  fetchMsg.queryId = htobe64(1);
  fetchMsg.taskId = htobe64(1);
  fetchRpc.pCont = &fetchMsg;
  fetchRpc.contLen = sizeof(SResFetchReq);

  STaskDropReq dropMsg = {0};  
  dropMsg.sId = htobe64(1);
  dropMsg.queryId = htobe64(1);
  dropMsg.taskId = htobe64(1);
  dropRpc.pCont = &dropMsg;
  dropRpc.contLen = sizeof(STaskDropReq);

  SSchTasksStatusReq statusMsg = {0};
  statusMsg.sId = htobe64(1);
  statusRpc.pCont = &statusMsg;
  statusRpc.contLen = sizeof(SSchTasksStatusReq);
  statusRpc.msgType = TDMT_VND_TASKS_STATUS;
  
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
  
  code = qWorkerInit(NODE_TYPE_VNODE, 1, NULL, &mgmt, mockPointer, qwtPutReqToQueue);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  qWorkerDestroy(&mgmt);
}

TEST(seqTest, cancelFirst) {
  void *mgmt = NULL;
  int32_t code = 0;
  void *mockPointer = (void *)0x1;
  SRpcMsg queryRpc = {0};
  SRpcMsg dropRpc = {0};
  SRpcMsg statusRpc = {0};

  qwtInitLogFile();
  
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc.pCont = queryMsg;
  queryRpc.contLen = sizeof(SSubQueryMsg) + 100;

  STaskDropReq dropMsg = {0};  
  dropMsg.sId = htobe64(1);
  dropMsg.queryId = htobe64(1);
  dropMsg.taskId = htobe64(1);
  dropRpc.pCont = &dropMsg;
  dropRpc.contLen = sizeof(STaskDropReq);

  SSchTasksStatusReq statusMsg = {0};
  statusMsg.sId = htobe64(1);
  statusRpc.pCont = &statusMsg;
  statusRpc.contLen = sizeof(SSchTasksStatusReq);
  statusRpc.msgType = TDMT_VND_TASKS_STATUS;

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  
  code = qWorkerInit(NODE_TYPE_VNODE, 1, NULL, &mgmt, mockPointer, qwtPutReqToQueue);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
  ASSERT_EQ(code, 0);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);
  ASSERT_EQ(code, TSDB_CODE_QRY_TASK_DROPPED);

  statusMsg.sId = htobe64(1);
  code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
  ASSERT_EQ(code, 0);

  qWorkerDestroy(&mgmt);
}

TEST(seqTest, randCase) {
  void *mgmt = NULL;
  int32_t code = 0;
  void *mockPointer = (void *)0x1;
  SRpcMsg queryRpc = {0};
  SRpcMsg readyRpc = {0};
  SRpcMsg fetchRpc = {0};
  SRpcMsg dropRpc = {0};
  SRpcMsg statusRpc = {0};
  SResReadyReq readyMsg = {0};
  SResFetchReq fetchMsg = {0};
  STaskDropReq dropMsg = {0};  
  SSchTasksStatusReq statusMsg = {0};

  qwtInitLogFile();
  
  stubSetStringToPlan();
  stubSetRpcSendResponse();
  stubSetCreateExecTask();

  srand(time(NULL));
  
  code = qWorkerInit(NODE_TYPE_VNODE, 1, NULL, &mgmt, mockPointer, qwtPutReqToQueue);
  ASSERT_EQ(code, 0);

  int32_t t = 0;
  int32_t maxr = 10001;
  while (true) {
    int32_t r = rand() % maxr;
    
    if (r >= 0 && r < maxr/5) {
      printf("Query,%d\n", t++);      
      qwtBuildQueryReqMsg(&queryRpc);
      code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);
      free(queryRpc.pCont);
    } else if (r >= maxr/5 && r < maxr * 2/5) {
      printf("Ready,%d\n", t++);
      qwtBuildReadyReqMsg(&readyMsg, &readyRpc);
      code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
    } else if (r >= maxr * 2/5 && r < maxr* 3/5) {
      printf("Fetch,%d\n", t++);
      qwtBuildFetchReqMsg(&fetchMsg, &fetchRpc);
      code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc);
    } else if (r >= maxr * 3/5 && r < maxr * 4/5) {
      printf("Drop,%d\n", t++);
      qwtBuildDropReqMsg(&dropMsg, &dropRpc);
      code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
    } else if (r >= maxr * 4/5 && r < maxr-1) {
      printf("Status,%d\n", t++);
      qwtBuildStatusReqMsg(&statusMsg, &statusRpc);
      code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
      ASSERT_EQ(code, 0);
    } else {
      printf("QUIT RAND NOW");
      break;
    }
  }

  qWorkerDestroy(&mgmt);
}

TEST(seqTest, multithreadRand) {
  void *mgmt = NULL;
  int32_t code = 0;
  void *mockPointer = (void *)0x1;

  qwtInitLogFile();
  
  stubSetStringToPlan();
  stubSetRpcSendResponse();

  srand(time(NULL));
  
  code = qWorkerInit(NODE_TYPE_VNODE, 1, NULL, &mgmt, mockPointer, qwtPutReqToQueue);
  ASSERT_EQ(code, 0);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t t1,t2,t3,t4,t5;
  pthread_create(&(t1), &thattr, queryThread, mgmt);
  pthread_create(&(t2), &thattr, readyThread, NULL);
  pthread_create(&(t3), &thattr, fetchThread, NULL);
  pthread_create(&(t4), &thattr, dropThread, NULL);
  pthread_create(&(t5), &thattr, statusThread, NULL);

  while (true) {
    if (qwtTestDeadLoop) {
      sleep(1);
    } else {
      sleep(qwtTestMTRunSec);
      break;
    }
  }
  
  qwtTestStop = true;
  sleep(3);
  
  qWorkerDestroy(&mgmt);
}

TEST(rcTest, multithread) {
  void *mgmt = NULL;
  int32_t code = 0;
  void *mockPointer = (void *)0x1;

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

  srand(time(NULL));
  
  code = qWorkerInit(NODE_TYPE_VNODE, 1, NULL, &mgmt, mockPointer, qwtPutReqToQueue);
  ASSERT_EQ(code, 0);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t t1,t2,t3,t4,t5;
  pthread_create(&(t1), &thattr, controlThread, mgmt);
  pthread_create(&(t2), &thattr, queryQueueThread, NULL);
  pthread_create(&(t3), &thattr, fetchQueueThread, NULL);

  while (true) {
    if (qwtTestDeadLoop) {
      sleep(1);
    } else {
      sleep(qwtTestMTRunSec);
      break;
    }
  }
  
  qwtTestStop = true;
  sleep(3);
  
  qWorkerDestroy(&mgmt);
}



int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



