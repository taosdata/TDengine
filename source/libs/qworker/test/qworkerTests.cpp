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


namespace {

bool testStop = false;

int32_t qwtStringToPlan(const char* str, SSubplan** subplan) {
  return 0;
}

void qwtRpcSendResponse(const SRpcMsg *pRsp) {
  if (TDMT_VND_TASKS_STATUS_RSP == pRsp->msgType) {
    SSchedulerStatusRsp *rsp = (SSchedulerStatusRsp *)pRsp->pCont;
    printf("task num:%d\n", rsp->num);
    for (int32_t i = 0; i < rsp->num; ++i) {
      STaskStatus *task = &rsp->status[i];
      printf("qId:%"PRIx64",tId:%"PRIx64",status:%d\n", task->queryId, task->taskId, task->status);
    }
  }
  return;
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

void stubSetRpcSendResponse() {
  static Stub stub;
  stub.set(rpcSendResponse, qwtRpcSendResponse);
  {
    AddrAny any("libplanner.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendResponse$", result);
    for (const auto& f : result) {
      stub.set(f.second, qwtRpcSendResponse);
    }
  }
}

void *queryThread(void *param) {
  SRpcMsg queryRpc = {0};
  int32_t code = 0;
  uint32_t n = 0;
  void *mockPointer = (void *)0x1;    
  void *mgmt = param;
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc.pCont = queryMsg;
  queryRpc.contLen = sizeof(SSubQueryMsg) + 100;

  while (!testStop) {
    qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);
    usleep(rand()%5);
    if (++n % 50000 == 0) {
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
  SResReadyMsg readyMsg = {0};
  readyMsg.sId = htobe64(1);
  readyMsg.queryId = htobe64(1);
  readyMsg.taskId = htobe64(1);
  readyRpc.pCont = &readyMsg;
  readyRpc.contLen = sizeof(SResReadyMsg);

  while (!testStop) {
    code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
    usleep(rand()%5);
    if (++n % 50000 == 0) {
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
  SResFetchMsg fetchMsg = {0};
  fetchMsg.sId = htobe64(1);
  fetchMsg.queryId = htobe64(1);
  fetchMsg.taskId = htobe64(1);
  fetchRpc.pCont = &fetchMsg;
  fetchRpc.contLen = sizeof(SResFetchMsg);

  while (!testStop) {
    code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc);
    usleep(rand()%5);
    if (++n % 50000 == 0) {
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
  STaskDropMsg dropMsg = {0};  
  dropMsg.sId = htobe64(1);
  dropMsg.queryId = htobe64(1);
  dropMsg.taskId = htobe64(1);
  dropRpc.pCont = &dropMsg;
  dropRpc.contLen = sizeof(STaskDropMsg);

  while (!testStop) {
    code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
    usleep(rand()%5);
    if (++n % 50000 == 0) {
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
  SSchTasksStatusMsg statusMsg = {0};
  statusMsg.sId = htobe64(1);
  statusRpc.pCont = &statusMsg;
  statusRpc.contLen = sizeof(SSchTasksStatusMsg);
  statusRpc.msgType = TDMT_VND_TASKS_STATUS;

  while (!testStop) {
    statusMsg.sId = htobe64(1);
    code = qWorkerProcessStatusMsg(mockPointer, mgmt, &statusRpc);
    usleep(rand()%5);
    if (++n % 50000 == 0) {
      printf("status:%d\n", n);
    }    
  }

  return NULL;
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
  
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc.pCont = queryMsg;
  queryRpc.contLen = sizeof(SSubQueryMsg) + 100;

  SResReadyMsg readyMsg = {0};
  readyMsg.sId = htobe64(1);
  readyMsg.queryId = htobe64(1);
  readyMsg.taskId = htobe64(1);
  readyRpc.pCont = &readyMsg;
  readyRpc.contLen = sizeof(SResReadyMsg);

  SResFetchMsg fetchMsg = {0};
  fetchMsg.sId = htobe64(1);
  fetchMsg.queryId = htobe64(1);
  fetchMsg.taskId = htobe64(1);
  fetchRpc.pCont = &fetchMsg;
  fetchRpc.contLen = sizeof(SResFetchMsg);

  STaskDropMsg dropMsg = {0};  
  dropMsg.sId = htobe64(1);
  dropMsg.queryId = htobe64(1);
  dropMsg.taskId = htobe64(1);
  dropRpc.pCont = &dropMsg;
  dropRpc.contLen = sizeof(STaskDropMsg);

  SSchTasksStatusMsg statusMsg = {0};
  statusMsg.sId = htobe64(1);
  statusRpc.pCont = &statusMsg;
  statusRpc.contLen = sizeof(SSchTasksStatusMsg);
  statusRpc.msgType = TDMT_VND_TASKS_STATUS;
  
  stubSetStringToPlan();
  stubSetRpcSendResponse();
  
  code = qWorkerInit(NULL, &mgmt);
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
  
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc.pCont = queryMsg;
  queryRpc.contLen = sizeof(SSubQueryMsg) + 100;

  STaskDropMsg dropMsg = {0};  
  dropMsg.sId = htobe64(1);
  dropMsg.queryId = htobe64(1);
  dropMsg.taskId = htobe64(1);
  dropRpc.pCont = &dropMsg;
  dropRpc.contLen = sizeof(STaskDropMsg);

  SSchTasksStatusMsg statusMsg = {0};
  statusMsg.sId = htobe64(1);
  statusRpc.pCont = &statusMsg;
  statusRpc.contLen = sizeof(SSchTasksStatusMsg);
  statusRpc.msgType = TDMT_VND_TASKS_STATUS;

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  
  code = qWorkerInit(NULL, &mgmt);
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
  ASSERT_EQ(code, 0);

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
  
  SSubQueryMsg *queryMsg = (SSubQueryMsg *)calloc(1, sizeof(SSubQueryMsg) + 100);
  queryMsg->queryId = htobe64(1);
  queryMsg->sId = htobe64(1);
  queryMsg->taskId = htobe64(1);
  queryMsg->contentLen = htonl(100);
  queryRpc.pCont = queryMsg;
  queryRpc.contLen = sizeof(SSubQueryMsg) + 100;

  SResReadyMsg readyMsg = {0};
  readyMsg.sId = htobe64(1);
  readyMsg.queryId = htobe64(1);
  readyMsg.taskId = htobe64(1);
  readyRpc.pCont = &readyMsg;
  readyRpc.contLen = sizeof(SResReadyMsg);

  SResFetchMsg fetchMsg = {0};
  fetchMsg.sId = htobe64(1);
  fetchMsg.queryId = htobe64(1);
  fetchMsg.taskId = htobe64(1);
  fetchRpc.pCont = &fetchMsg;
  fetchRpc.contLen = sizeof(SResFetchMsg);

  STaskDropMsg dropMsg = {0};  
  dropMsg.sId = htobe64(1);
  dropMsg.queryId = htobe64(1);
  dropMsg.taskId = htobe64(1);
  dropRpc.pCont = &dropMsg;
  dropRpc.contLen = sizeof(STaskDropMsg);

  SSchTasksStatusMsg statusMsg = {0};
  statusMsg.sId = htobe64(1);
  statusRpc.pCont = &statusMsg;
  statusRpc.contLen = sizeof(SSchTasksStatusMsg);
  statusRpc.msgType = TDMT_VND_TASKS_STATUS;
  
  stubSetStringToPlan();
  stubSetRpcSendResponse();

  srand(time(NULL));
  
  code = qWorkerInit(NULL, &mgmt);
  ASSERT_EQ(code, 0);

  int32_t t = 0;
  int32_t maxr = 10001;
  while (true) {
    int32_t r = rand() % maxr;
    
    if (r >= 0 && r < maxr/5) {
      printf("Query,%d\n", t++);
      code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);
    } else if (r >= maxr/5 && r < maxr * 2/5) {
      printf("Ready,%d\n", t++);
      code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
    } else if (r >= maxr * 2/5 && r < maxr* 3/5) {
      printf("Fetch,%d\n", t++);
      code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc);
    } else if (r >= maxr * 3/5 && r < maxr * 4/5) {
      printf("Drop,%d\n", t++);
      code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
    } else if (r >= maxr * 4/5 && r < maxr-1) {
      printf("Status,%d\n", t++);
      statusMsg.sId = htobe64(1);
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
  
  stubSetStringToPlan();
  stubSetRpcSendResponse();

  srand(time(NULL));
  
  code = qWorkerInit(NULL, &mgmt);
  ASSERT_EQ(code, 0);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t t1,t2,t3,t4,t5;
  pthread_create(&(t1), &thattr, queryThread, mgmt);
  pthread_create(&(t2), &thattr, readyThread, NULL);
  pthread_create(&(t3), &thattr, fetchThread, NULL);
  pthread_create(&(t4), &thattr, dropThread, NULL);
  pthread_create(&(t5), &thattr, statusThread, NULL);

  int32_t t = 0;
  int32_t maxr = 10001;
  sleep(300);
  testStop = true;
  sleep(1);
  
  qWorkerDestroy(&mgmt);
}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



