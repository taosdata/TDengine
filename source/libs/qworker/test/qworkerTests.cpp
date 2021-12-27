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

int32_t qwtStringToPlan(const char* str, SSubplan** subplan) {
  return 0;
}

void qwtRpcSendResponse(const SRpcMsg *pRsp) {
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



}


TEST(testCase, normalCase) {
  void *mgmt = NULL;
  int32_t code = 0;
  void *mockPointer = (void *)0x1;
  SRpcMsg queryRpc = {0};
  SRpcMsg readyRpc = {0};
  SRpcMsg fetchRpc = {0};
  SRpcMsg dropRpc = {0};
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

  stubSetStringToPlan();
  stubSetRpcSendResponse();
  
  code = qWorkerInit(NULL, &mgmt);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessQueryMsg(mockPointer, mgmt, &queryRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessReadyMsg(mockPointer, mgmt, &readyRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessFetchMsg(mockPointer, mgmt, &fetchRpc);
  ASSERT_EQ(code, 0);

  code = qWorkerProcessDropMsg(mockPointer, mgmt, &dropRpc);
  ASSERT_EQ(code, 0);

  qWorkerDestroy(&mgmt);
}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



