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
#include "catalog.h"
#include "scheduler.h"
#include "tep.h"
#include "trpc.h"
#include "schedulerInt.h"
#include "stub.h"
#include "addr_any.h"

namespace {

extern "C" int32_t schHandleRspMsg(SQueryJob *job, SQueryTask *task, int32_t msgType, char *msg, int32_t msgSize, int32_t rspCode);

void schtBuildDag(SQueryDag *dag) {
  uint64_t qId = 0x0000000000000001;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = taosArrayInit(dag->numOfSubplans, POINTER_BYTES);
  SArray *scan = taosArrayInit(1, sizeof(SSubplan));
  SArray *merge = taosArrayInit(1, sizeof(SSubplan));
  
  SSubplan scanPlan = {0};
  SSubplan mergePlan = {0};

  scanPlan.id.queryId = qId;
  scanPlan.id.templateId = 0x0000000000000002;
  scanPlan.id.subplanId = 0x0000000000000003;
  scanPlan.type = QUERY_TYPE_SCAN;
  scanPlan.level = 1;
  scanPlan.execEpSet.numOfEps = 1;
  scanPlan.execEpSet.port[0] = 6030;
  strcpy(scanPlan.execEpSet.fqdn[0], "ep0");
  scanPlan.pChildern = NULL;
  scanPlan.pParents = taosArrayInit(1, POINTER_BYTES);
  scanPlan.pNode = (SPhyNode*)calloc(1, sizeof(SPhyNode));

  mergePlan.id.queryId = qId;
  mergePlan.id.templateId = 0x4444444444;
  mergePlan.id.subplanId = 0x5555555555;
  mergePlan.type = QUERY_TYPE_MERGE;
  mergePlan.level = 0;
  mergePlan.execEpSet.numOfEps = 0;
  mergePlan.pChildern = taosArrayInit(1, POINTER_BYTES);
  mergePlan.pParents = NULL;
  mergePlan.pNode = (SPhyNode*)calloc(1, sizeof(SPhyNode));

  SSubplan *mergePointer = (SSubplan *)taosArrayPush(merge, &mergePlan);
  SSubplan *scanPointer = (SSubplan *)taosArrayPush(scan, &scanPlan);

  taosArrayPush(mergePointer->pChildern, &scanPointer);
  taosArrayPush(scanPointer->pParents, &mergePointer);

  taosArrayPush(dag->pSubplans, &merge);  
  taosArrayPush(dag->pSubplans, &scan);
}

int32_t schtPlanToString(const SSubplan *subplan, char** str, int32_t* len) {
  *str = (char *)calloc(1, 20);
  *len = 20;
  return 0;
}

int32_t schtExecNode(SSubplan* subplan, uint64_t templateId, SEpAddr* ep) {
  return 0;
}


void schtSetPlanToString() {
  static Stub stub;
  stub.set(qSubPlanToString, schtPlanToString);
  {
    AddrAny any("libplanner.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qSubPlanToString$", result);
    for (const auto& f : result) {
      stub.set(f.second, schtPlanToString);
    }
  }
}

void schtSetExecNode() {
  static Stub stub;
  stub.set(qSetSubplanExecutionNode, schtExecNode);
  {
    AddrAny any("libplanner.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^qSetSubplanExecutionNode$", result);
    for (const auto& f : result) {
      stub.set(f.second, schtExecNode);
    }
  }
}


}

TEST(queryTest, normalCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  void *pJob = NULL;
  SQueryDag dag = {0};
  SArray *qnodeList = taosArrayInit(1, sizeof(SEpAddr));

  SEpAddr qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);
  
  int32_t code = schedulerInit(NULL);
  ASSERT_EQ(code, 0);

  schtBuildDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  
  code = scheduleAsyncExecJob(mockPointer, qnodeList, &dag, &pJob);
  ASSERT_EQ(code, 0);

  SQueryJob *job = (SQueryJob *)pJob;
  void *pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SQueryTask *task = *(SQueryTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleRspMsg(job, task, TSDB_MSG_TYPE_QUERY, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SQueryTask *task = *(SQueryTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleRspMsg(job, task, TSDB_MSG_TYPE_RES_READY, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }  

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SQueryTask *task = *(SQueryTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleRspMsg(job, task, TSDB_MSG_TYPE_QUERY, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SQueryTask *task = *(SQueryTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleRspMsg(job, task, TSDB_MSG_TYPE_RES_READY, (char *)&rsp, sizeof(rsp), 0);
    ASSERT_EQ(code, 0);
    
    pIter = taosHashIterate(job->execTasks, pIter);
  }  

  SRetrieveTableRsp rsp = {0};
  rsp.completed = 1;
  rsp.numOfRows = 10;
  code = schHandleRspMsg(job, NULL, TSDB_MSG_TYPE_FETCH, (char *)&rsp, sizeof(rsp), 0);
    
  ASSERT_EQ(code, 0);


  void *data = NULL;
  
  code = scheduleFetchRows(job, &data);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);

  data = NULL;
  code = scheduleFetchRows(job, &data);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(data, (void*)NULL);

  scheduleFreeJob(pJob);
}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}




