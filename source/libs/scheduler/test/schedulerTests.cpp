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

extern "C" int32_t schHandleResponseMsg(SSchJob *job, SSchTask *task, int32_t msgType, char *msg, int32_t msgSize, int32_t rspCode);

void schtBuildQueryDag(SQueryDag *dag) {
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
  scanPlan.execNode.numOfEps = 1;
  scanPlan.execNode.nodeId = 1;
  scanPlan.execNode.inUse = 0;
  scanPlan.execNode.epAddr[0].port = 6030;
  strcpy(scanPlan.execNode.epAddr[0].fqdn, "ep0");
  scanPlan.pChildren = NULL;
  scanPlan.level = 1;
  scanPlan.pParents = taosArrayInit(1, POINTER_BYTES);
  scanPlan.pNode = (SPhyNode*)calloc(1, sizeof(SPhyNode));

  mergePlan.id.queryId = qId;
  mergePlan.id.templateId = 0x4444444444;
  mergePlan.id.subplanId = 0x5555555555;
  mergePlan.type = QUERY_TYPE_MERGE;
  mergePlan.level = 0;
  mergePlan.execNode.numOfEps = 0;
  mergePlan.pChildren = taosArrayInit(1, POINTER_BYTES);
  mergePlan.pParents = NULL;
  mergePlan.pNode = (SPhyNode*)calloc(1, sizeof(SPhyNode));

  SSubplan *mergePointer = (SSubplan *)taosArrayPush(merge, &mergePlan);
  SSubplan *scanPointer = (SSubplan *)taosArrayPush(scan, &scanPlan);

  taosArrayPush(mergePointer->pChildren, &scanPointer);
  taosArrayPush(scanPointer->pParents, &mergePointer);

  taosArrayPush(dag->pSubplans, &merge);  
  taosArrayPush(dag->pSubplans, &scan);
}

void schtBuildInsertDag(SQueryDag *dag) {
  uint64_t qId = 0x0000000000000002;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = taosArrayInit(1, POINTER_BYTES);
  SArray *inserta = taosArrayInit(dag->numOfSubplans, sizeof(SSubplan));
  
  SSubplan insertPlan[2] = {0};

  insertPlan[0].id.queryId = qId;
  insertPlan[0].id.templateId = 0x0000000000000003;
  insertPlan[0].id.subplanId = 0x0000000000000004;
  insertPlan[0].type = QUERY_TYPE_MODIFY;
  insertPlan[0].level = 0;
  insertPlan[0].execNode.numOfEps = 1;
  insertPlan[0].execNode.nodeId = 1;
  insertPlan[0].execNode.inUse = 0;
  insertPlan[0].execNode.epAddr[0].port = 6030;
  strcpy(insertPlan[0].execNode.epAddr[0].fqdn, "ep0");
  insertPlan[0].pChildren = NULL;
  insertPlan[0].pParents = NULL;
  insertPlan[0].pNode = NULL;
  insertPlan[0].pDataSink = (SDataSink*)calloc(1, sizeof(SDataSink));

  insertPlan[1].id.queryId = qId;
  insertPlan[1].id.templateId = 0x0000000000000003;
  insertPlan[1].id.subplanId = 0x0000000000000005;
  insertPlan[1].type = QUERY_TYPE_MODIFY;
  insertPlan[1].level = 0;
  insertPlan[1].execNode.numOfEps = 1;
  insertPlan[1].execNode.nodeId = 1;
  insertPlan[1].execNode.inUse = 1;
  insertPlan[1].execNode.epAddr[0].port = 6030;
  strcpy(insertPlan[1].execNode.epAddr[0].fqdn, "ep1");
  insertPlan[1].pChildren = NULL;
  insertPlan[1].pParents = NULL;
  insertPlan[1].pNode = NULL;
  insertPlan[1].pDataSink = (SDataSink*)calloc(1, sizeof(SDataSink));


  taosArrayPush(inserta, &insertPlan[0]);
  taosArrayPush(inserta, &insertPlan[1]);

  taosArrayPush(dag->pSubplans, &inserta);  
}


int32_t schtPlanToString(const SSubplan *subplan, char** str, int32_t* len) {
  *str = (char *)calloc(1, 20);
  *len = 20;
  return 0;
}

int32_t schtExecNode(SSubplan* subplan, uint64_t templateId, SQueryNodeAddr* ep) {
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

void *schtSendRsp(void *param) {
  SSchJob *job = NULL;
  int32_t code = 0;

  while (true) {
    job = *(SSchJob **)param;
    if (job) {
      break;
    }

    usleep(1000);
  }
  
  void *pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SShellSubmitRsp rsp = {0};
    rsp.affectedRows = 10;
    schHandleResponseMsg(job, task, TDMT_VND_SUBMIT, (char *)&rsp, sizeof(rsp), 0);
    
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  return NULL;
}

void *pInsertJob = NULL;


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

  schtBuildQueryDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  
  code = scheduleAsyncExecJob(mockPointer, qnodeList, &dag, &pJob);
  ASSERT_EQ(code, 0);

  SSchJob *job = (SSchJob *)pJob;
  void *pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_QUERY, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_RES_READY, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }  

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_QUERY, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_RES_READY, (char *)&rsp, sizeof(rsp), 0);
    ASSERT_EQ(code, 0);
    
    pIter = taosHashIterate(job->execTasks, pIter);
  }  

  SRetrieveTableRsp rsp = {0};
  rsp.completed = 1;
  rsp.numOfRows = 10;
  code = schHandleResponseMsg(job, NULL, TDMT_VND_FETCH, (char *)&rsp, sizeof(rsp), 0);
    
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




TEST(insertTest, normalCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryDag dag = {0};
  uint64_t numOfRows = 0;
  SArray *qnodeList = taosArrayInit(1, sizeof(SEpAddr));

  SEpAddr qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);
  
  int32_t code = schedulerInit(NULL);
  ASSERT_EQ(code, 0);

  schtBuildInsertDag(&dag);

  schtSetPlanToString();

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t thread1;
  pthread_create(&(thread1), &thattr, schtSendRsp, &pInsertJob);

  SQueryResult res = {0};
  code = scheduleExecJob(mockPointer, qnodeList, &dag, &pInsertJob, &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.numOfRows, 20);

  scheduleFreeJob(pInsertJob);
}




int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}




