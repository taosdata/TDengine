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
#pragma GCC diagnostic ignored "-Wreturn-type"
#pragma GCC diagnostic ignored "-Wformat"
#include <addr_any.h>


#include "os.h"

#include "tglobal.h"
#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "catalog.h"
#include "scheduler.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "trpc.h"
#include "tvariant.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wreturn-type"
#pragma GCC diagnostic ignored "-Wformat"

#include "schedulerInt.h"
#include "stub.h"
#include "tref.h"

namespace {

extern "C" int32_t schHandleResponseMsg(SSchJob *job, SSchTask *task, int32_t msgType, char *msg, int32_t msgSize, int32_t rspCode);
extern "C" int32_t schHandleCallback(void* param, const SDataBuf* pMsg, int32_t msgType, int32_t rspCode);

int64_t insertJobRefId = 0;
int64_t queryJobRefId = 0;

uint64_t schtMergeTemplateId = 0x4;
uint64_t schtFetchTaskId = 0;
uint64_t schtQueryId = 1;

bool schtTestStop = false;
bool schtTestDeadLoop = false;
int32_t schtTestMTRunSec = 10;
int32_t schtTestPrintNum = 1000;
int32_t schtStartFetch = 0;


void schtInitLogFile() {
  const char    *defaultLogFileNamePrefix = "taoslog";
  const int32_t  maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, "/var/log/taos");

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }

}


void schtBuildQueryDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *scan = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *merge = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  
  SSubplan *scanPlan = (SSubplan *)calloc(1, sizeof(SSubplan));
  SSubplan *mergePlan = (SSubplan *)calloc(1, sizeof(SSubplan));

  scanPlan->id.queryId = qId;
  scanPlan->id.groupId = 0x0000000000000002;
  scanPlan->id.subplanId = 0x0000000000000003;
  scanPlan->subplanType = SUBPLAN_TYPE_SCAN;

  scanPlan->execNode.nodeId = 1;
  scanPlan->execNode.epSet.inUse = 0;
  addEpIntoEpSet(&scanPlan->execNode.epSet, "ep0", 6030);

  scanPlan->pChildren = NULL;
  scanPlan->level = 1;
  scanPlan->pParents = nodesMakeList();
  scanPlan->pNode = (SPhysiNode*)calloc(1, sizeof(SPhysiNode));
  scanPlan->msgType = TDMT_VND_QUERY;

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pChildren = nodesMakeList();
  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhysiNode*)calloc(1, sizeof(SPhysiNode));
  mergePlan->msgType = TDMT_VND_QUERY;

  merge->pNodeList = nodesMakeList();
  scan->pNodeList = nodesMakeList();

  nodesListAppend(merge->pNodeList, (SNode*)mergePlan);
  nodesListAppend(scan->pNodeList, (SNode*)scanPlan);

  nodesListAppend(mergePlan->pChildren, (SNode*)scanPlan);
  nodesListAppend(scanPlan->pParents, (SNode*)mergePlan);

  nodesListAppend(dag->pSubplans, (SNode*)merge);  
  nodesListAppend(dag->pSubplans, (SNode*)scan);
}

void schtBuildQueryFlowCtrlDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;
  int32_t scanPlanNum = 20;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *scan = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *merge = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  
  SSubplan *scanPlan = (SSubplan *)calloc(scanPlanNum, sizeof(SSubplan));
  SSubplan *mergePlan = (SSubplan *)calloc(1, sizeof(SSubplan));

  merge->pNodeList = nodesMakeList();
  scan->pNodeList = nodesMakeList();

  mergePlan->pChildren = nodesMakeList();

  for (int32_t i = 0; i < scanPlanNum; ++i) {
    scanPlan[i].id.queryId = qId;
    scanPlan[i].id.groupId = 0x0000000000000002;
    scanPlan[i].id.subplanId = 0x0000000000000003 + i;
    scanPlan[i].subplanType = SUBPLAN_TYPE_SCAN;

    scanPlan[i].execNode.nodeId = 1 + i;
    scanPlan[i].execNode.epSet.inUse = 0;
    scanPlan[i].execNodeStat.tableNum = taosRand() % 30;
    addEpIntoEpSet(&scanPlan[i].execNode.epSet, "ep0", 6030);
    addEpIntoEpSet(&scanPlan[i].execNode.epSet, "ep1", 6030);
    addEpIntoEpSet(&scanPlan[i].execNode.epSet, "ep2", 6030);
    scanPlan[i].execNode.epSet.inUse = taosRand() % 3;

    scanPlan[i].pChildren = NULL;
    scanPlan[i].level = 1;
    scanPlan[i].pParents = nodesMakeList();
    scanPlan[i].pNode = (SPhysiNode*)calloc(1, sizeof(SPhysiNode));
    scanPlan[i].msgType = TDMT_VND_QUERY;

    nodesListAppend(scanPlan[i].pParents, (SNode*)mergePlan);
    nodesListAppend(mergePlan->pChildren, (SNode*)(scanPlan + i));

    nodesListAppend(scan->pNodeList, (SNode*)(scanPlan + i));
  }

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhysiNode*)calloc(1, sizeof(SPhysiNode));
  mergePlan->msgType = TDMT_VND_QUERY;

  nodesListAppend(merge->pNodeList, (SNode*)mergePlan);

  nodesListAppend(dag->pSubplans, (SNode*)merge);  
  nodesListAppend(dag->pSubplans, (SNode*)scan);
}


void schtFreeQueryDag(SQueryPlan *dag) {

}


void schtBuildInsertDag(SQueryPlan *dag) {
  uint64_t qId = 0x0000000000000002;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *inserta = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  
  SSubplan *insertPlan = (SSubplan *)calloc(2, sizeof(SSubplan));

  insertPlan[0].id.queryId = qId;
  insertPlan[0].id.groupId = 0x0000000000000003;
  insertPlan[0].id.subplanId = 0x0000000000000004;
  insertPlan[0].subplanType = SUBPLAN_TYPE_MODIFY;
  insertPlan[0].level = 0;

  insertPlan[0].execNode.nodeId = 1;
  insertPlan[0].execNode.epSet.inUse = 0;
  addEpIntoEpSet(&insertPlan[0].execNode.epSet, "ep0", 6030);

  insertPlan[0].pChildren = NULL;
  insertPlan[0].pParents = NULL;
  insertPlan[0].pNode = NULL;
  insertPlan[0].pDataSink = (SDataSinkNode*)calloc(1, sizeof(SDataSinkNode));
  insertPlan[0].msgType = TDMT_VND_SUBMIT;

  insertPlan[1].id.queryId = qId;
  insertPlan[1].id.groupId = 0x0000000000000003;
  insertPlan[1].id.subplanId = 0x0000000000000005;
  insertPlan[1].subplanType = SUBPLAN_TYPE_MODIFY;
  insertPlan[1].level = 0;

  insertPlan[1].execNode.nodeId = 1;
  insertPlan[1].execNode.epSet.inUse = 0;
  addEpIntoEpSet(&insertPlan[1].execNode.epSet, "ep0", 6030);

  insertPlan[1].pChildren = NULL;
  insertPlan[1].pParents = NULL;
  insertPlan[1].pNode = NULL;
  insertPlan[1].pDataSink = (SDataSinkNode*)calloc(1, sizeof(SDataSinkNode));
  insertPlan[1].msgType = TDMT_VND_SUBMIT;

  inserta->pNodeList = nodesMakeList();

  nodesListAppend(inserta->pNodeList, (SNode*)insertPlan);
  insertPlan += 1;
  nodesListAppend(inserta->pNodeList, (SNode*)insertPlan);

  nodesListAppend(dag->pSubplans, (SNode*)inserta);  
}


int32_t schtPlanToString(const SSubplan *subplan, char** str, int32_t* len) {
  *str = (char *)calloc(1, 20);
  *len = 20;
  return 0;
}

void schtExecNode(SSubplan* subplan, uint64_t groupId, SQueryNodeAddr* ep) {

}

void schtRpcSendRequest(void *shandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *pRid) {

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

void schtSetRpcSendRequest() {
  static Stub stub;
  stub.set(rpcSendRequest, schtRpcSendRequest);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^rpcSendRequest$", result);
    for (const auto& f : result) {
      stub.set(f.second, schtRpcSendRequest);
    }
  }
}

int32_t schtAsyncSendMsgToServer(void *pTransporter, SEpSet* epSet, int64_t* pTransporterId, SMsgSendInfo* pInfo) {
  if (pInfo) {
    tfree(pInfo->param);
    tfree(pInfo->msgInfo.pData);
    free(pInfo);
  }
  return 0;
}


void schtSetAsyncSendMsgToServer() {
  static Stub stub;
  stub.set(asyncSendMsgToServer, schtAsyncSendMsgToServer);
  {
    AddrAny any("libtransport.so");
    std::map<std::string,void*> result;
    any.get_global_func_addr_dynsym("^asyncSendMsgToServer$", result);
    for (const auto& f : result) {
      stub.set(f.second, schtAsyncSendMsgToServer);
    }
  }
}


void *schtSendRsp(void *param) {
  SSchJob *pJob = NULL;
  int64_t job = 0;
  int32_t code = 0;

  while (true) {
    job = *(int64_t *)param;
    if (job) {
      break;
    }

    taosMsleep(1);
  }

  pJob = schAcquireJob(job);
  
  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SSubmitRsp rsp = {0};
    rsp.affectedRows = 10;
    schHandleResponseMsg(pJob, task, TDMT_VND_SUBMIT_RSP, (char *)&rsp, sizeof(rsp), 0);
    
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }    

  schReleaseJob(job);

  return NULL;
}

void *schtCreateFetchRspThread(void *param) {
  int64_t job = *(int64_t *)param;
  SSchJob* pJob = schAcquireJob(job);

  taosSsleep(1);

  int32_t code = 0;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)calloc(1, sizeof(SRetrieveTableRsp));
  rsp->completed = 1;
  rsp->numOfRows = 10;
 
  code = schHandleResponseMsg(pJob, pJob->fetchTask, TDMT_VND_FETCH_RSP, (char *)rsp, sizeof(*rsp), 0);

  schReleaseJob(job);
  
  assert(code == 0);
}


void *schtFetchRspThread(void *aa) {
  SDataBuf dataBuf = {0};
  SSchCallbackParam* param = NULL;

  while (!schtTestStop) {
    if (0 == atomic_val_compare_exchange_32(&schtStartFetch, 1, 0)) {
      continue;
    }

    taosUsleep(1);
    
    param = (SSchCallbackParam *)calloc(1, sizeof(*param));

    param->queryId = schtQueryId;  
    param->taskId = schtFetchTaskId;

    int32_t code = 0;
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)calloc(1, sizeof(SRetrieveTableRsp));
    rsp->completed = 1;
    rsp->numOfRows = 10;

    dataBuf.pData = rsp;
    dataBuf.len = sizeof(*rsp);

    code = schHandleCallback(param, &dataBuf, TDMT_VND_FETCH_RSP, 0);
      
    assert(code == 0 || code);
  }
}

void schtFreeQueryJob(int32_t freeThread) {
  static uint32_t freeNum = 0;
  int64_t job = queryJobRefId;
  
  if (job && atomic_val_compare_exchange_64(&queryJobRefId, job, 0)) {
    schedulerFreeJob(job);
    if (freeThread) {
      if (++freeNum % schtTestPrintNum == 0) {
        printf("FreeNum:%d\n", freeNum);
      }
    }
  }
}

void* schtRunJobThread(void *aa) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryPlan dag;

  schtInitLogFile();

  
  int32_t code = schedulerInit(NULL);
  assert(code == 0);


  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  SSchJob *pJob = NULL;
  SSchCallbackParam *param = NULL;
  SHashObj *execTasks = NULL;
  SDataBuf dataBuf = {0};
  uint32_t jobFinished = 0;

  while (!schtTestStop) {
    schtBuildQueryDag(&dag);

    SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

    SEp qnodeAddr = {0};
    strcpy(qnodeAddr.fqdn, "qnode0.ep");
    qnodeAddr.port = 6031;
    taosArrayPush(qnodeList, &qnodeAddr);

    code = schedulerAsyncExecJob(mockPointer, qnodeList, &dag, "select * from tb", &queryJobRefId);
    assert(code == 0);

    pJob = schAcquireJob(queryJobRefId);
    if (NULL == pJob) {
      taosArrayDestroy(qnodeList);
      schtFreeQueryDag(&dag);
      continue;
    }
    
    execTasks = taosHashInit(5, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
    void *pIter = taosHashIterate(pJob->execTasks, NULL);
    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;
      schtFetchTaskId = task->taskId - 1;
      
      taosHashPut(execTasks, &task->taskId, sizeof(task->taskId), task, sizeof(*task));
      pIter = taosHashIterate(pJob->execTasks, pIter);
    }    

    param = (SSchCallbackParam *)calloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;   

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId;
      SQueryTableRsp rsp = {0};
      dataBuf.pData = &rsp;
      dataBuf.len = sizeof(rsp);
      
      code = schHandleCallback(param, &dataBuf, TDMT_VND_QUERY_RSP, 0);
      assert(code == 0 || code);

      pIter = taosHashIterate(execTasks, pIter);
    }    


    param = (SSchCallbackParam *)calloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;   
    
    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId;
      SResReadyRsp rsp = {0};
      dataBuf.pData = &rsp;
      dataBuf.len = sizeof(rsp);
      
      code = schHandleCallback(param, &dataBuf, TDMT_VND_RES_READY_RSP, 0);
      assert(code == 0 || code);
      
      pIter = taosHashIterate(execTasks, pIter);
    }  


    param = (SSchCallbackParam *)calloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;   

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId - 1;
      SQueryTableRsp rsp = {0};
      dataBuf.pData = &rsp;
      dataBuf.len = sizeof(rsp);
      
      code = schHandleCallback(param, &dataBuf, TDMT_VND_QUERY_RSP, 0);
      assert(code == 0 || code);
      
      pIter = taosHashIterate(execTasks, pIter);
    }    


    param = (SSchCallbackParam *)calloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;   

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId - 1;
      SResReadyRsp rsp = {0};
      dataBuf.pData = &rsp;
      dataBuf.len = sizeof(rsp);
      
      code = schHandleCallback(param, &dataBuf, TDMT_VND_RES_READY_RSP, 0);
      assert(code == 0 || code);
      
      pIter = taosHashIterate(execTasks, pIter);
    }  

    atomic_store_32(&schtStartFetch, 1);

    void *data = NULL;  
    code = schedulerFetchRows(queryJobRefId, &data);
    assert(code == 0 || code);

    if (0 == code) {
      SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
      assert(pRsp->completed == 1);
      assert(pRsp->numOfRows == 10);
    }

    data = NULL;
    code = schedulerFetchRows(queryJobRefId, &data);
    assert(code == 0 || code);
    
    schtFreeQueryJob(0);

    taosHashCleanup(execTasks);
    taosArrayDestroy(qnodeList);

    schtFreeQueryDag(&dag);

    if (++jobFinished % schtTestPrintNum == 0) {
      printf("jobFinished:%d\n", jobFinished);
    }

    ++schtQueryId;
  }

  schedulerDestroy();

}

void* schtFreeJobThread(void *aa) {
  while (!schtTestStop) {
    taosUsleep(taosRand() % 100);
    schtFreeQueryJob(1);
  }
}


}

TEST(queryTest, normalCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t job = 0;
  SQueryPlan dag;

  memset(&dag, 0, sizeof(dag));

  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);
  
  int32_t code = schedulerInit(NULL);
  ASSERT_EQ(code, 0);

  schtBuildQueryDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();
  
  code = schedulerAsyncExecJob(mockPointer, qnodeList, &dag, "select * from tb", &job);
  ASSERT_EQ(code, 0);

  
  SSchJob *pJob = schAcquireJob(job);
  
  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_VND_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }    

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_VND_RES_READY_RSP, (char *)&rsp, sizeof(rsp), 0);
    printf("code:%d", code);
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }  

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_VND_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }    

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_VND_RES_READY_RSP, (char *)&rsp, sizeof(rsp), 0);
    ASSERT_EQ(code, 0);
    
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }  

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job);

  void *data = NULL;  
  code = schedulerFetchRows(job, &data);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  tfree(data);

  data = NULL;
  code = schedulerFetchRows(job, &data);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(data == NULL);

  schReleaseJob(job);

  schedulerFreeJob(job);

  schtFreeQueryDag(&dag);

  schedulerDestroy();
}

TEST(queryTest, flowCtrlCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t job = 0;
  SQueryPlan dag;

  schtInitLogFile();

  taosSeedRand(taosGetTimestampSec());
  
  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);
  
  int32_t code = schedulerInit(NULL);
  ASSERT_EQ(code, 0);

  schtBuildQueryFlowCtrlDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();
  
  code = schedulerAsyncExecJob(mockPointer, qnodeList, &dag, "select * from tb", &job);
  ASSERT_EQ(code, 0);

  
  SSchJob *pJob = schAcquireJob(job);

  bool queryDone = false;
  
  while (!queryDone) {
    void *pIter = taosHashIterate(pJob->execTasks, NULL);
    if (NULL == pIter) {
      break;
    }
    
    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;

      taosHashCancelIterate(pJob->execTasks, pIter);

      if (task->lastMsgType == TDMT_VND_QUERY) {
        SQueryTableRsp rsp = {0};
        code = schHandleResponseMsg(pJob, task, TDMT_VND_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);
        
        ASSERT_EQ(code, 0);
      } else if (task->lastMsgType == TDMT_VND_RES_READY) {
        SResReadyRsp rsp = {0};
        code = schHandleResponseMsg(pJob, task, TDMT_VND_RES_READY_RSP, (char *)&rsp, sizeof(rsp), 0);
        ASSERT_EQ(code, 0);
      } else {
        queryDone = true;
        break;
      }
      
      pIter = NULL;
    }    
  }


  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job);

  void *data = NULL;  
  code = schedulerFetchRows(job, &data);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  tfree(data);

  data = NULL;
  code = schedulerFetchRows(job, &data);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(data == NULL);

  schReleaseJob(job);

  schedulerFreeJob(job);

  schtFreeQueryDag(&dag);

  schedulerDestroy();
}


TEST(insertTest, normalCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryPlan dag;
  uint64_t numOfRows = 0;

  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);
  
  int32_t code = schedulerInit(NULL);
  ASSERT_EQ(code, 0);

  schtBuildInsertDag(&dag);

  schtSetPlanToString();
  schtSetAsyncSendMsgToServer();

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtSendRsp, &insertJobRefId);

  SQueryResult res = {0};
  code = schedulerExecJob(mockPointer, qnodeList, &dag, &insertJobRefId, "insert into tb values(now,1)", &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.numOfRows, 20);

  schedulerFreeJob(insertJobRefId);

  schedulerDestroy();  
}

TEST(multiThread, forceFree) {
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1, thread2, thread3;
  taosThreadCreate(&(thread1), &thattr, schtRunJobThread, NULL);
  taosThreadCreate(&(thread2), &thattr, schtFreeJobThread, NULL);
  taosThreadCreate(&(thread3), &thattr, schtFetchRspThread, NULL);

  while (true) {
    if (schtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(schtTestMTRunSec);
      break;
    }
  }
  
  schtTestStop = true;
  taosSsleep(3);
}

int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
