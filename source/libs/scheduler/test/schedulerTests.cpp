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

#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "catalog.h"
#include "scheduler.h"
#include "tep.h"
#include "trpc.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wreturn-type"
#pragma GCC diagnostic ignored "-Wformat"

#include "schedulerInt.h"
#include "stub.h"
#include "addr_any.h"


namespace {

extern "C" int32_t schHandleResponseMsg(SSchJob *job, SSchTask *task, int32_t msgType, char *msg, int32_t msgSize, int32_t rspCode);
extern "C" int32_t schHandleCallback(void* param, const SDataBuf* pMsg, int32_t msgType, int32_t rspCode);

struct SSchJob *pInsertJob = NULL;
struct SSchJob *pQueryJob = NULL;

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

  char temp[128] = {0};
  sprintf(temp, "%s/%s", tsLogDir, defaultLogFileNamePrefix);
  if (taosInitLog(temp, tsNumOfLogLines, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }

}


void schtBuildQueryDag(SQueryDag *dag) {
  uint64_t qId = schtQueryId;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = taosArrayInit(dag->numOfSubplans, POINTER_BYTES);
  SArray *scan = taosArrayInit(1, POINTER_BYTES);
  SArray *merge = taosArrayInit(1, POINTER_BYTES);
  
  SSubplan *scanPlan = (SSubplan *)calloc(1, sizeof(SSubplan));
  SSubplan *mergePlan = (SSubplan *)calloc(1, sizeof(SSubplan));

  scanPlan->id.queryId = qId;
  scanPlan->id.templateId = 0x0000000000000002;
  scanPlan->id.subplanId = 0x0000000000000003;
  scanPlan->type = QUERY_TYPE_SCAN;

  scanPlan->execNode.nodeId = 1;
  scanPlan->execNode.epset.inUse = 0;
  addEpIntoEpSet(&scanPlan->execNode.epset, "ep0", 6030);

  scanPlan->pChildren = NULL;
  scanPlan->level = 1;
  scanPlan->pParents = taosArrayInit(1, POINTER_BYTES);
  scanPlan->pNode = (SPhyNode*)calloc(1, sizeof(SPhyNode));
  scanPlan->msgType = TDMT_VND_QUERY;

  mergePlan->id.queryId = qId;
  mergePlan->id.templateId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555555555;
  mergePlan->type = QUERY_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epset.numOfEps = 0;

  mergePlan->pChildren = taosArrayInit(1, POINTER_BYTES);
  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhyNode*)calloc(1, sizeof(SPhyNode));
  mergePlan->msgType = TDMT_VND_QUERY;

  SSubplan *mergePointer = (SSubplan *)taosArrayPush(merge, &mergePlan);
  SSubplan *scanPointer = (SSubplan *)taosArrayPush(scan, &scanPlan);

  taosArrayPush(mergePlan->pChildren, &scanPlan);
  taosArrayPush(scanPlan->pParents, &mergePlan);

  taosArrayPush(dag->pSubplans, &merge);  
  taosArrayPush(dag->pSubplans, &scan);
}

void schtFreeQueryDag(SQueryDag *dag) {

}


void schtBuildInsertDag(SQueryDag *dag) {
  uint64_t qId = 0x0000000000000002;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = taosArrayInit(1, POINTER_BYTES);
  SArray *inserta = taosArrayInit(dag->numOfSubplans, POINTER_BYTES);
  
  SSubplan *insertPlan = (SSubplan *)calloc(2, sizeof(SSubplan));

  insertPlan[0].id.queryId = qId;
  insertPlan[0].id.templateId = 0x0000000000000003;
  insertPlan[0].id.subplanId = 0x0000000000000004;
  insertPlan[0].type = QUERY_TYPE_MODIFY;
  insertPlan[0].level = 0;

  insertPlan[0].execNode.nodeId = 1;
  insertPlan[0].execNode.epset.inUse = 0;
  addEpIntoEpSet(&insertPlan[0].execNode.epset, "ep0", 6030);

  insertPlan[0].pChildren = NULL;
  insertPlan[0].pParents = NULL;
  insertPlan[0].pNode = NULL;
  insertPlan[0].pDataSink = (SDataSink*)calloc(1, sizeof(SDataSink));
  insertPlan[0].msgType = TDMT_VND_SUBMIT;

  insertPlan[1].id.queryId = qId;
  insertPlan[1].id.templateId = 0x0000000000000003;
  insertPlan[1].id.subplanId = 0x0000000000000005;
  insertPlan[1].type = QUERY_TYPE_MODIFY;
  insertPlan[1].level = 0;

  insertPlan[1].execNode.nodeId = 1;
  insertPlan[1].execNode.epset.inUse = 0;
  addEpIntoEpSet(&insertPlan[1].execNode.epset, "ep0", 6030);

  insertPlan[1].pChildren = NULL;
  insertPlan[1].pParents = NULL;
  insertPlan[1].pNode = NULL;
  insertPlan[1].pDataSink = (SDataSink*)calloc(1, sizeof(SDataSink));
  insertPlan[1].msgType = TDMT_VND_SUBMIT;

  taosArrayPush(inserta, &insertPlan);
  insertPlan += 1;
  taosArrayPush(inserta, &insertPlan);

  taosArrayPush(dag->pSubplans, &inserta);  
}


int32_t schtPlanToString(const SSubplan *subplan, char** str, int32_t* len) {
  *str = (char *)calloc(1, 20);
  *len = 20;
  return 0;
}

void schtExecNode(SSubplan* subplan, uint64_t templateId, SQueryNodeAddr* ep) {

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

    SSubmitRsp rsp = {0};
    rsp.affectedRows = 10;
    schHandleResponseMsg(job, task, TDMT_VND_SUBMIT_RSP, (char *)&rsp, sizeof(rsp), 0);
    
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  return NULL;
}

void *schtCreateFetchRspThread(void *param) {
  struct SSchJob* job = (struct SSchJob*)param;

  sleep(1);

  int32_t code = 0;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)calloc(1, sizeof(SRetrieveTableRsp));
  rsp->completed = 1;
  rsp->numOfRows = 10;
 
  code = schHandleResponseMsg(job, job->fetchTask, TDMT_VND_FETCH_RSP, (char *)rsp, sizeof(*rsp), 0);
    
  assert(code == 0);
}


void *schtFetchRspThread(void *aa) {
  SDataBuf dataBuf = {0};
  SSchCallbackParam* param = NULL;

  while (!schtTestStop) {
    if (0 == atomic_val_compare_exchange_32(&schtStartFetch, 1, 0)) {
      continue;
    }

    usleep(1);
    
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
  SSchJob *job = atomic_load_ptr(&pQueryJob);
  
  if (job && atomic_val_compare_exchange_ptr(&pQueryJob, job, NULL)) {
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
  SQueryDag dag = {0};

  schtInitLogFile();

  
  int32_t code = schedulerInit(NULL);
  assert(code == 0);


  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  SSchJob *job = NULL;
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

    code = schedulerAsyncExecJob(mockPointer, qnodeList, &dag, "select * from tb", &job);
    assert(code == 0);

    execTasks = taosHashInit(5, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
    void *pIter = taosHashIterate(job->execTasks, NULL);
    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;
      schtFetchTaskId = task->taskId - 1;
      
      taosHashPut(execTasks, &task->taskId, sizeof(task->taskId), task, sizeof(*task));
      pIter = taosHashIterate(job->execTasks, pIter);
    }    

    param = (SSchCallbackParam *)calloc(1, sizeof(*param));
    param->queryId = schtQueryId;
    
    pQueryJob = job;
    

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
    param->queryId = schtQueryId;

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
    param->queryId = schtQueryId;

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
    param->queryId = schtQueryId;

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
    code = schedulerFetchRows(pQueryJob, &data);
    assert(code == 0 || code);

    if (0 == code) {
      SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
      assert(pRsp->completed == 1);
      assert(pRsp->numOfRows == 10);
    }

    data = NULL;
    code = schedulerFetchRows(pQueryJob, &data);
    assert(code == 0 || code);
    
    schtFreeQueryJob(0);

    taosHashCleanup(execTasks);

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
    usleep(rand() % 100);
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
  SSchJob *pJob = NULL;
  SQueryDag dag = {0};

  schtInitLogFile();

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
  
  code = schedulerAsyncExecJob(mockPointer, qnodeList, &dag, "select * from tb", &pJob);
  ASSERT_EQ(code, 0);

  SSchJob *job = (SSchJob *)pJob;
  void *pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_RES_READY_RSP, (char *)&rsp, sizeof(rsp), 0);
    printf("code:%d", code);
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }  

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(job->execTasks, pIter);
  }    

  pIter = taosHashIterate(job->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SResReadyRsp rsp = {0};
    code = schHandleResponseMsg(job, task, TDMT_VND_RES_READY_RSP, (char *)&rsp, sizeof(rsp), 0);
    ASSERT_EQ(code, 0);
    
    pIter = taosHashIterate(job->execTasks, pIter);
  }  

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t thread1;
  pthread_create(&(thread1), &thattr, schtCreateFetchRspThread, job);

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
  ASSERT_TRUE(data);

  schedulerFreeJob(pJob);

  schtFreeQueryDag(&dag);

  schedulerDestroy();
}



TEST(insertTest, normalCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryDag dag = {0};
  uint64_t numOfRows = 0;

  schtInitLogFile();

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

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t thread1;
  pthread_create(&(thread1), &thattr, schtSendRsp, &pInsertJob);

  SQueryResult res = {0};
  code = schedulerExecJob(mockPointer, qnodeList, &dag, &pInsertJob, "insert into tb values(now,1)", &res);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.numOfRows, 20);

  schedulerFreeJob(pInsertJob);

  schedulerDestroy();  
}

TEST(multiThread, forceFree) {
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);

  pthread_t thread1, thread2, thread3;
  pthread_create(&(thread1), &thattr, schtRunJobThread, NULL);
  pthread_create(&(thread2), &thattr, schtFreeJobThread, NULL);
  pthread_create(&(thread3), &thattr, schtFetchRspThread, NULL);

  while (true) {
    if (schtTestDeadLoop) {
      sleep(1);
    } else {
      sleep(schtTestMTRunSec);
      break;
    }
  }
  
  schtTestStop = true;
  sleep(3);
}

int main(int argc, char** argv) {
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop