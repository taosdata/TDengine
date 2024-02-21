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

#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif
#include "os.h"

#include "catalog.h"
#include "scheduler.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "trpc.h"
#include "tvariant.h"
#include "tmisce.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wreturn-type"
#pragma GCC diagnostic ignored "-Wformat"

#include "schInt.h"
#include "stub.h"
#include "tref.h"

namespace {

extern "C" int32_t schHandleResponseMsg(SSchJob *pJob, SSchTask *pTask, int32_t execId, SDataBuf *pMsg, int32_t rspCode);
extern "C" int32_t schHandleCallback(void *param, const SDataBuf *pMsg, int32_t rspCode);

int64_t insertJobRefId = 0;
int64_t queryJobRefId = 0;

bool     schtJobDone = false;
uint64_t schtMergeTemplateId = 0x4;
uint64_t schtFetchTaskId = 0;
uint64_t schtQueryId = 1;

bool    schtTestStop = false;
bool    schtTestDeadLoop = false;
int32_t schtTestMTRunSec = 1;
int32_t schtTestPrintNum = 1000;
int32_t schtStartFetch = 0;

void schtInitLogFile() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

void schtQueryCb(SExecResult *pResult, void *param, int32_t code) {
  *(int32_t *)param = 1;
}

int32_t schtBuildQueryRspMsg(uint32_t *msize, void** rspMsg) {
  SQueryTableRsp  rsp = {0};
  rsp.code = 0;
  rsp.affectedRows = 0;
  rsp.tbVerInfo = NULL;

  int32_t msgSize = tSerializeSQueryTableRsp(NULL, 0, &rsp);
  if (msgSize < 0) {
    qError("tSerializeSQueryTableRsp failed");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  void *pRsp = taosMemoryCalloc(msgSize, 1);
  if (NULL == pRsp) {
    qError("rpcMallocCont %d failed", msgSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (tSerializeSQueryTableRsp(pRsp, msgSize, &rsp) < 0) {
    qError("tSerializeSQueryTableRsp %d failed", msgSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *rspMsg = pRsp;
  *msize = msgSize;

  return TSDB_CODE_SUCCESS;
}


int32_t schtBuildFetchRspMsg(uint32_t *msize, void** rspMsg) {
  SRetrieveTableRsp* rsp = (SRetrieveTableRsp*)taosMemoryCalloc(sizeof(SRetrieveTableRsp), 1);
  rsp->completed = 1;
  rsp->numOfRows = 10;
  rsp->compLen = 0;

  *rspMsg = rsp;
  *msize = sizeof(SRetrieveTableRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t schtBuildSubmitRspMsg(uint32_t *msize, void** rspMsg) {
  SSubmitRsp2 submitRsp = {0};
  int32_t msgSize = 0, ret = 0;
  SEncoder     ec = {0};
  
  tEncodeSize(tEncodeSSubmitRsp2, &submitRsp, msgSize, ret);
  void* msg = taosMemoryCalloc(1, msgSize);
  tEncoderInit(&ec, (uint8_t*)msg, msgSize);
  tEncodeSSubmitRsp2(&ec, &submitRsp);
  tEncoderClear(&ec);

  *rspMsg = msg;
  *msize = msgSize;

  return TSDB_CODE_SUCCESS;
}


void schtBuildQueryDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;

  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *scan = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *merge = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);

  SSubplan *scanPlan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
  SSubplan *mergePlan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);

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
  scanPlan->pNode = (SPhysiNode *)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  scanPlan->msgType = TDMT_SCH_QUERY;

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pChildren = nodesMakeList();
  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhysiNode *)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE);
  mergePlan->msgType = TDMT_SCH_QUERY;

  merge->pNodeList = nodesMakeList();
  scan->pNodeList = nodesMakeList();

  nodesListAppend(merge->pNodeList, (SNode *)mergePlan);
  nodesListAppend(scan->pNodeList, (SNode *)scanPlan);

  nodesListAppend(mergePlan->pChildren, (SNode *)scanPlan);
  nodesListAppend(scanPlan->pParents, (SNode *)mergePlan);

  nodesListAppend(dag->pSubplans, (SNode *)merge);
  nodesListAppend(dag->pSubplans, (SNode *)scan);
}

void schtBuildQueryFlowCtrlDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;
  int32_t  scanPlanNum = 20;

  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *scan = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *merge = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);

  SSubplan *mergePlan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);

  merge->pNodeList = nodesMakeList();
  scan->pNodeList = nodesMakeList();

  mergePlan->pChildren = nodesMakeList();

  for (int32_t i = 0; i < scanPlanNum; ++i) {
    SSubplan *scanPlan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
    scanPlan->id.queryId = qId;
    scanPlan->id.groupId = 0x0000000000000002;
    scanPlan->id.subplanId = 0x0000000000000003 + i;
    scanPlan->subplanType = SUBPLAN_TYPE_SCAN;

    scanPlan->execNode.nodeId = 1 + i;
    scanPlan->execNode.epSet.inUse = 0;
    scanPlan->execNodeStat.tableNum = taosRand() % 30;
    addEpIntoEpSet(&scanPlan->execNode.epSet, "ep0", 6030);
    addEpIntoEpSet(&scanPlan->execNode.epSet, "ep1", 6030);
    addEpIntoEpSet(&scanPlan->execNode.epSet, "ep2", 6030);
    scanPlan->execNode.epSet.inUse = taosRand() % 3;

    scanPlan->pChildren = NULL;
    scanPlan->level = 1;
    scanPlan->pParents = nodesMakeList();
    scanPlan->pNode = (SPhysiNode *)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
    scanPlan->msgType = TDMT_SCH_QUERY;

    nodesListAppend(scanPlan->pParents, (SNode *)mergePlan);
    nodesListAppend(mergePlan->pChildren, (SNode *)scanPlan);

    nodesListAppend(scan->pNodeList, (SNode *)scanPlan);
  }

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhysiNode *)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE);
  mergePlan->msgType = TDMT_SCH_QUERY;

  nodesListAppend(merge->pNodeList, (SNode *)mergePlan);

  nodesListAppend(dag->pSubplans, (SNode *)merge);
  nodesListAppend(dag->pSubplans, (SNode *)scan);
}

void schtFreeQueryDag(SQueryPlan *dag) {}

void schtBuildInsertDag(SQueryPlan *dag) {
  uint64_t qId = 0x0000000000000002;

  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *inserta = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);
  inserta->pNodeList = nodesMakeList();

  SSubplan *insertPlan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);

  insertPlan->id.queryId = qId;
  insertPlan->id.groupId = 0x0000000000000003;
  insertPlan->id.subplanId = 0x0000000000000004;
  insertPlan->subplanType = SUBPLAN_TYPE_MODIFY;
  insertPlan->level = 0;

  insertPlan->execNode.nodeId = 1;
  insertPlan->execNode.epSet.inUse = 0;
  addEpIntoEpSet(&insertPlan->execNode.epSet, "ep0", 6030);

  insertPlan->pChildren = NULL;
  insertPlan->pParents = NULL;
  insertPlan->pNode = NULL;
  insertPlan->pDataSink = (SDataSinkNode*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT);
  ((SDataInserterNode*)insertPlan->pDataSink)->size = 1;
  ((SDataInserterNode*)insertPlan->pDataSink)->pData = taosMemoryCalloc(1, 1);
  insertPlan->msgType = TDMT_VND_SUBMIT;

  nodesListAppend(inserta->pNodeList, (SNode *)insertPlan);

  insertPlan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);

  insertPlan->id.queryId = qId;
  insertPlan->id.groupId = 0x0000000000000003;
  insertPlan->id.subplanId = 0x0000000000000005;
  insertPlan->subplanType = SUBPLAN_TYPE_MODIFY;
  insertPlan->level = 0;

  insertPlan->execNode.nodeId = 1;
  insertPlan->execNode.epSet.inUse = 0;
  addEpIntoEpSet(&insertPlan->execNode.epSet, "ep0", 6030);

  insertPlan->pChildren = NULL;
  insertPlan->pParents = NULL;
  insertPlan->pNode = NULL;
  insertPlan->pDataSink = (SDataSinkNode*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT);
  ((SDataInserterNode*)insertPlan->pDataSink)->size = 1;
  ((SDataInserterNode*)insertPlan->pDataSink)->pData = taosMemoryCalloc(1, 1);
  insertPlan->msgType = TDMT_VND_SUBMIT;

  nodesListAppend(inserta->pNodeList, (SNode *)insertPlan);

  nodesListAppend(dag->pSubplans, (SNode *)inserta);
}

int32_t schtPlanToString(const SSubplan *subplan, char **str, int32_t *len) {
  *str = (char *)taosMemoryCalloc(1, 20);
  *len = 20;
  return 0;
}

void schtExecNode(SSubplan *subplan, uint64_t groupId, SQueryNodeAddr *ep) {}

void schtRpcSendRequest(void *shandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *pRid) {}

void schtSetPlanToString() {
  static Stub stub;
  stub.set(qSubPlanToString, schtPlanToString);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qSubPlanToString", result);
#endif
#ifdef LINUX
    AddrAny                       any("libplanner.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qSubPlanToString$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, schtPlanToString);
    }
  }
}

void schtSetExecNode() {
  static Stub stub;
  stub.set(qSetSubplanExecutionNode, schtExecNode);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("qSetSubplanExecutionNode", result);
#endif
#ifdef LINUX
    AddrAny                       any("libplanner.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^qSetSubplanExecutionNode$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, schtExecNode);
    }
  }
}

void schtSetRpcSendRequest() {
  static Stub stub;
  stub.set(rpcSendRequest, schtRpcSendRequest);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("rpcSendRequest", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRequest$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, schtRpcSendRequest);
    }
  }
}

int32_t schtAsyncSendMsgToServer(void *pTransporter, SEpSet *epSet, int64_t *pTransporterId, SMsgSendInfo *pInfo, bool persistHandle, void* rpcCtx) {
  if (pInfo) {
    taosMemoryFreeClear(pInfo->param);
    taosMemoryFreeClear(pInfo->msgInfo.pData);
    taosMemoryFree(pInfo);
  }
  return 0;
}

void schtSetAsyncSendMsgToServer() {
  static Stub stub;
  stub.set(asyncSendMsgToServerExt, schtAsyncSendMsgToServer);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("asyncSendMsgToServerExt", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^asyncSendMsgToServerExt$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, schtAsyncSendMsgToServer);
    }
  }
}

void *schtSendRsp(void *param) {
  SSchJob *pJob = NULL;
  int64_t  job = 0;
  int32_t  code = 0;

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

    SDataBuf msg = {0};
    void* rmsg = NULL;
    schtBuildSubmitRspMsg(&msg.len, &rmsg);
    msg.msgType = TDMT_VND_SUBMIT_RSP;
    msg.pData = rmsg;
    
    schHandleResponseMsg(pJob, task, task->execId, &msg, 0);

    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  schReleaseJob(job);

  schtJobDone = true;
  
  return NULL;
}

void *schtCreateFetchRspThread(void *param) {
  int64_t  job = *(int64_t *)param;
  SSchJob *pJob = schAcquireJob(job);

  taosSsleep(1);

  int32_t            code = 0;
  SDataBuf msg = {0};
  void* rmsg = NULL;
  schtBuildFetchRspMsg(&msg.len, &rmsg);
  msg.msgType = TDMT_SCH_MERGE_FETCH_RSP;
  msg.pData = rmsg;
  
  code = schHandleResponseMsg(pJob, pJob->fetchTask, pJob->fetchTask->execId, &msg, 0);

  schReleaseJob(job);

  assert(code == 0);
  return NULL;
}

void *schtFetchRspThread(void *aa) {
  SDataBuf               dataBuf = {0};
  SSchTaskCallbackParam *param = NULL;

  while (!schtTestStop) {
    if (0 == atomic_val_compare_exchange_32(&schtStartFetch, 1, 0)) {
      continue;
    }

    taosUsleep(100);

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));

    param->queryId = schtQueryId;
    param->taskId = schtFetchTaskId;

    int32_t            code = 0;
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
    rsp->completed = 1;
    rsp->numOfRows = 10;

    dataBuf.msgType = TDMT_SCH_FETCH_RSP;
    dataBuf.pData = rsp;
    dataBuf.len = sizeof(*rsp);

    code = schHandleCallback(param, &dataBuf, 0);

    assert(code == 0 || code);
  }
  return NULL;
}

void schtFreeQueryJob(int32_t freeThread) {
  static uint32_t freeNum = 0;
  int64_t         job = queryJobRefId;

  if (job && atomic_val_compare_exchange_64(&queryJobRefId, job, 0)) {
    schedulerFreeJob(&job, 0);
    if (freeThread) {
      if (++freeNum % schtTestPrintNum == 0) {
        printf("FreeNum:%d\n", freeNum);
      }
    }
  }
}

void *schtRunJobThread(void *aa) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryPlan* dag = (SQueryPlan*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);

  schtInitLogFile();

  int32_t code = schedulerInit();
  assert(code == 0);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  SSchJob               *pJob = NULL;
  SSchTaskCallbackParam *param = NULL;
  SHashObj              *execTasks = NULL;
  uint32_t               jobFinished = 0;
  int32_t                queryDone = 0;

  while (!schtTestStop) {
    schtBuildQueryDag(dag);

    SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

    SQueryNodeLoad load = {0};
    load.addr.epSet.numOfEps = 1;
    strcpy(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
    load.addr.epSet.eps[0].port = 6031;
    taosArrayPush(qnodeList, &load);

    queryDone = 0;

    SRequestConnInfo conn = {0};
    conn.pTrans = mockPointer;
    SSchedulerReq req = {0};
    req.syncReq = false;
    req.pConn = &conn;
    req.pNodeList = qnodeList;
    req.pDag = dag;
    req.sql = "select * from tb";
    req.execFp = schtQueryCb;
    req.cbParam = &queryDone;

    code = schedulerExecJob(&req, &queryJobRefId);
    assert(code == 0);

    pJob = schAcquireJob(queryJobRefId);
    if (NULL == pJob) {
      taosArrayDestroy(qnodeList);
      schtFreeQueryDag(dag);
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

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId;

      SDataBuf msg = {0};
      void* rmsg = NULL;
      schtBuildQueryRspMsg(&msg.len, &rmsg);
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleCallback(param, &msg, 0);
      assert(code == 0 || code);

      pIter = taosHashIterate(execTasks, pIter);
    }

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId - 1;
      SDataBuf msg = {0};
      void* rmsg = NULL;
      schtBuildQueryRspMsg(&msg.len, &rmsg);
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleCallback(param, &msg, 0);
      assert(code == 0 || code);

      pIter = taosHashIterate(execTasks, pIter);
    }

    while (true) {
      if (queryDone) {
        break;
      }

      taosUsleep(10000);
    }

    atomic_store_32(&schtStartFetch, 1);

    void *data = NULL;
    req.syncReq = true;
    req.pFetchRes = &data;

    code = schedulerFetchRows(queryJobRefId, &req);
    assert(code == 0 || code);

    if (0 == code) {
      SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
      assert(pRsp->completed == 1);
    }

    data = NULL;
    code = schedulerFetchRows(queryJobRefId, &req);
    assert(code == 0 || code);

    schtFreeQueryJob(0);

    taosHashCleanup(execTasks);
    taosArrayDestroy(qnodeList);

    schtFreeQueryDag(dag);

    if (++jobFinished % schtTestPrintNum == 0) {
      printf("jobFinished:%d\n", jobFinished);
    }

    ++schtQueryId;
  }

  schedulerDestroy();

  return NULL;
}

void *schtFreeJobThread(void *aa) {
  while (!schtTestStop) {
    taosUsleep(taosRand() % 100);
    schtFreeQueryJob(1);
  }
  return NULL;
}


}  // namespace

TEST(queryTest, normalCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan* dag = (SQueryPlan*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  strcpy(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  taosArrayPush(qnodeList, &load);

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryDag(dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  int32_t queryDone = 0;

  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = dag;
  req.sql = "select * from tb";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;

  code = schedulerExecJob(&req, &job);
  ASSERT_EQ(code, 0);

  SSchJob *pJob = schAcquireJob(job);

  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SDataBuf msg = {0};
    void* rmsg = NULL;
    schtBuildQueryRspMsg(&msg.len, &rmsg);
    msg.msgType = TDMT_SCH_QUERY_RSP;
    msg.pData = rmsg;
    
    code = schHandleResponseMsg(pJob, task, task->execId, &msg, 0);
    
    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;
    if (JOB_TASK_STATUS_EXEC == task->status) {
      SDataBuf msg = {0};
      void* rmsg = NULL;
      schtBuildQueryRspMsg(&msg.len, &rmsg);
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;
      
      code = schHandleResponseMsg(pJob, task, task->execId, &msg, 0);
      
      ASSERT_EQ(code, 0);
    }

    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  while (true) {
    if (queryDone) {
      break;
    }

    taosUsleep(10000);
  }

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job);

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;

  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  schReleaseJob(job);
  
  schedulerDestroy();

  schedulerFreeJob(&job, 0);

}

TEST(queryTest, readyFirstCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan* dag = (SQueryPlan*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  strcpy(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  taosArrayPush(qnodeList, &load);  

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryDag(dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  int32_t queryDone = 0;

  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = dag;
  req.sql = "select * from tb";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;
  code = schedulerExecJob(&req, &job);
  ASSERT_EQ(code, 0);

  SSchJob *pJob = schAcquireJob(job);

  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SDataBuf msg = {0};
    void* rmsg = NULL;
    schtBuildQueryRspMsg(&msg.len, &rmsg);
    msg.msgType = TDMT_SCH_QUERY_RSP;
    msg.pData = rmsg;
    
    code = schHandleResponseMsg(pJob, task, task->execId, &msg, 0);

    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    if (JOB_TASK_STATUS_EXEC == task->status) {
      SDataBuf msg = {0};
      void* rmsg = NULL;
      schtBuildQueryRspMsg(&msg.len, &rmsg);
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;
      
      code = schHandleResponseMsg(pJob, task, task->execId, &msg, 0);
      
      ASSERT_EQ(code, 0);
    }

    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  while (true) {
    if (queryDone) {
      break;
    }

    taosUsleep(10000);
  }

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job);

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  schReleaseJob(job);

  schedulerDestroy();

  schedulerFreeJob(&job, 0);
}

TEST(queryTest, flowCtrlCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan* dag = (SQueryPlan*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);

  schtInitLogFile();

  taosSeedRand(taosGetTimestampSec());

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  strcpy(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  taosArrayPush(qnodeList, &load);


  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryFlowCtrlDag(dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  initTaskQueue();

  int32_t          queryDone = 0;
  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = dag;
  req.sql = "select * from tb";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;

  code = schedulerExecJob(&req, &job);
  ASSERT_EQ(code, 0);

  SSchJob *pJob = schAcquireJob(job);

  while (!queryDone) {
    void *pIter = taosHashIterate(pJob->execTasks, NULL);
    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;

      if (JOB_TASK_STATUS_EXEC == task->status && 0 != task->lastMsgType) {
        SDataBuf msg = {0};
        void* rmsg = NULL;
        schtBuildQueryRspMsg(&msg.len, &rmsg);
        msg.msgType = TDMT_SCH_QUERY_RSP;
        msg.pData = rmsg;
        
        code = schHandleResponseMsg(pJob, task, task->execId, &msg, 0);
        
        ASSERT_EQ(code, 0);
      }

      pIter = taosHashIterate(pJob->execTasks, pIter);
    }
  }

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job);

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  schReleaseJob(job);

  schedulerDestroy();

  schedulerFreeJob(&job, 0);
}

TEST(insertTest, normalCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryPlan* dag = (SQueryPlan*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);
  uint64_t    numOfRows = 0;

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  strcpy(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  taosArrayPush(qnodeList, &load);

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildInsertDag(dag);

  schtSetPlanToString();
  schtSetAsyncSendMsgToServer();

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  schtJobDone = false;

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtSendRsp, &insertJobRefId);

  int32_t          queryDone = 0;
  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = dag;
  req.sql = "insert into tb values(now,1)";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;

  code = schedulerExecJob(&req, &insertJobRefId);
  ASSERT_EQ(code, 0);

  while (true) {
    if (schtJobDone) {
      break;
    }

    taosUsleep(10000);
  }

  schedulerFreeJob(&insertJobRefId, 0);

  schedulerDestroy();
}

TEST(multiThread, forceFree) {
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1, thread2, thread3;
  taosThreadCreate(&(thread1), &thattr, schtRunJobThread, NULL);
//  taosThreadCreate(&(thread2), &thattr, schtFreeJobThread, NULL);
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
  //taosSsleep(3);
}

TEST(otherTest, otherCase) {
  // excpet test
  schReleaseJob(0);
  schFreeRpcCtx(NULL);

  ASSERT_EQ(schDumpEpSet(NULL), (char*)NULL);
  ASSERT_EQ(strcmp(schGetOpStr(SCH_OP_NULL), "NULL"), 0);
  ASSERT_EQ(strcmp(schGetOpStr((SCH_OP_TYPE)100), "UNKNOWN"), 0);
}

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
