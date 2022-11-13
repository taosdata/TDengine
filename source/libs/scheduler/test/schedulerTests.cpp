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

extern "C" int32_t schHandleResponseMsg(SSchJob *job, SSchTask *task, int32_t msgType, char *msg, int32_t msgSize,
                                        int32_t rspCode);
extern "C" int32_t schHandleCallback(void *param, const SDataBuf *pMsg, int32_t msgType, int32_t rspCode);

int64_t insertJobRefId = 0;
int64_t queryJobRefId = 0;

uint64_t schtMergeTemplateId = 0x4;
uint64_t schtFetchTaskId = 0;
uint64_t schtQueryId = 1;

bool    schtTestStop = false;
bool    schtTestDeadLoop = false;
int32_t schtTestMTRunSec = 10;
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
  assert(TSDB_CODE_SUCCESS == code);
  *(int32_t *)param = 1;
}

void schtBuildQueryDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;

  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = nodesMakeList();
  SNodeListNode *scan = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);
  SNodeListNode *merge = (SNodeListNode *)nodesMakeNode(QUERY_NODE_NODE_LIST);

  SSubplan *scanPlan = (SSubplan *)taosMemoryCalloc(1, sizeof(SSubplan));
  SSubplan *mergePlan = (SSubplan *)taosMemoryCalloc(1, sizeof(SSubplan));

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
  scanPlan->pNode = (SPhysiNode *)taosMemoryCalloc(1, sizeof(SPhysiNode));
  scanPlan->msgType = TDMT_SCH_QUERY;

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pChildren = nodesMakeList();
  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhysiNode *)taosMemoryCalloc(1, sizeof(SPhysiNode));
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

  SSubplan *scanPlan = (SSubplan *)taosMemoryCalloc(scanPlanNum, sizeof(SSubplan));
  SSubplan *mergePlan = (SSubplan *)taosMemoryCalloc(1, sizeof(SSubplan));

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
    scanPlan[i].pNode = (SPhysiNode *)taosMemoryCalloc(1, sizeof(SPhysiNode));
    scanPlan[i].msgType = TDMT_SCH_QUERY;

    nodesListAppend(scanPlan[i].pParents, (SNode *)mergePlan);
    nodesListAppend(mergePlan->pChildren, (SNode *)(scanPlan + i));

    nodesListAppend(scan->pNodeList, (SNode *)(scanPlan + i));
  }

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pParents = NULL;
  mergePlan->pNode = (SPhysiNode *)taosMemoryCalloc(1, sizeof(SPhysiNode));
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

  SSubplan *insertPlan = (SSubplan *)taosMemoryCalloc(2, sizeof(SSubplan));

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
  insertPlan[0].pDataSink = (SDataSinkNode *)taosMemoryCalloc(1, sizeof(SDataSinkNode));
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
  insertPlan[1].pDataSink = (SDataSinkNode *)taosMemoryCalloc(1, sizeof(SDataSinkNode));
  insertPlan[1].msgType = TDMT_VND_SUBMIT;

  inserta->pNodeList = nodesMakeList();

  nodesListAppend(inserta->pNodeList, (SNode *)insertPlan);
  insertPlan += 1;
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

int32_t schtAsyncSendMsgToServer(void *pTransporter, SEpSet *epSet, int64_t *pTransporterId, SMsgSendInfo *pInfo) {
  if (pInfo) {
    taosMemoryFreeClear(pInfo->param);
    taosMemoryFreeClear(pInfo->msgInfo.pData);
    taosMemoryFree(pInfo);
  }
  return 0;
}

void schtSetAsyncSendMsgToServer() {
  static Stub stub;
  stub.set(asyncSendMsgToServer, schtAsyncSendMsgToServer);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("asyncSendMsgToServer", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^asyncSendMsgToServer$", result);
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

    SSubmitRsp rsp = {0};
    rsp.affectedRows = 10;
    schHandleResponseMsg(pJob, task, TDMT_VND_SUBMIT_RSP, (char *)&rsp, sizeof(rsp), 0);

    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  schReleaseJob(job);

  return NULL;
}

void *schtCreateFetchRspThread(void *param) {
  int64_t  job = *(int64_t *)param;
  SSchJob *pJob = schAcquireJob(job);

  taosSsleep(1);

  int32_t            code = 0;
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
  rsp->completed = 1;
  rsp->numOfRows = 10;

  code = schHandleResponseMsg(pJob, pJob->fetchTask, TDMT_SCH_FETCH_RSP, (char *)rsp, sizeof(*rsp), 0);

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

    taosUsleep(1);

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));

    param->queryId = schtQueryId;
    param->taskId = schtFetchTaskId;

    int32_t            code = 0;
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
    rsp->completed = 1;
    rsp->numOfRows = 10;

    dataBuf.pData = rsp;
    dataBuf.len = sizeof(*rsp);

    code = schHandleCallback(param, &dataBuf, TDMT_SCH_FETCH_RSP, 0);

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
  SQueryPlan  dag;

  schtInitLogFile();

  int32_t code = schedulerInit();
  assert(code == 0);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  SSchJob               *pJob = NULL;
  SSchTaskCallbackParam *param = NULL;
  SHashObj              *execTasks = NULL;
  SDataBuf               dataBuf = {0};
  uint32_t               jobFinished = 0;
  int32_t                queryDone = 0;

  while (!schtTestStop) {
    schtBuildQueryDag(&dag);

    SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

    SEp qnodeAddr = {0};
    strcpy(qnodeAddr.fqdn, "qnode0.ep");
    qnodeAddr.port = 6031;
    taosArrayPush(qnodeList, &qnodeAddr);

    queryDone = 0;

    SRequestConnInfo conn = {0};
    conn.pTrans = mockPointer;
    SSchedulerReq req = {0};
    req.syncReq = false;
    req.pConn = &conn;
    req.pNodeList = qnodeList;
    req.pDag = &dag;
    req.sql = "select * from tb";
    req.execFp = schtQueryCb;
    req.cbParam = &queryDone;

    code = schedulerExecJob(&req, &queryJobRefId);
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

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId;
      SQueryTableRsp rsp = {0};
      dataBuf.pData = &rsp;
      dataBuf.len = sizeof(rsp);

      code = schHandleCallback(param, &dataBuf, TDMT_SCH_QUERY_RSP, 0);
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
      SQueryTableRsp rsp = {0};
      dataBuf.pData = &rsp;
      dataBuf.len = sizeof(rsp);

      code = schHandleCallback(param, &dataBuf, TDMT_SCH_QUERY_RSP, 0);
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
      assert(pRsp->numOfRows == 10);
    }

    data = NULL;
    code = schedulerFetchRows(queryJobRefId, &req);
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
  SQueryPlan  dag;

  memset(&dag, 0, sizeof(dag));

  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  int32_t queryDone = 0;

  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = &dag;
  req.sql = "select * from tb";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;

  code = schedulerExecJob(&req, &job);
  ASSERT_EQ(code, 0);

  SSchJob *pJob = schAcquireJob(job);

  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_SCH_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);

    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_SCH_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);

    ASSERT_EQ(code, 0);
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

  data = NULL;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(data == NULL);

  schReleaseJob(job);

  schedulerFreeJob(&job, 0);

  schtFreeQueryDag(&dag);

  schedulerDestroy();
}

TEST(queryTest, readyFirstCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan  dag;

  memset(&dag, 0, sizeof(dag));

  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  int32_t queryDone = 0;

  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = &dag;
  req.sql = "select * from tb";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;
  code = schedulerExecJob(&req, &job);
  ASSERT_EQ(code, 0);

  SSchJob *pJob = schAcquireJob(job);

  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_SCH_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);

    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SQueryTableRsp rsp = {0};
    code = schHandleResponseMsg(pJob, task, TDMT_SCH_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);

    ASSERT_EQ(code, 0);
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

  data = NULL;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(data == NULL);

  schReleaseJob(job);

  schedulerFreeJob(&job, 0);

  schtFreeQueryDag(&dag);

  schedulerDestroy();
}

TEST(queryTest, flowCtrlCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan  dag;

  schtInitLogFile();

  taosSeedRand(taosGetTimestampSec());

  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryFlowCtrlDag(&dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  int32_t          queryDone = 0;
  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = &dag;
  req.sql = "select * from tb";
  req.execFp = schtQueryCb;
  req.cbParam = &queryDone;

  code = schedulerExecJob(&req, &job);
  ASSERT_EQ(code, 0);

  SSchJob *pJob = schAcquireJob(job);

  bool qDone = false;

  while (!qDone) {
    void *pIter = taosHashIterate(pJob->execTasks, NULL);
    if (NULL == pIter) {
      break;
    }

    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;

      taosHashCancelIterate(pJob->execTasks, pIter);

      if (task->lastMsgType == TDMT_SCH_QUERY) {
        SQueryTableRsp rsp = {0};
        code = schHandleResponseMsg(pJob, task, TDMT_SCH_QUERY_RSP, (char *)&rsp, sizeof(rsp), 0);

        ASSERT_EQ(code, 0);
      } else {
        qDone = true;
        break;
      }

      pIter = NULL;
    }
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

  data = NULL;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(data == NULL);

  schReleaseJob(job);

  schedulerFreeJob(&job, 0);

  schtFreeQueryDag(&dag);

  schedulerDestroy();
}

TEST(insertTest, normalCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryPlan  dag;
  uint64_t    numOfRows = 0;

  SArray *qnodeList = taosArrayInit(1, sizeof(SEp));

  SEp qnodeAddr = {0};
  strcpy(qnodeAddr.fqdn, "qnode0.ep");
  qnodeAddr.port = 6031;
  taosArrayPush(qnodeList, &qnodeAddr);

  int32_t code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildInsertDag(&dag);

  schtSetPlanToString();
  schtSetAsyncSendMsgToServer();

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1;
  taosThreadCreate(&(thread1), &thattr, schtSendRsp, &insertJobRefId);

  SExecResult res = {0};

  SRequestConnInfo conn = {0};
  conn.pTrans = mockPointer;
  SSchedulerReq req = {0};
  req.pConn = &conn;
  req.pNodeList = qnodeList;
  req.pDag = &dag;
  req.sql = "insert into tb values(now,1)";
  req.execFp = schtQueryCb;
  req.cbParam = NULL;

  code = schedulerExecJob(&req, &insertJobRefId);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(res.numOfRows, 20);

  schedulerFreeJob(&insertJobRefId, 0);

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

int main(int argc, char **argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#pragma GCC diagnostic pop
