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
#include "tmisce.h"
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

extern "C" int32_t schHandleResponseMsg(SSchJob *pJob, SSchTask *pTask, uint64_t sId, int32_t execId, SDataBuf *pMsg,
                                        int32_t rspCode);
extern "C" int32_t schHandleCallback(void *param, const SDataBuf *pMsg, int32_t rspCode);
extern "C" int32_t schHandleNotifyCallback(void *param, SDataBuf *pMsg, int32_t code);
extern "C" int32_t schHandleLinkBrokenCallback(void *param, SDataBuf *pMsg, int32_t code);
extern "C" int32_t schRescheduleTask(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schValidateRspMsgType(SSchJob *pJob, SSchTask *pTask, int32_t msgType);
//extern "C" int32_t schProcessFetchRsp(SSchJob *pJob, SSchTask *pTask, char *msg, int32_t rspCode);
extern "C" int32_t schProcessResponseMsg(SSchJob *pJob, SSchTask *pTask, SDataBuf *pMsg, int32_t rspCode);
extern "C" void schInitTaskRetryInfo(SSchJob *pJob, SSchTask *pTask, SSchLevel *pLevel);
extern "C" int32_t schRecordTaskSucceedNode(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schDropTaskExecNode(SSchJob *pJob, SSchTask *pTask, void *handle, int32_t execId);
extern "C" int32_t schPushTaskToExecList(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schSetAddrsFromNodeList(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schNotifyTaskOnExecNode(SSchJob *pJob, SSchTask *pTask, ETaskNotifyType type);
extern "C" int32_t schLaunchRemoteTask(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schLaunchTaskImpl(void *param);
extern "C" void schHandleTimerEvent(void *param, void *tmrId);


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
  TAOS_STRCPY(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, false) < 0) {
    (void)printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

void schtQueryCb(SExecResult *pResult, void *param, int32_t code) { *(int32_t *)param = 1; }

int32_t schtBuildQueryRspMsg(uint32_t *msize, void **rspMsg) {
  SQueryTableRsp rsp = {0};
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

int32_t schtBuildFetchRspMsg(uint32_t *msize, void **rspMsg) {
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(sizeof(SRetrieveTableRsp), 1);
  rsp->completed = 1;
  rsp->numOfRows = 10;
  rsp->compLen = 0;

  *rspMsg = rsp;
  *msize = sizeof(SRetrieveTableRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t schtBuildSubmitRspMsg(uint32_t *msize, void **rspMsg) {
  SSubmitRsp2 submitRsp = {0};
  int32_t     msgSize = 0, ret = 0;
  SEncoder    ec = {0};

  tEncodeSize(tEncodeSSubmitRsp2, &submitRsp, msgSize, ret);
  void *msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    return terrno;
  }
  tEncoderInit(&ec, (uint8_t *)msg, msgSize);
  if (tEncodeSSubmitRsp2(&ec, &submitRsp) < 0) {
    return -1;
  }
  tEncoderClear(&ec);

  *rspMsg = msg;
  *msize = msgSize;

  return TSDB_CODE_SUCCESS;
}

void schtBuildQueryDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;

  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = NULL;
  int32_t code = nodesMakeList(&dag->pSubplans);
  if (NULL == dag->pSubplans) {
    return;
  }
  SNodeListNode *scan = NULL;
  code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&scan);
  if (NULL == scan) {
    return;
  }
  SNodeListNode *merge = NULL;
  code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&merge);
  if (NULL == merge) {
    return;
  }

  SSubplan *scanPlan = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&scanPlan);
  if (NULL == scanPlan) {
    return;
  }
  SSubplan *mergePlan = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&mergePlan);
  if (NULL == mergePlan) {
    return;
  }

  scanPlan->id.queryId = qId;
  scanPlan->id.groupId = 0x0000000000000002;
  scanPlan->id.subplanId = 0x0000000000000003;
  scanPlan->subplanType = SUBPLAN_TYPE_SCAN;

  scanPlan->execNode.nodeId = 1;
  scanPlan->execNode.epSet.inUse = 0;
  addEpIntoEpSet(&scanPlan->execNode.epSet, "ep0", 6030);

  scanPlan->pChildren = NULL;
  scanPlan->level = 1;
  scanPlan->pParents = NULL;
  code = nodesMakeList(&scanPlan->pParents);
  if (NULL == scanPlan->pParents) {
    return;
  }
  scanPlan->pNode = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, (SNode**)&scanPlan->pNode);
  if (NULL == scanPlan->pNode) {
    return;
  }
  scanPlan->msgType = TDMT_SCH_QUERY;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&scanPlan->pDataSink);
  if (NULL == scanPlan->pDataSink) {
    return;
  }

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pChildren = NULL;
  code = nodesMakeList(&mergePlan->pChildren);
  if (NULL == mergePlan->pChildren) {
    return;
  }
  mergePlan->pParents = NULL;
  mergePlan->pNode = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE, (SNode**)&mergePlan->pNode);
  if (NULL == mergePlan->pNode) {
    return;
  }
  mergePlan->msgType = TDMT_SCH_QUERY;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&mergePlan->pDataSink);
  if (NULL == mergePlan->pDataSink) {
    return;
  }

  merge->pNodeList = NULL;
  code = nodesMakeList(&merge->pNodeList);
  if (NULL == merge->pNodeList) {
    return;
  }
  scan->pNodeList = NULL;
  code = nodesMakeList(&scan->pNodeList);
  if (NULL == scan->pNodeList) {
    return;
  }


  (void)nodesListAppend(merge->pNodeList, (SNode *)mergePlan);
  (void)nodesListAppend(scan->pNodeList, (SNode *)scanPlan);

  (void)nodesListAppend(mergePlan->pChildren, (SNode *)scanPlan);
  (void)nodesListAppend(scanPlan->pParents, (SNode *)mergePlan);

  (void)nodesListAppend(dag->pSubplans, (SNode *)merge);
  (void)nodesListAppend(dag->pSubplans, (SNode *)scan);
}

void schtBuildQueryFlowCtrlDag(SQueryPlan *dag) {
  uint64_t qId = schtQueryId;
  int32_t  scanPlanNum = 20;

  dag->queryId = qId;
  dag->numOfSubplans = scanPlanNum + 1;
  dag->pSubplans = NULL;
  int32_t code = nodesMakeList(&dag->pSubplans);
  if (NULL == dag->pSubplans) {
    return;
  }
  SNodeListNode *scan = NULL;
  code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&scan);
  if (NULL == scan) {
    return;
  }
  SNodeListNode *merge = NULL;
  code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&merge);
  if (NULL == merge) {
    return;
  }

  SSubplan *mergePlan = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&mergePlan);
  if (NULL == mergePlan) {
    return;
  }

  merge->pNodeList = NULL;
  code = nodesMakeList(&merge->pNodeList);
  if (NULL == merge->pNodeList) {
    return;
  }
  scan->pNodeList = NULL;
  code = nodesMakeList(&scan->pNodeList);
  if (NULL == scan->pNodeList) {
    return;
  }

  mergePlan->pChildren = NULL;
  code = nodesMakeList(&mergePlan->pChildren);
  if (NULL == mergePlan->pChildren) {
    return;
  }
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&mergePlan->pDataSink);
  if (NULL == mergePlan->pDataSink) {
    return;
  }

  for (int32_t i = 0; i < scanPlanNum; ++i) {
    SSubplan *scanPlan = NULL;
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&scanPlan);
    if (NULL == scanPlan) {
      return;
    }
    scanPlan->id.queryId = qId;
    scanPlan->id.groupId = 0x0000000000000002;
    scanPlan->id.subplanId = 0x0000000000000003 + i;
    scanPlan->subplanType = SUBPLAN_TYPE_SCAN;

    scanPlan->execNode.nodeId = 1 + i;
    scanPlan->execNode.epSet.inUse = 0;
    scanPlan->execNodeStat.tableNum = taosRand() % 100;
    addEpIntoEpSet(&scanPlan->execNode.epSet, "ep0", 6030);
    addEpIntoEpSet(&scanPlan->execNode.epSet, "ep1", 6030);
    addEpIntoEpSet(&scanPlan->execNode.epSet, "ep2", 6030);
    scanPlan->execNode.epSet.inUse = taosRand() % 3;

    scanPlan->pChildren = NULL;
    scanPlan->level = 1;
    scanPlan->pParents = NULL;
    code = nodesMakeList(&scanPlan->pParents);
    if (NULL == scanPlan->pParents) {
      return;
    }
    scanPlan->pNode = NULL;
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, (SNode**)&scanPlan->pNode);
    if (NULL == scanPlan->pNode) {
      return;
    }
    scanPlan->msgType = TDMT_SCH_QUERY;
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH, (SNode**)&scanPlan->pDataSink);
    if (NULL == scanPlan->pDataSink) {
      return;
    }

    (void)nodesListAppend(scanPlan->pParents, (SNode *)mergePlan);
    (void)nodesListAppend(mergePlan->pChildren, (SNode *)scanPlan);

    (void)nodesListAppend(scan->pNodeList, (SNode *)scanPlan);
  }

  mergePlan->id.queryId = qId;
  mergePlan->id.groupId = schtMergeTemplateId;
  mergePlan->id.subplanId = 0x5555;
  mergePlan->subplanType = SUBPLAN_TYPE_MERGE;
  mergePlan->level = 0;
  mergePlan->execNode.epSet.numOfEps = 0;

  mergePlan->pParents = NULL;
  mergePlan->pNode = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE, (SNode**)&mergePlan->pNode);
  if (NULL == mergePlan->pNode) {
    return;
  }
  mergePlan->msgType = TDMT_SCH_QUERY;

  (void)nodesListAppend(merge->pNodeList, (SNode *)mergePlan);

  (void)nodesListAppend(dag->pSubplans, (SNode *)merge);
  (void)nodesListAppend(dag->pSubplans, (SNode *)scan);
}

void schtFreeQueryDag(SQueryPlan *dag) {}

void schtBuildInsertDag(SQueryPlan *dag) {
  uint64_t qId = 0x0000000000000002;

  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = NULL;
  int32_t code = nodesMakeList(&dag->pSubplans);
  if (NULL == dag->pSubplans) {
    return;
  }
  SNodeListNode *inserta = NULL;
  code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&inserta);
  if (NULL == inserta) {
    return;
  }
  inserta->pNodeList = NULL;
  code = nodesMakeList(&inserta->pNodeList);
  if (NULL == inserta->pNodeList) {
    return;
  }

  SSubplan *insertPlan = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&insertPlan);
  if (NULL == insertPlan) {
    return;
  }

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
  insertPlan->pDataSink = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT, (SNode**)&insertPlan->pDataSink);
  if (NULL == insertPlan->pDataSink) {
    return;
  }
  ((SDataInserterNode *)insertPlan->pDataSink)->size = 1;
  ((SDataInserterNode *)insertPlan->pDataSink)->pData = taosMemoryCalloc(1, 1);
  if (NULL == ((SDataInserterNode *)insertPlan->pDataSink)->pData) {
    return;
  }
  insertPlan->msgType = TDMT_VND_SUBMIT;

  (void)nodesListAppend(inserta->pNodeList, (SNode *)insertPlan);

  insertPlan = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&insertPlan);
  if (NULL == insertPlan) {
    return;
  }

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
  insertPlan->pDataSink = NULL;
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT, (SNode**)&insertPlan->pDataSink);
  if (NULL == insertPlan->pDataSink) {
    return;
  }
  ((SDataInserterNode *)insertPlan->pDataSink)->size = 1;
  ((SDataInserterNode *)insertPlan->pDataSink)->pData = taosMemoryCalloc(1, 1);
  if (NULL == ((SDataInserterNode *)insertPlan->pDataSink)->pData) {
    return;
  }
  insertPlan->msgType = TDMT_VND_SUBMIT;

  (void)nodesListAppend(inserta->pNodeList, (SNode *)insertPlan);

  (void)nodesListAppend(dag->pSubplans, (SNode *)inserta);
}

int32_t schtPlanToString(const SSubplan *subplan, char **str, int32_t *len) {
  *str = (char *)taosMemoryCalloc(1, 20);
  if (NULL == *str) {
    return -1;
  }
  *len = 20;
  return 0;
}

int32_t schtExecNode(SSubplan *subplan, uint64_t groupId, SQueryNodeAddr *ep) { return 0; }

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

int32_t schtAsyncSendMsgToServer(void *pTransporter, SEpSet *epSet, int64_t *pTransporterId, SMsgSendInfo *pInfo,
                                 bool persistHandle, void *rpcCtx) {
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

  code = schAcquireJob(job, &pJob);
  if (code) {
    return NULL;
  }
  
  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SDataBuf msg = {0};
    void    *rmsg = NULL;
    (void)schtBuildSubmitRspMsg(&msg.len, &rmsg);
    msg.msgType = TDMT_VND_SUBMIT_RSP;
    msg.pData = rmsg;

    (void)schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  (void)schReleaseJob(job);

  schtJobDone = true;

  return NULL;
}

void *schtCreateFetchRspThread(void *param) {
  int64_t  job = *(int64_t *)param;
  SSchJob *pJob = NULL;

  (void)schAcquireJob(job, &pJob);
  if (NULL == pJob) {
    return NULL;
  }
  
  taosSsleep(1);

  int32_t  code = 0;
  SDataBuf msg = {0};
  void    *rmsg = NULL;
  (void)schtBuildFetchRspMsg(&msg.len, &rmsg);
  msg.msgType = TDMT_SCH_MERGE_FETCH_RSP;
  msg.pData = rmsg;

  code = schHandleResponseMsg(pJob, pJob->fetchTask, pJob->fetchTask->seriesId, pJob->fetchTask->execId, &msg, 0);

  (void)schReleaseJob(job);

  TD_ALWAYS_ASSERT(code == 0);
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
    if (NULL == param) {
      return NULL;
    }
    param->queryId = schtQueryId;
    param->taskId = schtFetchTaskId;

    int32_t            code = 0;
    SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
    if (NULL == rsp) {
      return NULL;
    }
    rsp->completed = 1;
    rsp->numOfRows = 10;

    dataBuf.msgType = TDMT_SCH_FETCH_RSP;
    dataBuf.pData = rsp;
    dataBuf.len = sizeof(*rsp);

    code = schHandleCallback(param, &dataBuf, 0);

    TD_ALWAYS_ASSERT(code == 0 || code);
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
        (void)printf("FreeNum:%d\n", freeNum);
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
  SQueryPlan *dag = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&dag);
  TD_ALWAYS_ASSERT(code == 0);
  schtInitLogFile();

  code = schedulerInit();
  TD_ALWAYS_ASSERT(code == 0);

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
    if (NULL == qnodeList) {
      TD_ALWAYS_ASSERT(0);
    }
    
    SQueryNodeLoad load = {0};
    load.addr.epSet.numOfEps = 1;
    TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
    load.addr.epSet.eps[0].port = 6031;
    if (NULL == taosArrayPush(qnodeList, &load)) {
      TD_ALWAYS_ASSERT(0);
    }

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
    TD_ALWAYS_ASSERT(code == 0);

    pJob = NULL;
    code = schAcquireJob(queryJobRefId, &pJob);

    if (NULL == pJob) {
      taosArrayDestroy(qnodeList);
      schtFreeQueryDag(dag);
      continue;
    }

    execTasks = taosHashInit(5, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
    if (NULL == execTasks) {
      TD_ALWAYS_ASSERT(0);
    }
    void *pIter = taosHashIterate(pJob->execTasks, NULL);
    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;
      schtFetchTaskId = task->taskId - 1;

      if (taosHashPut(execTasks, &task->taskId, sizeof(task->taskId), task, sizeof(*task))) {
        TD_ALWAYS_ASSERT(0);
      }
      pIter = taosHashIterate(pJob->execTasks, pIter);
    }

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));
    if (NULL == param) {
      TD_ALWAYS_ASSERT(0);
    }
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId;

      SDataBuf msg = {0};
      void    *rmsg = NULL;
      if (schtBuildQueryRspMsg(&msg.len, &rmsg)) {
        TD_ALWAYS_ASSERT(0);
      }
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleCallback(param, &msg, 0);
      TD_ALWAYS_ASSERT(code == 0 || code);

      pIter = taosHashIterate(execTasks, pIter);
    }

    param = (SSchTaskCallbackParam *)taosMemoryCalloc(1, sizeof(*param));
    if (NULL == param) {
      TD_ALWAYS_ASSERT(0);
    }
    param->refId = queryJobRefId;
    param->queryId = pJob->queryId;

    pIter = taosHashIterate(execTasks, NULL);
    while (pIter) {
      SSchTask *task = (SSchTask *)pIter;

      param->taskId = task->taskId - 1;
      SDataBuf msg = {0};
      void    *rmsg = NULL;
      if (schtBuildQueryRspMsg(&msg.len, &rmsg)) {
         TD_ALWAYS_ASSERT(0);
      }
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleCallback(param, &msg, 0);
      TD_ALWAYS_ASSERT(code == 0 || code);

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
    TD_ALWAYS_ASSERT(code == 0 || code);

    if (0 == code) {
      SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
      TD_ALWAYS_ASSERT(pRsp->completed == 1);
    }

    data = NULL;
    code = schedulerFetchRows(queryJobRefId, &req);
    TD_ALWAYS_ASSERT(code == 0 || code);

    schtFreeQueryJob(0);

    taosHashCleanup(execTasks);
    taosArrayDestroy(qnodeList);

    schtFreeQueryDag(dag);

    if (++jobFinished % schtTestPrintNum == 0) {
      (void)printf("jobFinished:%d\n", jobFinished);
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
  SQueryPlan *dag = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&dag);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  ASSERT_NE(taosArrayPush(qnodeList, &load), nullptr);

  code = schedulerInit();
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

  SSchJob *pJob = NULL;
  code = schAcquireJob(job, &pJob);
  ASSERT_EQ(code, 0);

  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SDataBuf msg = {0};
    void    *rmsg = NULL;
    ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
    msg.msgType = TDMT_SCH_QUERY_RSP;
    msg.pData = rmsg;

    code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;
    if (JOB_TASK_STATUS_EXEC == task->status) {
      SDataBuf msg = {0};
      void    *rmsg = NULL;
      ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

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
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));

  TdThread thread1;
  ASSERT_EQ(0, taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job));

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;

  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  (void)schReleaseJob(job);

  schedulerDestroy();

  schedulerFreeJob(&job, 0);

  (void)taosThreadJoin(thread1, NULL);

  schMgmt.jobRef = -1;
}

TEST(queryTest, rescheduleCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan *dag = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&dag);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  ASSERT_NE(taosArrayPush(qnodeList, &load), nullptr);

  TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode1.ep");
  ASSERT_NE(taosArrayPush(qnodeList, &load), nullptr);

  code = schedulerInit();
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

  SSchJob *pJob = NULL;
  code = schAcquireJob(job, &pJob);
  ASSERT_EQ(code, 0);

  schedulerEnableReSchedule(true);

  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;
    task->timeoutUsec = -1;

    code = schRescheduleTask(pJob, task);
    ASSERT_EQ(code, 0);

    task->timeoutUsec = SCH_DEFAULT_TASK_TIMEOUT_USEC;
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SDataBuf msg = {0};
    void    *rmsg = NULL;
    ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
    msg.msgType = TDMT_SCH_QUERY_RSP;
    msg.pData = rmsg;

    code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }


  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;
    task->timeoutUsec = -1;

    code = schRescheduleTask(pJob, task);
    ASSERT_EQ(code, 0);

    task->timeoutUsec = SCH_DEFAULT_TASK_TIMEOUT_USEC;
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;
    if (JOB_TASK_STATUS_EXEC == task->status) {
      SDataBuf msg = {0};
      void    *rmsg = NULL;
      ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

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
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));

  TdThread thread1;
  ASSERT_EQ(0,  taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job));

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;

  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  (void)schReleaseJob(job);

  schedulerDestroy();

  schedulerFreeJob(&job, 0);

  (void)taosThreadJoin(thread1, NULL);

  schMgmt.jobRef = -1;
}


TEST(queryTest, readyFirstCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan *dag = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&dag);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  ASSERT_NE(nullptr, taosArrayPush(qnodeList, &load));

  code = schedulerInit();
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

  SSchJob *pJob = NULL;
  code = schAcquireJob(job, &pJob);
  ASSERT_EQ(code, 0);
  
  void *pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    SDataBuf msg = {0};
    void    *rmsg = NULL;
    ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
    msg.msgType = TDMT_SCH_QUERY_RSP;
    msg.pData = rmsg;

    code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

    ASSERT_EQ(code, 0);
    pIter = taosHashIterate(pJob->execTasks, pIter);
  }

  pIter = taosHashIterate(pJob->execTasks, NULL);
  while (pIter) {
    SSchTask *task = *(SSchTask **)pIter;

    if (JOB_TASK_STATUS_EXEC == task->status) {
      SDataBuf msg = {0};
      void    *rmsg = NULL;
      ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
      msg.msgType = TDMT_SCH_QUERY_RSP;
      msg.pData = rmsg;

      code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

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
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));

  TdThread thread1;
  ASSERT_EQ(0, taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job));

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  (void)schReleaseJob(job);

  schedulerDestroy();

  schedulerFreeJob(&job, 0);

  (void)taosThreadJoin(thread1, NULL);
  schMgmt.jobRef = -1;
}

TEST(queryTest, flowCtrlCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  int64_t     job = 0;
  SQueryPlan *dag = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&dag);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);

  schtInitLogFile();

  taosSeedRand(taosGetTimestampSec());

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  ASSERT_NE(nullptr, taosArrayPush(qnodeList, &load));

  code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildQueryFlowCtrlDag(dag);

  schtSetPlanToString();
  schtSetExecNode();
  schtSetAsyncSendMsgToServer();

  ASSERT_EQ(0, initTaskQueue());

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

  SSchJob *pJob = NULL;
  code = schAcquireJob(job, &pJob);
  ASSERT_EQ(code, 0);

  while (!queryDone) {
    void *pIter = taosHashIterate(pJob->execTasks, NULL);
    while (pIter) {
      SSchTask *task = *(SSchTask **)pIter;

      if (JOB_TASK_STATUS_EXEC == task->status && 0 != task->lastMsgType) {
        SDataBuf msg = {0};
        void    *rmsg = NULL;
        ASSERT_EQ(0, schtBuildQueryRspMsg(&msg.len, &rmsg));
        msg.msgType = TDMT_SCH_QUERY_RSP;
        msg.pData = rmsg;

        code = schHandleResponseMsg(pJob, task, task->seriesId, task->execId, &msg, 0);

        ASSERT_EQ(code, 0);
      }

      pIter = taosHashIterate(pJob->execTasks, pIter);
    }
  }

  TdThreadAttr thattr;
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));

  TdThread thread1;
  ASSERT_EQ(0, taosThreadCreate(&(thread1), &thattr, schtCreateFetchRspThread, &job));

  void *data = NULL;
  req.syncReq = true;
  req.pFetchRes = &data;
  code = schedulerFetchRows(job, &req);
  ASSERT_EQ(code, 0);

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)data;
  ASSERT_EQ(pRsp->completed, 1);
  ASSERT_EQ(pRsp->numOfRows, 10);
  taosMemoryFreeClear(data);

  (void)schReleaseJob(job);

  schedulerDestroy();

  schedulerFreeJob(&job, 0);

  (void)taosThreadJoin(thread1, NULL);
  schMgmt.jobRef = -1;

  cleanupTaskQueue();
}

TEST(insertTest, normalCase) {
  void       *mockPointer = (void *)0x1;
  char       *clusterId = "cluster1";
  char       *dbname = "1.db1";
  char       *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  SQueryPlan *dag = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&dag);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  uint64_t    numOfRows = 0;

  SArray *qnodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));

  SQueryNodeLoad load = {0};
  load.addr.epSet.numOfEps = 1;
  TAOS_STRCPY(load.addr.epSet.eps[0].fqdn, "qnode0.ep");
  load.addr.epSet.eps[0].port = 6031;
  ASSERT_NE(nullptr, taosArrayPush(qnodeList, &load));

  code = schedulerInit();
  ASSERT_EQ(code, 0);

  schtBuildInsertDag(dag);

  schtSetPlanToString();
  schtSetAsyncSendMsgToServer();

  TdThreadAttr thattr;
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));

  schtJobDone = false;

  TdThread thread1;
  ASSERT_EQ(0, taosThreadCreate(&(thread1), &thattr, schtSendRsp, &insertJobRefId));

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

  (void)taosThreadJoin(thread1, NULL);
  schMgmt.jobRef = -1;
}

TEST(multiThread, forceFree) {
  TdThreadAttr thattr;
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));

  TdThread thread1, thread2, thread3;
  ASSERT_EQ(0, taosThreadCreate(&(thread1), &thattr, schtRunJobThread, NULL));
  //  taosThreadCreate(&(thread2), &thattr, schtFreeJobThread, NULL);
  ASSERT_EQ(0, taosThreadCreate(&(thread3), &thattr, schtFetchRspThread, NULL));

  while (true) {
    if (schtTestDeadLoop) {
      taosSsleep(1);
    } else {
      taosSsleep(schtTestMTRunSec);
      break;
    }
  }

  schtTestStop = true;
  // taosSsleep(3);
  
  schMgmt.jobRef = -1;
}

TEST(otherTest, function) {
  // excpet test
  (void)schReleaseJob(0);
  schFreeRpcCtx(NULL);

  char* ep = NULL;
  ASSERT_EQ(schDumpEpSet(NULL, &ep), TSDB_CODE_SUCCESS);
  ASSERT_EQ(strcmp(schGetOpStr(SCH_OP_NULL), "NULL"), 0);
  ASSERT_EQ(strcmp(schGetOpStr((SCH_OP_TYPE)100), "UNKNOWN"), 0);

  SSchTaskCallbackParam param = {0};
  SDataBuf dataBuf = {0};
  dataBuf.pData = taosMemoryMalloc(1);
  dataBuf.pEpSet = (SEpSet*)taosMemoryMalloc(sizeof(*dataBuf.pEpSet));
  ASSERT_EQ(schHandleNotifyCallback(&param, &dataBuf, TSDB_CODE_SUCCESS), TSDB_CODE_SUCCESS);

  SSchCallbackParamHeader param2 = {0};
  dataBuf.pData = taosMemoryMalloc(1);
  dataBuf.pEpSet = (SEpSet*)taosMemoryMalloc(sizeof(*dataBuf.pEpSet));
  schHandleLinkBrokenCallback(&param2, &dataBuf, TSDB_CODE_SUCCESS);
  param2.isHbParam = true;
  dataBuf.pData = taosMemoryMalloc(1);
  dataBuf.pEpSet = (SEpSet*)taosMemoryMalloc(sizeof(*dataBuf.pEpSet));
  schHandleLinkBrokenCallback(&param2, &dataBuf, TSDB_CODE_SUCCESS);
  
  schMgmt.jobRef = -1;
}

TEST(otherTest, branch) {
  SSchJob job = {0};
  SSchTask task = {0};
  memset(&schMgmt, 0, sizeof(schMgmt));
  
  schValidateRspMsgType(&job, &task, TDMT_SCH_MERGE_FETCH_RSP);

  task.lastMsgType = TDMT_SCH_MERGE_FETCH_RSP - 1;
  schValidateRspMsgType(&job, &task, TDMT_SCH_MERGE_FETCH_RSP);

  schValidateRspMsgType(&job, &task, 0);

  schValidateRspMsgType(&job, &task, TDMT_SCH_QUERY_RSP);

  task.lastMsgType = TDMT_SCH_QUERY_RSP - 1;
  schValidateRspMsgType(&job, &task, TDMT_SCH_QUERY_RSP);

  schProcessFetchRsp(&job, &task, NULL, -1);
  schProcessFetchRsp(&job, &task, NULL, 0);

  job.fetchRes = (void*)0x1;
  schProcessFetchRsp(&job, &task, (char*)taosMemoryMalloc(0), 0);
  job.fetchRes = NULL;

  SDataBuf databuf = {0};
  databuf.msgType = TDMT_VND_ALTER_TABLE_RSP;
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_VND_SUBMIT_RSP;
  databuf.pData = taosMemoryMalloc(0);
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_VND_DELETE_RSP;
  databuf.pData = taosMemoryMalloc(0);
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_SCH_QUERY_RSP;
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_SCH_QUERY_RSP;
  databuf.pData = taosMemoryMalloc(0);
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);
  

  databuf.msgType = TDMT_SCH_EXPLAIN_RSP;
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_SCH_EXPLAIN_RSP;
  databuf.pData = taosMemoryMalloc(0);
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);
  job.status = 0;

  job.attr.explainMode = EXPLAIN_MODE_ANALYZE;
  databuf.msgType = TDMT_SCH_EXPLAIN_RSP;
  databuf.pData = taosMemoryMalloc(0);
  job.status = JOB_TASK_STATUS_FAIL;
  job.fetchRes = (void*)0x1;
  schProcessResponseMsg(&job, &task, &databuf, 0);
  job.fetchRes = NULL;

  job.attr.explainMode = EXPLAIN_MODE_ANALYZE;
  databuf.msgType = TDMT_SCH_EXPLAIN_RSP;
  databuf.pData = taosMemoryMalloc(0);
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_SCH_DROP_TASK_RSP;
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = TDMT_SCH_LINK_BROKEN;
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.msgType = 0;
  job.status = JOB_TASK_STATUS_FAIL;
  schProcessResponseMsg(&job, &task, &databuf, 0);

  databuf.pData = taosMemoryMalloc(0);
  schHandleHbCallback(NULL, &databuf, 0);

  __async_send_cb_fn_t fp = NULL;
  schGetCallbackFp(TDMT_SCH_TASK_NOTIFY, &fp);
  schGetCallbackFp(0, &fp);

  SQueryNodeEpId ep = {0};
  schBuildAndSendHbMsg(&ep, NULL);

  schBuildAndSendMsg(&job, &task, NULL, 0, NULL);

  SSchLevel level = {0};
  SSubplan subplan;
  memset(&subplan, 0, sizeof(subplan));
  job.attr.type = JOB_TYPE_QUERY;
  schMgmt.cfg.schPolicy = SCH_ALL;
  task.plan = &subplan;
  schInitTaskRetryInfo(&job, &task, &level);
  
  job.attr.type = JOB_TYPE_INSERT;
  memset(&schMgmt.cfg, 0, sizeof(schMgmt.cfg));
  memset(&level, 0, sizeof(level));

  schRecordTaskSucceedNode(&job, &task);

  schDropTaskExecNode(&job, &task, NULL, 0);

  task.execNodes = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  schDropTaskExecNode(&job, &task, NULL, 0);

  int32_t execId = 0;
  task.execId = 1;
  (void)taosHashPut(task.execNodes, &execId, sizeof(execId), &execId, sizeof(execId));
  schDropTaskExecNode(&job, &task, NULL, execId);
  task.execId = 0;
  taosHashCleanup(task.execNodes);
  task.execNodes = NULL;

  job.status = JOB_TASK_STATUS_FAIL;
  schProcessOnTaskFailure(&job, &task, 0);
  job.status = 0;

  task.status = JOB_TASK_STATUS_FAIL;
  schProcessOnTaskFailure(&job, &task, 0);
  task.status = 0;

  task.level = &level;
  schProcessOnTaskFailure(&job, &task, TSDB_CODE_SCH_TIMEOUT_ERROR);
  memset(&level, 0, sizeof(level));
  task.level = NULL;

  subplan.subplanType = SUBPLAN_TYPE_SCAN;
  task.plan = &subplan;
  schChkUpdateRedirectCtx(&job, &task, 0);

  task.plan = NULL;
  schPushTaskToExecList(&job, &task);

  job.execTasks = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  taosHashPut(job.execTasks, &task.taskId, sizeof(task.taskId), &task, POINTER_BYTES);
  schPushTaskToExecList(&job, &task);
  taosHashCleanup(job.execTasks);
  job.execTasks = NULL;

  bool needRetry = false;
  task.timeoutUsec = SCH_MAX_TASK_TIMEOUT_USEC / 2 + 1;
  task.retryTimes = 0;
  task.maxRetryTimes = 0;
  schTaskCheckSetRetry(&job, &task, TSDB_CODE_SCH_TIMEOUT_ERROR, &needRetry);

  task.execId = 0;
  task.retryTimes = 0;
  task.maxRetryTimes = 100;
  task.maxExecTimes = 1;
  schTaskCheckSetRetry(&job, &task, TSDB_CODE_SCH_TIMEOUT_ERROR, &needRetry);


  task.execId = 0;
  task.retryTimes = 0;
  task.maxRetryTimes = 100;
  task.maxExecTimes = 100;
  task.lastMsgType = TDMT_SCH_LINK_BROKEN;
  schTaskCheckSetRetry(&job, &task, TSDB_CODE_SCH_TIMEOUT_ERROR, &needRetry);

  schSetAddrsFromNodeList(&job, &task);

  schSwitchTaskCandidateAddr(&job, &task);


  task.candidateAddrs = taosArrayInit(SCHEDULE_DEFAULT_MAX_NODE_NUM, sizeof(SQueryNodeAddr));
  SQueryNodeAddr addr = {0};
  taosArrayPush(task.candidateAddrs, &addr);
  taosArrayPush(task.candidateAddrs, &addr);
  schMgmt.cfg.schPolicy = SCH_LOAD_SEQ;
  task.candidateIdx = 1;
  schSwitchTaskCandidateAddr(&job, &task);

  schMgmt.cfg.schPolicy = SCH_RANDOM;
  schSwitchTaskCandidateAddr(&job, &task);
  taosArrayDestroy(task.candidateAddrs);
  task.candidateAddrs = NULL;
  memset(&schMgmt.cfg, 0, sizeof(schMgmt.cfg));
  task.candidateIdx = 0;

  schDropTaskOnExecNode(&job, &task);

  schNotifyTaskOnExecNode(&job, &task, TASK_NOTIFY_FINISHED);

  schLaunchRemoteTask(&job, &task);

  SSchTaskCtx* pCtx = (SSchTaskCtx*)taosMemoryCalloc(1, sizeof(SSchTaskCtx));
  pCtx->jobRid = -1;
  schLaunchTaskImpl((void*)pCtx);

  task.plan = &subplan;
  subplan.subplanType = SUBPLAN_TYPE_SCAN;
  job.attr.needFlowCtrl = true;
  level.taskNum = 1000;
  task.level = &level;
  schLaunchTask(&job, &task);
  task.plan = NULL;
  task.level = NULL;
  job.attr.needFlowCtrl = false;

  SSchTimerParam param = {0};
  param.rId = -1;
  schHandleTimerEvent(&param, NULL);

  job.execTasks = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  task.delayExecMs = 1;
  schMgmt.timer = NULL;
  schDelayLaunchTask(&job, &task);
  task.delayExecMs = 0;
  taosHashCleanup(job.execTasks);
  job.execTasks = NULL;

  job.fetchRes = (void*)0x1;
  schLaunchFetchTask(&job);
  job.fetchRes = NULL;

  job.fetchTask = &task;
  job.attr.localExec = true;
  job.attr.type = JOB_TYPE_QUERY;
  subplan.subplanType = SUBPLAN_TYPE_MERGE;
  task.plan = &subplan;
  void* p = taosMemoryCalloc(1, 1024);
  schMgmt.queryMgmt = p;
  schLaunchFetchTask(&job);
  memset(&job, 0, sizeof(job));
  memset(&subplan, 0, sizeof(subplan));
  task.plan = NULL;
  taosMemoryFreeClear(schMgmt.queryMgmt);

  // flow ctrl

  job.flowCtrl = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  SEp sep = {0};
  SSchFlowControl nctrl = {0};
  nctrl.taskList = taosArrayInit(1, POINTER_BYTES);
  taosHashPut(job.flowCtrl, &sep, sizeof(SEp), &nctrl, sizeof(nctrl));
  schFreeFlowCtrl(&job);



  schMgmt.jobRef = -1;
}


void schtReset() {
  insertJobRefId = 0;
  queryJobRefId = 0;
  
  schtJobDone = false;
  schtMergeTemplateId = 0x4;
  schtFetchTaskId = 0;
  schtQueryId = 1;
  
  schtTestStop = false;
  schtTestDeadLoop = false;
  schtTestMTRunSec = 1;
  schtTestPrintNum = 1000;
  schtStartFetch = 0;
}

int main(int argc, char **argv) {
  schtInitLogFile();
  if (rpcInit()) {
    TD_ALWAYS_ASSERT(0);
  }
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);

  int code = 0;
  for (int32_t i = 0; i < 10; ++i) {
    schtReset();
    code = RUN_ALL_TESTS();
    if (code) {
      break;
    }
  }
  
  return code;
}

#pragma GCC diagnostic pop
