/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#endif

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wreturn-type"
#pragma GCC diagnostic ignored "-Wformat"
#include <addr_any.h>

#include "os.h"

#include "catalog.h"
#include "scheduler.h"
#include "query.h"
#include "tmsg.h"
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
#include "schPhase.h"
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
extern "C" int32_t schSetTaskCandidateAddrs(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schSetAddrsFromNodeList(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schNotifyTaskOnExecNode(SSchJob *pJob, SSchTask *pTask, ETaskNotifyType type);
extern "C" int32_t schLaunchRemoteTask(SSchJob *pJob, SSchTask *pTask);
extern "C" int32_t schAsyncSendMsg(SSchJob *pJob, SSchTask *pTask, SSchTrans *trans, SQueryNodeAddr *addr,
                                   int32_t msgType, void *msg, uint32_t msgSize, bool persistHandle, SRpcCtx *ctx);
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

int32_t schtExecNode(SSubplan *subplan, int32_t groupId, SDownstreamSourceNode *source) { return 0; }

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
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler transport/planner hooks is unreliable in Windows release builds";
#endif
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
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler transport/planner hooks is unreliable in Windows release builds";
#endif
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
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler transport/planner hooks is unreliable in Windows release builds";
#endif
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
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler transport/planner hooks is unreliable in Windows release builds";
#endif
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
}

TEST(insertTest, normalCase) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler transport/planner hooks is unreliable in Windows release builds";
#endif
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
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler transport/planner hooks is unreliable in Windows release builds";
#endif
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

  SSchTaskCallbackParam param2 = {0};
  dataBuf.pData = taosMemoryMalloc(1);
  dataBuf.pEpSet = (SEpSet*)taosMemoryMalloc(sizeof(*dataBuf.pEpSet));
  schHandleLinkBrokenCallback(&param2, &dataBuf, TSDB_CODE_SUCCESS);
  SSchHbCallbackParam hbParam = {0};
  hbParam.head.isHbParam = true;
  dataBuf.pData = taosMemoryMalloc(1);
  dataBuf.pEpSet = (SEpSet*)taosMemoryMalloc(sizeof(*dataBuf.pEpSet));
  schHandleLinkBrokenCallback(&hbParam, &dataBuf, TSDB_CODE_SUCCESS);
  
  schMgmt.jobRef = -1;
}

TEST(otherTest, taskRetryConfig) {
  int32_t    originalMaxRetryWaitTime = tsMaxRetryWaitTime;
  int32_t    originalMaxRetryTimes = tsMaxRetryTimes;
  SCH_POLICY originalSchPolicy = schMgmt.cfg.schPolicy;

  SSchJob   job = {0};
  SSchTask  task = {0};
  SSchLevel level = {0};
  SSubplan  plan = {};

  job.attr.type = JOB_TYPE_QUERY;
  task.plan = &plan;
  plan.subplanType = SUBPLAN_TYPE_SCAN;
  schMgmt.cfg.schPolicy = SCH_ALL;
  tsMaxRetryWaitTime = 3000;
  tsMaxRetryTimes = 2;

  schInitTaskRetryInfo(&job, &task, &level);
  EXPECT_EQ(task.maxRetryTimes, 6);

  tsMaxRetryWaitTime = originalMaxRetryWaitTime;
  tsMaxRetryTimes = originalMaxRetryTimes;
  schMgmt.cfg.schPolicy = originalSchPolicy;
}

TEST(otherTest, branch) {
  SSchJob job = {0};
  SSchTask task = {0};
  // SSchStat contains empty structs, whose size differs between C and C++.
  memset(&schMgmt, 0, offsetof(SSchedulerMgmt, stat));

  job.subJobId = -1;
  
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
  taosArrayDestroy(task.candidateAddrs);
  task.candidateAddrs = NULL;
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

TEST(otherTest, followerReadEligibility) {
  bool     savedEnableFollowerRead = tsEnableFollowerRead;
  SSchJob  job = {0};
  SSchTask task = {0};
  SSubplan plan = {};

  task.plan = &plan;
  plan.subplanType = SUBPLAN_TYPE_SCAN;
  plan.msgType = TDMT_SCH_QUERY;
  job.attr.type = JOB_TYPE_QUERY;

  tsEnableFollowerRead = false;
  EXPECT_FALSE(schTaskAllowFollowerRead(&job, &task));

  tsEnableFollowerRead = true;
  EXPECT_TRUE(schTaskAllowFollowerRead(&job, &task));

  task.plan = nullptr;
  EXPECT_FALSE(schTaskAllowFollowerRead(&job, &task));
  task.plan = &plan;

  job.attr.type = (EJobType)(JOB_TYPE_QUERY | JOB_TYPE_INSERT);
  EXPECT_FALSE(schTaskAllowFollowerRead(&job, &task));

  job.attr.type = JOB_TYPE_HQUERY;
  EXPECT_FALSE(schTaskAllowFollowerRead(&job, &task));

  job.attr.type = JOB_TYPE_QUERY;
  plan.subplanType = SUBPLAN_TYPE_MERGE;
  EXPECT_TRUE(schTaskAllowFollowerRead(&job, &task));

  plan.msgType = TDMT_SCH_MERGE_QUERY;
  EXPECT_TRUE(schTaskAllowFollowerRead(&job, &task));

  plan.subplanType = SUBPLAN_TYPE_SCAN;
  EXPECT_TRUE(schTaskAllowFollowerRead(&job, &task));

  tsEnableFollowerRead = savedEnableFollowerRead;
}

TEST(otherTest, followerReadSelectsReplicaEndpoint) {
  bool     savedEnableFollowerRead = tsEnableFollowerRead;
  SSchJob  job = {0};
  SSchTask task = {0};
  SSubplan plan = {};

  task.plan = &plan;
  plan.subplanType = SUBPLAN_TYPE_SCAN;
  plan.msgType = TDMT_SCH_QUERY;
  plan.execNode.epSet.numOfEps = 3;
  plan.execNode.epSet.inUse = 1;
  job.attr.type = JOB_TYPE_QUERY;

  tsEnableFollowerRead = false;
  ASSERT_EQ(schSetTaskCandidateAddrs(&job, &task), TSDB_CODE_SUCCESS);
  SQueryNodeAddr *pAddr = (SQueryNodeAddr *)taosArrayGet(task.candidateAddrs, 0);
  ASSERT_NE(pAddr, nullptr);
  EXPECT_EQ(pAddr->epSet.inUse, 1);
  taosArrayDestroy(task.candidateAddrs);
  task.candidateAddrs = nullptr;

  tsEnableFollowerRead = true;
  ASSERT_EQ(schSetTaskCandidateAddrs(&job, &task), TSDB_CODE_SUCCESS);
  pAddr = (SQueryNodeAddr *)taosArrayGet(task.candidateAddrs, 0);
  ASSERT_NE(pAddr, nullptr);
  EXPECT_GE(pAddr->epSet.inUse, 0);
  EXPECT_LT(pAddr->epSet.inUse, pAddr->epSet.numOfEps);

  taosArrayDestroy(task.candidateAddrs);
  tsEnableFollowerRead = savedEnableFollowerRead;
}

TEST(otherTest, followerReadSelectsReplicaEndpointFromNodeList) {
  bool           savedEnableFollowerRead = tsEnableFollowerRead;
  SSchJob        job = {0};
  SSchTask       task = {0};
  SSubplan       plan = {};
  SQueryNodeLoad load = {0};

  task.plan = &plan;
  plan.subplanType = SUBPLAN_TYPE_MERGE;
  plan.msgType = TDMT_SCH_MERGE_QUERY;
  job.attr.type = JOB_TYPE_QUERY;
  job.nodeList = taosArrayInit(1, sizeof(SQueryNodeLoad));
  ASSERT_NE(job.nodeList, nullptr);

  load.addr.epSet.numOfEps = 3;
  load.addr.epSet.inUse = 2;
  ASSERT_NE(taosArrayPush(job.nodeList, &load), nullptr);

  tsEnableFollowerRead = false;
  ASSERT_EQ(schSetTaskCandidateAddrs(&job, &task), TSDB_CODE_SUCCESS);
  SQueryNodeAddr *pAddr = (SQueryNodeAddr *)taosArrayGet(task.candidateAddrs, 0);
  ASSERT_NE(pAddr, nullptr);
  EXPECT_EQ(pAddr->epSet.inUse, 2);
  taosArrayDestroy(task.candidateAddrs);
  task.candidateAddrs = nullptr;

  taosSeedRand(12345);
  int32_t expectedInUse = taosRand() % load.addr.epSet.numOfEps;
  taosSeedRand(12345);
  tsEnableFollowerRead = true;
  ASSERT_EQ(schSetTaskCandidateAddrs(&job, &task), TSDB_CODE_SUCCESS);
  pAddr = (SQueryNodeAddr *)taosArrayGet(task.candidateAddrs, 0);
  ASSERT_NE(pAddr, nullptr);
  EXPECT_EQ(pAddr->epSet.inUse, expectedInUse);

  SQueryNodeLoad *pLoad = (SQueryNodeLoad *)taosArrayGet(job.nodeList, 0);
  ASSERT_NE(pLoad, nullptr);
  EXPECT_EQ(pLoad->addr.epSet.inUse, 2);

  taosArrayDestroy(task.candidateAddrs);
  taosArrayDestroy(job.nodeList);
  tsEnableFollowerRead = savedEnableFollowerRead;
}

TEST(otherTest, followerReadMessageRoundTrip) {
  SSubQueryMsg request = {0};
  request.sId = 1;
  request.queryId = 2;
  request.taskId = 3;
  request.subEndPoints = taosArrayInit(0, POINTER_BYTES);
  ASSERT_NE(request.subEndPoints, nullptr);

  int32_t msgSize = tSerializeSSubQueryMsg(nullptr, 0, &request);
  ASSERT_GT(msgSize, 0);

  void *pMsg = taosMemoryCalloc(1, msgSize);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(tSerializeSSubQueryMsg(pMsg, msgSize, &request), msgSize);

  SSubQueryMsg response = {0};
  ASSERT_EQ(tDeserializeSSubQueryMsg(pMsg, msgSize, &response), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(response.allowFollowerRead);

  tFreeSSubQueryMsg(&response);
  taosMemoryFree(pMsg);

  request.allowFollowerRead = true;
  msgSize = tSerializeSSubQueryMsg(nullptr, 0, &request);
  ASSERT_GT(msgSize, 0);
  pMsg = taosMemoryCalloc(1, msgSize);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(tSerializeSSubQueryMsg(pMsg, msgSize, &request), msgSize);

  response = {};
  ASSERT_EQ(tDeserializeSSubQueryMsg(pMsg, msgSize, &response), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(response.allowFollowerRead);

  tFreeSSubQueryMsg(&response);
  taosArrayDestroy(request.subEndPoints);
  taosMemoryFree(pMsg);
}

TEST(otherTest, followerReadFetchMessageRoundTrip) {
  SResFetchReq request = {0};
  request.sId = 1;
  request.queryId = 2;
  request.taskId = 3;
  request.reset = true;

  int32_t msgSize = tSerializeSResFetchReq(nullptr, 0, &request, false, false);
  ASSERT_GT(msgSize, 0);

  void *pMsg = taosMemoryCalloc(1, msgSize);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(tSerializeSResFetchReq(pMsg, msgSize, &request, false, false), msgSize);

  SResFetchReq legacyResponse = {0};
  ASSERT_EQ(tDeserializeSResFetchReq(pMsg, msgSize, &legacyResponse), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(legacyResponse.allowFollowerRead);
  tDestroySResFetchReq(&legacyResponse);
  taosMemoryFree(pMsg);

  request.allowFollowerRead = true;
  msgSize = tSerializeSResFetchReq(nullptr, 0, &request, false, false);
  ASSERT_GT(msgSize, 0);
  pMsg = taosMemoryCalloc(1, msgSize);
  ASSERT_NE(pMsg, nullptr);
  ASSERT_EQ(tSerializeSResFetchReq(pMsg, msgSize, &request, false, false), msgSize);

  SResFetchReq response = {0};
  ASSERT_EQ(tDeserializeSResFetchReq(pMsg, msgSize, &response), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(response.allowFollowerRead);

  tDestroySResFetchReq(&response);
  taosMemoryFree(pMsg);
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

TEST(correlationTest, reuseProducerEndpointsWithoutChangingIdentity) {
  SNode *pSourceNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_DOWNSTREAM_SOURCE, &pSourceNode), TSDB_CODE_SUCCESS);
  auto *pSource = reinterpret_cast<SDownstreamSourceNode *>(pSourceNode);
  pSource->producerQueryId = 101;
  pSource->taskId = 202;
  SArray *pExternal = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(pExternal, nullptr);
  ASSERT_NE(taosArrayPush(pExternal, &pSource), nullptr);

  SSchJob job = {};
  job.queryId = 303;
  job.pExternalSubquerySources = pExternal;
  SArray *pEndpoints = nullptr;
  ASSERT_EQ(schBuildSubJobEndpoints(nullptr, &pEndpoints, &job), TSDB_CODE_SUCCESS);
  ASSERT_NE(pEndpoints, nullptr);
  ASSERT_EQ(taosArrayGetSize(pEndpoints), 1);
  auto *pCopied = static_cast<SDownstreamSourceNode *>(taosArrayGetP(pEndpoints, 0));
  ASSERT_NE(pCopied, nullptr);
  EXPECT_NE(pCopied, pSource);
  EXPECT_NE(pCopied->producerQueryId, job.queryId);
  EXPECT_EQ(pCopied->producerQueryId, 101);
  EXPECT_EQ(pCopied->taskId, 202);

  taosArrayDestroyP(pEndpoints, reinterpret_cast<FDelete>(nodesDestroyNode));
  taosArrayDestroyP(pExternal, reinterpret_cast<FDelete>(nodesDestroyNode));
}

TEST(correlationTest, ignoresResponseFromPreviousExecution) {
  SSchJob job = {};
  job.attr.type = JOB_TYPE_QUERY;
  job.seriesId = 2;
  SSchTask task = {};
  task.seriesId = 2;
  task.execId = 2;
  task.status = JOB_TASK_STATUS_EXEC;
  task.handle = reinterpret_cast<void*>(0x2);
  task.execNodes = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  ASSERT_NE(nullptr, task.execNodes);
  SSchNodeInfo node = {};
  node.handle = task.handle;
  ASSERT_EQ(TSDB_CODE_SUCCESS, taosHashPut(task.execNodes, &task.execId, sizeof(task.execId), &node, sizeof(node)));

  SDataBuf oldExecution = {};
  oldExecution.msgType = TDMT_SCH_QUERY_RSP;
  oldExecution.pData = taosMemoryMalloc(1);
  ASSERT_NE(nullptr, oldExecution.pData);
  oldExecution.len = 1;
  EXPECT_EQ(TSDB_CODE_SCH_IGNORE_ERROR, schHandleResponseMsg(&job, &task, 2, 1, &oldExecution, TSDB_CODE_SUCCESS));
  EXPECT_EQ(nullptr, oldExecution.pData);
  EXPECT_EQ(2, task.execId);
  EXPECT_EQ(JOB_TASK_STATUS_EXEC, task.status);
  EXPECT_EQ(reinterpret_cast<void*>(0x2), task.handle);

  SDataBuf oldSeries = {};
  oldSeries.msgType = TDMT_SCH_QUERY_RSP;
  oldSeries.pData = taosMemoryMalloc(1);
  ASSERT_NE(nullptr, oldSeries.pData);
  oldSeries.len = 1;
  oldSeries.handle = reinterpret_cast<void*>(0x3);
  EXPECT_EQ(TSDB_CODE_SCH_IGNORE_ERROR, schHandleResponseMsg(&job, &task, 1, 2, &oldSeries, TSDB_CODE_SUCCESS));
  EXPECT_EQ(nullptr, oldSeries.pData);
  EXPECT_EQ(2, task.execId);
  EXPECT_EQ(JOB_TASK_STATUS_EXEC, task.status);
  EXPECT_EQ(reinterpret_cast<void*>(0x2), task.handle);
  auto* savedNode = static_cast<SSchNodeInfo*>(taosHashGet(task.execNodes, &task.execId, sizeof(task.execId)));
  ASSERT_NE(nullptr, savedNode);
  EXPECT_EQ(reinterpret_cast<void*>(0x2), savedNode->handle);

  SDataBuf oldNetworkError = {};
  oldNetworkError.msgType = TDMT_SCH_LINK_BROKEN;
  oldNetworkError.pData = taosMemoryMalloc(1);
  ASSERT_NE(nullptr, oldNetworkError.pData);
  oldNetworkError.len = 1;
  EXPECT_EQ(TSDB_CODE_SCH_IGNORE_ERROR,
            schHandleResponseMsg(&job, &task, 1, 2, &oldNetworkError, TSDB_CODE_SUCCESS));
  EXPECT_EQ(nullptr, oldNetworkError.pData);
  EXPECT_NE(nullptr, taosHashGet(task.execNodes, &task.execId, sizeof(task.execId)));
  EXPECT_EQ(reinterpret_cast<void*>(0x2), task.handle);
  taosHashCleanup(task.execNodes);
}

TEST(correlationTest, retainsParameterBytesAfterCallerReleasesThem) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of scheduler planner hooks is unreliable in Windows release builds";
#endif
  ASSERT_EQ(schedulerInit(), TSDB_CODE_SUCCESS);
  schtSetPlanToString();
  schtSetExecNode();

  for (int32_t input : {101, 202}) {
    SQueryPlan *pDag = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode **>(&pDag)), TSDB_CODE_SUCCESS);
    schtBuildQueryDag(pDag);

    SArray *pDefs = taosArrayInit(1, sizeof(SCorrelationParamDef));
    SArray *pValues = taosArrayInit(1, sizeof(SCorrelationWireValue));
    ASSERT_NE(pDefs, nullptr);
    ASSERT_NE(pValues, nullptr);
    SCorrelationParamDef def = {};
    def.paramId = 0;
    def.dataType.type = TSDB_DATA_TYPE_INT;
    def.dataType.bytes = sizeof(input);
    auto *pBytes = static_cast<uint8_t *>(taosMemoryMalloc(sizeof(input)));
    ASSERT_NE(pBytes, nullptr);
    memcpy(pBytes, &input, sizeof(input));
    SCorrelationWireValue value = {};
    value.paramId = 0;
    value.dataType = def.dataType;
    value.dataLen = sizeof(input);
    value.pData = pBytes;
    ASSERT_NE(taosArrayPush(pDefs, &def), nullptr);
    ASSERT_NE(taosArrayPush(pValues, &value), nullptr);

    SRequestConnInfo conn = {};
    SSchedulerReq req = {};
    req.pConn = &conn;
    req.pDag = pDag;
    req.pCorrelationParamDefs = pDefs;
    req.pCorrelationParamValues = pValues;
    int64_t jobId = 0;
    ASSERT_EQ(schInitJob(&jobId, &req), TSDB_CODE_SUCCESS);

    SSchJob *pJob = nullptr;
    ASSERT_EQ(schAcquireJob(jobId, &pJob), TSDB_CODE_SUCCESS);
    ASSERT_NE(pJob, nullptr);
    auto *pSaved = static_cast<SCorrelationWireValue *>(taosArrayGet(pJob->pCorrelationParamValues, 0));
    ASSERT_NE(pSaved, nullptr);
    ASSERT_NE(pSaved->pData, pBytes);
    taosMemoryFree(pBytes);
    taosArrayDestroy(pValues);
    taosArrayDestroy(pDefs);
    int32_t saved = 0;
    memcpy(&saved, pSaved->pData, sizeof(saved));
    EXPECT_EQ(saved, input);

    char planByte = 'x';
    SSubQueryMsg message = {};
    message.queryId = pJob->queryId;
    message.msg = &planByte;
    message.msgLen = 1;
    message.correlation.rootExecutionId = 1;
    message.correlation.rootQueryId = 1;
    message.correlation.executionGeneration = 1;
    strcpy(message.correlation.timezoneName, "UTC");
    strcpy(message.correlation.charsetName, "UTF-8");
    message.pCorrelationParamDefs = pJob->pCorrelationParamDefs;
    message.pCorrelationParamValues = pJob->pCorrelationParamValues;
    int32_t wireLen = tSerializeSSubQueryMsg(nullptr, 0, &message);
    ASSERT_GT(wireLen, 0);
    void *pWire = taosMemoryMalloc(wireLen);
    ASSERT_NE(pWire, nullptr);
    ASSERT_EQ(tSerializeSSubQueryMsg(pWire, wireLen, &message), wireLen);
    schReleaseJob(jobId);
    schedulerFreeJob(&jobId, TSDB_CODE_SUCCESS);

    SSubQueryMsg received = {};
    ASSERT_EQ(tDeserializeSSubQueryMsg(pWire, wireLen, &received), TSDB_CODE_SUCCESS);
    taosMemoryFree(pWire);
    ASSERT_EQ(taosArrayGetSize(received.pCorrelationParamValues), 1);
    auto *pReceived = static_cast<SCorrelationWireValue *>(taosArrayGet(received.pCorrelationParamValues, 0));
    ASSERT_NE(pReceived, nullptr);
    ASSERT_NE(pReceived->pData, nullptr);
    int32_t actual = 0;
    memcpy(&actual, pReceived->pData, sizeof(actual));
    EXPECT_EQ(actual, input);
    tFreeSSubQueryMsg(&received);
  }

  schedulerDestroy();
  schMgmt.jobRef = -1;
}

int main(int argc, char **argv) {
  schtInitLogFile();
  if (rpcInit()) {
    TD_ALWAYS_ASSERT(0);
  }
  if (initTaskQueue() != 0) {
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
  (void)cleanupTaskQueue();
  
  return code;
}

#pragma GCC diagnostic pop

namespace {
int32_t failStmtJobInit(int64_t*, SSchedulerReq*) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

void captureStmtJobInitError(SExecResult* result, void* param, int32_t code) {
  EXPECT_EQ(result, nullptr);
  *static_cast<int32_t*>(param) = code;
}
}  // namespace

TEST(schedulerTest, stmt_job_init_failure_has_no_execution_result) {
  Stub stub;
  stub.set(schInitJob, failStmtJobInit);
  SRequestConnInfo conn = {};
  SSchedulerReq req = {};
  req.pConn = &conn;
  req.syncReq = false;
  req.execFp = captureStmtJobInitError;
  int32_t callbackCode = TSDB_CODE_SUCCESS;
  req.cbParam = &callbackCode;
  int64_t jobId = 0;
  EXPECT_EQ(schedulerExecJob(&req, &jobId), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(callbackCode, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(jobId, 0);

  req.syncReq = true;
  SExecResult result = {};
  req.pExecRes = &result;
  EXPECT_EQ(schedulerExecJob(&req, &jobId), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_FALSE(result.execDispatched);
}

namespace {
SSchJob* dispatchTestJob = nullptr;
SSchTask* dispatchTestTask = nullptr;
int32_t dispatchTestSendCode = TSDB_CODE_SUCCESS;
bool dispatchTestInlineCallback = false;
int32_t dispatchTestSendCalls = 0;
int32_t dispatchTestResponseCalls = 0;

int32_t dispatchTestCbBegin(SSchJob** job, SSchTask** task, uint64_t, int64_t, int32_t, uint64_t) {
  *job = dispatchTestJob;
  *task = dispatchTestTask;
  return TSDB_CODE_SUCCESS;
}

int32_t dispatchTestResponse(SSchJob* job, SSchTask*, uint64_t, int32_t, SDataBuf*, int32_t) {
  ++dispatchTestResponseCalls;
  EXPECT_EQ(atomic_load_8(&job->execDispatched), 1);
  return TSDB_CODE_SUCCESS;
}

void dispatchTestCbEnd(SSchJob*, SSchTask*, int32_t) {}

int32_t dispatchTestSend(void*, SEpSet*, int64_t*, SMsgSendInfo* info, bool, void*) {
  ++dispatchTestSendCalls;
  if (dispatchTestInlineCallback) {
    SDataBuf response = {};
    response.msgType = info->msgType + 1;
    EXPECT_EQ(info->fp(info->param, &response, TSDB_CODE_RPC_NETWORK_UNAVAIL), TSDB_CODE_SUCCESS);
  }
  destroySendMsgInfo(info);
  return dispatchTestSendCode;
}
}  // namespace

TEST(schedulerTest, stmt_dispatch_tracks_accepted_work) {
#ifndef LINUX
  GTEST_SKIP() << "This scheduler dispatch test uses address stubs and runs only on Linux";
#endif
  Stub stub;
  stub.set(asyncSendMsgToServerExt, dispatchTestSend);
  stub.set(schProcessOnCbBegin, dispatchTestCbBegin);
  stub.set(schHandleResponseMsg, dispatchTestResponse);
  stub.set(schProcessOnCbEnd, dispatchTestCbEnd);

  for (int8_t previous : {0, 1}) {
    for (int mode = 0; mode < 3; ++mode) {
      SCOPED_TRACE(::testing::Message() << "previous=" << static_cast<int>(previous) << ", mode=" << mode);
      SSchJob job = {};
      SSchTask task = {};
      SSubplan plan = {};
      plan.msgType = TDMT_VND_SUBMIT;
      task.plan = &plan;
      job.execDispatched = previous;
      dispatchTestJob = &job;
      dispatchTestTask = &task;
      // Cover synchronous local failure, accepted send, and callback-before-return.
      dispatchTestSendCode = mode == 0 ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS;
      dispatchTestInlineCallback = mode == 2;
      dispatchTestSendCalls = 0;
      dispatchTestResponseCalls = 0;
      SSchTrans trans = {};
      SQueryNodeAddr addr = {};
      addr.nodeId = MNODE_HANDLE - 1;
      void* message = taosMemoryCalloc(1, 1);
      ASSERT_NE(message, nullptr);
      EXPECT_EQ(schAsyncSendMsg(&job, &task, &trans, &addr, plan.msgType, message, 1, false, nullptr),
                dispatchTestSendCode);
      EXPECT_EQ(dispatchTestSendCalls, 1);
      EXPECT_EQ(dispatchTestResponseCalls, mode == 2 ? 1 : 0);
      EXPECT_EQ(atomic_load_8(&job.execDispatched), mode == 0 ? previous : 1);
    }
  }
  dispatchTestJob = nullptr;
  dispatchTestTask = nullptr;
}

TEST(schPhase, mainPhaseMapping) {
  // Main phase values map to themselves, including the fetch main phase which
  // must agree with QUERY_PHASE_IS_FETCH_STAGE().
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_NONE), QUERY_PHASE_NONE);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_PARSE), QUERY_PHASE_PARSE);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_CATALOG), QUERY_PHASE_CATALOG);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_PLAN), QUERY_PHASE_PLAN);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_SCHEDULE), QUERY_PHASE_SCHEDULE);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_EXECUTE), QUERY_PHASE_EXECUTE);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_FETCH), QUERY_PHASE_FETCH);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_DONE), QUERY_PHASE_DONE);

  // Sub-phase range boundaries and middle values map to their parent phase.
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_SCHEDULE_ANALYSIS), QUERY_PHASE_SCHEDULE);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_SCHEDULE_PLANNING), QUERY_PHASE_SCHEDULE);
  EXPECT_EQ(schGetMainPhase(QUERY_PHASE_SCHEDULE_NODE_SELECTION), QUERY_PHASE_SCHEDULE);
  for (int32_t phase = QUERY_PHASE_SCHEDULE_ANALYSIS; phase <= QUERY_PHASE_SCHEDULE_NODE_SELECTION; ++phase) {
    EXPECT_EQ(schGetMainPhase(phase), QUERY_PHASE_SCHEDULE) << "schedule sub-phase " << phase;
  }
  for (int32_t phase = QUERY_PHASE_EXEC_DATA_QUERY; phase <= QUERY_PHASE_EXEC_WAITING_CHILDREN; ++phase) {
    EXPECT_EQ(schGetMainPhase(phase), QUERY_PHASE_EXECUTE) << "execute sub-phase " << phase;
  }
  for (int32_t phase = QUERY_PHASE_FETCH_IN_PROGRESS; phase <= QUERY_PHASE_FETCH_RETURNED; ++phase) {
    EXPECT_EQ(schGetMainPhase(phase), QUERY_PHASE_FETCH) << "fetch sub-phase " << phase;
  }

  // Values in the gaps between the sub-phase ranges are returned unchanged so
  // a future phase cannot be routed to an unrelated main phase silently.
  // Derive the gaps from the enum bounds: if a sub-phase is added there, the
  // gap value moves with it and the test keeps asserting the same intent.
  const int32_t scheduleGap = QUERY_PHASE_SCHEDULE_NODE_SELECTION + 1;
  const int32_t execGap = QUERY_PHASE_EXEC_WAITING_CHILDREN + 1;
  const int32_t fetchGap = QUERY_PHASE_FETCH_RETURNED + 1;
  const int32_t afterDone = QUERY_PHASE_DONE + 1;
  EXPECT_EQ(schGetMainPhase(-1), -1);
  EXPECT_EQ(schGetMainPhase(scheduleGap), scheduleGap);
  EXPECT_EQ(schGetMainPhase(execGap), execGap);
  EXPECT_EQ(schGetMainPhase(fetchGap), fetchGap);
  EXPECT_EQ(schGetMainPhase(afterDone), afterDone);
}

TEST(schPhase, publishReadAndCasSemantics) {
  SSchJob job = {};
  int32_t phase = -1;
  int64_t start = 0;

  // schPublishJobPhase publishes the phase and stamps a start time.
  schPublishJobPhase(&job, QUERY_PHASE_SCHEDULE_PLANNING);
  schReadJobPhase(&job, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_SCHEDULE_PLANNING);
  EXPECT_GT(start, 0);

  // A CAS with a wrong expected value fails and leaves the pair untouched.
  EXPECT_FALSE(schCasJobPhase(&job, QUERY_PHASE_EXEC_DATA_QUERY, QUERY_PHASE_FETCH_IN_PROGRESS, false));
  int32_t phase2 = -1;
  int64_t start2 = 0;
  schReadJobPhase(&job, &phase2, &start2);
  EXPECT_EQ(phase2, QUERY_PHASE_SCHEDULE_PLANNING);
  EXPECT_EQ(start2, start);

  // keepTime=true switches the sub-phase without restarting the timer.
  EXPECT_TRUE(schCasJobPhase(&job, QUERY_PHASE_SCHEDULE_PLANNING, QUERY_PHASE_FETCH_IN_PROGRESS, true));
  schReadJobPhase(&job, &phase2, &start2);
  EXPECT_EQ(phase2, QUERY_PHASE_FETCH_IN_PROGRESS);
  EXPECT_EQ(start2, start);

  // keepTime=false resets the timer (it never moves backwards).
  EXPECT_TRUE(schCasJobPhase(&job, QUERY_PHASE_FETCH_IN_PROGRESS, QUERY_PHASE_DONE, false));
  schReadJobPhase(&job, &phase2, &start2);
  EXPECT_EQ(phase2, QUERY_PHASE_DONE);
  EXPECT_GE(start2, start);
}

TEST(schPhase, enterFetchPhaseKeepsStageTimer) {
  SSchJob job = {};
  job.execPhase = QUERY_PHASE_EXEC_DATA_QUERY;
  job.phaseStartTime = 1234;
  int32_t phase = -1;
  int64_t start = 0;

  // Entering from execute stamps the fetch stage and a new start time.
  schEnterFetchPhase(&job);
  schReadJobPhase(&job, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_FETCH_IN_PROGRESS);
  EXPECT_GE(start, 1234);
  int64_t fetchStart = start;

  // Re-entering while already in the fetch stage is a no-op.
  schEnterFetchPhase(&job);
  schReadJobPhase(&job, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_FETCH_IN_PROGRESS);
  EXPECT_EQ(start, fetchStart);

  // Returning to in_progress after a returned block keeps one stage timer.
  EXPECT_TRUE(schCasJobPhase(&job, QUERY_PHASE_FETCH_IN_PROGRESS, QUERY_PHASE_FETCH_RETURNED, true));
  schEnterFetchPhase(&job);
  schReadJobPhase(&job, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_FETCH_IN_PROGRESS);
  EXPECT_EQ(start, fetchStart);

  // DONE is terminal: entering fetch must not resurrect a finished query.
  schPublishJobPhase(&job, QUERY_PHASE_DONE);
  schReadJobPhase(&job, &phase, &start);
  int64_t doneStart = start;
  schEnterFetchPhase(&job);
  schReadJobPhase(&job, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_DONE);
  EXPECT_EQ(start, doneStart);
}

TEST(schPhase, promoteParentForSubJobs) {
  SSchJob parent = {};
  parent.execPhase = QUERY_PHASE_SCHEDULE_PLANNING;
  parent.phaseStartTime = 100;
  int32_t phase = -1;
  int64_t start = 0;

  // A schedule sub-phase is promoted to the coarse execute/waiting_children.
  schPromoteParentForSubJobs(&parent);
  schReadJobPhase(&parent, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_EXEC_WAITING_CHILDREN);
  EXPECT_GT(start, 100);

  // A second promotion (parent already in execute) never moves it back.
  int64_t execStart = start;
  schPromoteParentForSubJobs(&parent);
  schReadJobPhase(&parent, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_EXEC_WAITING_CHILDREN);
  EXPECT_EQ(start, execStart);

  // An execute sub-phase is promoted as well: the sample/CAS gap may leave the
  // parent on an intermediate execute sub-phase, and moving to waiting_children
  // stays inside the same main phase, so it is not a backwards transition.
  SSchJob execJob = {};
  execJob.execPhase = QUERY_PHASE_EXEC_DATA_QUERY;
  execJob.phaseStartTime = 9;
  schPromoteParentForSubJobs(&execJob);
  schReadJobPhase(&execJob, &phase, &start);
  EXPECT_EQ(phase, QUERY_PHASE_EXEC_WAITING_CHILDREN);
  EXPECT_GT(start, 9);

  // Non schedule-sub-phases are left untouched: not yet scheduled, the coarse
  // SCHEDULE main-phase value itself, and the terminal DONE phase.
  const int32_t untouched[] = {QUERY_PHASE_NONE, QUERY_PHASE_PARSE, QUERY_PHASE_SCHEDULE, QUERY_PHASE_DONE};
  for (size_t i = 0; i < sizeof(untouched) / sizeof(untouched[0]); ++i) {
    SSchJob job = {};
    job.execPhase = untouched[i];
    job.phaseStartTime = 7;
    schPromoteParentForSubJobs(&job);
    schReadJobPhase(&job, &phase, &start);
    EXPECT_EQ(phase, untouched[i]) << "phase " << untouched[i];
    EXPECT_EQ(start, 7) << "phase " << untouched[i];
  }
}

TEST(schPhase, seqlockSnapshotIsConsistent) {
  SSchJob job = {};
  // Writers publish (phase, start) as a pair following the seqlock protocol;
  // a reader must never observe a phase of one pair with the start time of the
  // other one.
  //
  // The writer runs until the readers finish instead of for a fixed number of
  // iterations, so the concurrent window covers the whole read loop. It keeps
  // referencing a job that outlives it: the stop flag is only set after the
  // read loop, and the guard joins the thread while `job` is still alive even
  // if an assertion aborts the test body early.
  std::atomic<bool> stop{false};
  std::thread writer([&job, &stop]() {
    int32_t i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      while (atomic_val_compare_exchange_32(&job.phaseWriteLock, 0, 1) != 0) {
        taosMsleep(1);
      }
      atomic_add_fetch_32(&job.phaseSeq, 1);
      bool executePair = (i++ % 2) == 0;
      atomic_store_32(&job.execPhase,
                      executePair ? QUERY_PHASE_EXEC_DATA_QUERY : QUERY_PHASE_FETCH_IN_PROGRESS);
      atomic_store_64(&job.phaseStartTime, executePair ? 5100 : 6000);
      atomic_add_fetch_32(&job.phaseSeq, 1);
      atomic_store_32(&job.phaseWriteLock, 0);
      taosMsleep(1);
    }
  });
  struct WriterStop {
    std::thread          &thread;
    std::atomic<bool>    &stop;
    ~WriterStop() {
      stop.store(true);
      if (thread.joinable()) {
        thread.join();
      }
    }
  } writerStop{writer, stop};

  for (int32_t i = 0; i < 3000; ++i) {
    int32_t phase = -1;
    int64_t start = -1;
    schReadJobPhase(&job, &phase, &start);
    if (phase == QUERY_PHASE_NONE) {
      EXPECT_EQ(start, 0);
    } else if (phase == QUERY_PHASE_EXEC_DATA_QUERY) {
      EXPECT_EQ(start, 5100);
    } else if (phase == QUERY_PHASE_FETCH_IN_PROGRESS) {
      EXPECT_EQ(start, 6000);
    } else {
      ADD_FAILURE() << "unexpected phase " << phase;
    }
  }
  // The guard stops and joins the writer here, before `job` goes out of scope.
}

namespace {
std::vector<int32_t> subqueryLaunches;
extern "C" void schPostJobRes(SSchJob* job, int32_t op);
std::vector<int32_t> subqueryDrops;
std::vector<std::pair<__async_exec_fn_t, void*>> subqueryAsyncWork;
SSchJob* subqueryParent = nullptr;
bool subqueryInlineSuccess = false;
bool subqueryCleanupBeforeCompletion = false;
int32_t subqueryFailOnLaunch = -2;

int32_t recordSubqueryLaunch(SSchJob* job) {
  subqueryLaunches.push_back(job->subJobId);
  if (job->fetchTask) job->fetchTask->seriesId = job->seriesId;
  if (job->subJobId == subqueryFailOnLaunch) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (subqueryInlineSuccess && job->subJobId >= 0) {
    return schProcessOnSubJobSuccess(job);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t recordFlowTaskLaunch(SSchJob* job, const SSchFlowTask* task) {
  subqueryLaunches.push_back(job->subJobId);
  return TSDB_CODE_SUCCESS;
}

int32_t acquireSubqueryTestJob(int64_t refId, SSchJob** job) {
  *job = subqueryParent;
  return TSDB_CODE_SUCCESS;
}

int32_t releaseSubqueryTestJob(int64_t refId) { return TSDB_CODE_SUCCESS; }

int32_t allowSubqueryRetry(SSchTask* task, SSchJob* job, int32_t code) { return TSDB_CODE_SUCCESS; }

void* failSubqueryReadyAppend(SArray* array, const void* data, int32_t count) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

int32_t deferSubqueryWork(__async_exec_fn_t fn, void* param, int32_t* code) {
  subqueryAsyncWork.emplace_back(fn, param);
  return TSDB_CODE_SUCCESS;
}

void recordSubqueryDrop(SSchJob* job) { subqueryDrops.push_back(job->subJobId); }

void recordSubqueryCompletion(SSchJob* job, int32_t op) {
  if (job == subqueryParent) {
    subqueryCleanupBeforeCompletion = !subqueryAsyncWork.empty();
  }
}

class SubquerySchedulingTest : public testing::Test {
 protected:
  void SetUp() override {
    subqueryLaunches.clear();
    subqueryDrops.clear();
    subqueryAsyncWork.clear();
    subqueryInlineSuccess = false;
    subqueryCleanupBeforeCompletion = false;
    subqueryFailOnLaunch = -2;
    savedQuota = schMgmt.cfg.maxNodeTableNum;
    stub.set(schLaunchJobImpl, recordSubqueryLaunch);
    stub.set(schAsyncLaunchFlowTask, recordFlowTaskLaunch);
    stub.set(schAcquireJob, acquireSubqueryTestJob);
    stub.set(schReleaseJob, releaseSubqueryTestJob);
    stub.set(taosAsyncExec, deferSubqueryWork);
    stub.set(schDropJobAllTasks, recordSubqueryDrop);
    stub.set(schFailedTaskNeedRetry, allowSubqueryRetry);
  }

  void init(int32_t count) {
    jobs.resize(count);
    tasks.resize(count);
    plans.resize(count);
    parent.subJobId = -1;
    parent.refId = 1;
    parent.seriesId = 1;
    parent.status = JOB_TASK_STATUS_EXEC;
    parent.attr.type = JOB_TYPE_QUERY;
    subqueryParent = &parent;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&parent.pDag)), 0);
    parent.pDag->subqueryDepsKnown = true;
    parent.subJobs = taosArrayInit(count, POINTER_BYTES);
    ASSERT_NE(parent.subJobs, nullptr);
    for (int32_t i = 0; i < count; ++i) {
      auto* job = &jobs[i];
      job->parent = &parent;
      job->subJobId = i;
      job->refId = parent.refId;
      job->seriesId = 1;
      job->attr.type = JOB_TYPE_QUERY;
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&job->pDag)), 0);
      job->pDag->subqueryDepsKnown = true;
      tasks[i].seriesId = 1;
      tasks[i].taskId = 100 + i;
      tasks[i].plan = &plans[i];
      plans[i].msgType = TDMT_SCH_QUERY;
      plans[i].subplanType = SUBPLAN_TYPE_SCAN;
      addEpIntoEpSet(&plans[i].execNode.epSet, "subquery-node", 6030);
      job->fetchTask = &tasks[i];
      ASSERT_NE(taosArrayPush(parent.subJobs, &job), nullptr);
    }
  }

  void depends(int32_t consumer, int32_t producer) {
    auto* job = consumer < 0 ? &parent : &jobs[consumer];
    if (!job->pDag->pSubqueryDeps) job->pDag->pSubqueryDeps = taosArrayInit(4, sizeof(int32_t));
    ASSERT_NE(taosArrayPush(job->pDag->pSubqueryDeps, &producer), nullptr);
  }

  void addScan(SSchJob* job, SSchTask* task, int32_t weight) {
    if (!job->dataSrcTasks) job->dataSrcTasks = taosArrayInit(2, POINTER_BYTES);
    ASSERT_NE(job->dataSrcTasks, nullptr);
    task->plan->execNodeStat.tableNum = weight;
    ASSERT_NE(taosArrayPush(job->dataSrcTasks, &task), nullptr);
  }

  void addSingleTaskLevel(SSchJob& job, SSubplan& plan) {
    job.taskNum = job.levelNum = 1;
    job.levels = taosArrayInit_s(sizeof(SSchLevel), 1);
    ASSERT_NE(job.levels, nullptr);
    auto* level = static_cast<SSchLevel*>(taosArrayGet(job.levels, 0));
    level->taskNum = 1;
    level->subTasks = taosArrayInit_s(sizeof(SSchTask), 1);
    ASSERT_NE(level->subTasks, nullptr);
    auto* task = static_cast<SSchTask*>(taosArrayGet(level->subTasks, 0));
    ASSERT_EQ(schInitTask(&job, task, &plan, level), 0);
    auto hash = taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT);
    job.execTasks = taosHashInit(1, hash, false, HASH_ENTRY_LOCK);
    job.taskList = taosHashInit(1, hash, false, HASH_ENTRY_LOCK);
    ASSERT_NE(job.execTasks, nullptr);
    ASSERT_NE(job.taskList, nullptr);
    ASSERT_EQ(taosHashPut(job.taskList, &task->taskId, sizeof(task->taskId), &task, sizeof(task)), 0);
    job.fetchTask = task;
  }

  void publishRootScanForFetch(SPhysiNode& scan) {
    init(1);
    scan.type = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
    plans[0].pNode = &scan;
    plans[0].subqueryDepsKnown = true;
    parent.attr.explainMode = EXPLAIN_MODE_DISABLE;
    parent.attr.needFetch = true;
    addSingleTaskLevel(parent, plans[0]);
    ASSERT_EQ(schInitJobDependencies(&parent), 0);
    ASSERT_TRUE(parent.earlyMainTasks);
    ASSERT_EQ(schLaunchJob(&parent), 0);
    ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
    auto* root = parent.fetchTask;
    root->execId = 0;
    ASSERT_EQ(schSetTaskCandidateAddrs(&parent, root), 0);
    ASSERT_EQ(schProcessOnTaskSuccess(&parent, root), 0);
    ASSERT_EQ(parent.status, JOB_TASK_STATUS_PART_SUCC);
  }

  void runQueuedWork() {
    while (!subqueryAsyncWork.empty()) {
      auto work = subqueryAsyncWork.front();
      subqueryAsyncWork.erase(subqueryAsyncWork.begin());
      EXPECT_EQ(work.first(work.second), 0);
    }
  }

  void freeLevels(SSchJob* job) {
    for (int32_t i = 0; i < taosArrayGetSize(job->levels); ++i) {
      auto* level = static_cast<SSchLevel*>(taosArrayGet(job->levels, i));
      for (int32_t j = 0; j < taosArrayGetSize(level->subTasks); ++j) {
        schFreeTask(job, static_cast<SSchTask*>(taosArrayGet(level->subTasks, j)));
      }
      taosArrayDestroy(level->subTasks);
    }
    taosArrayDestroy(job->levels);
  }

  void TearDown() override {
    runQueuedWork();
    for (auto& job : jobs) {
      freeLevels(&job);
      taosHashCleanup(job.taskList);
      taosHashCleanup(job.execTasks);
      taosArrayDestroy(job.dependentJobs);
      taosArrayDestroy(job.waitingMainTasks);
      taosArrayDestroy(job.dataSrcTasks);
      nodesDestroyNode(reinterpret_cast<SNode*>(job.pDag));
    }
    for (auto& task : tasks) {
      taosArrayDestroy(task.candidateAddrs);
      taosHashCleanup(task.execNodes);
      taosArrayDestroy(task.profile.execTime);
    }
    taosHashCleanup(parent.execTasks);
    taosHashCleanup(parent.taskList);
    taosArrayDestroy(parent.dataSrcTasks);
    freeLevels(&parent);
    schFreeFlowCtrl(&parent);
    taosArrayDestroy(parent.subJobs);
    nodesDestroyNode(reinterpret_cast<SNode*>(parent.pDag));
    schMgmt.cfg.maxNodeTableNum = savedQuota;
    subqueryParent = nullptr;
  }

  Stub stub;
  SSchJob parent = {};
  std::vector<SSchJob> jobs;
  std::vector<SSchTask> tasks;
  std::vector<SSubplan> plans;
  int64_t savedQuota = 0;
};

TEST_F(SubquerySchedulingTest, mixedQueryCanSendSparseEndpointSlots) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  auto& job = jobs[1];
  job.queryId = 123;
  job.correlation.rootExecutionId = 1;
  job.correlation.rootQueryId = job.queryId;
  job.correlation.executionGeneration = 1;
  SSubQueryMsg msg = {};
  msg.correlation = job.correlation;
  msg.queryId = job.queryId;
  ASSERT_EQ(schBuildSubJobEndpoints(&tasks[1], &msg.subEndPoints, &job), 0);
  ASSERT_EQ(taosArrayGetSize(msg.subEndPoints), 1);
  EXPECT_EQ(static_cast<SDownstreamSourceNode*>(taosArrayGetP(msg.subEndPoints, 0))->fetchMsgType, 0);
  EXPECT_GT(tSerializeSSubQueryMsg(nullptr, 0, &msg), 0);
  taosArrayDestroyP(msg.subEndPoints, reinterpret_cast<FDelete>(nodesDestroyNode));
}

TEST_F(SubquerySchedulingTest, independentJobsStartBeforeAnyCompletion) {
  init(4);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, 2, 3}));
  EXPECT_EQ(parent.subJobDoneNum, 0);
  for (int32_t i : {3, 1, 0, 2}) ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[i]), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, 2, 3, -1}));
  EXPECT_EQ(parent.subJobDoneNum, 4);
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[2]), TSDB_CODE_SCH_IGNORE_ERROR);
  EXPECT_EQ(parent.subJobDoneNum, 4);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches.size(), 5);
  ++parent.seriesId;
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, 2, 3, -1, -1}));
}

TEST_F(SubquerySchedulingTest, mainTasksWaitOnlyForTheirOwnInputs) {
  init(2);
  depends(-1, 0);
  SSubplan independent = {};
  independent.subqueryDepsKnown = true;
  addSingleTaskLevel(parent, independent);
  parent.attr.explainMode = EXPLAIN_MODE_DISABLE;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_TRUE(parent.earlyMainTasks);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, -1}));
  auto record = +[](SSchJob*, SSchTask* task) -> int32_t {
    subqueryLaunches.push_back(static_cast<int32_t>(task->taskId));
    return 0;
  };
  stub.set(schLaunchTask, record);
  SSchTask scan = {};
  scan.taskId = 101;
  scan.plan = &independent;
  ASSERT_EQ(schDelayLaunchTask(&parent, &scan), 0);
  SArray* endpoints = nullptr;
  ASSERT_EQ(schBuildSubJobEndpoints(&scan, &endpoints, &parent), 0);
  EXPECT_EQ(endpoints, nullptr);

  SSubplan consumerPlan = {};
  consumerPlan.subqueryDepsKnown = true;
  consumerPlan.pSubqueryDeps = parent.pDag->pSubqueryDeps;
  SSchTask consumer = {};
  consumer.plan = &consumerPlan;
  consumer.seriesId = parent.seriesId;
  consumer.taskId = 102;
  ASSERT_EQ(schDelayLaunchTask(&parent, &consumer), 0);
  EXPECT_TRUE(consumer.waitingSubquery);
  EXPECT_EQ(subqueryLaunches.back(), 101);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  runQueuedWork();
  ASSERT_FALSE(consumer.waitingSubquery);
  EXPECT_EQ(subqueryLaunches.back(), 102);
  ASSERT_EQ(schBuildSubJobEndpoints(&consumer, &endpoints, &parent), 0);
  ASSERT_EQ(taosArrayGetSize(endpoints), 2);
  EXPECT_NE(static_cast<SDownstreamSourceNode*>(taosArrayGetP(endpoints, 0))->fetchMsgType, 0);
  EXPECT_EQ(static_cast<SDownstreamSourceNode*>(taosArrayGetP(endpoints, 1))->fetchMsgType, 0);
  taosArrayDestroyP(endpoints, reinterpret_cast<FDelete>(nodesDestroyNode));

  // A second consumer's exchange input arrives after the scalar result.
  consumer.taskId = 103;
  ASSERT_EQ(schDelayLaunchTask(&parent, &consumer), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[1]), 0);
  runQueuedWork();
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, -1, 101, 102, 103}));
}

TEST_F(SubquerySchedulingTest, shallowLeafStartsWhileDeepLeafWaits) {
  init(1);
  depends(-1, 0);
  SSubplan fragment[3] = {};
  parent.attr.explainMode = EXPLAIN_MODE_DISABLE;
  parent.levelNum = 3;
  parent.levelIdx = 2;
  parent.levels = taosArrayInit_s(sizeof(SSchLevel), 3);
  ASSERT_NE(parent.levels, nullptr);
  for (int i = 0; i < 3; ++i) {
    auto* level = static_cast<SSchLevel*>(taosArrayGet(parent.levels, i));
    level->level = i;
    level->taskNum = 1;
    level->subTasks = taosArrayInit_s(sizeof(SSchTask), 1);
    ASSERT_NE(level->subTasks, nullptr);
    auto* task = static_cast<SSchTask*>(taosArrayGet(level->subTasks, 0));
    task->plan = &fragment[i];
    task->level = level;
    task->taskId = i;
    fragment[i].subqueryDepsKnown = true;
    if (i == 0) task->children = taosArrayInit_s(POINTER_BYTES, 2);
  }
  fragment[2].pSubqueryDeps = parent.pDag->pSubqueryDeps;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_TRUE(parent.earlyMainTasks);
  stub.reset(schLaunchJobImpl);
  stub.set(schLaunchTask, +[](SSchJob*, SSchTask* task) -> int32_t {
    subqueryLaunches.push_back(static_cast<int32_t>(task->taskId));
    return 0;
  });
  parent.subJobStarted = true;
  ASSERT_EQ(schLaunchJobImpl(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{1}));
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  runQueuedWork();
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{1, 2}));
}

TEST_F(SubquerySchedulingTest, retryInvalidatesDeferredMainResult) {
  init(1);
  SPhysiNode node = {};
  node.type = QUERY_NODE_PHYSICAL_PLAN_PROJECT;
  SSubplan rootPlan = {};
  rootPlan.pNode = &node;
  rootPlan.subqueryDepsKnown = true;
  parent.attr.explainMode = EXPLAIN_MODE_DISABLE;
  addSingleTaskLevel(parent, rootPlan);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  parent.fetchTask->execId = 1;
  parent.fetchTask->status = JOB_TASK_STATUS_PART_SUCC;
  parent.pendingMainResult = {parent.fetchTask, parent.seriesId, parent.fetchTask->execId};
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  ASSERT_EQ(parent.status, JOB_TASK_STATUS_EXEC);
  ASSERT_EQ(subqueryAsyncWork.size(), 1);
  auto* root = parent.fetchTask;
  SDataBuf message = {};
  SCH_LOCK_TASK(root);
  int32_t code = schHandleJobRetry(&parent, root, &message, TSDB_CODE_RPC_BROKEN_LINK);
  SCH_UNLOCK_TASK(root);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(parent.seriesId, 2);
  runQueuedWork();
  EXPECT_EQ(parent.status, JOB_TASK_STATUS_EXEC);
}

TEST_F(SubquerySchedulingTest, failedRootAttemptCannotPublishEvenInSameSeries) {
  init(1);
  addSingleTaskLevel(parent, plans[0]);
  parent.attr.explainMode = EXPLAIN_MODE_DISABLE;
  plans[0].subqueryDepsKnown = true;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  auto* root = parent.fetchTask;
  root->execId = 1;
  root->maxExecTimes = 3;
  parent.execPhase = QUERY_PHASE_EXEC_DATA_QUERY;
  stub.set(schRecordTaskSucceedNode, +[](SSchJob*, SSchTask*) -> int32_t { return 0; });
  ASSERT_EQ(schProcessOnTaskSuccess(&parent, root), 0);
  EXPECT_EQ(parent.execPhase, QUERY_PHASE_EXEC_WAITING_CHILDREN);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  root->failedExecId = root->execId;
  root->status = JOB_TASK_STATUS_INIT;
  runQueuedWork();
  EXPECT_EQ(parent.status, JOB_TASK_STATUS_EXEC);
  ++root->execId;
  ASSERT_EQ(schProcessOnTaskSuccess(&parent, root), 0);
  EXPECT_EQ(parent.status, JOB_TASK_STATUS_PART_SUCC);
}

TEST_F(SubquerySchedulingTest, firstFetchRetriesPublishedRootScanAndLaterFetchDoesNotReplayRows) {
  SPhysiNode scan = {};
  ASSERT_NO_FATAL_FAILURE(publishRootScanForFetch(scan));
  auto* root = parent.fetchTask;
  stub.reset(schFailedTaskNeedRetry);

  // Record each outgoing FETCH while driving its response through the RPC callback.
  stub.set(schBuildAndSendMsg, +[](SSchJob*, SSchTask* task, SQueryNodeAddr*, int32_t type, void*) -> int32_t {
    task->lastMsgType = type;
    subqueryLaunches.push_back(type);
    return 0;
  });
  std::vector<int32_t> results;
  SSchedulerReq req = {};
  req.cbParam = &results;
  req.fetchFp = +[](void* data, void* param, int32_t code) {
    auto* rows = static_cast<SRetrieveTableRsp*>(data);
    static_cast<std::vector<int32_t>*>(param)->push_back(code ? code : static_cast<int32_t>(be64toh(rows->numOfRows)));
    taosMemoryFree(data);
  };
  auto deliver = [&](SDataBuf& response, int32_t code) {
    SSchTaskCallbackParam param = {};
    param.refId = parent.refId;
    param.taskId = root->taskId;
    param.subJobId = parent.subJobId;
    param.seriesId = root->seriesId;
    param.execId = root->execId;
    return schHandleCallback(&param, &response, code);
  };
  auto disconnect = [&]() {
    SSchNodeInfo node = {};
    ASSERT_EQ(taosHashPut(root->execNodes, &root->execId, sizeof(root->execId), &node, sizeof(node)), 0);
    SDataBuf response = {};
    response.msgType = TDMT_SCH_FETCH_RSP;
    (void)deliver(response, TSDB_CODE_RPC_BROKEN_LINK);
  };

  ASSERT_EQ(schedulerFetchRows(parent.refId, &req), 0);
  ASSERT_EQ(root->lastMsgType, TDMT_SCH_FETCH);
  ASSERT_NO_FATAL_FAILURE(disconnect());
  ASSERT_EQ(parent.status, JOB_TASK_STATUS_EXEC);
  ASSERT_EQ(parent.seriesId, 2);
  EXPECT_TRUE(results.empty());
  EXPECT_TRUE(jobs[0].resultReady);

  // Complete the reissued root query, then deliver the response to the resumed FETCH.
  ++root->execId;
  root->waitRetry = false;
  ASSERT_EQ(schProcessOnTaskSuccess(&parent, root), 0);
  ASSERT_EQ(root->status, JOB_TASK_STATUS_FETCH);
  ASSERT_EQ(std::count(subqueryLaunches.begin(), subqueryLaunches.end(), TDMT_SCH_FETCH), 2);
  auto* block = static_cast<SRetrieveTableRsp*>(taosMemoryCalloc(1, sizeof(SRetrieveTableRsp)));
  ASSERT_NE(block, nullptr);
  block->numOfRows = htobe64(2);
  SDataBuf response = {};
  response.msgType = TDMT_SCH_FETCH_RSP;
  response.pData = block;
  response.len = sizeof(*block);
  ASSERT_EQ(deliver(response, 0), 0);
  ASSERT_EQ(results, (std::vector<int32_t>{2}));

  ASSERT_EQ(schedulerFetchRows(parent.refId, &req), 0);
  ASSERT_NO_FATAL_FAILURE(disconnect());
  EXPECT_EQ(results, (std::vector<int32_t>{2, TSDB_CODE_RPC_BROKEN_LINK}));
  EXPECT_EQ(parent.seriesId, 2);
  EXPECT_TRUE(parent.noMoreRetry);
}

TEST_F(SubquerySchedulingTest, fetchDispatchFailureCompletesPendingRequest) {
  SPhysiNode scan = {};
  ASSERT_NO_FATAL_FAILURE(publishRootScanForFetch(scan));
  stub.set(schBuildAndSendMsg, +[](SSchJob*, SSchTask* task, SQueryNodeAddr*, int32_t type, void*) -> int32_t {
    task->lastMsgType = type;
    return TSDB_CODE_RPC_BROKEN_LINK;
  });

  std::vector<int32_t> errors;
  SSchedulerReq req = {};
  req.cbParam = &errors;
  req.fetchFp = +[](void* data, void* param, int32_t code) {
    static_cast<std::vector<int32_t>*>(param)->push_back(code);
    taosMemoryFree(data);
  };
  EXPECT_EQ(schedulerFetchRows(parent.refId, &req), TSDB_CODE_RPC_BROKEN_LINK);
  EXPECT_EQ(errors, (std::vector<int32_t>{TSDB_CODE_RPC_BROKEN_LINK}));
  EXPECT_EQ(parent.opStatus.op, SCH_OP_NULL);
  EXPECT_EQ(parent.seriesId, 1);
}

TEST_F(SubquerySchedulingTest, lateExchangeInputCannotEnterResetParent) {
  init(1);
  parent.earlyMainTasks = true;
  SSchTask child = {};
  SSchTask receiver = {};
  SSchLevel level = {};
  child.plan = &plans[0];
  child.level = &level;
  child.maxExecTimes = 3;
  child.seriesId = parent.seriesId;
  receiver.plan = &plans[0];
  child.parents = taosArrayInit(1, POINTER_BYTES);
  auto* receiverPtr = &receiver;
  ASSERT_NE(taosArrayPush(child.parents, &receiverPtr), nullptr);
  stub.set(schRecordTaskSucceedNode, +[](SSchJob* job, SSchTask*) -> int32_t {
    ++job->seriesId;  // Retry reset the receiving task after the RPC callback passed its first check.
    return 0;
  });
  EXPECT_EQ(schProcessOnTaskSuccess(&parent, &child), TSDB_CODE_SCH_IGNORE_ERROR);
  EXPECT_EQ(receiver.childReady, 0);
  EXPECT_TRUE(subqueryLaunches.empty());
  taosArrayDestroy(child.parents);
}

TEST_F(SubquerySchedulingTest, consumerChecksEachDependencyAtMostTwice) {
  constexpr int count = 32;
  init(count);
  for (int i = 0; i < count; ++i) depends(-1, i);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  parent.earlyMainTasks = parent.subJobStarted = true;
  SSchTask task = {};
  task.plan = &plans[0];
  task.seriesId = parent.seriesId;
  static const SArray* dependencies;
  static int reads;
  dependencies = parent.pDag->pSubqueryDeps;
  reads = 0;
  stub.set(taosArrayGet, +[](const SArray* array, size_t i) -> void* {
    if (array == dependencies) ++reads;
    return array && i < array->size ? TARRAY_GET_ELEM(array, i) : nullptr;
  });
  stub.set(schLaunchTask, +[](SSchJob*, SSchTask*) -> int32_t {
    subqueryLaunches.push_back(-1);
    return 0;
  });
  ASSERT_EQ(schDelayLaunchTask(&parent, &task), 0);
  for (auto& job : jobs) {
    ASSERT_TRUE(task.waitingSubquery);
    ASSERT_EQ(schProcessOnSubJobSuccess(&job), 0);
    runQueuedWork();
  }
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{-1}));
  EXPECT_LE(reads, 2 * count);
  stub.reset(taosArrayGet);
}

TEST_F(SubquerySchedulingTest, staleOrCancelledDependencyWakeCannotDispatchMainTask) {
  init(2);
  depends(-1, 0);
  depends(-1, 1);
  parent.earlyMainTasks = true;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  SSchTask consumer = {};
  consumer.plan = &plans[0];
  consumer.seriesId = parent.seriesId;
  stub.set(schLaunchTask, +[](SSchJob*, SSchTask*) -> int32_t {
    ADD_FAILURE() << "stale or cancelled wake dispatched a task";
    return 0;
  });
  ASSERT_EQ(schDelayLaunchTask(&parent, &consumer), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  ASSERT_TRUE(consumer.waitingSubquery);
  ++parent.seriesId;
  ++consumer.seriesId;
  ++consumer.launchSequence;
  consumer.waitingSubquery = false;  // The new attempt now waits on a different producer.
  ASSERT_EQ(schDelayLaunchTask(&parent, &consumer), 0);
  runQueuedWork();
  EXPECT_TRUE(consumer.waitingSubquery);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[1]), 0);
  parent.status = JOB_TASK_STATUS_DROP;
  runQueuedWork();
}

TEST_F(SubquerySchedulingTest, dependencyWaitsForItsProducer) {
  init(3);
  depends(2, 0);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1}));
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[1]), 0);
  EXPECT_EQ(subqueryLaunches.size(), 2);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, 2}));
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[2]), 0);
  EXPECT_EQ(subqueryLaunches.back(), -1);
}

TEST_F(SubquerySchedulingTest, completionOnlyVisitsDirectConsumers) {
  init(64);
  depends(63, 61);
  depends(63, 62);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(63, subqueryLaunches.size());

  static int lookups;
  lookups = 0;
  stub.set(taosArrayGetP, +[](const SArray* array, size_t index) -> void* {
    if (array == subqueryParent->subJobs) ++lookups;
    auto* slot = static_cast<void**>(taosArrayGet(array, index));
    return slot ? *slot : nullptr;
  });
  stub.set(taosArrayInit, +[](size_t, size_t) -> SArray* {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  });
  for (int i = 0; i < 61; ++i) ASSERT_EQ(0, schProcessOnSubJobSuccess(&jobs[i]));
  EXPECT_EQ(0, lookups);
  ASSERT_EQ(0, schProcessOnSubJobSuccess(&jobs[61]));
  EXPECT_EQ(63, subqueryLaunches.size());
  stub.reset(taosArrayInit);

  ASSERT_EQ(0, schProcessOnSubJobSuccess(&jobs[62]));
  EXPECT_EQ(63, subqueryLaunches.back());
  ASSERT_EQ(0, schProcessOnSubJobSuccess(&jobs[63]));
  EXPECT_EQ(-1, subqueryLaunches.back());
  EXPECT_EQ(65, subqueryLaunches.size());
  EXPECT_LE(lookups, 4);
}

TEST_F(SubquerySchedulingTest, callbackBeforeLaunchReturnsDoesNotLaunchTwice) {
  init(4);
  depends(2, 0);
  depends(3, 1);
  subqueryInlineSuccess = true;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 2, 1, 3, -1}));
  EXPECT_EQ(parent.subJobDoneNum, 4);
}

TEST_F(SubquerySchedulingTest, rejectsSelfForwardAndDuplicateDependencies) {
  init(2);
  depends(1, 1);
  EXPECT_EQ(schInitJobDependencies(&parent), TSDB_CODE_PLAN_INTERNAL_ERROR);
  taosArrayClear(jobs[1].pDag->pSubqueryDeps);
  depends(0, 1);
  EXPECT_EQ(schInitJobDependencies(&parent), TSDB_CODE_PLAN_INTERNAL_ERROR);
  taosArrayClear(jobs[0].pDag->pSubqueryDeps);
  depends(1, 0);
  depends(1, 0);
  EXPECT_EQ(schInitJobDependencies(&parent), TSDB_CODE_PLAN_INTERNAL_ERROR);
}

TEST_F(SubquerySchedulingTest, legacyPlansKeepSequentialStartup) {
  init(2);
  jobs[1].pDag->subqueryDepsKnown = false;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  EXPECT_FALSE(parent.concurrentSubJobs);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0}));
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1}));
}

TEST_F(SubquerySchedulingTest, staleAndResettingSuccessCannotPublish) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ++jobs[0].seriesId;
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[0]), TSDB_CODE_SCH_IGNORE_ERROR);
  tasks[0].seriesId = jobs[0].seriesId;
  jobs[0].subJobRestarting = true;
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[0]), TSDB_CODE_SCH_IGNORE_ERROR);
  jobs[0].subJobRestarting = false;
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  EXPECT_EQ(parent.subJobDoneNum, 1);
}

TEST_F(SubquerySchedulingTest, failureStopsEveryJobOnceAndSkipsReservedSiblings) {
  init(3);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  subqueryFailOnLaunch = 0;
  EXPECT_EQ(schLaunchJob(&parent), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0}));
  EXPECT_EQ(SCH_GET_JOB_STATUS(&parent), JOB_TASK_STATUS_FAIL);
  EXPECT_EQ(subqueryAsyncWork.size(), 1);
  (void)schHandleJobFailure(&jobs[1], TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(subqueryAsyncWork.size(), 1);
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[2]), TSDB_CODE_SCH_IGNORE_ERROR);
}

TEST_F(SubquerySchedulingTest, readyListFailureDoesNotMarkChildrenStarted) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  stub.set(taosArrayAddBatch, failSubqueryReadyAppend);
  EXPECT_EQ(schLaunchJob(&parent), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_TRUE(subqueryLaunches.empty());
  EXPECT_FALSE(jobs[0].subJobStarted);
  EXPECT_FALSE(jobs[1].subJobStarted);

  stub.reset(taosArrayAddBatch);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1}));
}

TEST_F(SubquerySchedulingTest, readyListFailureDoesNotMarkMainStarted) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);

  stub.set(taosArrayAddBatch, failSubqueryReadyAppend);
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[1]), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_FALSE(parent.subJobStarted);
  EXPECT_EQ(parent.launchedSeriesId, 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1}));

  stub.reset(taosArrayAddBatch);
  ASSERT_EQ(schLaunchReadySubJobs(&parent), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, -1}));
}

TEST_F(SubquerySchedulingTest, sharedQuotaWakesTaskInItsOwnJob) {
  init(2);
  schMgmt.cfg.maxNodeTableNum = 10;
  for (int32_t i = 0; i < 2; ++i) {
    addScan(&jobs[i], &tasks[i], 6);
  }
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schInitJobFlowCtrl(&parent), 0);
  ASSERT_EQ(parent.flowCtrl, jobs[0].flowCtrl);
  ASSERT_EQ(parent.flowCtrl, jobs[1].flowCtrl);
  bool enough = false;
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[0], &tasks[0], &enough), 0);
  ASSERT_TRUE(enough);
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[1], &tasks[1], &enough), 0);
  ASSERT_FALSE(enough);
  EXPECT_TRUE(tasks[1].flowQueued);
  // Replace an old queued attempt, then prove only its current attempt wakes.
  ++tasks[1].launchSequence;
  ASSERT_EQ(schDecTaskFlowQuota(&jobs[1], &tasks[1]), 0);
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[1], &tasks[1], &enough), 0);
  ASSERT_FALSE(enough);
  ASSERT_EQ(schLaunchTasksInFlowCtrlList(&jobs[0], &tasks[0]), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{1}));
  EXPECT_TRUE(tasks[1].flowQuotaHeld);
  EXPECT_FALSE(tasks[1].flowQueued);
  ASSERT_EQ(schDecTaskFlowQuota(&jobs[1], &tasks[1]), 0);
  ASSERT_EQ(schDecTaskFlowQuota(&jobs[1], &tasks[1]), 0);
  plans[0].execNodeStat.tableNum = 20;
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[0], &tasks[0], &enough), 0);
  EXPECT_TRUE(enough);
}

TEST_F(SubquerySchedulingTest, sharedQuotaFollowsSelectedReplicaOnRetry) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  schMgmt.cfg.maxNodeTableNum = 1;
  SQueryNodeAddr* candidates[2] = {};
  for (int32_t i = 0; i < 2; ++i) {
    addScan(&jobs[i], &tasks[i], 1);
    auto& addr = plans[i].execNode;
    addr.epSet.eps[0].port = 6031 + i;  // Different leaders, same follower.
    addEpIntoEpSet(&addr.epSet, "follower", 6040);
    ASSERT_EQ(schSetTaskCandidateAddrs(&jobs[i], &tasks[i]), 0);
    ASSERT_EQ(schGetTaskCurrentNodeAddr(&tasks[i], &jobs[i], &candidates[i]), 0);
    candidates[i]->epSet.inUse = 1;
  }
  ASSERT_EQ(schInitJobFlowCtrl(&parent), 0);
  bool enough = false;
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[0], &tasks[0], &enough), 0);
  ASSERT_TRUE(enough);
  EXPECT_EQ(tasks[0].flowEp.port, 6040);
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[1], &tasks[1], &enough), 0);
  EXPECT_FALSE(enough);

  ASSERT_EQ(schLaunchTasksInFlowCtrlList(&jobs[0], &tasks[0]), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{1}));
  candidates[0]->epSet.inUse = 0;  // Retry on the first vgroup's leader.
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[0], &tasks[0], &enough), 0);
  EXPECT_TRUE(enough);
  EXPECT_EQ(tasks[0].flowEp.port, 6031);
  EXPECT_EQ(tasks[1].flowEp.port, 6040);
}

TEST_F(SubquerySchedulingTest, inlineRuntimeResumesWaitingVgroupAfterCallbackUnlocks) {
  init(2);
  parent.attr.inlineTaskLaunch = true;
  schMgmt.cfg.maxNodeTableNum = 1;
  stub.reset(schAsyncLaunchFlowTask);
  stub.set(schLaunchRemoteTask, +[](SSchJob*, SSchTask* task) -> int32_t {
    subqueryLaunches.push_back(task->plan->execNode.nodeId);
    return 0;
  });
  SSchLevel level = {};
  level.taskNum = 2;
  parent.execTasks = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_ENTRY_LOCK);
  ASSERT_NE(parent.execTasks, nullptr);
  for (int32_t i = 0; i < 2; ++i) {
    ASSERT_EQ(schInitTask(&parent, &tasks[i], &plans[i], &level), 0);
    addScan(&parent, &tasks[i], 1);
    plans[i].execNode.nodeId = i + 1;
  }
  ASSERT_EQ(schChkJobNeedFlowCtrl(&parent, &level), 0);
  ASSERT_EQ(schLaunchTask(&parent, &tasks[0]), 0);
  ASSERT_EQ(schLaunchTask(&parent, &tasks[1]), 0);
  ASSERT_TRUE(tasks[1].flowQueued);
  SCH_LOCK_TASK(&tasks[0]);
  EXPECT_EQ(schLaunchTasksInFlowCtrlList(&parent, &tasks[0]), 0);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{1}));
  schProcessOnCbEnd(&parent, &tasks[0], 0);
  EXPECT_TRUE(subqueryAsyncWork.empty());
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{1, 2}));
  for (const auto& work : subqueryAsyncWork) EXPECT_EQ(work.first(work.second), 0);
  subqueryAsyncWork.clear();
}

TEST_F(SubquerySchedulingTest, cancelledSendDoesNotAllocateRpcHandle) {
  init(1);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  parent.status = JOB_TASK_STATUS_FAIL;
  SSchLevel level = {};
  tasks[0].level = &level;
  stub.set(rpcAllocHandle, +[](int64_t* refId) -> int32_t {
    ADD_FAILURE() << "cancelled query allocated an RPC handle";
    *refId = 42;
    return 0;
  });
  EXPECT_EQ(schBuildAndSendMsg(&jobs[0], &tasks[0], &plans[0].execNode, TDMT_SCH_QUERY, nullptr),
            TSDB_CODE_SCH_IGNORE_ERROR);
  EXPECT_EQ(tasks[0].handle, nullptr);
}

TEST_F(SubquerySchedulingTest, cleanupRetriesFailedDropAndSkipsAcceptedAttempts) {
  init(1);
  auto& task = tasks[0];
  task.execNodes = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  ASSERT_NE(task.execNodes, nullptr);
  SSchNodeInfo node = {};
  node.handle = &task;
  stub.set(schBuildAndSendMsg, +[](SSchJob*, SSchTask*, SQueryNodeAddr*, int32_t type, void* param) -> int32_t {
    EXPECT_EQ(type, TDMT_SCH_DROP_TASK);
    subqueryDrops.push_back(*static_cast<int32_t*>(param));
    return subqueryDrops.size() == 1 ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS;
  });
  int32_t attempt = 1;
  ASSERT_EQ(taosHashPut(task.execNodes, &attempt, sizeof(attempt), &node, sizeof(node)), 0);
  schDropTaskOnExecNode(&jobs[0], &task);  // Failed before sending.
  schDropTaskOnExecNode(&jobs[0], &task);
  schDropTaskOnExecNode(&jobs[0], &task);
  ++attempt;
  ASSERT_EQ(taosHashPut(task.execNodes, &attempt, sizeof(attempt), &node, sizeof(node)), 0);
  schDropTaskOnExecNode(&jobs[0], &task);
  schDropTaskOnExecNode(&jobs[0], &task);
  EXPECT_EQ(subqueryDrops, (std::vector<int32_t>{1, 1, 2}));
}

TEST_F(SubquerySchedulingTest, sparseEndpointsKeepGlobalIndicesAndEncodeEmptySlots) {
  init(4);
  depends(3, 0);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  jobs[0].resultReady = true;
  SArray* endpoints = nullptr;
  ASSERT_EQ(schBuildSubJobEndpoints(&tasks[3], &endpoints, &jobs[3]), 0);
  ASSERT_EQ(taosArrayGetSize(endpoints), 3);
  auto* source = static_cast<SDownstreamSourceNode*>(taosArrayGetP(endpoints, 0));
  EXPECT_EQ(source->taskId, tasks[0].taskId);
  EXPECT_NE(source->fetchMsgType, 0);
  for (int32_t i : {1, 2}) {
    source = static_cast<SDownstreamSourceNode*>(taosArrayGetP(endpoints, i));
    EXPECT_EQ(source->type, QUERY_NODE_DOWNSTREAM_SOURCE);
    EXPECT_EQ(source->fetchMsgType, 0);
    EXPECT_EQ(source->taskId, 0);
  }
  SSubQueryMsg request = {};
  request.sql = const_cast<char*>("select 1");
  request.msg = const_cast<char*>("plan");
  request.msgLen = 4;
  request.subEndPoints = endpoints;
  int32_t size = tSerializeSSubQueryMsg(nullptr, 0, &request);
  ASSERT_GT(size, 0);
  SArray* compact = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(compact, nullptr);
  source = static_cast<SDownstreamSourceNode*>(taosArrayGetP(endpoints, 0));
  ASSERT_NE(taosArrayPush(compact, &source), nullptr);
  SSubQueryMsg compactRequest = request;
  compactRequest.subEndPoints = compact;
  int32_t compactSize = tSerializeSSubQueryMsg(nullptr, 0, &compactRequest);
  EXPECT_GT(size, compactSize);
  RecordProperty("bytes_per_empty_endpoint", (size - compactSize) / 2);
  taosArrayDestroy(compact);
  std::vector<char> buffer(size);
  ASSERT_EQ(tSerializeSSubQueryMsg(buffer.data(), size, &request), size);
  SSubQueryMsg decoded = {};
  ASSERT_EQ(tDeserializeSSubQueryMsg(buffer.data(), size, &decoded), 0);
  ASSERT_EQ(taosArrayGetSize(decoded.subEndPoints), 3);
  source = static_cast<SDownstreamSourceNode*>(taosArrayGetP(decoded.subEndPoints, 1));
  EXPECT_EQ(source->fetchMsgType, 0);
  tFreeSSubQueryMsg(&decoded);
  taosArrayDestroyP(endpoints, reinterpret_cast<FDelete>(nodesDestroyNode));
}

TEST_F(SubquerySchedulingTest, unpublishedChildRetryKeepsSiblingResultsAndRejectsOldCallback) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[1]), 0);
  SDataBuf msg = {};
  SCH_LOCK_TASK(&tasks[0]);
  int32_t code = schHandleJobRetry(&jobs[0], &tasks[0], &msg, TSDB_CODE_RPC_BROKEN_LINK);
  SCH_UNLOCK_TASK(&tasks[0]);
  ASSERT_EQ(code, 0);
  EXPECT_EQ(jobs[0].seriesId, 2);
  EXPECT_EQ(parent.subJobDoneNum, 1);
  EXPECT_TRUE(jobs[1].resultReady);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, 0}));
  SDataBuf old = {};
  old.msgType = TDMT_SCH_QUERY_RSP;
  EXPECT_EQ(schHandleResponseMsg(&jobs[0], &tasks[0], 1, tasks[0].execId, &old, 0),
            TSDB_CODE_SCH_IGNORE_ERROR);
  EXPECT_EQ(parent.subJobDoneNum, 1);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  EXPECT_EQ(parent.subJobDoneNum, 2);
  EXPECT_EQ(subqueryLaunches.back(), -1);
}

// Hold the retry slot until the callback releases the task lock to wait for it.
template <typename FinishRetry>
int32_t completeRetryWhileCallbackWaits(SSchJob& job, SSchTask& task, FinishRetry finishRetry) {
  atomic_store_8(&job.inRetry, 1);
  std::promise<void> entered;
  auto ready = entered.get_future();
  int32_t code = 0;
  std::thread callback([&] {
    SDataBuf msg = {};
    SCH_LOCK_TASK(&task);
    entered.set_value();
    code = schHandleJobRetry(&job, &task, &msg, TSDB_CODE_RPC_BROKEN_LINK);
    schProcessOnCbEnd(&job, &task, code);
  });
  ready.wait();
  SCH_LOCK_TASK(&task);
  finishRetry();
  atomic_store_8(&job.inRetry, 0);
  SCH_UNLOCK_TASK(&task);
  callback.join();
  return code;
}

TEST_F(SubquerySchedulingTest, waitingRetryIgnoresResultPublishedByNewSeries) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  auto& job = jobs[0];
  auto& task = tasks[0];
  int32_t code = completeRetryWhileCallbackWaits(job, task, [&] {
    task.seriesId = ++job.seriesId;
    EXPECT_EQ(0, schProcessOnSubJobSuccess(&job));
  });
  EXPECT_EQ(TSDB_CODE_SCH_IGNORE_ERROR, code);
  EXPECT_EQ(JOB_TASK_STATUS_EXEC, SCH_GET_JOB_STATUS(&parent));
  EXPECT_EQ(2, job.seriesId);
  EXPECT_TRUE(job.resultReady);
  EXPECT_EQ(1, parent.subJobDoneNum);
  EXPECT_TRUE(subqueryAsyncWork.empty());
}

TEST_F(SubquerySchedulingTest, waitingRetryIgnoresReplacedTaskAttemptInSameSeries) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  auto& task = tasks[0];
  int32_t code = completeRetryWhileCallbackWaits(jobs[0], task, [&] { ++task.execId; });
  EXPECT_EQ(TSDB_CODE_SCH_IGNORE_ERROR, code);
  EXPECT_EQ(1, jobs[0].seriesId);
  EXPECT_EQ(0, jobs[0].inRetry);
  EXPECT_EQ((std::vector<int32_t>{0, 1}), subqueryLaunches);
}

TEST_F(SubquerySchedulingTest, mainEndpointsKeepIndicesForUnsortedDependencies) {
  init(64);
  for (int i = 63; i >= 0; i -= 2) {
    depends(-1, i);
    jobs[i].resultReady = true;
  }
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  SArray* endpoints = nullptr;
  ASSERT_EQ(0, schBuildSubJobEndpoints(&tasks[0], &endpoints, &parent));
  ASSERT_EQ(64, taosArrayGetSize(endpoints));
  for (int i = 0; i < 64; ++i) {
    auto* source = static_cast<SDownstreamSourceNode*>(taosArrayGetP(endpoints, i));
    EXPECT_EQ(i % 2 ? tasks[i].taskId : 0, source->taskId);
  }
  taosArrayDestroyP(endpoints, reinterpret_cast<FDelete>(nodesDestroyNode));
}

TEST_F(SubquerySchedulingTest, siblingFailureDropsRelaunchedMergeTask) {
  init(2);
  SMergePhysiNode merge = {};
  merge.node.type = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  plans[0].pNode = &merge.node;
  plans[0].subplanType = SUBPLAN_TYPE_MERGE;
  plans[0].msgType = TDMT_SCH_MERGE_QUERY;
  ASSERT_NO_FATAL_FAILURE(addSingleTaskLevel(jobs[0], plans[0]));
  ASSERT_NO_FATAL_FAILURE(addSingleTaskLevel(jobs[1], plans[1]));
  stub.reset(schLaunchJobImpl);
  stub.reset(schDropJobAllTasks);
  stub.set(schLaunchRemoteTask, +[](SSchJob* job, SSchTask* task) -> int32_t {
    int32_t code = schSetTaskCandidateAddrs(job, task);
    if (code != 0) return code;
    SSchNodeInfo node = {};
    node.addr = task->plan->execNode;
    node.handle = task;
    return taosHashPut(task->execNodes, &task->execId, sizeof(task->execId), &node, sizeof(node));
  });
  stub.set(schBuildAndSendMsg, +[](SSchJob* job, SSchTask*, SQueryNodeAddr*, int32_t type, void* param) -> int32_t {
    EXPECT_EQ(type, TDMT_SCH_DROP_TASK);
    if (job->subJobId == 0) subqueryDrops.push_back(*static_cast<int32_t*>(param));
    return 0;
  });
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  runQueuedWork();
  auto& job = jobs[0];
  auto* task = job.fetchTask;
  ASSERT_EQ(SCH_GET_TASK_STATUS(task), JOB_TASK_STATUS_EXEC);
  ASSERT_EQ(task->execId, 0);

  SDataBuf msg = {};
  SCH_LOCK_TASK(task);
  int32_t code = schHandleJobRetry(&job, task, &msg, TSDB_CODE_RPC_BROKEN_LINK);
  SCH_UNLOCK_TASK(task);
  ASSERT_EQ(code, 0);
  runQueuedWork();
  ASSERT_EQ(job.seriesId, 2);
  ASSERT_EQ(task->execId, 1);
  EXPECT_NE(taosHashGet(job.execTasks, &task->taskId, sizeof(task->taskId)), nullptr);
  ASSERT_EQ(schProcessOnTaskSuccess(&job, task), 0);
  ASSERT_TRUE(job.resultReady);

  (void)schHandleJobFailure(&jobs[1], TSDB_CODE_OUT_OF_MEMORY);
  runQueuedWork();
  EXPECT_EQ(subqueryDrops, (std::vector<int32_t>{0, 1}));
}

TEST_F(SubquerySchedulingTest, mainRetryReusesPublishedChildren) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[1]), 0);
  SSchTask mainTask = {};
  mainTask.seriesId = parent.seriesId;
  SDataBuf msg = {};
  SCH_LOCK_TASK(&mainTask);
  int32_t code = schHandleJobRetry(&parent, &mainTask, &msg, TSDB_CODE_RPC_BROKEN_LINK);
  SCH_UNLOCK_TASK(&mainTask);
  ASSERT_EQ(code, 0);
  EXPECT_EQ(parent.subJobDoneNum, 2);
  EXPECT_EQ(parent.seriesId, 2);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, -1, -1}));
}

TEST_F(SubquerySchedulingTest, delayedChildReadyCheckDoesNotLaunchMainDuringRetry) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[1]), 0);
  ASSERT_EQ(parent.launchedSeriesId, 1);

  // Resume a child callback after the main retry advances its series, but
  // before its tasks have been reset. Only the retry entry may restart it.
  parent.inRetry = 1;
  ++parent.seriesId;
  ASSERT_EQ(schLaunchReadySubJobs(&parent), 0);
  EXPECT_EQ(parent.launchedSeriesId, 1);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, -1}));

  // The retry entry runs after task reset, while inRetry is still set.
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(parent.launchedSeriesId, 2);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1, -1, -1}));
  parent.inRetry = 0;
  ASSERT_EQ(schLaunchReadySubJobs(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  EXPECT_EQ(subqueryLaunches.size(), 4);
  EXPECT_EQ(parent.subJobDoneNum, 2);
  EXPECT_TRUE(jobs[0].resultReady);
  EXPECT_TRUE(jobs[1].resultReady);
}

TEST_F(SubquerySchedulingTest, publishedChildRetryFailsWholeQueryWithoutRepublishing) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schLaunchJob(&parent), 0);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  SDataBuf msg = {};
  SCH_LOCK_TASK(&tasks[0]);
  int32_t code = schHandleJobRetry(&jobs[0], &tasks[0], &msg, TSDB_CODE_RPC_BROKEN_LINK);
  SCH_UNLOCK_TASK(&tasks[0]);
  EXPECT_EQ(code, TSDB_CODE_SCH_IGNORE_ERROR);
  EXPECT_EQ(SCH_GET_JOB_STATUS(&parent), JOB_TASK_STATUS_FAIL);
  EXPECT_EQ(parent.subJobDoneNum, 1);
  EXPECT_EQ(jobs[0].seriesId, 1);
  EXPECT_EQ(subqueryLaunches, (std::vector<int32_t>{0, 1}));
  EXPECT_EQ(subqueryAsyncWork.size(), 1);
  EXPECT_EQ(schProcessOnSubJobSuccess(&jobs[0]), TSDB_CODE_SCH_IGNORE_ERROR);
  EXPECT_EQ(parent.subJobDoneNum, 1);
}

TEST_F(SubquerySchedulingTest, statusIncludesEachJobAndDistinguishesItsWaitReason) {
  init(3);
  depends(2, 0);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  jobs[0].subJobStarted = true;
  jobs[1].subJobStarted = true;
  for (int32_t i = -1; i < 3; ++i) {
    auto* job = i < 0 ? &parent : &jobs[i];
    job->levelNum = 1;
    job->levels = taosArrayInit(1, sizeof(SSchLevel));
    SSchLevel level = {};
    level.taskNum = 1;
    level.subTasks = taosArrayInit(1, sizeof(SSchTask));
    SSchTask task = {};
    task.taskId = i + 100;
    task.plan = &plans[0];
    task.status = i == 0 ? JOB_TASK_STATUS_EXEC : JOB_TASK_STATUS_INIT;
    task.retryTimes = i == 0 ? 1 : 0;
    task.flowQueued = i == 1;
    task.waitRetry = i == 1;
    ASSERT_NE(taosArrayPush(level.subTasks, &task), nullptr);
    ASSERT_NE(taosArrayPush(job->levels, &level), nullptr);
  }
  SArray* status = taosArrayInit(4, sizeof(SQuerySubDesc));
  ASSERT_NE(status, nullptr);
  ASSERT_EQ(schedulerGetTasksStatus(parent.refId, status), 0);
  ASSERT_EQ(taosArrayGetSize(status), 4);
  const char* expected[] = {"main/scan/wait_dep", "j0/scan/executing", "j1/scan/waiting_flow", "j2/scan/wait_dep"};
  for (int32_t i = 0; i < 4; ++i) {
    auto* desc = static_cast<SQuerySubDesc*>(taosArrayGet(status, i));
    EXPECT_STREQ(desc->status, expected[i]);
  }
  taosArrayDestroy(status);
}

TEST_F(SubquerySchedulingTest, heartbeatDropsExhaustedJobsFromRoundRobin) {
  constexpr int shortJobs = 99;
  constexpr int longTasks = 1000;
  init(shortJobs);
  for (int i = -1; i < shortJobs; ++i) {
    SSchJob& job = i < 0 ? parent : jobs[i];
    job.levelNum = 1;
    job.levels = taosArrayInit_s(sizeof(SSchLevel), 1);
    ASSERT_NE(job.levels, nullptr);
    auto* level = static_cast<SSchLevel*>(taosArrayGet(job.levels, 0));
    level->taskNum = i == 0 ? longTasks : 1;
    level->subTasks = taosArrayInit_s(sizeof(SSchTask), level->taskNum);
    ASSERT_NE(level->subTasks, nullptr);
    for (int j = 0; j < level->taskNum; ++j) {
      auto* task = static_cast<SSchTask*>(taosArrayGet(level->subTasks, j));
      task->taskId = (i + 1) * longTasks + j;
      task->status = JOB_TASK_STATUS_INIT;
    }
  }
  SArray* status = taosArrayInit(shortJobs + longTasks, sizeof(SQuerySubDesc));
  ASSERT_NE(status, nullptr);
  // Count array accesses only during the heartbeat, independently of machine speed.
  static size_t reads;
  reads = 0;
  stub.set(taosArrayGet, +[](const SArray* array, size_t index) -> void* {
    ++reads;
    return array && index < array->size ? TARRAY_GET_ELEM(array, index) : nullptr;
  });
  int32_t code = schedulerGetTasksStatus(parent.refId, status);
  stub.reset(taosArrayGet);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(taosArrayGetSize(status), shortJobs + longTasks);
  RecordProperty("heartbeat_array_reads", static_cast<int>(reads));
  EXPECT_LT(reads, 12 * (shortJobs + longTasks));
  for (int i = 0; i <= shortJobs; ++i) {
    EXPECT_EQ(static_cast<SQuerySubDesc*>(taosArrayGet(status, i))->tid, i * longTasks);
  }
  for (int j = 1; j < longTasks; ++j) {
    EXPECT_EQ(static_cast<SQuerySubDesc*>(taosArrayGet(status, shortJobs + j))->tid, longTasks + j);
  }
  taosArrayDestroy(status);
}

TEST_F(SubquerySchedulingTest, heartbeatKeepsParentPhaseAcrossConcurrentDispatches) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  schPublishJobPhase(&parent, QUERY_PHASE_SCHEDULE_PLANNING);
  ASSERT_EQ(schLaunchJob(&parent), 0);

  int32_t phase = 0;
  int64_t start = 0;
  ASSERT_EQ(schedulerGetJobPhase(parent.refId, &phase, &start), 0);
  EXPECT_EQ(phase, QUERY_PHASE_SCHEDULE_PLANNING);

  // Model the phase publications made when each child sends its query RPC.
  schPublishJobPhase(&jobs[0], QUERY_PHASE_EXEC_DATA_QUERY);
  schPromoteParentForSubJobs(&parent);
  ASSERT_EQ(schedulerGetJobPhase(parent.refId, &phase, &start), 0);
  EXPECT_EQ(phase, QUERY_PHASE_EXEC_WAITING_CHILDREN);
  const int64_t waitingSince = start;
  EXPECT_GT(waitingSince, 0);

  schPublishJobPhase(&jobs[1], QUERY_PHASE_EXEC_MERGE_QUERY);
  schPromoteParentForSubJobs(&parent);
  ASSERT_EQ(schProcessOnSubJobSuccess(&jobs[0]), 0);
  ASSERT_EQ(schedulerGetJobPhase(parent.refId, &phase, &start), 0);
  EXPECT_EQ(phase, QUERY_PHASE_EXEC_WAITING_CHILDREN);
  EXPECT_EQ(start, waitingSince);
}

TEST_F(SubquerySchedulingTest, delayedRetryReportsWaitingFlowUntilDispatched) {
  init(2);
  SSubplan mainPlan = {};
  mainPlan.subplanType = SUBPLAN_TYPE_MERGE;
  ASSERT_NO_FATAL_FAILURE(addSingleTaskLevel(parent, mainPlan));
  ASSERT_NO_FATAL_FAILURE(addSingleTaskLevel(jobs[0], plans[0]));
  ASSERT_NO_FATAL_FAILURE(addSingleTaskLevel(jobs[1], plans[1]));
  auto& job = jobs[1];
  auto* task = job.fetchTask;
  addScan(&jobs[0], jobs[0].fetchTask, 1);
  addScan(&job, task, 1);
  schMgmt.cfg.maxNodeTableNum = 1;
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  ASSERT_EQ(schInitJobFlowCtrl(&parent), 0);
  job.subJobStarted = true;
  bool enough = false;
  ASSERT_EQ(schCheckIncTaskFlowQuota(&jobs[0], jobs[0].fetchTask, &enough), 0);
  ASSERT_TRUE(enough);

  // Fire the retry timer explicitly while the sibling owns the target's quota.
  stub.set(taosTmrStart, +[](TAOS_TMR_CALLBACK, int32_t, void* param, void*) -> tmr_h { return param; });
  stub.reset(schAsyncLaunchFlowTask);
  stub.set(schLaunchRemoteTask, +[](SSchJob*, SSchTask*) -> int32_t { return 0; });
  task->delayExecMs = 10;
  task->execId = 1;
  ASSERT_EQ(schDelayLaunchTask(&job, task), 0);
  ASSERT_EQ(SCH_GET_TASK_STATUS(task), JOB_TASK_STATUS_EXEC);
  schHandleTimerEvent(&task->delayLaunchPar, task->delayTimer);
  ASSERT_TRUE(task->flowQueued);
  auto checkStatus = [&](const char* expected) {
    SArray* status = taosArrayInit(3, sizeof(SQuerySubDesc));
    ASSERT_NE(status, nullptr);
    EXPECT_EQ(schedulerGetTasksStatus(parent.refId, status), 0);
    EXPECT_EQ(taosArrayGetSize(status), 3);
    auto* desc = static_cast<SQuerySubDesc*>(taosArrayGet(status, 2));
    if (desc) {
      EXPECT_EQ(desc->tid, task->taskId);
      EXPECT_STREQ(desc->status, expected);
    }
    taosArrayDestroy(status);
  };
  checkStatus("j1/scan/waiting_flow");

  ASSERT_EQ(schLaunchTasksInFlowCtrlList(&jobs[0], jobs[0].fetchTask), 0);
  runQueuedWork();
  EXPECT_FALSE(task->flowQueued);
  EXPECT_EQ(task->execId, 2);
  checkStatus("j1/scan/retry(2)");
}

TEST_F(SubquerySchedulingTest, cleanupRetainsJobBeforePublishingFailure) {
  init(2);
  ASSERT_EQ(schInitJobDependencies(&parent), 0);
  stub.set(schPostJobRes, recordSubqueryCompletion);
  (void)schHandleJobFailure(&jobs[0], TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_TRUE(subqueryCleanupBeforeCompletion);
  EXPECT_EQ(subqueryAsyncWork.size(), 1);
}
}  // namespace
