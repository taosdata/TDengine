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

#include "executor.h"
#include "sndInt.h"
#include "tuuid.h"
#include "stream.h"
#include "streamRunner.h"

// clang-format off
#define sndError(...) do {  if (sndDebugFlag & DEBUG_ERROR) { taosPrintLog("SND ERROR ", DEBUG_ERROR, sndDebugFlag, __VA_ARGS__);}} while (0)
#define sndInfo(...)  do {  if (sndDebugFlag & DEBUG_INFO)  { taosPrintLog("SND INFO  ", DEBUG_INFO,  sndDebugFlag, __VA_ARGS__);}} while (0)
#define sndDebug(...) do {  if (sndDebugFlag & DEBUG_DEBUG) { taosPrintLog("SND DEBUG ", DEBUG_DEBUG, sndDebugFlag, __VA_ARGS__);}} while (0)

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  int32_t code = 0;
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    return NULL;
  }

  pSnode->msgCb = pOption->msgCb;

  return pSnode;
}

int32_t sndInit(SSnode *pSnode) {
  streamSetSnodeEnabled();
  return 0;
}

void sndClose(SSnode *pSnode) {
  streamSetSnodeDisabled();
  taosMemoryFree(pSnode);
}

static int32_t handleTriggerCalcReq(const char* pMsg, int32_t msgLen) {
  SSTriggerCalcRequest req = {0};
  SStreamRunnerTask* pTask = NULL;
  int32_t code = tDeserializeSTriggerCalcRequest((void*)pMsg, msgLen, &req);
  if (code == 0) {
    //code = streamGetTask(req.streamId, req.runnerTaskId, &pTask);
  }
  if (code == 0) {
    req.brandNew = true;
    req.execId = -1;
    code = stRunnerTaskExecute(pTask, &req);
  }
  return code;
}

static int32_t handleStreamFetchData(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  int32_t code = 0;
  SResFetchReq req = {0};
  SSTriggerCalcRequest calcReq = {0};
  SStreamRunnerTask* pTask = NULL;
  code = tDeserializeSResFetchReq(pRpcMsg->pCont, pRpcMsg->contLen, &req);
  if (code == 0) {
    //code =  make one strigger calc req
    calcReq.streamId = req.queryId;
    calcReq.runnerTaskId = req.taskId;
    calcReq.brandNew = req.reset;
    calcReq.execId = req.execId;
    TSWAP(calcReq.groupColVals, req.pStRtFuncInfo->pStreamPartColVals);
    TSWAP(calcReq.params, req.pStRtFuncInfo->pStreamPesudoFuncVals);
    calcReq.gid = req.pStRtFuncInfo->groupId;
  }
  if (code == 0) {
    // code = streamGetTask(calcReq.streamId, calcReq.runnerTaskId, &pTask);
  }
  if (code == 0) {
    code = stRunnerTaskExecute(pTask, &calcReq);
  }
  return code;
}

int32_t sndProcessStreamMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  switch (pMsg->msgType) {
    case TDMT_STREAM_TRIGGER_CALC:
      code = handleTriggerCalcReq(pMsg->pCont, pMsg->contLen);
      break;
    case TDMT_STREAM_FETCH:
      code = handleStreamFetchData(pSnode, pMsg);
      break;
    default:
      sndError("invalid snode msg:%d", pMsg->msgType);
      return TSDB_CODE_INVALID_MSG;
  }
  return code;
}
