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
#include "tdatablock.h"
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

static int32_t handleTriggerCalcReq(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  SSTriggerCalcRequest req = {0};
  SStreamRunnerTask* pTask = NULL;
  int32_t code = tDeserializeSTriggerCalcRequest(POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead)), pRpcMsg->contLen - sizeof(SMsgHead), &req);
  if (code == 0) {
    code = streamGetTask(req.streamId, req.runnerTaskId, (SStreamTask**)&pTask);
  }
  if (code == 0) {
    req.brandNew = true;
    req.execId = -1;
    pTask->pMsgCb = &pSnode->msgCb;
    req.curWinIdx = 0;
    code = stRunnerTaskExecute(pTask, &req);
  }
  tDestroySTriggerCalcRequest(&req);
  SRpcMsg rsp = {.code = code, .msgType = TDMT_STREAM_TRIGGER_CALC_RSP, .contLen = 0, .pCont = NULL, .info = pRpcMsg->info};
  rpcSendResponse(&rsp);
  return code;
}

static int32_t buildFetchRsp(SSDataBlock* pBlock, void** data, size_t* size, int8_t precision) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf =  NULL;

  int32_t blockSize = pBlock == NULL ? 0 : blockGetEncodeSize(pBlock);
  size_t dataEncodeBufSize = sizeof(SRetrieveTableRsp) + INT_BYTES * 2 + blockSize;
  buf = rpcMallocCont(dataEncodeBufSize);
  if (!buf) {
    code = terrno;
    goto end;
  }

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->version = 0;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  *((int32_t*)(pRetrieve->data)) = blockSize;
  *((int32_t*)(pRetrieve->data + INT_BYTES)) = blockSize;
  if (pBlock == NULL || pBlock->info.rows == 0) {
    pRetrieve->numOfRows = 0;
    pRetrieve->numOfBlocks = 0;
    pRetrieve->completed = 1;
  } else {
    pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);
    pRetrieve->numOfBlocks = htonl(1);
    int32_t actualLen = blockEncode(pBlock, pRetrieve->data + INT_BYTES * 2, blockSize, taosArrayGetSize(pBlock->pDataBlock));
    if (actualLen < 0) {
      code = terrno;
      goto end;
    }
  }

  *data = buf;
  *size = dataEncodeBufSize;
  buf = NULL;

end:
  rpcFreeCont(buf);
  return code;
}

static int32_t handleStreamFetchData(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  int32_t code = 0;
  SResFetchReq req = {0};
  SSTriggerCalcRequest calcReq = {0};
  SStreamRunnerTask* pTask = NULL;
  code = tDeserializeSResFetchReq(pRpcMsg->pCont,pRpcMsg->contLen, &req);
  if (code == 0) {
    //code =  make one strigger calc req
    calcReq.streamId = req.queryId;
    calcReq.runnerTaskId = req.taskId;
    calcReq.brandNew = req.reset;
    calcReq.execId = req.execId;
    TSWAP(calcReq.groupColVals, req.pStRtFuncInfo->pStreamPartColVals);
    TSWAP(calcReq.params, req.pStRtFuncInfo->pStreamPesudoFuncVals);
    calcReq.gid = req.pStRtFuncInfo->groupId;
    calcReq.curWinIdx = req.pStRtFuncInfo->curIdx;
    calcReq.pOutBlock = NULL;
  }
  if (code == 0) {
    code = streamGetTask(calcReq.streamId, calcReq.runnerTaskId, (SStreamTask**)&pTask);
  }
  if (code == 0) {
    pTask->pMsgCb = &pSnode->msgCb;
    code = stRunnerTaskExecute(pTask, &calcReq);
  }
  void* buf = NULL;
  size_t size = 0;
  if (code == 0) {
    code = buildFetchRsp(calcReq.pOutBlock, &buf, &size, 0);
  }
  tDestroySTriggerCalcRequest(&calcReq);
  tDestroySResFetchReq(&req);
  SRpcMsg rsp = {.code = code, .msgType = TDMT_STREAM_FETCH_FROM_RUNNER_RSP, .contLen = size, .pCont = buf, .info = pRpcMsg->info};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t handleStreamFetchFromCache(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  int32_t code = 0;
  SResFetchReq req = {0};
  SStreamCacheReadInfo readInfo = {0};
  code = tDeserializeSResFetchReq(pRpcMsg->pCont, pRpcMsg->contLen, &req);
  if (code == 0) {
    readInfo.taskInfo.streamId = req.queryId;
    readInfo.taskInfo.taskId = req.taskId;
    readInfo.taskInfo.sessionId = req.pStRtFuncInfo->sessionId;
    readInfo.gid = req.pStRtFuncInfo->groupId;
    SSTriggerCalcParam* pParam = taosArrayGet(req.pStRtFuncInfo->pStreamPesudoFuncVals, req.pStRtFuncInfo->curIdx);
    readInfo.start = pParam->wstart;
    readInfo.end = pParam->wend;
    code = stRunnerFetchDataFromCache(&readInfo);
  }
  void* buf = NULL;
  size_t size = 0;
  if (code == 0) {
    code = buildFetchRsp(readInfo.pBlock, &buf, &size, 0);
  }
  blockDataDestroy(readInfo.pBlock);
  tDestroySResFetchReq(&req);
  SRpcMsg rsp = {.code = code, .msgType = TDMT_STREAM_FETCH_FROM_CACHE_RSP, .contLen = size, .pCont = buf, .info = pRpcMsg->info};
  tmsgSendRsp(&rsp);
  return code;
}

int32_t sndProcessStreamMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  switch (pMsg->msgType) {
    case TDMT_STREAM_TRIGGER_CALC:
      code = handleTriggerCalcReq(pSnode, pMsg);
      break;
    case TDMT_STREAM_SYNC_CHECKPOINT:
      code = handleTriggerCalcReq(pSnode, pMsg);
      break;
    case TDMT_STREAM_FETCH_FROM_RUNNER:
      code = handleStreamFetchData(pSnode, pMsg);
      break;
    case TDMT_STREAM_FETCH_FROM_CACHE:
      code = handleStreamFetchFromCache(pSnode, pMsg);
      break;
    default:
      sndError("invalid snode msg:%d", pMsg->msgType);
      return TSDB_CODE_INVALID_MSG;
  }
  return code;
}
