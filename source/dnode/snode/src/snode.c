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
  streamSetSnodeEnabled(&pSnode->msgCb);
  return 0;
}

void sndClose(SSnode *pSnode) {
  streamSetSnodeDisabled(false);
  taosMemoryFree(pSnode);
}

static int32_t handleTriggerCalcReq(SSnode* pSnode, void* pWorkerCb, SRpcMsg* pRpcMsg) {
  SSTriggerCalcRequest req = {0};
  SStreamRunnerTask* pTask = NULL;
  void* taskAddr = NULL;
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tDeserializeSTriggerCalcRequest(POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead)), pRpcMsg->contLen - sizeof(SMsgHead), &req));
  TAOS_CHECK_EXIT(streamAcquireTask(req.streamId, req.runnerTaskId, (SStreamTask**)&pTask, &taskAddr));

  req.brandNew = true;
  req.execId = -1;
  pTask->msgCb = pSnode->msgCb;
  //pTask->pMsgCb = &pSnode->msgCb;
  pTask->pWorkerCb = pWorkerCb;
  req.curWinIdx = 0;
  TAOS_CHECK_EXIT(stRunnerTaskExecute(pTask, &req));

_exit:

  tDestroySTriggerCalcRequest(&req);
  SRpcMsg rsp = {.code = code, .msgType = TDMT_STREAM_TRIGGER_CALC_RSP, .contLen = 0, .pCont = NULL, .info = pRpcMsg->info};
  if (rpcSendResponse(&rsp) != 0) {
    sndError("failed to send response, msg:%p", &rsp);
  }
  
  streamReleaseTask(taskAddr);

  if(code == TSDB_CODE_MND_STREAM_TABLE_NOT_CREATE) {
    code = 0; // not real error, just notify trigger the table is not created
  }

  if (code) {
    sndError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t handleSyncDeleteCheckPointReq(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  int64_t streamId = *(int64_t*)POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead));
  streamDeleteCheckPoint(streamId);
  return 0;
}

static int32_t handleSyncWriteCheckPointReq(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  int32_t ver = *(int32_t*)POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead) + INT_BYTES);
  int64_t streamId = *(int64_t*)POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead) + 2 * INT_BYTES);
  SRpcMsg rsp = {.code = 0, .msgType = TDMT_STREAM_SYNC_CHECKPOINT_RSP, .info = pRpcMsg->info};

  stDebug("[checkpoint] handleSyncWriteCheckPointReq streamId:%" PRIx64 ",ver:%d", streamId, ver);
  void*   data = NULL;
  int64_t dataLen = 0;
  int32_t code = streamReadCheckPoint(streamId, &data, &dataLen);
  if (code != 0 || (terrno == TAOS_SYSTEM_ERROR(ENOENT) && ver == -1)){
    goto end;
  }
  if (terrno == TAOS_SYSTEM_ERROR(ENOENT) || ver > *(int32_t*)POINTER_SHIFT(data, INT_BYTES)) {
    int32_t ret = streamWriteCheckPoint(streamId, POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead)), pRpcMsg->contLen - sizeof(SMsgHead));
    stDebug("[checkpoint] streamId:%" PRIx64 ", checkpoint local updated, ver:%d, dataLen:%" PRId64 ", ret:%d", streamId, ver, dataLen, ret);
  }
  if (terrno == TAOS_SYSTEM_ERROR(ENOENT) || ver >= *(int32_t*)POINTER_SHIFT(data, INT_BYTES)) {
    stDebug("[checkpoint] streamId:%" PRIx64 ", checkpoint no need send back, ver:%d, dataLen:%" PRId64, streamId, ver, dataLen);
    dataLen = 0;
    taosMemoryFreeClear(data);
  }
  
end:
  if (data == NULL) {
    rsp.contLen = 2 * INT_BYTES + LONG_BYTES;
    rsp.pCont = rpcMallocCont(rsp.contLen);
    if (rsp.pCont == NULL) {
      rsp.code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      *(int32_t*)(POINTER_SHIFT(rsp.pCont, INT_BYTES)) = -1;  // no checkpoint
      *(int64_t*)(POINTER_SHIFT(rsp.pCont, 2 * INT_BYTES)) = streamId;
    }
  } else {
    rsp.pCont = rpcMallocCont(dataLen);
    if (rsp.pCont == NULL) {
      rsp.code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(rsp.pCont, data, dataLen);
      rsp.contLen = dataLen;
      taosMemoryFreeClear(data); 
    } 
  }
  
  if (rpcSendResponse(&rsp) != 0) {
    sndError("failed to send write checkpoint response, msg:%p", &rsp);
  }

  return 0;
}

static int32_t handleSyncWriteCheckPointRsp(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  if (pRpcMsg->code != 0) {
    stError("[checkpoint] handleSyncWriteCheckPointRsp, code:%d, msgType:%d", pRpcMsg->code, pRpcMsg->msgType);
    return pRpcMsg->code;
  } 
  void* data = pRpcMsg->pCont;
  int32_t dataLen = pRpcMsg->contLen;
  
  int32_t ver = *(int32_t*)(POINTER_SHIFT(data, INT_BYTES));
  int64_t streamId = *(int64_t*)(POINTER_SHIFT(data, 2 * INT_BYTES));
  stDebug("[checkpoint] handleSyncWriteCheckPointRsp, ver:%d, streamId:%"PRIx64",dataLen:%d", 
    ver, streamId, dataLen);

  if (ver != -1){
    (void)streamWriteCheckPoint(streamId, data, dataLen);
  }
  return streamCheckpointSetReady(streamId);
}

static int32_t buildStreamFetchRsp(SSDataBlock* pBlock, void** data, size_t* size, int8_t precision, bool finished) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf =  NULL;

  int32_t blockSize = pBlock == NULL ? 0 : blockGetInternalEncodeSize(pBlock);
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
    int32_t actualLen = blockEncodeInternal(pBlock, pRetrieve->data + INT_BYTES * 2, blockSize, taosArrayGetSize(pBlock->pDataBlock));
    if (actualLen < 0) {
      code = terrno;
      goto end;
    }
  }
  if (finished) {
    pRetrieve->completed = 1;
  }

  *data = buf;
  *size = dataEncodeBufSize;
  buf = NULL;

end:
  rpcFreeCont(buf);
  return code;
}

static int32_t handleStreamFetchData(SSnode* pSnode, void *pWorkerCb, SRpcMsg* pRpcMsg) {
  int32_t code = 0, lino = 0;
  void* taskAddr = NULL;
  SResFetchReq req = {0};
  SSTriggerCalcRequest calcReq = {0};
  SStreamRunnerTask* pTask = NULL;
  void* buf = NULL;
  size_t size = 0;

  stDebug("handleStreamFetchData, msgType:%s, contLen:%d 0x%" PRIx64 ":0x%" PRIx64, 
      TMSG_INFO(pRpcMsg->msgType), pRpcMsg->contLen, TRACE_GET_ROOTID(&pRpcMsg->info.traceId), TRACE_GET_MSGID(&pRpcMsg->info.traceId));
  
  TAOS_CHECK_EXIT(tDeserializeSResFetchReq(pRpcMsg->pCont,pRpcMsg->contLen, &req));

  calcReq.streamId = req.queryId;
  calcReq.runnerTaskId = req.taskId;
  calcReq.brandNew = req.reset;
  calcReq.execId = req.execId;
  calcReq.sessionId = req.pStRtFuncInfo->sessionId;
  calcReq.triggerType = req.pStRtFuncInfo->triggerType;
  calcReq.isWindowTrigger = req.pStRtFuncInfo->isWindowTrigger;
  calcReq.precision = req.pStRtFuncInfo->precision;
  TSWAP(calcReq.groupColVals, req.pStRtFuncInfo->pStreamPartColVals);
  TSWAP(calcReq.params, req.pStRtFuncInfo->pStreamPesudoFuncVals);
  calcReq.gid = req.pStRtFuncInfo->groupId;
  calcReq.curWinIdx = req.pStRtFuncInfo->curIdx;
  calcReq.pOutBlock = NULL;

  TAOS_CHECK_EXIT(streamAcquireTask(calcReq.streamId, calcReq.runnerTaskId, (SStreamTask**)&pTask, &taskAddr));

  pTask->msgCb = pSnode->msgCb;
  //pTask->pMsgCb = &pSnode->msgCb;
  pTask->pWorkerCb = pWorkerCb;
  
  TAOS_CHECK_EXIT(stRunnerTaskExecute(pTask, &calcReq));

  TAOS_CHECK_EXIT(buildStreamFetchRsp(calcReq.pOutBlock, &buf, &size, 0, false));

_exit:

  tDestroySTriggerCalcRequest(&calcReq);
  tDestroySResFetchReq(&req);
  SRpcMsg rsp = {.code = code, .msgType = TDMT_STREAM_FETCH_FROM_RUNNER_RSP, .contLen = size, .pCont = buf, .info = pRpcMsg->info};
  tmsgSendRsp(&rsp);
  
  streamReleaseTask(taskAddr);

  if (code) {
    sndError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t handleStreamFetchFromCache(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  int32_t code = 0, lino = 0;
  SResFetchReq req = {0};
  SStreamCacheReadInfo readInfo = {0};
  void* buf = NULL;
  int64_t streamId = 0;
  size_t size = 0;
  TAOS_CHECK_EXIT(tDeserializeSResFetchReq(pRpcMsg->pCont, pRpcMsg->contLen, &req));

  streamId = req.queryId;
  readInfo.taskInfo.streamId = req.queryId;
  readInfo.taskInfo.taskId = req.taskId;
  readInfo.taskInfo.sessionId = req.pStRtFuncInfo->sessionId;
  readInfo.gid = req.pStRtFuncInfo->groupId;
  //SSTriggerCalcParam* pParam = taosArrayGet(req.pStRtFuncInfo->pStreamPesudoFuncVals, req.pStRtFuncInfo->curIdx);
  readInfo.start = req.pStRtFuncInfo->curWindow.skey;
  readInfo.end = req.pStRtFuncInfo->curWindow.ekey;
  bool finished;
  TAOS_CHECK_EXIT(stRunnerFetchDataFromCache(&readInfo,&finished));

  TAOS_CHECK_EXIT(buildStreamFetchRsp(readInfo.pBlock, &buf, &size, 0, finished));

_exit:

  printDataBlock(readInfo.pBlock, __func__, "fetchFromCache", streamId);

  stsDebug("task %" PRIx64 " TDMT_STREAM_FETCH_FROM_CACHE_RSP with code:%d rows:%" PRId64 ", size:%d, time range:[%" PRId64 ", %" PRId64 "]", 
      req.taskId, code, readInfo.pBlock ? readInfo.pBlock->info.rows : 0, (int32_t)size, readInfo.start, readInfo.end);  
      
  SRpcMsg rsp = {.code = code, .msgType = TDMT_STREAM_FETCH_FROM_CACHE_RSP, .contLen = size, .pCont = buf, .info = pRpcMsg->info};
  tmsgSendRsp(&rsp);

  if (code) {
    sndError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  blockDataDestroy(readInfo.pBlock);
  tDestroySResFetchReq(&req);
  
  return code;
}

static void sndSendErrorRrsp(SRpcMsg *pMsg, int32_t errCode) {
  SRpcMsg             rspMsg = {0};

  rspMsg.info = pMsg->info;
  rspMsg.pCont = NULL;
  rspMsg.contLen = 0;
  rspMsg.code = errCode;
  rspMsg.msgType = pMsg->msgType;

  tmsgSendRsp(&rspMsg);
}

static int32_t handleStreamDropTableReq(SSnode* pSnode, SRpcMsg* pRpcMsg) {
  SSTriggerDropRequest req = {0};
  SStreamRunnerTask* pTask = NULL;
  void* taskAddr = NULL;
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(tDeserializeSTriggerDropTableRequest(POINTER_SHIFT(pRpcMsg->pCont, sizeof(SMsgHead)), pRpcMsg->contLen - sizeof(SMsgHead), &req));
  TAOS_CHECK_EXIT(streamAcquireTask(req.streamId, req.runnerTaskId, (SStreamTask**)&pTask, &taskAddr));
  
  pTask->msgCb = pSnode->msgCb;
  TAOS_CHECK_EXIT(stRunnerTaskDropTable(pTask, &req));

_exit:
  tDestroySSTriggerDropRequest(&req);
  if (code) {
    sndError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
    sndSendErrorRrsp(pRpcMsg, code);
  } else {
    SRpcMsg rsp = {.code = 0, .msgType = TDMT_STREAM_TRIGGER_DROP_RSP, .contLen = 0, .pCont = NULL, .info = pRpcMsg->info};
    tmsgSendRsp(&rsp);
  }
  streamReleaseTask(taskAddr);

  return code;
}


int32_t sndProcessStreamMsg(SSnode *pSnode, void *pWorkerCb, SRpcMsg *pMsg) {
  int32_t code = 0, lino = 0;
  switch (pMsg->msgType) {
    case TDMT_STREAM_TRIGGER_CALC:
      TAOS_CHECK_EXIT(handleTriggerCalcReq(pSnode, pWorkerCb, pMsg));
      break;
    case TDMT_STREAM_DELETE_CHECKPOINT:
      TAOS_CHECK_EXIT(handleSyncDeleteCheckPointReq(pSnode, pMsg));
      break;
    case TDMT_STREAM_SYNC_CHECKPOINT:
      TAOS_CHECK_EXIT(handleSyncWriteCheckPointReq(pSnode, pMsg));
      break;
    case TDMT_STREAM_SYNC_CHECKPOINT_RSP:
      TAOS_CHECK_EXIT(handleSyncWriteCheckPointRsp(pSnode, pMsg));
      break;
    case TDMT_STREAM_FETCH_FROM_RUNNER:
      TAOS_CHECK_EXIT(handleStreamFetchData(pSnode, pWorkerCb, pMsg));
      break;
    case TDMT_STREAM_FETCH_FROM_CACHE:
      TAOS_CHECK_EXIT(handleStreamFetchFromCache(pSnode, pMsg));
      break;
      case TDMT_STREAM_TRIGGER_DROP:
     TAOS_CHECK_EXIT(handleStreamDropTableReq(pSnode, pMsg));
      break;
    default:
      sndError("invalid snode msg:%d", pMsg->msgType);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

_exit:

  if (code) {
    sndError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}
