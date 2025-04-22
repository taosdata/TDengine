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
#include "streamInt.h"
#include "tmisce.h"
#include "tref.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

static int32_t streamHbSendRequestMsg(SStreamHbMsg* pMsg, SEpSet* pEpset) {
  int32_t code = 0;
  int32_t tlen = 0;

  tEncodeSize(tEncodeStreamHbMsg, pMsg, tlen, code);
  if (code < 0) {
    stError("vgId:%d encode stream hb msg failed, code:%s", tstrerror(code));
    return TSDB_CODE_FAILED;
  }

  void* buf = rpcMallocCont(tlen + sizeof(SStreamMsgGrpHeader));
  if (buf == NULL) {
    stError("vgId:%d encode stream hb msg failed, code:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_FAILED;
  }

  ((SStreamMsgGrpHeader *)buf)->streamGid = htonl(pMsg->streamGId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SStreamMsgGrpHeader));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamHbMsg(&encoder, pMsg)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("vgId:%d encode stream hb msg failed, code:%s", tstrerror(code));
    return TSDB_CODE_FAILED;
  }
  tEncoderClear(&encoder);

  stDebug("vgId:%d send hb to mnode, numOfTasks:%d msgId:%d", pMsg->numOfTasks, pMsg->msgId);

  SRpcMsg msg = {.msgType = TDMT_MND_STREAM_HEARTBEAT, .pCont = buf, .contLen = tlen};
  
  return tmsgSendReq(pEpset, &msg);
}

int32_t streamHbBuildRequestMsg(SStreamHbMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  pMsg.dnodeId = gStreamMgmt.dnodeId;
  pMsg.snodeId = gStreamMgmt.snodeId;
  pMsg.streamGId = stmAddFetchStreamGid();

  taosRLockLatch(&gStreamMgmt.vgLeadersLock);
  pMsg.pVgLeaders = taosArrayDup(gStreamMgmt.vgLeaders, NULL);
  taosRUnLockLatch(&gStreamMgmt.vgLeadersLock);
  
  TAOS_CHECK_EXIT(stmBuildStreamsStatus(&pMsg->pStreamStatus, pMsg->streamGId));

_exit:

  return code;
}

void streamHbStart(void* param, void* tmrId) {
  int32_t code = 0;
  SStreamHbMsg reqMsg = {0};
  
  TAOS_CHECK_EXIT(streamHbBuildRequestMsg(&reqMsg));

  TAOS_CHECK_EXIT(streamHbSendRequestMsg(reqMsg));

_exit:

  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &gStreamMgmt.hb.hbTmr, 0, "stream-hb");
  
  if (code) {
    stError("stream hb start failed, error:%s", tstrerror(code));
  }
}

int32_t streamHbInit(int64_t* pRid, SStreamHbInfo* pHb) {
  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &pHb->hbTmr, 0, "stream-hb");
  
  return TSDB_CODE_SUCCESS;
}

int32_t streamHbHandleRspErr(int32_t errCode, int64_t currTs) {
  if (gStreamMgmt.hb.lastErrCode) {
    int64_t errTime = currTs - gStreamMgmt.hb.lastErrTs;
    if (errTime >= STREAM_HB_ERR_HANDLE_MAX_DELAY) {
      stError("stream hb error(current:%s, last:%s) last for %" PRId64 "ms, try to undeploy all tasks", 
          tstrerror(errCode), tstrerror(gStreamMgmt.hb.lastErrCode), errTime);
          
      smUndeployAllTasks();
    }
    
    stError("stream hb got error:%s, lastError:%s, lastErrTs:%" PRId64, tstrerror(errCode), 
        tstrerror(gStreamMgmt.hb.lastErrCode), gStreamMgmt.hb.lastErrTs);
        
    gStreamMgmt.hb.lastErrCode = errCode;

    return TSDB_CODE_SUCCESS;
  }
  
  gStreamMgmt.hb.lastErrCode = errCode;
  stError("stream hb got error:%s, currTs:%" PRId64, tstrerror(errCode), currTs);

  return TSDB_CODE_SUCCESS;
}

int32_t streamHbProcessRspMsg(SMStreamHbRspMsg* pRsp) {
  int32_t      code = 0;

  if (pRsp->deploy.streamList) {
    TAOS_CHECK_EXIT(smDeployTasks(&pRsp->deploy));
  }

  if (pRsp->start.taskList) {
    TAOS_CHECK_EXIT(smStartTasks(&pRsp->start));
  }

  if (pRsp->undeploy.taskList) {
    TAOS_CHECK_EXIT(smUndeployTasks(&pRsp->undeploy));
  }

_exit:

  stDebug("vgId:%d process hbMsg rsp, msgId:%d rsp confirmed", pRsp->msgId);

  return code;
}
