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
#include "stream.h"
#include "ttimer.h"
#include "wal.h"

static int32_t streamHbSendRequestMsg(SStreamHbMsg* pMsg, SEpSet* pEpset) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t tlen = 0;
  SEncoder encoder;

  tEncodeSize(tEncodeStreamHbMsg, pMsg, tlen, code);
  TAOS_CHECK_EXIT(code);

  void* buf = rpcMallocCont(tlen + sizeof(SStreamMsgGrpHeader));
  TSDB_CHECK_NULL(buf, code, lino, _exit, terrno);

  ((SStreamMsgGrpHeader *)buf)->streamGid = htonl(pMsg->streamGId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SStreamMsgGrpHeader));

  tEncoderInit(&encoder, abuf, tlen);
  TAOS_CHECK_EXIT(tEncodeStreamHbMsg(&encoder, pMsg));
  tEncoderClear(&encoder);

  stDebug("try to send stream hb to mnode, gid:%d, snodeId:%d", pMsg->streamGId, pMsg->snodeId);

  SRpcMsg msg = {.msgType = TDMT_MND_STREAM_HEARTBEAT, .pCont = buf, .contLen = tlen};
  
  TAOS_CHECK_EXIT(tmsgSendReq(pEpset, &msg));

  return code;

_exit:

  rpcFreeCont(buf);
  tEncoderClear(&encoder);

  stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

int32_t streamHbBuildRequestMsg(SStreamHbMsg* pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  pMsg->dnodeId = gStreamMgmt.dnodeId;
  pMsg->snodeId = gStreamMgmt.snodeId;
  pMsg->streamGId = stmAddFetchStreamGid();

  taosRLockLatch(&gStreamMgmt.vgLeadersLock);
  pMsg->pVgLeaders = taosArrayDup(gStreamMgmt.vgLeaders, NULL);
  taosRUnLockLatch(&gStreamMgmt.vgLeadersLock);
  
  TAOS_CHECK_EXIT(stmBuildStreamsStatus(&pMsg->pStreamStatus, pMsg->streamGId));

  return code;
  
_exit:

  stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

void streamHbStart(void* param, void* tmrId) {
  int32_t code = 0;
  int32_t lino = 0;
  SStreamHbMsg reqMsg = {0};
  SEpSet epSet = {0};
  
  TAOS_CHECK_EXIT(streamHbBuildRequestMsg(&reqMsg));

  (*gStreamMgmt.cb)(gStreamMgmt.dnode, &epSet);
  TAOS_CHECK_EXIT(streamHbSendRequestMsg(&reqMsg, &epSet));

_exit:

  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &gStreamMgmt.hb.hbTmr, "stream-hb");
  
  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}

int32_t streamHbInit(SStreamHbInfo* pHb) {
  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &pHb->hbTmr, "stream-hb");
  
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
  int32_t      lino = 0;

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

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}
