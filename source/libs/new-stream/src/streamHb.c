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

  ((SStreamMsgGrpHeader *)buf)->streamGid = pMsg->streamGId;
  void *abuf = POINTER_SHIFT(buf, sizeof(SStreamMsgGrpHeader));

  tEncoderInit(&encoder, abuf, tlen);
  TAOS_CHECK_EXIT(tEncodeStreamHbMsg(&encoder, pMsg));
  tEncoderClear(&encoder);

  stDebug("try to send stream hb to mnode, gid:%d, snodeId:%d, vgLeaders:%d, streamStatus:%d", 
      pMsg->streamGId, pMsg->snodeId, (int32_t)taosArrayGetSize(pMsg->pVgLeaders), (int32_t)taosArrayGetSize(pMsg->pStreamStatus));

  SRpcMsg msg = {.msgType = TDMT_MND_STREAM_HEARTBEAT, .pCont = buf, .contLen = tlen + sizeof(SStreamMsgGrpHeader)};

  buf = NULL;
  
  TAOS_CHECK_EXIT(tmsgSendReq(pEpset, &msg));

  stTrace("stream hb sent");

  return code;

_exit:

  rpcFreeCont(buf);
  tEncoderClear(&encoder);

  stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

int32_t streamHbBuildRequestMsg(SStreamHbMsg* pMsg, bool* skipHb) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  pMsg->dnodeId = (*gStreamMgmt.getDnode)(gStreamMgmt.dnode);
  pMsg->snodeId = gStreamMgmt.snodeEnabled ? pMsg->dnodeId : 0;
  pMsg->runnerThreadNum = tsNumOfStreamRunnerThreads;
  pMsg->streamGId = stmAddFetchStreamGid();

  TAOS_CHECK_EXIT(stmBuildHbStreamsStatusReq(pMsg));

  if (NULL == pMsg->pStreamStatus) {
    if (0 != pMsg->streamGId || gStreamMgmt.hbReported) {
      *skipHb = true;
      
      stTrace("no stream in streamGid %d, skip hb", pMsg->streamGId);
      if (0 == pMsg->streamGId) {
        gStreamMgmt.hbReported = false;
      }
      
      return code;
    }
  } else {
    gStreamMgmt.hbReported = true;
  }

  taosRLockLatch(&gStreamMgmt.vgLeadersLock);
  pMsg->pVgLeaders = taosArrayDup(gStreamMgmt.vgLeaders, NULL);
  taosRUnLockLatch(&gStreamMgmt.vgLeadersLock);
  
  return code;
  
_exit:

  stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

void streamHbStart(void* param, void* tmrId) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    skipHb = false;
  SStreamHbMsg reqMsg = {0};
  SEpSet epSet = {0};

  stTrace("stream hb begin");
  
  TAOS_CHECK_EXIT(streamHbBuildRequestMsg(&reqMsg, &skipHb));
  if (skipHb) {
    stTrace("stream hb skipped");
    goto _exit;
  }
  
  (*gStreamMgmt.getMnode)(gStreamMgmt.dnode, &epSet);
  TAOS_CHECK_EXIT(streamHbSendRequestMsg(&reqMsg, &epSet));

_exit:

  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &gStreamMgmt.hb.hbTmr, "stream-hb");

  tCleanupStreamHbMsg(&reqMsg, false);

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stTrace("stream hb end");
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
  gStreamMgmt.hb.lastErrTs = currTs;
  stError("stream hb got error:%s, currTs:%" PRId64, tstrerror(errCode), currTs);

  return TSDB_CODE_SUCCESS;
}

int32_t streamHbProcessRspMsg(SMStreamHbRspMsg* pRsp) {
  int32_t      code = 0;
  int32_t      lino = 0;

  stDebug("start to process stream hb rsp msg, gid:%d", pRsp->streamGId);

  gStreamMgmt.hb.lastErrCode = 0;

  if (pRsp->undeploy.undeployAll) {
    //STREAMTODO
  } else if (pRsp->undeploy.taskList) {
    TAOS_CHECK_EXIT(smUndeployTasks(&pRsp->undeploy));
  }

  if (pRsp->deploy.streamList) {
    TAOS_CHECK_EXIT(smDeployStreams(&pRsp->deploy));
  }

  if (pRsp->rsps.rspList) {
    TAOS_CHECK_EXIT(smHandleMgmtRsp(&pRsp->rsps));
  }

  if (pRsp->start.taskList) {
    TAOS_CHECK_EXIT(smStartTasks(&pRsp->start));
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stDebug("end to process stream hb rsp msg");
  }

  tDeepFreeSMStreamHbRspMsg(pRsp);
  
  return code;
}
