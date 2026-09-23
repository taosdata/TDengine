/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "mndTrans.h"

uint32_t seed = 0;

static SRpcMsg createRpcMsg(STransAction* pAction, int64_t traceId, int64_t signature) {
  SRpcMsg rpcMsg = {.msgType = pAction->msgType, .contLen = pAction->contLen, .info.ahandle = (void *)signature};
  rpcMsg.pCont = rpcMallocCont(pAction->contLen);
  if (rpcMsg.pCont == NULL) {
    return rpcMsg;
  }

  rpcMsg.info.traceId.rootId = traceId;
  rpcMsg.info.notFreeAhandle = 1;

  memcpy(rpcMsg.pCont, pAction->pCont, pAction->contLen);
  return rpcMsg;
}

void streamTransRandomErrorGen(STransAction *pAction, STrans *pTrans, int64_t signature) {
  if ((pAction->msgType == TDMT_STREAM_TASK_UPDATE_CHKPT && pAction->id > 2) ||
      (pAction->msgType == TDMT_STREAM_CONSEN_CHKPT) ||
      (pAction->msgType == TDMT_VND_STREAM_CHECK_POINT_SOURCE && pAction->id > 2)) {
    if (seed == 0) {
      seed = taosGetTimestampSec();
    }

    uint32_t v = taosRandR(&seed);
    int32_t  choseItem = v % 5;

    if (choseItem == 0) {
      // 1. one of update-checkpoint not send, restart and send it again
      taosMsleep(5000);
      if (pAction->msgType == TDMT_STREAM_TASK_UPDATE_CHKPT) {
        mError(
            "***sleep 5s and core dump, following tasks will not recv update-checkpoint info, so the checkpoint will "
            "rollback***");
        exit(-1);
      } else if (pAction->msgType == TDMT_STREAM_CONSEN_CHKPT) {  // pAction->msgType == TDMT_STREAM_CONSEN_CHKPT
        mError(
            "***sleep 5s and core dump, following tasks will not recv consen-checkpoint info, so the tasks will "
            "not started***");
      } else {  // pAction->msgType == TDMT_VND_STREAM_CHECK_POINT_SOURCE
        mError(
            "***sleep 5s and core dump, following tasks will not recv checkpoint-source info, so the tasks will "
            "started after restart***");
        exit(-1);
      }
    } else if (choseItem == 1) {
      // 2. repeat send update chkpt msg
      mError("***repeat send update-checkpoint/consensus/checkpoint trans msg 3times to vnode***");

      mError("***repeat 1***");
      SRpcMsg rpcMsg1 = createRpcMsg(pAction, pTrans->mTraceId, signature);
      int32_t code = tmsgSendReq(&pAction->epSet, &rpcMsg1);

      mError("***repeat 2***");
      SRpcMsg rpcMsg2 = createRpcMsg(pAction, pTrans->mTraceId, signature);
      code = tmsgSendReq(&pAction->epSet, &rpcMsg2);

      mError("***repeat 3***");
      SRpcMsg rpcMsg3 = createRpcMsg(pAction, pTrans->mTraceId, signature);
      code = tmsgSendReq(&pAction->epSet, &rpcMsg3);
    } else if (choseItem == 2) {
      // 3. sleep 40s and then send msg
      mError("***idle for 30s, and then send msg***");
      taosMsleep(30000);
    } else {
      // do nothing
      //      mInfo("no error triggered");
    }
  }
}
