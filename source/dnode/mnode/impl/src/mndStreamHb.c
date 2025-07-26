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

#include "mndStream.h"
#include "mndTrans.h"
#include "mndMnode.h"
#include "tmisce.h"


void mndStreamHbSendRsp(SRpcHandleInfo *pRpcInfo, SRpcMsg* pRsp) {
  tmsgSendRsp(pRsp);
  pRpcInfo->handle = NULL;  // disable auto rsp
}

int32_t mndProcessStreamHb(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SStreamHbMsg req = {0};
  SMStreamHbRspMsg rsp = {0};
  int32_t      code = 0;
  int32_t      lino = 0;
  SDecoder     decoder = {0};
  char*        msg = POINTER_SHIFT(pReq->pCont, sizeof(SStreamMsgGrpHeader));
  int32_t      len = pReq->contLen - sizeof(SStreamMsgGrpHeader);
  int64_t      currTs = taosGetTimestampMs();
  SRpcMsg      rspMsg = {0};

  mstDebug("start to process stream hb req msg");

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    TAOS_CHECK_EXIT(msmHandleGrantExpired(pMnode, code));
  }

  tDecoderInit(&decoder, msg, len);
  code = tDecodeStreamHbMsg(&decoder, &req);
  if (code < 0) {
    mstError("failed to decode stream hb msg, error:%s", tstrerror(terrno));
    tCleanupStreamHbMsg(&req, true);
    tDecoderClear(&decoder);
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  tDecoderClear(&decoder);

  mstDebug("start to process grp %d stream-hb from dnode:%d, snodeId:%d, vgLeaders:%d, streamStatus:%d", 
      req.streamGId, req.dnodeId, req.snodeId, (int32_t)taosArrayGetSize(req.pVgLeaders), (int32_t)taosArrayGetSize(req.pStreamStatus));

  rsp.streamGId = req.streamGId;

  (void)msmHandleStreamHbMsg(pMnode, currTs, &req, pReq, &rspMsg);

_exit:

  if (code) {
    msmEncodeStreamHbRsp(code, &pReq->info, &rsp, &rspMsg);
  }
  
  mndStreamHbSendRsp(&pReq->info, &rspMsg);
  tCleanupStreamHbMsg(&req, true);
  tFreeSMStreamHbRspMsg(&rsp);

  mstDebug("end to process stream hb req msg, code:%d", code);

  return code;
}
