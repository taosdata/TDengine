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


void mndStreamHbSendRsp(int32_t code, SRpcHandleInfo *pRpcInfo, SMStreamHbRspMsg* pRsp) {
  int32_t ret = 0;
  int32_t tlen = 0;
  void   *buf = NULL;

  tEncodeSize(tEncodeStreamHbRsp, pRsp, tlen, ret);
  if (ret < 0) {
    stError("encode stream hb msg rsp failed, code:%s", tstrerror(code));
  }

  buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    stError("encode stream hb msg rsp failed, code:%s", tstrerror(terrno));
    return;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeStreamHbRsp(&encoder, pRsp)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    stError("encode stream hb msg rsp failed, code:%s", tstrerror(code));
    return;
  }
  tEncoderClear(&encoder);

  SRpcMsg rsp = {.code = code, .info = *pRpcInfo, .contLen = tlen, .pCont = buf};

  tmsgSendRsp(&rsp);
  pRpcInfo->handle = NULL;  // disable auto rsp
}

int32_t mndProcessStreamHb(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SStreamHbMsg req = {0};
  SMStreamHbRspMsg rsp = {0};
  int32_t      code = 0;
  SDecoder     decoder = {0};

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    if (msmHandleGrantExpired(pMnode) < 0) {
      return code;
    }
  }

  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamHbMsg(&decoder, &req) < 0) {
    tCleanupStreamHbMsg(&req);
    tDecoderClear(&decoder);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }
  tDecoderClear(&decoder);

  stDebug("receive stream-hb grp %d from dnode:%d, snodeId:%d, vgLeaders:%d, streamStatus:%d", 
      req.streamGId, req.dnodeId, req.snodeId, taosArrayGetSize(req.pVgLeaders), taosArrayGetSize(req.pStreamStatus));


  int64_t currTs = taosGetTimestampMs();
  
  msmHandleStreamHbMsg(pMnode, currTs, &req, &rsp);

  mndStreamHbSendRsp(TSDB_CODE_SUCCESS, &pReq->info, &rsp);

  msmCleanStreamGrpCtx(&req);

  return code;
}
