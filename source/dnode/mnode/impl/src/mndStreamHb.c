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

  buf = rpcMallocCont(tlen + sizeof(SStreamMsgGrpHeader));
  if (buf == NULL) {
    stError("encode stream hb msg rsp failed, code:%s", tstrerror(terrno));
    return;
  }

  ((SStreamMsgGrpHeader *)buf)->streamGid = htonl(pRsp->streamGId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SStreamMsgGrpHeader));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
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
  char*        msg = POINTER_SHIFT(pReq->pCont, sizeof(SStreamMsgGrpHeader));
  int32_t      len = pReq->contLen - sizeof(SStreamMsgGrpHeader);
  int64_t      currTs = taosGetTimestampMs();

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    TAOS_CHECK_EXIT(msmHandleGrantExpired(pMnode));
  }

  tDecoderInit(&decoder, msg, len);
  code = tDecodeStreamHbMsg(&decoder, &req);
  if (code < 0) {
    stError("failed to decode stream hb msg, error:%s", tstrerror(terrno));
    tCleanupStreamHbMsg(&req);
    tDecoderClear(&decoder);
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  tDecoderClear(&decoder);

  stDebug("receive stream-hb grp %d from dnode:%d, snodeId:%d, vgLeaders:%d, streamStatus:%d", 
      req.streamGId, req.dnodeId, req.snodeId, taosArrayGetSize(req.pVgLeaders), taosArrayGetSize(req.pStreamStatus));
  
  (void)msmHandleStreamHbMsg(pMnode, currTs, &req, &rsp);

  msmCleanStreamGrpCtx(&req);

_exit:

  mndStreamHbSendRsp(code, &pReq->info, &rsp);

  return code;
}
