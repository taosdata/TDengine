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

static void    doSendHbMsgRsp(int32_t code, SRpcHandleInfo *pRpcInfo, SEpSet* pEpset, int32_t vgId, int32_t msgId);


int32_t mndProcessStreamHb(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SStreamHbMsg req = {0};
  SMStreamHbRspMsg rsp = {0};
  int32_t      code = 0;
  SDecoder     decoder = {0};
  SEpSet       mnodeEpset = {0};

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

  mDebug("receive stream-meta hb from vgId:%d, active numOfTasks:%d, HbMsgId:%d, HbMsgTs:%" PRId64, req.vgId,
         req.numOfTasks, req.msgId, req.ts);

  msmHandleStreamHbMsg(pMnode, &req, &rsp);

  doSendHbMsgRsp(TSDB_CODE_SUCCESS, &pReq->info, &mnodeEpset, req.vgId, req.msgId);

  return code;
}

bool validateHbMsg(const SArray *pNodeList, int32_t vgId) {
  for (int32_t i = 0; i < taosArrayGetSize(pNodeList); ++i) {
    SNodeEntry *pEntry = taosArrayGet(pNodeList, i);
    if ((pEntry) && (pEntry->nodeId == vgId)) {
      return true;
    }
  }

  return false;
}

void doSendHbMsgRsp(int32_t code, SRpcHandleInfo *pRpcInfo, SEpSet* pMndEpset, int32_t vgId, int32_t msgId) {
  int32_t ret = 0;
  int32_t tlen = 0;
  void   *buf = NULL;

  SMStreamHbRspMsg msg = {.msgId = msgId};//, .mndEpset = *pMndEpset};
  epsetAssign(&msg.mndEpset, pMndEpset);

  tEncodeSize(tEncodeStreamHbRsp, &msg, tlen, ret);
  if (ret < 0) {
    mError("encode stream hb msg rsp failed, code:%s", tstrerror(code));
  }

  buf = rpcMallocCont(tlen + sizeof(SMsgHead));
  if (buf == NULL) {
    mError("encode stream hb msg rsp failed, code:%s", tstrerror(terrno));
    return;
  }

  ((SMStreamHbRspMsg *)buf)->head.vgId = htonl(vgId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamHbRsp(&encoder, &msg)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    mError("encode stream hb msg rsp failed, code:%s", tstrerror(code));
    return;
  }
  tEncoderClear(&encoder);

  SRpcMsg rsp = {.code = code, .info = *pRpcInfo, .contLen = tlen + sizeof(SMsgHead), .pCont = buf};

  tmsgSendRsp(&rsp);
  pRpcInfo->handle = NULL;  // disable auto rsp
}