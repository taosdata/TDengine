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

#include "tq.h"

// Response header initialization (shared helper)

void tqInitMqRspHead(SMqRspHead* pMsgHead, int32_t type, int32_t epoch, int64_t consumerId, int64_t sver,
                   int64_t ever) {
  if (pMsgHead == NULL) {
    return;
  }
  pMsgHead->consumerId = consumerId;
  pMsgHead->epoch = epoch;
  pMsgHead->mqMsgType = type;
  pMsgHead->walsver = sver;
  pMsgHead->walever = ever;
}

// Data response initialization

int32_t tqInitDataRsp(SMqDataRsp* pRsp, STqOffsetVal pOffset) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  tqDebug("%s called", __FUNCTION__);
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);

  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockData, code, lino, END, terrno);

  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));
  TSDB_CHECK_NULL(pRsp->blockDataLen, code, lino, END, terrno);

  pRsp->blockSchema = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockSchema, code, lino, END, terrno);

  tOffsetCopy(&pRsp->reqOffset, &pOffset);
  tOffsetCopy(&pRsp->rspOffset, &pOffset);
  pRsp->withTbName = 0;
  pRsp->withSchema = 1;

END:
  if (code != 0) {
    tqError("%s failed at:%d, code:%s", __FUNCTION__, lino, tstrerror(code));
    taosArrayDestroy(pRsp->blockData);
    taosArrayDestroy(pRsp->blockDataLen);
    taosArrayDestroy(pRsp->blockSchema);
  }
  return code;
}

// Send data response functions

int32_t tqDoSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, SMqDataRsp* pRsp, int32_t epoch, int64_t consumerId,
                        int32_t type, int64_t sver, int64_t ever) {
  if (pRpcHandleInfo == NULL || pRsp == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t len = 0;
  int32_t code = 0;

  if (type == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    pRsp->withSchema = 0;
  }
  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP || type == TMQ_MSG_TYPE__WALINFO_RSP ||
      type == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    tEncodeSize(tEncodeMqDataRsp, pRsp, len, code);
  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    tEncodeSize(tEncodeSTaosxRsp, pRsp, len, code);
  }

  if (code < 0) {
    return TAOS_GET_TERRNO(code);
  }

  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return terrno;
  }

  SMqRspHead* pHead = (SMqRspHead*)buf;
  tqInitMqRspHead(pHead, type, epoch, consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);

  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP || type == TMQ_MSG_TYPE__WALINFO_RSP ||
      type == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    code = tEncodeMqDataRsp(&encoder, pRsp);
  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    code = tEncodeSTaosxRsp(&encoder, pRsp);
  }
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(code);
  }
  SRpcMsg rsp = {.info = *pRpcHandleInfo, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&rsp);
  return 0;
}

int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, SMqDataRsp* pRsp,
                      int32_t type, int32_t vgId) {
  if (pHandle == NULL || pMsg == NULL || pReq == NULL || pRsp == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  char buf1[TSDB_OFFSET_LEN] = {0};
  char buf2[TSDB_OFFSET_LEN] = {0};
  (void)tFormatOffset(buf1, TSDB_OFFSET_LEN, &(pRsp->reqOffset));
  (void)tFormatOffset(buf2, TSDB_OFFSET_LEN, &(pRsp->rspOffset));

  tqDebug("tmq poll vgId:%d consumer:0x%" PRIx64 " (epoch %d) start to send rsp, block num:%d, req:%s, rsp:%s, QID:0x%" PRIx64,
          vgId, pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2, pReq->reqId);

  return tqDoSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type, sver, ever);
}
