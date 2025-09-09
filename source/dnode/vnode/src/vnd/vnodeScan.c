/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

int32_t vnodeProcessScanVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  //   if (!pVnode->restored) {
  //     vInfo("vgId:%d, ignore scan req during restoring. ver:%" PRId64, TD_VID(pVnode), ver);
  //     return 0;
  //   }
  //   return vnodeAsyncScan(pVnode, ver, pReq, len, pRsp);
  return 0;
}

int32_t vnodeQueryScanProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  //   SQueryCompactProgressReq req = {0};

  //   int32_t                  rspSize = 0;
  //   SRpcMsg                  rspMsg = {0};
  //   void                    *pRsp = NULL;
  //   SQueryCompactProgressRsp rsp = {0};

  //   // deserialize request
  //   code = tDeserializeSQueryCompactProgressReq(pMsg->pCont, pMsg->contLen, &req);
  //   if (code) {
  //     code = TSDB_CODE_INVALID_MSG;
  //     goto _exit;
  //   }

  //   // query compact progress
  //   rsp.dnodeId = req.dnodeId;
  //   TAOS_UNUSED(tsdbCompMonitorGetInfo(pVnode->pTsdb, &rsp));
  //   vInfo("update compact progress, compactId:%d vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", rsp.compactId,
  //         rsp.vgId, rsp.dnodeId, rsp.numberFileset, rsp.finished);
  //   rsp.compactId = req.compactId;

  //   // serialize response
  //   rspSize = tSerializeSQueryCompactProgressRsp(NULL, 0, &rsp);
  //   if (rspSize < 0) {
  //     code = TSDB_CODE_INVALID_MSG;
  //     goto _exit;
  //   }

  //   pRsp = rpcMallocCont(rspSize);
  //   if (pRsp == NULL) {
  //     vError("rpcMallocCont %d failed", rspSize);
  //     code = TSDB_CODE_OUT_OF_MEMORY;
  //     goto _exit;
  //   }
  //   code = tSerializeSQueryCompactProgressRsp(pRsp, rspSize, &rsp);
  //   if (code < 0) {
  //     goto _exit;
  //   }
  //   code = 0;

  // _exit:
  //   rspMsg.info = pMsg->info;
  //   rspMsg.pCont = pRsp;
  //   rspMsg.contLen = rspSize;
  //   rspMsg.code = code;
  //   rspMsg.msgType = TDMT_VND_QUERY_COMPACT_PROGRESS_RSP;

  //   tmsgSendRsp(&rspMsg);

  return 0;
}

int32_t vnodeProcessKillScanReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  // TODO
  return 0;
}