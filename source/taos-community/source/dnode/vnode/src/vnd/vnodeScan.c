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

extern int32_t tsdbAsyncScan(STsdb *tsdb, const STimeWindow *tw);

static int32_t vnodeAsyncScan(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SScanVnodeReq req = {0};

  int32_t code = tDeserializeSScanVnodeReq(pReq, len, &req);
  if (code) return code;

  vInfo("vgId:%d, scan msg will be processed, db:%s dbUid:%" PRId64 " scanStartTime:%" PRId64, TD_VID(pVnode), req.db,
        req.dbUid, req.scanStartTime);

  return tsdbAsyncScan(pVnode->pTsdb, &req.tw);
}

int32_t vnodeProcessScanVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  if (!pVnode->restored) {
    vInfo("vgId:%d, ignore scan req during restoring. ver:%" PRId64, TD_VID(pVnode), ver);
    return 0;
  }
  return vnodeAsyncScan(pVnode, ver, pReq, len, pRsp);
}

extern void tsdbScanMonitorGetInfo(STsdb *tsdb, SQueryScanProgressRsp *rsp);

int32_t vnodeQueryScanProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  SQueryScanProgressReq req = {0};

  int32_t               rspSize = 0;
  SRpcMsg               rspMsg = {0};
  void                 *pRsp = NULL;
  SQueryScanProgressRsp rsp = {0};

  // deserialize request
  code = tDeserializeSQueryScanProgressReq(pMsg->pCont, pMsg->contLen, &req);
  if (code) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  // query scan progress
  rsp.dnodeId = req.dnodeId;
  tsdbScanMonitorGetInfo(pVnode->pTsdb, &rsp);
  vDebug("update scan progress, scanId:%d vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", rsp.scanId, rsp.vgId,
         rsp.dnodeId, rsp.numberFileset, rsp.finished);
  rsp.scanId = req.scanId;

  // serialize response
  rspSize = tSerializeSQueryScanProgressRsp(NULL, 0, &rsp);
  if (rspSize < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    vError("rpcMallocCont %d failed", rspSize);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  code = tSerializeSQueryScanProgressRsp(pRsp, rspSize, &rsp);
  if (code < 0) {
    goto _exit;
  }
  code = 0;

_exit:
  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = TDMT_VND_QUERY_COMPACT_PROGRESS_RSP;

  tmsgSendRsp(&rspMsg);

  return 0;
}

extern void tsdbCancelScanTask(STsdb *tsdb);

int32_t vnodeProcessKillScanReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVKillScanReq req = {0};

  vDebug("vgId:%d, kill scan msg will be processed, pReq:%p, len:%d", TD_VID(pVnode), pReq, len);
  int32_t code = tDeserializeSVKillScanReq(pReq, len, &req);
  if (code) {
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, kill scan msg will be processed, scanId:%d, dnodeId:%d, vgId:%d", TD_VID(pVnode), req.scanId,
        req.dnodeId, req.vgId);

  tsdbCancelScanTask(pVnode->pTsdb);

  pRsp->msgType = TDMT_VND_KILL_SCAN_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}