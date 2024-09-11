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

#include "vnd.h"

extern void    tsdbStopAllCompTask(STsdb *tsdb);
extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw);
extern int32_t tsdbCompMonitorGetInfo(STsdb *tsdb, SQueryCompactProgressRsp *rsp);

int32_t vnodeAsyncCompact(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SCompactVnodeReq req = {0};

  int32_t code = tDeserializeSCompactVnodeReq(pReq, len, &req);
  if (code) return code;

  vInfo("vgId:%d, compact msg will be processed, db:%s dbUid:%" PRId64 " compactStartTime:%" PRId64, TD_VID(pVnode),
        req.db, req.dbUid, req.compactStartTime);

  return tsdbAsyncCompact(pVnode->pTsdb, &req.tw);
}

int32_t vnodeProcessKillCompactReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVKillCompactReq req = {0};

  int32_t code = tDeserializeSVKillCompactReq(pReq, len, &req);
  if (code) {
    return TSDB_CODE_INVALID_MSG;
  }
  vInfo("vgId:%d, kill compact msg will be processed, compactId:%d", TD_VID(pVnode), req.compactId);

  tsdbStopAllCompTask(pVnode->pTsdb);

  pRsp->msgType = TDMT_VND_KILL_COMPACT_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}

int32_t vnodeQueryCompactProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  SQueryCompactProgressReq req = {0};

  int32_t                  rspSize = 0;
  SRpcMsg                  rspMsg = {0};
  void                    *pRsp = NULL;
  SQueryCompactProgressRsp rsp = {0};

  // deserialize request
  code = tDeserializeSQueryCompactProgressReq(pMsg->pCont, pMsg->contLen, &req);
  if (code) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  // query compact progress
  rsp.dnodeId = req.dnodeId;
  TAOS_UNUSED(tsdbCompMonitorGetInfo(pVnode->pTsdb, &rsp));
  vInfo("update compact progress, compactId:%d vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d", rsp.compactId,
        rsp.vgId, rsp.dnodeId, rsp.numberFileset, rsp.finished);
  rsp.compactId = req.compactId;

  // serialize response
  rspSize = tSerializeSQueryCompactProgressRsp(NULL, 0, &rsp);
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
  code = tSerializeSQueryCompactProgressRsp(pRsp, rspSize, &rsp);
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
